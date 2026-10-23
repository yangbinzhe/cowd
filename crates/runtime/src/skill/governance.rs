//! Human-governed Skill revision pointer.
//!
//! Maintenance Drafts are inert evidence. A pointer can move only through a
//! pending approval and a one-time verified human decision lease.

use std::sync::{Arc, RwLock};

use harness_contract::{
    core::TaskRisk,
    skill::{
        SkillActivePointer, SkillMaintenanceDraft, SkillMaintenanceRecommendation,
        SkillRevisionReview, SkillRevisionReviewAction, SkillRevisionReviewDecision,
        SkillRevisionReviewStatus,
    },
};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    GlobalApprovalRequest, GlobalApprovalStatus, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, VerifiedDecisionLease, VerifiedPrincipal,
    runtime_event_store::{AppendTransactionRequest, ExpectedStreamRevision},
};

const REVIEW_STREAM_PREFIX: &str = "skill-revision-review:";
const POINTER_STREAM_PREFIX: &str = "skill-revision-pointer:";
const REVIEW_REQUESTED_KIND: &str = "skill.revision.review.requested.v1";
const REVIEW_DECIDED_KIND: &str = "skill.revision.review.decided.v1";
const POINTER_CHANGED_KIND: &str = "skill.revision.pointer.changed.v1";

/// Original caller intent is stored with its registration. It does not carry
/// authority; the same request ID cannot be rebound after the pointer advances.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SkillRevisionInput {
    FirstInstall {
        skill_id: String,
        target_revision: String,
        receipt_id: String,
    },
    Activate {
        skill_id: String,
        draft_id: String,
        draft_digest: String,
        target_revision: String,
        receipt_id: String,
    },
    Rollback {
        skill_id: String,
        target_revision: String,
        reason_evidence_digest: String,
        receipt_id: String,
    },
}

impl SkillRevisionInput {
    fn matches_review(&self, review: &SkillRevisionReview) -> bool {
        match self {
            Self::FirstInstall {
                skill_id,
                target_revision,
                receipt_id,
            } => {
                review.action == SkillRevisionReviewAction::FirstInstall
                    && review.skill_id == *skill_id
                    && review.target_revision == *target_revision
                    && review.draft_id.is_none()
                    && review
                        .installation
                        .as_ref()
                        .is_some_and(|installed| installed.receipt_id == *receipt_id)
            }
            Self::Activate {
                skill_id,
                draft_id,
                target_revision,
                receipt_id,
                ..
            } => {
                review.action == SkillRevisionReviewAction::Activate
                    && review.skill_id == *skill_id
                    && review.draft_id.as_deref() == Some(draft_id.as_str())
                    && review.target_revision == *target_revision
                    && review
                        .installation
                        .as_ref()
                        .is_some_and(|installed| installed.receipt_id == *receipt_id)
            }
            Self::Rollback {
                skill_id,
                target_revision,
                receipt_id,
                ..
            } => {
                review.action == SkillRevisionReviewAction::Rollback
                    && review.skill_id == *skill_id
                    && review.draft_id.is_none()
                    && review.target_revision == *target_revision
                    && review
                        .installation
                        .as_ref()
                        .is_some_and(|installed| installed.receipt_id == *receipt_id)
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SkillRevisionGovernanceError {
    #[error("interactive human capability skill.revision.manage is required")]
    HumanCapabilityRequired,
    #[error("Skill maintenance Draft was not found")]
    DraftNotFound,
    #[error("Skill maintenance Draft is not eligible: {0}")]
    DraftNotEligible(String),
    #[error("Skill revision review was not found")]
    ReviewNotFound,
    #[error("Skill revision review is no longer pending")]
    ReviewNotPending,
    #[error("Skill revision generation changed")]
    GenerationChanged,
    #[error("Skill revision request is invalid: {0}")]
    InvalidRequest(String),
    #[error("Skill revision store failed: {0}")]
    Store(String),
}

pub struct SkillRevisionGovernanceService {
    event_store: Arc<RuntimeEventStore>,
    approvals: Arc<ApprovalQueue>,
    catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    #[cfg(test)]
    decision_read_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl SkillRevisionGovernanceService {
    #[must_use]
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        approvals: Arc<ApprovalQueue>,
        catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    ) -> Self {
        Self {
            event_store,
            approvals,
            catalog,
            #[cfg(test)]
            decision_read_hook: None,
        }
    }

    fn installed(
        &self,
        skill_id: &str,
        revision: &str,
        receipt_id: &str,
    ) -> Result<harness_contract::skill::SkillInstallationEvidence, SkillRevisionGovernanceError>
    {
        let source = self
            .catalog
            .read()
            .map_err(|_| SkillRevisionGovernanceError::Store("Skill catalog lock failed".into()))?
            .instruction_source()
            .ok_or_else(|| {
                SkillRevisionGovernanceError::InvalidRequest(
                    "installed Skill reader is unavailable".into(),
                )
            })?;
        // No catalog lock crosses package I/O or the original transaction.
        let installed = source
            .inspect_revision(skill_id, revision, receipt_id)
            .map_err(SkillRevisionGovernanceError::InvalidRequest)?;
        if installed.skill_id != skill_id
            || installed.revision != revision
            || installed.scope_ref.trim().is_empty()
            || installed.receipt_id.trim().is_empty()
            || installed.receipt_digest.trim().is_empty()
            || installed.inspection_fingerprint.trim().is_empty()
            || receipt_id != installed.receipt_id
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "installed Skill reader identity mismatch".into(),
            ));
        }
        Ok(installed)
    }

    pub fn request_first_install(
        &self,
        principal: &VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        receipt_id: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        require_human(principal)?;
        if [request_id, skill_id, target_revision, receipt_id]
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "first-install input is required".into(),
            ));
        }
        let input = SkillRevisionInput::FirstInstall {
            skill_id: skill_id.into(),
            target_revision: target_revision.into(),
            receipt_id: receipt_id.into(),
        };
        if let Some(existing) = self.existing_request(request_id, &input)? {
            return Ok(existing);
        }
        let pointer_cut = self.pointer_snapshot(skill_id)?;
        if pointer_cut.0.is_some() || pointer_cut.1 != 0 {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "first install requires an absent approved pointer".into(),
            ));
        }
        let installation = self.installed(skill_id, target_revision, receipt_id)?;
        self.request_review(
            request_id,
            SkillRevisionReviewAction::FirstInstall,
            None,
            skill_id,
            target_revision,
            None,
            &digest_json(&json!({"first_install": installation})),
            pointer_cut,
            input,
            installation,
        )
    }

    pub fn request_activation(
        &self,
        principal: &VerifiedPrincipal,
        request_id: &str,
        draft: &SkillMaintenanceDraft,
        target_revision: &str,
        receipt_id: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        require_human(principal)?;
        if request_id.trim().is_empty()
            || target_revision.trim().is_empty()
            || receipt_id.trim().is_empty()
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "request_id and target_revision are required".to_string(),
            ));
        }
        if draft.recommendation == SkillMaintenanceRecommendation::Keep
            || !draft.validation.receipt_schema_valid
            || !draft.validation.evidence_closed
            || draft.validation.outcome_association_count == 0
            || draft.base_revision == target_revision
        {
            return Err(SkillRevisionGovernanceError::DraftNotEligible(
                "Draft must require change, close its evidence, join an Outcome and target a new inspected immutable package fingerprint"
                    .to_string(),
            ));
        }
        let input = SkillRevisionInput::Activate {
            skill_id: draft.skill_id.clone(),
            draft_id: draft.draft_id.clone(),
            draft_digest: draft.digest(),
            target_revision: target_revision.into(),
            receipt_id: receipt_id.into(),
        };
        if let Some(existing) = self.existing_request(request_id, &input)? {
            return Ok(existing);
        }
        let installation = self.installed(&draft.skill_id, target_revision, receipt_id)?;
        let pointer_cut = self.pointer_snapshot(&draft.skill_id)?;
        let current = pointer_cut.0.as_ref().ok_or_else(|| {
            SkillRevisionGovernanceError::InvalidRequest(
                "maintenance requires an existing approved pointer; use first install".into(),
            )
        })?;
        if current.active_revision != draft.base_revision
            || current
                .installation
                .as_ref()
                .is_none_or(|installed| installed.scope_ref != installation.scope_ref)
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "Skill installation scope changed or is unknown".into(),
            ));
        }
        self.request_review(
            request_id,
            SkillRevisionReviewAction::Activate,
            Some(draft.draft_id.clone()),
            &draft.skill_id,
            target_revision,
            Some(draft.base_revision.clone()),
            &digest_json(&json!({
                "draft_digest": draft.digest(),
                "installation": installation,
                "target_revision": target_revision,
            })),
            pointer_cut,
            input,
            installation,
        )
    }

    pub fn request_rollback(
        &self,
        principal: &VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        reason_evidence_digest: &str,
        receipt_id: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        require_human(principal)?;
        if request_id.trim().is_empty()
            || target_revision.trim().is_empty()
            || reason_evidence_digest.trim().is_empty()
            || receipt_id.trim().is_empty()
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "rollback input is required".into(),
            ));
        }
        let input = SkillRevisionInput::Rollback {
            skill_id: skill_id.into(),
            target_revision: target_revision.into(),
            reason_evidence_digest: reason_evidence_digest.into(),
            receipt_id: receipt_id.into(),
        };
        if let Some(existing) = self.existing_request(request_id, &input)? {
            return Ok(existing);
        }
        let pointer_cut = self.pointer_snapshot(skill_id)?;
        let pointer = pointer_cut.0.as_ref().ok_or_else(|| {
            SkillRevisionGovernanceError::InvalidRequest("no active pointer".into())
        })?;
        let installation = self.installed(skill_id, target_revision, receipt_id)?;
        if pointer
            .installation
            .as_ref()
            .is_none_or(|installed| installed.scope_ref != installation.scope_ref)
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "rollback installation scope changed or is unknown".into(),
            ));
        }
        if request_id.trim().is_empty()
            || target_revision.trim().is_empty()
            || reason_evidence_digest.trim().is_empty()
            || pointer.previous_revision.as_deref() != Some(target_revision)
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "rollback target must equal the previous approved revision and include evidence"
                    .to_string(),
            ));
        }
        self.request_review(
            request_id,
            SkillRevisionReviewAction::Rollback,
            None,
            skill_id,
            target_revision,
            Some(pointer.active_revision.clone()),
            &digest_json(&json!({
                "active_pointer": pointer,
                "reason_evidence_digest": reason_evidence_digest,
                "installation": installation,
            })),
            pointer_cut,
            input,
            installation,
        )
    }

    fn request_review(
        &self,
        request_id: &str,
        action: SkillRevisionReviewAction,
        draft_id: Option<String>,
        skill_id: &str,
        target_revision: &str,
        previous_revision: Option<String>,
        evidence_digest: &str,
        pointer_cut: (Option<SkillActivePointer>, u64),
        input: SkillRevisionInput,
        installation: harness_contract::skill::SkillInstallationEvidence,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        let review_id = format!("skill-revision-review:{}", request_id.trim());
        let review = SkillRevisionReview {
            review_id: review_id.clone(),
            approval_id: format!("approval:{review_id}"),
            action,
            draft_id,
            skill_id: skill_id.to_string(),
            target_revision: target_revision.to_string(),
            previous_revision,
            evidence_digest: evidence_digest.to_string(),
            expected_generation: pointer_cut
                .0
                .as_ref()
                .map_or(0, |pointer| pointer.generation),
            status: SkillRevisionReviewStatus::Pending,
            created_at_ms: now_ms(),
            installation: Some(installation),
        };
        if let Some(existing) = self.existing_request(request_id, &input)? {
            return Ok(existing);
        }
        let approval = GlobalApprovalRequest {
            approval_id: review.approval_id.clone(),
            context: harness_contract::policy::ApprovalContext::owned(
                &ApprovalSource {
                    kind: ApprovalSourceKind::Evolution,
                    session_id: None,
                    agent_id: None,
                    team_id: None,
                    mission_id: Some(review.skill_id.clone()),
                    resource_ref: Some(review.scope_ref()),
                    review_ref: Some(review.review_id.clone()),
                    application: None,
                },
                review.action.action_key(),
                "skill",
            ),
            source: ApprovalSource {
                kind: ApprovalSourceKind::Evolution,
                session_id: None,
                agent_id: None,
                team_id: None,
                mission_id: Some(review.skill_id.clone()),
                resource_ref: Some(review.scope_ref()),
                review_ref: Some(review.review_id.clone()),
                application: None,
            },
            action: review.action.action_key().to_string(),
            summary: format!(
                "{:?} Skill {} revision {}",
                review.action, review.skill_id, review.target_revision
            ),
            risk: TaskRisk::High,
            domain: harness_contract::policy::ApprovalDomain::Skill,
            blocks_execution: false,
            skippable: false,
            allowed_scopes: vec![harness_contract::policy::ApprovalGrantScope::Once],
            evidence_refs: vec![review.evidence_digest.clone()],
            timeout_policy: ApprovalTimeoutPolicy::Pending,
            status: GlobalApprovalStatus::Pending,
            decision: None,
            created_at_ms: review.created_at_ms,
            expires_at_ms: None,
            resolved_at_ms: None,
        };
        let approval_stream = format!("approval:{}", review.approval_id);
        let review_stream = review_stream(&review.review_id);
        let appended = self
            .event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("skill-revision-request:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: approval_stream.clone(),
                        expected_revision: 0,
                    },
                    ExpectedStreamRevision {
                        stream_id: review_stream.clone(),
                        expected_revision: 0,
                    },
                    ExpectedStreamRevision {
                        stream_id: pointer_stream(skill_id),
                        expected_revision: pointer_cut.1,
                    },
                ],
                events: vec![
                    RuntimeEventInput {
                        stream_id: approval_stream,
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.submitted".to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.skill_revision_governance".to_string()),
                        refs: vec![skill_ref(&review.skill_id)],
                        payload: json!({"request": approval}),
                    }
                    .into(),
                    RuntimeEventInput {
                        stream_id: review_stream,
                        scope: RuntimeEventScope::Skill,
                        kind: REVIEW_REQUESTED_KIND.to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.skill_revision_governance".to_string()),
                        refs: vec![skill_ref(&review.skill_id)],
                        payload: json!({"review": review, "input": input}),
                    }
                    .into(),
                ],
            });
        if let Err(error) = appended {
            // Another identical request (or a lost response) may already have
            // committed. Reuse only its exact original intent, never a new head.
            return match self.existing_request(request_id, &input) {
                Ok(Some(existing)) => Ok(existing),
                Ok(None) => Err(SkillRevisionGovernanceError::Store(error.to_string())),
                Err(error) => Err(error),
            };
        }
        self.approvals.refresh();
        Ok(review)
    }

    pub fn decide_review(
        &self,
        principal: &VerifiedPrincipal,
        lease: &VerifiedDecisionLease,
        review_id: &str,
        decision: SkillRevisionReviewDecision,
        reason: &str,
    ) -> Result<Option<SkillActivePointer>, SkillRevisionGovernanceError> {
        require_human(principal)?;
        let (review, review_revision, input) = self.review_snapshot(review_id)?;
        if decision == SkillRevisionReviewDecision::Approve && input.is_none() {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "original Skill caller input is unavailable".into(),
            ));
        }
        if review.status != SkillRevisionReviewStatus::Pending {
            return Err(SkillRevisionGovernanceError::ReviewNotPending);
        }
        if lease.review_id() != review.review_id
            || lease.action() != review.action.action_key()
            || lease.scope() != review.scope_ref()
            || lease.evidence_digest() != review.evidence_digest
        {
            return Err(SkillRevisionGovernanceError::HumanCapabilityRequired);
        }
        let (current, pointer_revision) = self.pointer_snapshot(&review.skill_id)?;
        let current_generation = current
            .as_ref()
            .map(|pointer| pointer.generation)
            .unwrap_or_default();
        if current_generation != review.expected_generation {
            return Err(SkillRevisionGovernanceError::GenerationChanged);
        }
        #[cfg(test)]
        if let Some(hook) = &self.decision_read_hook {
            hook();
        }
        // The original pending submission, not a cached queue projection or a
        // freshly rebound stream head, is the decision's approval read cut.
        let approval_stream = format!("approval:{}", review.approval_id);
        let approval_event = self
            .event_store
            .list_stream_page_desc(&approval_stream, 1, 0)
            .map_err(SkillRevisionGovernanceError::Store)?
            .into_iter()
            .next()
            .ok_or(SkillRevisionGovernanceError::ReviewNotFound)?;
        if approval_event.kind != "approval.submitted"
            || approval_event.scope != RuntimeEventScope::Approval
            || approval_event.sequence != 1
            || approval_event.status.as_deref() != Some("pending")
            || approval_event.stream_id != approval_stream
            || approval_event.actor.as_deref() != Some("runtime.skill_revision_governance")
        {
            return Err(SkillRevisionGovernanceError::ReviewNotPending);
        }
        let approval: GlobalApprovalRequest =
            serde_json::from_value(approval_event.payload.get("request").cloned().ok_or_else(
                || SkillRevisionGovernanceError::Store("Skill approval source is missing".into()),
            )?)
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
        if approval.status != GlobalApprovalStatus::Pending
            || approval.approval_id != review.approval_id
            || approval.action != review.action.action_key()
            || approval.source.review_ref.as_deref() != Some(review.review_id.as_str())
            || approval.source.resource_ref.as_deref() != Some(review.scope_ref().as_str())
            || approval.evidence_refs != vec![review.evidence_digest.clone()]
        {
            return Err(SkillRevisionGovernanceError::ReviewNotPending);
        }
        let approved = decision == SkillRevisionReviewDecision::Approve;
        if approved {
            let installation = review.installation.as_ref().ok_or_else(|| {
                SkillRevisionGovernanceError::InvalidRequest(
                    "original installed Skill evidence is unavailable".into(),
                )
            })?;
            let actual = self.installed(
                &review.skill_id,
                &review.target_revision,
                &installation.receipt_id,
            )?;
            if &actual != installation {
                return Err(SkillRevisionGovernanceError::InvalidRequest(
                    "installed Skill bytes, inspection or scope changed after review".into(),
                ));
            }
            if review.action == SkillRevisionReviewAction::FirstInstall && current.is_some() {
                return Err(SkillRevisionGovernanceError::GenerationChanged);
            }
        }
        let pointer = approved.then(|| SkillActivePointer {
            skill_id: review.skill_id.clone(),
            active_revision: review.target_revision.clone(),
            previous_revision: review.previous_revision.clone(),
            generation: current_generation.saturating_add(1),
            source_draft_id: review.draft_id.clone(),
            approval_ref: review.approval_id.clone(),
            activated_at_ms: now_ms(),
            installation: review.installation.clone(),
        });
        let approval_stream = format!("approval:{}", review.approval_id);
        let review_stream = review_stream(&review.review_id);
        let pointer_stream = pointer_stream(&review.skill_id);
        let decided_by = principal.claims().principal_id.clone();
        let mut expected_streams = vec![
            ExpectedStreamRevision {
                stream_id: approval_stream.clone(),
                expected_revision: approval_event.sequence,
            },
            ExpectedStreamRevision {
                stream_id: review_stream.clone(),
                expected_revision: review_revision,
            },
        ];
        let mut events = vec![
            RuntimeEventInput {
                stream_id: approval_stream,
                scope: RuntimeEventScope::Approval,
                kind: "approval.decided".to_string(),
                status: Some(if approved { "approved" } else { "denied" }.to_string()),
                actor: Some(decided_by.clone()),
                refs: vec![skill_ref(&review.skill_id)],
                payload: json!({
                    "approved": approved,
                    "reason": reason,
                    "message": format!("decided by {decided_by}"),
                    "resolved_at_ms": now_ms(),
                }),
            }
            .into(),
            RuntimeEventInput {
                stream_id: review_stream,
                scope: RuntimeEventScope::Skill,
                kind: REVIEW_DECIDED_KIND.to_string(),
                status: Some(if approved { "approved" } else { "denied" }.to_string()),
                actor: Some(decided_by),
                refs: vec![skill_ref(&review.skill_id)],
                payload: json!({"decision": decision, "reason": reason}),
            }
            .into(),
        ];
        if let Some(pointer) = pointer.as_ref() {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: pointer_stream.clone(),
                expected_revision: pointer_revision,
            });
            events.push(
                RuntimeEventInput {
                    stream_id: pointer_stream,
                    scope: RuntimeEventScope::Skill,
                    kind: POINTER_CHANGED_KIND.to_string(),
                    status: Some("active".to_string()),
                    actor: Some("runtime.skill_revision_governance".to_string()),
                    refs: vec![skill_ref(&review.skill_id)],
                    payload: json!({"pointer": pointer}),
                }
                .into(),
            );
        }
        self.event_store
            .append_transaction_with_verified_decision_lease(
                AppendTransactionRequest {
                    transaction_id: format!(
                        "skill-revision-decision:{}:{decision:?}",
                        review.review_id
                    ),
                    expected_streams,
                    events,
                },
                lease,
            )
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(pointer)
    }

    pub fn review(
        &self,
        review_id: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        let (mut review, _, _) = self.review_snapshot(review_id)?;
        if review.status == SkillRevisionReviewStatus::Pending
            && self
                .pointer(&review.skill_id)?
                .map_or(0, |pointer| pointer.generation)
                != review.expected_generation
        {
            review.status = SkillRevisionReviewStatus::Superseded;
        }
        Ok(review)
    }

    fn review_snapshot(
        &self,
        review_id: &str,
    ) -> Result<(SkillRevisionReview, u64, Option<SkillRevisionInput>), SkillRevisionGovernanceError>
    {
        let stream = review_stream(review_id);
        // This protocol writes one request and at most one terminal decision.
        // Fetching a third detects an invalid source instead of ignoring it.
        let mut events = self
            .event_store
            .list_stream_page_desc(&stream, 3, 0)
            .map_err(SkillRevisionGovernanceError::Store)?;
        if events.is_empty() {
            return Err(SkillRevisionGovernanceError::ReviewNotFound);
        }
        if events.len() > 2 {
            return Err(SkillRevisionGovernanceError::Store(
                "invalid Skill review journal".into(),
            ));
        }
        let revision = events[0].sequence;
        events.reverse();
        let requested = &events[0];
        if requested.kind != REVIEW_REQUESTED_KIND
            || requested.stream_id != stream
            || requested.scope != RuntimeEventScope::Skill
            || requested.sequence != 1
            || requested.status.as_deref() != Some("pending")
            || requested.actor.as_deref() != Some("runtime.skill_revision_governance")
        {
            return Err(SkillRevisionGovernanceError::Store(
                "invalid Skill review source".into(),
            ));
        }
        let mut review: SkillRevisionReview =
            serde_json::from_value(requested.payload.get("review").cloned().ok_or_else(|| {
                SkillRevisionGovernanceError::Store("Skill review payload is missing".into())
            })?)
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
        if review.review_id != stream
            || review.approval_id != format!("approval:{stream}")
            || review.status != SkillRevisionReviewStatus::Pending
        {
            return Err(SkillRevisionGovernanceError::Store(
                "Skill review identity mismatch".into(),
            ));
        }
        if let Some(event) = events.get(1) {
            if event.kind != REVIEW_DECIDED_KIND
                || event.stream_id != stream
                || event.scope != RuntimeEventScope::Skill
                || event.sequence != 2
            {
                return Err(SkillRevisionGovernanceError::Store(
                    "unknown Skill review tail".into(),
                ));
            }
            let decision: SkillRevisionReviewDecision = serde_json::from_value(
                event.payload.get("decision").cloned().ok_or_else(|| {
                    SkillRevisionGovernanceError::Store("Skill decision payload is missing".into())
                })?,
            )
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
            let expected_status = match decision {
                SkillRevisionReviewDecision::Approve => "approved",
                SkillRevisionReviewDecision::Deny => "denied",
            };
            if event.status.as_deref() != Some(expected_status)
                || event.actor.as_deref().is_none_or(|actor| actor.is_empty())
            {
                return Err(SkillRevisionGovernanceError::Store(
                    "Skill decision identity/status mismatch".into(),
                ));
            }
            review.status = match decision {
                SkillRevisionReviewDecision::Approve => SkillRevisionReviewStatus::Approved,
                SkillRevisionReviewDecision::Deny => SkillRevisionReviewStatus::Denied,
            };
        }
        let input: Option<SkillRevisionInput> = requested
            .payload
            .get("input")
            // Before installed evidence was bound, caller input could not
            // authorize these packages. Keep history visible without guessing
            // the missing original receipt or a new approval grant.
            .filter(|_| review.installation.is_some())
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
        if input
            .as_ref()
            .is_some_and(|input| !input.matches_review(&review))
        {
            return Err(SkillRevisionGovernanceError::Store(
                "Skill input/review identity mismatch".into(),
            ));
        }
        Ok((review, revision, input))
    }

    fn existing_request(
        &self,
        request_id: &str,
        input: &SkillRevisionInput,
    ) -> Result<Option<SkillRevisionReview>, SkillRevisionGovernanceError> {
        let (mut review, _, original) =
            match self.review_snapshot(&format!("{REVIEW_STREAM_PREFIX}{}", request_id.trim())) {
                Ok(snapshot) => snapshot,
                Err(SkillRevisionGovernanceError::ReviewNotFound) => return Ok(None),
                Err(error) => return Err(error),
            };
        if original.as_ref() != Some(input) {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "Skill request ID has different or unavailable original input".into(),
            ));
        }
        if review.status == SkillRevisionReviewStatus::Pending
            && self
                .pointer(&review.skill_id)?
                .map_or(0, |pointer| pointer.generation)
                != review.expected_generation
        {
            review.status = SkillRevisionReviewStatus::Superseded;
        }
        Ok(Some(review))
    }

    fn pointer_snapshot(
        &self,
        skill_id: &str,
    ) -> Result<(Option<SkillActivePointer>, u64), SkillRevisionGovernanceError> {
        pointer_snapshot_from_store(&self.event_store, skill_id)
            .map_err(SkillRevisionGovernanceError::Store)
    }

    pub fn pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<SkillActivePointer>, SkillRevisionGovernanceError> {
        active_pointer_from_store(&self.event_store, skill_id)
            .map_err(SkillRevisionGovernanceError::Store)
    }
}

pub(crate) fn active_pointer_from_store(
    event_store: &RuntimeEventStore,
    skill_id: &str,
) -> Result<Option<SkillActivePointer>, String> {
    pointer_snapshot_from_store(event_store, skill_id).map(|(pointer, _)| pointer)
}

fn pointer_snapshot_from_store(
    event_store: &RuntimeEventStore,
    skill_id: &str,
) -> Result<(Option<SkillActivePointer>, u64), String> {
    let stream = pointer_stream(skill_id);
    let Some(event) = event_store
        .list_stream_page_desc(&stream, 1, 0)?
        .into_iter()
        .next()
    else {
        return Ok((None, 0));
    };
    if event.kind != POINTER_CHANGED_KIND
        || event.stream_id != stream
        || event.scope != RuntimeEventScope::Skill
        || event.status.as_deref() != Some("active")
        || event.actor.as_deref() != Some("runtime.skill_revision_governance")
    {
        return Err("unknown Skill pointer source".into());
    }
    let pointer: SkillActivePointer = serde_json::from_value(
        event
            .payload
            .get("pointer")
            .cloned()
            .ok_or("Skill pointer payload is missing")?,
    )
    .map_err(|error| error.to_string())?;
    if pointer.skill_id != skill_id || pointer.generation != event.sequence {
        return Err("Skill pointer source identity/generation mismatch".into());
    }
    Ok((Some(pointer), event.sequence))
}

fn require_human(principal: &VerifiedPrincipal) -> Result<(), SkillRevisionGovernanceError> {
    if principal.is_human_interactive() && principal.has_capability("skill.revision.manage") {
        Ok(())
    } else {
        Err(SkillRevisionGovernanceError::HumanCapabilityRequired)
    }
}

fn review_stream(review_id: &str) -> String {
    if review_id.starts_with(REVIEW_STREAM_PREFIX) {
        review_id.to_string()
    } else {
        format!("{REVIEW_STREAM_PREFIX}{review_id}")
    }
}

fn pointer_stream(skill_id: &str) -> String {
    format!(
        "{POINTER_STREAM_PREFIX}{:x}",
        Sha256::digest(skill_id.as_bytes())
    )
}

fn skill_ref(skill_id: &str) -> RuntimeEventRef {
    RuntimeEventRef {
        kind: "skill".to_string(),
        id: skill_id.to_string(),
    }
}

fn digest_json(value: &impl serde::Serialize) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::{
        SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION, SkillMaintenanceValidation, SkillUsageCounts,
    };

    struct InstalledSource {
        inspection: &'static str,
        scope: &'static str,
    }

    #[async_trait::async_trait]
    impl crate::RuntimeSkillInstructionSource for InstalledSource {
        fn inspect_revision(
            &self,
            skill_id: &str,
            revision: &str,
            receipt_id: &str,
        ) -> Result<harness_contract::skill::SkillInstallationEvidence, String> {
            if skill_id != "review" || !["1.0.0", "1.1.0", "1.2.0"].contains(&revision) {
                return Err("installation is unavailable".into());
            }
            Ok(harness_contract::skill::SkillInstallationEvidence {
                scope_ref: self.scope.into(),
                skill_id: skill_id.into(),
                revision: revision.into(),
                receipt_id: receipt_id.into(),
                receipt_digest: "original-receipt".into(),
                inspection_fingerprint: self.inspection.into(),
            })
        }

        async fn load_instruction(
            &self,
            _: &crate::SkillInvocation,
            _: &crate::RuntimeSkillUsageContext,
        ) -> Result<Option<crate::RuntimeSkillPromptAsset>, String> {
            Err("not a page-in fixture".into())
        }
    }

    fn service() -> SkillRevisionGovernanceService {
        let store = Arc::new(RuntimeEventStore::for_test());
        let approvals = Arc::new(ApprovalQueue::new(Arc::clone(&store)));
        SkillRevisionGovernanceService::new(
            store,
            approvals,
            Arc::new(RwLock::new(
                crate::RuntimeSkillCatalog::default().with_instruction_source(Arc::new(
                    InstalledSource {
                        inspection: "original-inspection",
                        scope: "workspace-test",
                    },
                )),
            )),
        )
    }

    fn approve_first(service: &SkillRevisionGovernanceService) {
        let principal = crate::security::test_human_interactive_principal();
        let review = service
            .request_first_install(&principal, "bootstrap", "review", "1.0.0", "receipt-1.0")
            .unwrap();
        service
            .decide_review(
                &principal,
                &lease(&review),
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "first human approval",
            )
            .unwrap();
    }

    fn draft() -> SkillMaintenanceDraft {
        SkillMaintenanceDraft {
            draft_id: "draft-1".to_string(),
            skill_id: "review".to_string(),
            base_revision: "1.0.0".to_string(),
            proposed_revision: "1.1.0".to_string(),
            workspace_identity: "workspace".to_string(),
            workload_fingerprint: "workload".to_string(),
            config_revision: "config".to_string(),
            evaluation_environment: "production".to_string(),
            canonical_counts: SkillUsageCounts {
                failures: 3,
                ..SkillUsageCounts::default()
            },
            legacy_counts: SkillUsageCounts::default(),
            evidence_receipt_ids: vec!["receipt-1".to_string()],
            outcome_refs: vec!["outcome:execution-1".to_string()],
            evidence_digest: "sha256:evidence".to_string(),
            target: "validated revision".to_string(),
            recommendation: SkillMaintenanceRecommendation::Revise,
            validation: SkillMaintenanceValidation {
                receipt_schema_valid: true,
                evidence_closed: true,
                outcome_association_count: 1,
                verified_success_count: 0,
                terminal_failure_count: 1,
                missing_outcome_count: 0,
                notes: Vec::new(),
            },
            created_at_ms: 1,
            schema_version: SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION,
        }
    }

    fn lease(review: &SkillRevisionReview) -> VerifiedDecisionLease {
        crate::security::test_verified_decision_lease(
            &review.review_id,
            review.action.action_key(),
            review.scope_ref(),
            &review.evidence_digest,
        )
    }

    #[test]
    fn original_1064_review_inputs_remain_status_readable_without_guessing_installation_authority()
    {
        let service = service();
        let principal = crate::security::test_human_interactive_principal();
        for action in ["activate", "rollback"] {
            let request = format!("legacy-{action}");
            let stream = format!("skill-revision-review:{request}");
            let draft_id = (action == "activate").then_some("draft-1");
            let input = if action == "activate" {
                json!({"action":"activate", "skill_id":"review", "draft_id":"draft-1", "draft_digest":"old-draft", "target_revision":"1.1.0", "validation_digest":"old-inspection"})
            } else {
                json!({"action":"rollback", "skill_id":"review", "target_revision":"1.1.0", "reason_evidence_digest":"old-reason"})
            };
            service.event_store.append(RuntimeEventInput {
                stream_id: stream.clone(), scope: RuntimeEventScope::Skill,
                kind: "skill.revision.review.requested.v1".into(), status: Some("pending".into()), actor: Some("runtime.skill_revision_governance".into()), refs: vec![],
                payload: json!({"review": {"review_id":stream, "approval_id":format!("approval:{stream}"), "action":action, "draft_id":draft_id, "skill_id":"review", "target_revision":"1.1.0", "previous_revision":"1.0.0", "evidence_digest":"original-evidence", "expected_generation":0, "status":"pending", "created_at_ms":1}, "input":input}),
            }).unwrap();
            let review = service
                .review(&stream)
                .expect("original literal journal remains readable");
            assert_eq!(review.status, SkillRevisionReviewStatus::Pending);
            assert!(review.installation.is_none());
            assert_eq!(review.scope_ref(), "skill:review");
            assert!(matches!(
                service.decide_review(
                    &principal,
                    &lease(&review),
                    &stream,
                    SkillRevisionReviewDecision::Approve,
                    "cannot invent receipt"
                ),
                Err(SkillRevisionGovernanceError::InvalidRequest(_))
            ));
            let replay = if action == "activate" {
                service.request_activation(&principal, &request, &draft(), "1.1.0", "new-receipt")
            } else {
                service.request_rollback(
                    &principal,
                    &request,
                    "review",
                    "1.1.0",
                    "old-reason",
                    "new-receipt",
                )
            };
            assert!(matches!(
                replay,
                Err(SkillRevisionGovernanceError::InvalidRequest(_))
            ));
        }
        assert!(service.pointer("review").unwrap().is_none());
    }

    #[test]
    fn first_install_rechecks_installed_evidence_and_replays_original_commit_after_reopen() {
        let service = service();
        let principal = crate::security::test_human_interactive_principal();
        let review = service
            .request_first_install(&principal, "first", "review", "1.0.0", "receipt-original")
            .unwrap();
        assert!(service.pointer("review").unwrap().is_none());
        for (inspection, scope) in [
            ("changed-inspection", "workspace-test"),
            ("original-inspection", "foreign-workspace"),
        ] {
            *service.catalog.write().unwrap() = crate::RuntimeSkillCatalog::default()
                .with_instruction_source(Arc::new(InstalledSource { inspection, scope }));
            assert!(service
                .decide_review(
                    &principal,
                    &lease(&review),
                    &review.review_id,
                    SkillRevisionReviewDecision::Approve,
                    "must recheck"
                )
                .is_err());
            assert!(service.pointer("review").unwrap().is_none());
        }
        *service.catalog.write().unwrap() = crate::RuntimeSkillCatalog::default()
            .with_instruction_source(Arc::new(InstalledSource {
                inspection: "original-inspection",
                scope: "workspace-test",
            }));
        service
            .decide_review(
                &principal,
                &lease(&review),
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "checked original installation",
            )
            .unwrap();
        let reopened = SkillRevisionGovernanceService::new(
            Arc::clone(&service.event_store),
            Arc::clone(&service.approvals),
            Arc::clone(&service.catalog),
        );
        let replay = reopened
            .request_first_install(&principal, "first", "review", "1.0.0", "receipt-original")
            .unwrap();
        assert_eq!(replay.status, SkillRevisionReviewStatus::Approved);
        assert_eq!(replay.review_id, "skill-revision-review:first");
        assert!(reopened
            .request_first_install(&principal, "first", "review", "1.0.0", "other-receipt")
            .is_err());
        assert!(reopened
            .request_first_install(&principal, "new-first", "review", "1.1.0", "other-receipt")
            .is_err());
        let pointer = reopened.pointer("review").unwrap().unwrap();
        assert_eq!(pointer.generation, 1);
        assert_eq!(pointer.active_revision, "1.0.0");
        assert_eq!(pointer.installation.unwrap().scope_ref, "workspace-test");
    }

    #[test]
    fn maintenance_usage_gate_and_missing_installed_reader_do_not_become_bootstrap() {
        let service = service();
        let principal = crate::security::test_human_interactive_principal();
        assert!(service
            .request_activation(&principal, "no-first", &draft(), "1.1.0", "receipt")
            .is_err());
        approve_first(&service);
        let mut no_outcome = draft();
        no_outcome.validation.outcome_association_count = 0;
        let mut open_evidence = draft();
        open_evidence.validation.evidence_closed = false;
        let mut keep = draft();
        keep.recommendation = SkillMaintenanceRecommendation::Keep;
        for bad in [no_outcome, open_evidence, keep] {
            assert!(matches!(
                service.request_activation(&principal, "ineligible", &bad, "1.1.0", "receipt"),
                Err(SkillRevisionGovernanceError::DraftNotEligible(_))
            ));
        }
        *service.catalog.write().unwrap() = crate::RuntimeSkillCatalog::default();
        assert!(service
            .request_activation(&principal, "unavailable", &draft(), "1.1.0", "receipt")
            .is_err());
        assert_eq!(service.pointer("review").unwrap().unwrap().generation, 1);
    }

    #[tokio::test]
    async fn original_pointer_readers_observe_later_commits_without_cached_absence_or_generation() {
        use crate::RuntimeSkillUsageSink;
        let reader = service();
        let writer = SkillRevisionGovernanceService::new(
            Arc::clone(&reader.event_store),
            Arc::clone(&reader.approvals),
            Arc::clone(&reader.catalog),
        );
        let observer =
            crate::RuntimeSkillUsageRecorder::new(Arc::clone(&reader.event_store)).unwrap();
        assert!(reader.pointer("review").unwrap().is_none());
        assert!(observer.active_pointer("review").unwrap().is_none());
        approve_first(&writer);
        let principal = crate::security::test_human_interactive_principal();
        let review = writer
            .request_activation(&principal, "new", &draft(), "1.1.0", "sha256:inspection")
            .unwrap();
        writer
            .decide_review(
                &principal,
                &lease(&review),
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "validated",
            )
            .unwrap();
        assert_eq!(reader.pointer("review").unwrap().unwrap().generation, 2);
        let current = observer.active_pointer("review").unwrap().unwrap();
        assert_eq!(current.active_revision, "1.1.0");
        assert_eq!(current.generation, 2);
        let rollback = writer
            .request_rollback(
                &principal,
                "back",
                "review",
                "1.0.0",
                "sha256:regression",
                "receipt-1.0",
            )
            .unwrap();
        writer
            .decide_review(
                &principal,
                &lease(&rollback),
                &rollback.review_id,
                SkillRevisionReviewDecision::Approve,
                "regression",
            )
            .unwrap();
        let current = observer.active_pointer("review").unwrap().unwrap();
        assert_eq!(current.active_revision, "1.0.0");
        assert_eq!(current.generation, 3);
        assert_eq!(reader.pointer("review").unwrap().unwrap().generation, 3);
        assert!(
            service().pointer("review").unwrap().is_none(),
            "another original store cannot borrow this pointer"
        );
        assert!(
            observer
                .shutdown_and_drain(std::time::Duration::from_secs(2))
                .await
                .clean
        );
    }

    #[test]
    fn competing_review_between_pointer_read_and_commit_cannot_rebind_the_cut() {
        let mut loser = service();
        let winner = Arc::new(SkillRevisionGovernanceService::new(
            Arc::clone(&loser.event_store),
            Arc::clone(&loser.approvals),
            Arc::clone(&loser.catalog),
        ));
        let principal = crate::security::test_human_interactive_principal();
        let first = winner
            .request_first_install(&principal, "winner", "review", "1.2.0", "receipt-first")
            .unwrap();
        let second = loser
            .request_first_install(&principal, "loser", "review", "1.1.0", "receipt-second")
            .unwrap();
        loser.decision_read_hook = Some(Arc::new(move || {
            winner
                .decide_review(
                    &crate::security::test_human_interactive_principal(),
                    &lease(&first),
                    &first.review_id,
                    SkillRevisionReviewDecision::Approve,
                    "first wins",
                )
                .unwrap();
        }));
        assert!(loser
            .decide_review(
                &principal,
                &lease(&second),
                &second.review_id,
                SkillRevisionReviewDecision::Approve,
                "must not replace winner"
            )
            .is_err());
        let pointer = loser.pointer("review").unwrap().unwrap();
        assert_eq!(pointer.active_revision, "1.2.0");
        assert_eq!(pointer.generation, 1);
        assert_eq!(
            loser
                .event_store
                .stream_event_count(&pointer_stream("review"))
                .unwrap(),
            1
        );
        assert_eq!(
            loser.review(&second.review_id).unwrap().status,
            SkillRevisionReviewStatus::Superseded
        );
    }

    #[test]
    fn same_request_replays_but_changed_scope_package_or_evidence_cannot_borrow_it() {
        let service = service();
        approve_first(&service);
        let principal = crate::security::test_human_interactive_principal();
        let original = service
            .request_activation(&principal, "same", &draft(), "1.1.0", "sha256:inspection")
            .unwrap();
        assert_eq!(
            service
                .request_activation(&principal, "same", &draft(), "1.1.0", "sha256:inspection")
                .unwrap(),
            original
        );
        let mut foreign = draft();
        foreign.skill_id = "foreign".into();
        let mut other_draft = draft();
        other_draft.draft_id = "other-draft".into();
        for (input, target, evidence) in [
            (foreign, "1.1.0", "sha256:inspection"),
            (other_draft, "1.1.0", "sha256:inspection"),
            (draft(), "1.2.0", "sha256:inspection"),
            (draft(), "1.1.0", "sha256:other"),
        ] {
            assert!(matches!(
                service.request_activation(&principal, "same", &input, target, evidence),
                Err(SkillRevisionGovernanceError::InvalidRequest(_))
            ));
        }
        assert_eq!(
            service
                .event_store
                .stream_event_count(&review_stream(&original.review_id))
                .unwrap(),
            1
        );
        assert_eq!(
            service.pointer("review").unwrap().unwrap().active_revision,
            "1.0.0"
        );
    }

    #[test]
    fn unknown_pointer_and_terminal_approval_cannot_become_absent_or_pending() {
        let service = service();
        let principal = crate::security::test_human_interactive_principal();
        let review = service
            .request_first_install(&principal, "denied", "review", "1.1.0", "receipt-denied")
            .unwrap();
        // Retain the old queue cache while the original journal advances.
        assert_eq!(
            service.approvals.get(&review.approval_id).unwrap().status,
            GlobalApprovalStatus::Pending
        );
        service
            .event_store
            .append(RuntimeEventInput {
                stream_id: format!("approval:{}", review.approval_id),
                scope: RuntimeEventScope::Approval,
                kind: "approval.decided".into(),
                status: Some("denied".into()),
                actor: Some("other-human".into()),
                refs: vec![],
                payload: json!({"approved":false}),
            })
            .unwrap();
        assert!(service
            .decide_review(
                &principal,
                &lease(&review),
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "cached pending is not authority"
            )
            .is_err());
        assert!(service.pointer("review").unwrap().is_none());
        service
            .event_store
            .append(RuntimeEventInput {
                stream_id: pointer_stream("review"),
                scope: RuntimeEventScope::Skill,
                kind: "foreign.pointer.event".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: json!({}),
            })
            .unwrap();
        assert!(service.pointer("review").is_err());
        assert!(service
            .request_first_install(&principal, "fresh", "review", "1.1.0", "receipt-fresh")
            .is_err());
    }

    #[test]
    fn activation_and_rollback_require_human_lease_and_advance_generation() {
        let service = service();
        approve_first(&service);
        let principal = crate::security::test_human_interactive_principal();
        let review = service
            .request_activation(
                &principal,
                "activate",
                &draft(),
                "1.1.0",
                "sha256:inspection",
            )
            .expect("activation review");
        assert_eq!(
            service.pointer("review").unwrap().unwrap().active_revision,
            "1.0.0"
        );
        let pointer = service
            .decide_review(
                &principal,
                &lease(&review),
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "validated",
            )
            .expect("decision")
            .expect("pointer");
        assert_eq!(pointer.active_revision, "1.1.0");
        assert_eq!(pointer.previous_revision.as_deref(), Some("1.0.0"));
        assert_eq!(pointer.generation, 2);

        let rollback = service
            .request_rollback(
                &principal,
                "rollback",
                "review",
                "1.0.0",
                "sha256:regression",
                "receipt-1.0",
            )
            .expect("rollback review");
        let rolled_back = service
            .decide_review(
                &principal,
                &lease(&rollback),
                &rollback.review_id,
                SkillRevisionReviewDecision::Approve,
                "regression",
            )
            .expect("rollback")
            .expect("pointer");
        assert_eq!(rolled_back.active_revision, "1.0.0");
        assert_eq!(rolled_back.previous_revision.as_deref(), Some("1.1.0"));
        assert_eq!(rolled_back.generation, 3);
        let replay = service
            .request_rollback(
                &principal,
                "rollback",
                "review",
                "1.0.0",
                "sha256:regression",
                "receipt-1.0",
            )
            .expect("lost rollback response replays original request before reading new pointer");
        assert_eq!(replay.review_id, rollback.review_id);
        assert_eq!(replay.status, SkillRevisionReviewStatus::Approved);
        for (skill, target, reason) in [
            ("review", "1.2.0", "sha256:regression"),
            ("review", "1.0.0", "sha256:changed"),
            ("foreign", "1.0.0", "sha256:regression"),
        ] {
            assert!(matches!(
                service.request_rollback(
                    &principal,
                    "rollback",
                    skill,
                    target,
                    reason,
                    "receipt-1.0"
                ),
                Err(SkillRevisionGovernanceError::InvalidRequest(_))
            ));
        }
        assert_eq!(service.pointer("review").unwrap().unwrap(), rolled_back);
    }

    #[test]
    fn concurrent_review_is_fenced_by_pointer_generation() {
        let service = service();
        approve_first(&service);
        let principal = crate::security::test_human_interactive_principal();
        let first = service
            .request_activation(&principal, "first", &draft(), "1.1.0", "sha256:inspection")
            .expect("first");
        let second = service
            .request_activation(&principal, "second", &draft(), "1.1.0", "sha256:inspection")
            .expect("second");
        service
            .decide_review(
                &principal,
                &lease(&first),
                &first.review_id,
                SkillRevisionReviewDecision::Approve,
                "first wins",
            )
            .expect("first decision");
        assert!(matches!(
            service.decide_review(
                &principal,
                &lease(&second),
                &second.review_id,
                SkillRevisionReviewDecision::Approve,
                "stale",
            ),
            Err(SkillRevisionGovernanceError::GenerationChanged)
        ));
        assert_eq!(
            service
                .review(&second.review_id)
                .expect("second status")
                .status,
            SkillRevisionReviewStatus::Superseded
        );
    }
}
