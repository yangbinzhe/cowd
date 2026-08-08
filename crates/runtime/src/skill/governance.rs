//! Human-governed Skill revision pointer.
//!
//! Maintenance Drafts are inert evidence. A pointer can move only through a
//! pending approval and a one-time verified human decision lease.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

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
    runtime_event_store::{AppendTransactionRequest, ExpectedStreamRevision},
    ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    GlobalApprovalRequest, GlobalApprovalStatus, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, VerifiedDecisionLease, VerifiedPrincipal,
};

const REVIEW_STREAM_PREFIX: &str = "skill-revision-review:";
const POINTER_STREAM_PREFIX: &str = "skill-revision-pointer:";
const REVIEW_REQUESTED_KIND: &str = "skill.revision.review.requested.v1";
const REVIEW_DECIDED_KIND: &str = "skill.revision.review.decided.v1";
const POINTER_CHANGED_KIND: &str = "skill.revision.pointer.changed.v1";

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
    pointer_cache: Arc<SkillRevisionPointerCache>,
}

/// Shared, read-through view of approved Skill revisions.
///
/// The Gateway page-in path and Runtime governance service receive the same
/// instance from the composition root. Normal Skill activation therefore
/// performs no durable-store read, while every approved pointer transaction
/// updates the cache before the decision is returned.
#[derive(Debug, Default)]
pub struct SkillRevisionPointerCache {
    pointers: RwLock<BTreeMap<String, Option<SkillActivePointer>>>,
}

impl SkillRevisionPointerCache {
    #[must_use]
    pub fn pointer(
        &self,
        event_store: &RuntimeEventStore,
        skill_id: &str,
    ) -> Result<Option<SkillActivePointer>, String> {
        if let Some(pointer) = self
            .pointers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(skill_id)
            .cloned()
        {
            return Ok(pointer);
        }
        let pointer = active_pointer_from_store(event_store, skill_id)?;
        self.pointers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(skill_id.to_string(), pointer.clone());
        Ok(pointer)
    }

    fn publish(&self, skill_id: &str, pointer: Option<SkillActivePointer>) {
        self.pointers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(skill_id.to_string(), pointer);
    }
}

impl SkillRevisionGovernanceService {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>, approvals: Arc<ApprovalQueue>) -> Self {
        Self::with_pointer_cache(
            event_store,
            approvals,
            Arc::new(SkillRevisionPointerCache::default()),
        )
    }

    #[must_use]
    pub fn with_pointer_cache(
        event_store: Arc<RuntimeEventStore>,
        approvals: Arc<ApprovalQueue>,
        pointer_cache: Arc<SkillRevisionPointerCache>,
    ) -> Self {
        Self {
            event_store,
            approvals,
            pointer_cache,
        }
    }

    pub fn request_activation(
        &self,
        principal: &VerifiedPrincipal,
        request_id: &str,
        draft: &SkillMaintenanceDraft,
        target_revision: &str,
        validation_digest: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        require_human(principal)?;
        if request_id.trim().is_empty()
            || target_revision.trim().is_empty()
            || validation_digest.trim().is_empty()
        {
            return Err(SkillRevisionGovernanceError::InvalidRequest(
                "request_id, target_revision and validation_digest are required".to_string(),
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
        self.request_review(
            request_id,
            SkillRevisionReviewAction::Activate,
            Some(draft.draft_id.clone()),
            &draft.skill_id,
            target_revision,
            Some(draft.base_revision.clone()),
            &digest_json(&json!({
                "draft_digest": draft.digest(),
                "validation_digest": validation_digest,
                "target_revision": target_revision,
            })),
        )
    }

    pub fn request_rollback(
        &self,
        principal: &VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        reason_evidence_digest: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        require_human(principal)?;
        let pointer = self.pointer(skill_id)?.ok_or_else(|| {
            SkillRevisionGovernanceError::InvalidRequest("no active pointer".into())
        })?;
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
            })),
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
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        let review_id = format!("skill-revision-review:{}", request_id.trim());
        if let Ok(existing) = self.review(&review_id) {
            return Ok(existing);
        }
        let review = SkillRevisionReview {
            review_id: review_id.clone(),
            approval_id: format!("approval:{review_id}"),
            action,
            draft_id,
            skill_id: skill_id.to_string(),
            target_revision: target_revision.to_string(),
            previous_revision,
            evidence_digest: evidence_digest.to_string(),
            expected_generation: self
                .pointer(skill_id)?
                .map(|pointer| pointer.generation)
                .unwrap_or_default(),
            status: SkillRevisionReviewStatus::Pending,
            created_at_ms: now_ms(),
        };
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
            evidence_refs: vec![review.evidence_digest.clone()],
            timeout_policy: ApprovalTimeoutPolicy::Pending,
            status: GlobalApprovalStatus::Pending,
            decision: None,
            created_at_ms: review.created_at_ms,
            resolved_at_ms: None,
        };
        let approval_stream = format!("approval:{}", review.approval_id);
        let review_stream = review_stream(&review.review_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("skill-revision-request:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: approval_stream.clone(),
                        expected_revision: self.revision(&approval_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: review_stream.clone(),
                        expected_revision: self.revision(&review_stream)?,
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
                        payload: json!({"review": review}),
                    }
                    .into(),
                ],
            })
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))?;
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
        let review = self.review(review_id)?;
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
        let current = self.pointer(&review.skill_id)?;
        let current_generation = current
            .as_ref()
            .map(|pointer| pointer.generation)
            .unwrap_or_default();
        if current_generation != review.expected_generation {
            return Err(SkillRevisionGovernanceError::GenerationChanged);
        }
        let approval = self
            .approvals
            .get(&review.approval_id)
            .ok_or(SkillRevisionGovernanceError::ReviewNotFound)?;
        if approval.status != GlobalApprovalStatus::Pending {
            return Err(SkillRevisionGovernanceError::ReviewNotPending);
        }
        let approved = decision == SkillRevisionReviewDecision::Approve;
        let pointer = approved.then(|| SkillActivePointer {
            skill_id: review.skill_id.clone(),
            active_revision: review.target_revision.clone(),
            previous_revision: review.previous_revision.clone(),
            generation: current_generation.saturating_add(1),
            source_draft_id: review.draft_id.clone(),
            approval_ref: review.approval_id.clone(),
            activated_at_ms: now_ms(),
        });
        let approval_stream = format!("approval:{}", review.approval_id);
        let review_stream = review_stream(&review.review_id);
        let pointer_stream = pointer_stream(&review.skill_id);
        let decided_by = principal.claims().principal_id.clone();
        let mut expected_streams = vec![
            ExpectedStreamRevision {
                stream_id: approval_stream.clone(),
                expected_revision: self.revision(&approval_stream)?,
            },
            ExpectedStreamRevision {
                stream_id: review_stream.clone(),
                expected_revision: self.revision(&review_stream)?,
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
                expected_revision: self.revision(&pointer_stream)?,
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
        if approved {
            self.pointer_cache
                .publish(&review.skill_id, pointer.clone());
        }
        self.approvals.refresh();
        Ok(pointer)
    }

    pub fn review(
        &self,
        review_id: &str,
    ) -> Result<SkillRevisionReview, SkillRevisionGovernanceError> {
        let events = self
            .event_store
            .list_stream(&review_stream(review_id))
            .map_err(SkillRevisionGovernanceError::Store)?;
        let mut review: SkillRevisionReview = events
            .iter()
            .find(|event| event.kind == REVIEW_REQUESTED_KIND)
            .and_then(|event| event.payload.get("review"))
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .ok_or(SkillRevisionGovernanceError::ReviewNotFound)?;
        if let Some(decision) = events
            .iter()
            .rev()
            .find(|event| event.kind == REVIEW_DECIDED_KIND)
            .and_then(|event| event.payload.get("decision"))
            .cloned()
            .and_then(|value| serde_json::from_value::<SkillRevisionReviewDecision>(value).ok())
        {
            review.status = match decision {
                SkillRevisionReviewDecision::Approve => SkillRevisionReviewStatus::Approved,
                SkillRevisionReviewDecision::Deny => SkillRevisionReviewStatus::Denied,
            };
        } else if self
            .pointer(&review.skill_id)?
            .map(|pointer| pointer.generation)
            .unwrap_or_default()
            != review.expected_generation
        {
            review.status = SkillRevisionReviewStatus::Superseded;
        }
        Ok(review)
    }

    pub fn pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<SkillActivePointer>, SkillRevisionGovernanceError> {
        self.pointer_cache
            .pointer(&self.event_store, skill_id)
            .map_err(SkillRevisionGovernanceError::Store)
    }

    fn revision(&self, stream: &str) -> Result<u64, SkillRevisionGovernanceError> {
        self.event_store
            .stream_revision(stream)
            .map_err(|error| SkillRevisionGovernanceError::Store(error.to_string()))
    }
}

pub(crate) fn active_pointer_from_store(
    event_store: &RuntimeEventStore,
    skill_id: &str,
) -> Result<Option<SkillActivePointer>, String> {
    event_store
        .list_stream_page_desc(&pointer_stream(skill_id), 1, 0)
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|event| event.kind == POINTER_CHANGED_KIND)
        .map(|event| {
            event
                .payload
                .get("pointer")
                .cloned()
                .ok_or_else(|| "Skill active pointer payload is missing".to_string())
                .and_then(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
        })
        .transpose()
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
        SkillMaintenanceValidation, SkillUsageCounts, SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION,
    };

    fn service() -> SkillRevisionGovernanceService {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("store"));
        let approvals = Arc::new(ApprovalQueue::new(Arc::clone(&store)));
        SkillRevisionGovernanceService::new(store, approvals)
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
    fn activation_and_rollback_require_human_lease_and_advance_generation() {
        let service = service();
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
        assert!(service.pointer("review").expect("pointer").is_none());
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
        assert_eq!(pointer.generation, 1);

        let rollback = service
            .request_rollback(
                &principal,
                "rollback",
                "review",
                "1.0.0",
                "sha256:regression",
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
        assert_eq!(rolled_back.generation, 2);
    }

    #[test]
    fn concurrent_review_is_fenced_by_pointer_generation() {
        let service = service();
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
            Err(SkillRevisionGovernanceError::ReviewNotPending)
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
