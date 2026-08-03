//! Runtime-owned global approval queue.
//!
//! This queue is the common routing point for approvals raised by sessions,
//! agents, teams, and future steward agents. It records pending requests,
//! decisions, timeout policy, and the source that should receive the result.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub use harness_contract::policy::{
    ApprovalApplicationSource, ApprovalContext, ApprovalDecision, ApprovalDecisionActor,
    ApprovalDecisionActorKind, ApprovalDecisionCommand, ApprovalDecisionReceipt, ApprovalGrant,
    ApprovalGrantScope, ApprovalGrantStatus, ApprovalRequest, ApprovalSource, ApprovalSourceKind,
    ApprovalStatus, ApprovalTimeoutPolicy, SubmitApprovalRequest,
};

use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

pub type GlobalApprovalRequest = ApprovalRequest;
pub type SubmitGlobalApprovalRequest = SubmitApprovalRequest;
pub type GlobalApprovalStatus = ApprovalStatus;
pub type GlobalApprovalDecisionReceipt = ApprovalDecisionReceipt;

#[derive(Debug)]
pub struct ApprovalQueue {
    requests: Mutex<BTreeMap<String, GlobalApprovalRequest>>,
    grants: Mutex<BTreeMap<String, ApprovalGrant>>,
    event_store: Arc<RuntimeEventStore>,
}

impl ApprovalQueue {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let (requests, grants) = restore_approval_state(&event_store);
        Self {
            requests: Mutex::new(requests),
            grants: Mutex::new(grants),
            event_store,
        }
    }

    /// Rebuild the in-memory read model after another commit owner appended
    /// approval events as part of a larger transaction.
    pub fn refresh(&self) {
        let (requests, grants) = restore_approval_state(&self.event_store);
        *self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = requests;
        *self
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = grants;
    }

    pub fn submit(
        &self,
        request: SubmitGlobalApprovalRequest,
    ) -> Result<GlobalApprovalRequest, String> {
        self.submit_scoped(format!("approval-{}", uuid::Uuid::new_v4()), request)
    }

    /// Submit an approval under a caller-owned stable idempotency key.
    pub fn submit_scoped(
        &self,
        approval_id: impl Into<String>,
        request: SubmitGlobalApprovalRequest,
    ) -> Result<GlobalApprovalRequest, String> {
        request.source.validate()?;
        if request.action.trim().is_empty() {
            return Err("approval action must not be empty".to_string());
        }
        if request.summary.trim().is_empty() {
            return Err("approval summary must not be empty".to_string());
        }
        validate_approval_context(&request.context)?;
        let approval_id = approval_id.into();
        if approval_id.trim().is_empty() {
            return Err("approval id must not be empty".to_string());
        }
        if let Some(existing) = self.get(&approval_id) {
            if existing.source == request.source
                && existing.action == request.action
                && existing.summary == request.summary
            {
                return Ok(existing);
            }
            return Err(format!("approval idempotency conflict: {approval_id}"));
        }
        let approval = GlobalApprovalRequest {
            approval_id: approval_id.clone(),
            source: request.source,
            context: request.context,
            action: request.action,
            summary: request.summary,
            risk: request.risk,
            evidence_refs: request.evidence_refs,
            timeout_policy: request.timeout_policy,
            status: GlobalApprovalStatus::Pending,
            decision: None,
            created_at_ms: now_ms(),
            resolved_at_ms: None,
        };
        let stream_id = format!("approval:{}", approval.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|e| e.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-submit:{}", approval.approval_id),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.submitted".to_string(),
                    status: Some(approval.status.as_str().to_string()),
                    actor: Some("approval_queue".to_string()),
                    refs: approval_source_refs(&approval.source),
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "request": approval,
                        "action": approval.action,
                        "summary": approval.summary,
                        "risk": approval.risk,
                        "timeout_policy": approval.timeout_policy,
                    }),
                }
                .into()],
            )
            .map_err(|e| e.to_string())?;
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(approval_id, approval.clone());
        Ok(approval)
    }

    pub fn decide(
        &self,
        principal: &crate::VerifiedPrincipal,
        mut decision: ApprovalDecisionCommand,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        decision.actor = ApprovalDecisionActor {
            kind: ApprovalDecisionActorKind::Human,
            actor_id: principal.claims().principal_id.clone(),
        };
        self.decide_authorized(decision)
    }

    /// Commit a human decision received through a Gateway-authenticated
    /// external Surface. Gateway must first bind the actor to the same Session;
    /// text channels may never create a Global grant.
    pub fn decide_surface_human(
        &self,
        actor_id: &str,
        mut decision: ApprovalDecisionCommand,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if actor_id.trim().is_empty() {
            return Err("approval_surface_actor_required".to_string());
        }
        if decision.scope == ApprovalGrantScope::Global {
            return Err("global_approval_requires_interactive_control_surface".to_string());
        }
        decision.actor = ApprovalDecisionActor {
            kind: ApprovalDecisionActorKind::Human,
            actor_id: actor_id.to_string(),
        };
        self.decide_authorized(decision)
    }

    /// Commit a policy, Steward, timeout, or typed-owner decision after the
    /// caller has enforced its deterministic eligibility rules.
    pub fn decide_internal(
        &self,
        decision: ApprovalDecisionCommand,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if decision.scope == ApprovalGrantScope::Global
            && decision.actor.kind != ApprovalDecisionActorKind::Human
        {
            return Err("global_approval_requires_human_actor".to_string());
        }
        if decision.actor.kind == ApprovalDecisionActorKind::Human {
            return Err("human_approval_requires_verified_principal".to_string());
        }
        self.decide_authorized(decision)
    }

    fn decide_authorized(
        &self,
        decision: ApprovalDecisionCommand,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if decision.actor.actor_id.trim().is_empty() {
            return Err("approval decision actor must not be empty".to_string());
        }
        let decided_by = decision.actor.actor_id.clone();
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = requests
            .get_mut(&decision.approval_id)
            .ok_or_else(|| format!("approval request not found: {}", decision.approval_id))?;
        // Evolution release decisions bind a signed, one-time lease to the
        // immutable review evidence and must be committed by
        // EvolutionGovernanceService. Letting this generic queue decide the
        // approval would leave a release review approved without its matching
        // Runtime release assignment.
        if request.source.kind == ApprovalSourceKind::Evolution {
            return Err("evolution_release_requires_typed_decision_service".to_string());
        }
        if request.source.typed_application().is_some() {
            return Err("application_review_requires_typed_decision_service".to_string());
        }
        if request.status != GlobalApprovalStatus::Pending {
            return Ok(GlobalApprovalDecisionReceipt {
                approval_id: request.approval_id.clone(),
                status: request.status,
                route_back: request.source.clone(),
                message: format!("approval already {}", status_label(request.status)),
                grant_id: self
                    .grant_for_approval(&request.approval_id)
                    .map(|grant| grant.grant_id),
            });
        }
        let next_status = if decision.approved {
            GlobalApprovalStatus::Approved
        } else {
            GlobalApprovalStatus::Denied
        };
        let resolved_at_ms = now_ms();
        let receipt = GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status: next_status,
            route_back: request.source.clone(),
            message: if decision.approved {
                format!("approved by {decided_by}")
            } else {
                format!("denied by {decided_by}: {}", decision.reason)
            },
            grant_id: decision
                .approved
                .then(|| format!("approval-grant:{}", request.approval_id)),
        };
        let decided_at_ms = now_ms();
        let canonical_decision = ApprovalDecision {
            approved: decision.approved,
            reason: decision.reason.clone(),
            scope: decision.scope,
            actor: decision.actor.clone(),
            evidence_refs: decision.evidence_refs.clone(),
            decided_at_ms,
        };
        let grant = decision.approved.then(|| {
            approval_grant_from_request(
                request,
                decision.scope,
                decision.actor.clone(),
                decided_at_ms,
            )
        });
        let stream_id = format!("approval:{}", request.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|e| e.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-decision:{}", request.approval_id),
                {
                    let mut events = vec![RuntimeEventInput {
                        stream_id: stream_id.clone(),
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.decided".to_string(),
                        status: Some(next_status.as_str().to_string()),
                        actor: Some(decided_by),
                        refs: approval_source_refs(&request.source),
                        payload: serde_json::json!({
                            "schema_version": 2,
                            "decision": canonical_decision,
                            "approved": decision.approved,
                            "reason": decision.reason,
                            "scope": decision.scope,
                            "message": receipt.message,
                            "resolved_at_ms": resolved_at_ms,
                        }),
                    }];
                    if let Some(grant) = grant.as_ref() {
                        events.push(RuntimeEventInput {
                            stream_id,
                            scope: RuntimeEventScope::Approval,
                            kind: "approval.grant_issued".to_string(),
                            status: Some("active".to_string()),
                            actor: Some(grant.issued_by.actor_id.clone()),
                            refs: approval_source_refs(&request.source),
                            payload: serde_json::json!({
                                "schema_version": 2,
                                "grant": grant,
                            }),
                        });
                    }
                    events.into_iter().map(Into::into).collect()
                },
            )
            .map_err(|e| e.to_string())?;
        request.status = next_status;
        request.resolved_at_ms = Some(resolved_at_ms);
        request.decision = Some(canonical_decision);
        if let Some(grant) = grant {
            self.grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(grant.grant_id.clone(), grant);
        }
        Ok(receipt)
    }

    /// Record a decision already authorized by an APP-owned typed review.
    ///
    /// The caller must first persist its decision intent and consume its
    /// one-time decision lease. This method verifies the application, schema,
    /// and review correlation before appending the generic decision fact. It
    /// never owns APP effect or terminal state.
    #[allow(clippy::too_many_arguments)]
    pub fn record_application_decision_fact(
        &self,
        approval_id: &str,
        application: &ApprovalApplicationSource,
        review_ref: &str,
        decided_by: &str,
        approved: bool,
        decision: &str,
        reason: &str,
        decision_lease_ref: &str,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if review_ref.trim().is_empty()
            || decided_by.trim().is_empty()
            || decision.trim().is_empty()
            || decision_lease_ref.trim().is_empty()
        {
            return Err("application_typed_decision_fact_is_incomplete".to_string());
        }
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = requests
            .get_mut(approval_id)
            .ok_or_else(|| format!("approval request not found: {approval_id}"))?;
        if request.source.kind != ApprovalSourceKind::Application
            || request.source.application.as_ref() != Some(application)
            || request.source.review_ref.as_deref() != Some(review_ref)
        {
            return Err("application_approval_correlation_mismatch".to_string());
        }
        let next_status = if approved {
            GlobalApprovalStatus::Approved
        } else {
            GlobalApprovalStatus::Denied
        };
        if request.status != GlobalApprovalStatus::Pending {
            if request.status == next_status {
                return Ok(GlobalApprovalDecisionReceipt {
                    approval_id: request.approval_id.clone(),
                    status: request.status,
                    route_back: request.source.clone(),
                    message: format!("approval already {}", status_label(request.status)),
                    grant_id: None,
                });
            }
            return Err("application_approval_decision_conflict".to_string());
        }
        let resolved_at_ms = now_ms();
        let receipt = GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status: next_status,
            route_back: request.source.clone(),
            message: format!(
                "application {} {decision} recorded for review {review_ref} by {decided_by}",
                application.app_id
            ),
            grant_id: None,
        };
        let canonical_decision = ApprovalDecision {
            approved,
            reason: reason.to_string(),
            scope: ApprovalGrantScope::Once,
            actor: ApprovalDecisionActor {
                kind: ApprovalDecisionActorKind::TypedOwner,
                actor_id: decided_by.to_string(),
            },
            evidence_refs: vec![decision_lease_ref.to_string()],
            decided_at_ms: resolved_at_ms,
        };
        let stream_id = format!("approval:{}", request.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!(
                    "application-approval-decision:{}:{review_ref}:{decision}",
                    application.app_id
                ),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.decided.application".to_string(),
                    status: Some(next_status.as_str().to_string()),
                    actor: Some(decided_by.to_string()),
                    refs: approval_source_refs(&request.source),
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "decision_record": canonical_decision,
                        "approved": approved,
                        "decision": decision,
                        "reason": reason,
                        "review_ref": review_ref,
                        "decision_lease_ref": decision_lease_ref,
                        "application": application,
                        "message": receipt.message,
                        "resolved_at_ms": resolved_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        request.status = next_status;
        request.resolved_at_ms = Some(resolved_at_ms);
        request.decision = Some(canonical_decision);
        Ok(receipt)
    }

    pub fn timeout(&self, approval_id: &str) -> Result<GlobalApprovalDecisionReceipt, String> {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = requests
            .get_mut(approval_id)
            .ok_or_else(|| format!("approval request not found: {approval_id}"))?;
        if request.timeout_policy == ApprovalTimeoutPolicy::AutoApproveOnce
            && request.risk == harness_contract::core::TaskRisk::Low
            && !request.context.explicit_ask
        {
            let approval_id = request.approval_id.clone();
            drop(requests);
            return self.decide_internal(ApprovalDecisionCommand {
                approval_id,
                approved: true,
                reason: "known low-risk approval wait timed out; approved once by policy"
                    .to_string(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::TimeoutPolicy,
                    actor_id: "low-risk-timeout-policy".to_string(),
                },
                evidence_refs: vec!["approval.timeout.auto_approve_once".to_string()],
            });
        }
        if request.status != GlobalApprovalStatus::Pending {
            return Ok(GlobalApprovalDecisionReceipt {
                approval_id: request.approval_id.clone(),
                status: request.status,
                route_back: request.source.clone(),
                message: format!("approval already {}", status_label(request.status)),
                grant_id: self
                    .grant_for_approval(&request.approval_id)
                    .map(|grant| grant.grant_id),
            });
        }
        let next_status = match request.timeout_policy {
            ApprovalTimeoutPolicy::Pending => GlobalApprovalStatus::Pending,
            ApprovalTimeoutPolicy::AutoDeny
            | ApprovalTimeoutPolicy::ContinueAlternative
            | ApprovalTimeoutPolicy::AutoApproveOnce => GlobalApprovalStatus::TimedOut,
        };
        let resolved_at_ms = (next_status == GlobalApprovalStatus::TimedOut).then(now_ms);
        let receipt = GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status: next_status,
            route_back: request.source.clone(),
            message: match request.timeout_policy {
                ApprovalTimeoutPolicy::Pending => "approval remains pending".to_string(),
                ApprovalTimeoutPolicy::AutoDeny => "approval timed out and must deny".to_string(),
                ApprovalTimeoutPolicy::ContinueAlternative => {
                    "approval timed out; source should continue alternative path".to_string()
                }
                ApprovalTimeoutPolicy::AutoApproveOnce => {
                    "approval timed out; request was not eligible for low-risk auto approval"
                        .to_string()
                }
            },
            grant_id: None,
        };
        let stream_id = format!("approval:{}", request.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-timeout:{}", request.approval_id),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.timed_out".to_string(),
                    status: Some(next_status.as_str().to_string()),
                    actor: Some("approval_queue".to_string()),
                    refs: approval_source_refs(&request.source),
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "timeout_policy": request.timeout_policy,
                        "message": receipt.message,
                        "resolved_at_ms": resolved_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        request.status = next_status;
        request.resolved_at_ms = resolved_at_ms;
        request.decision =
            (next_status == GlobalApprovalStatus::TimedOut).then(|| ApprovalDecision {
                approved: false,
                reason: receipt.message.clone(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::TimeoutPolicy,
                    actor_id: "approval-timeout-policy".to_string(),
                },
                evidence_refs: vec!["approval.timeout".to_string()],
                decided_at_ms: resolved_at_ms.unwrap_or_else(now_ms),
            });
        Ok(receipt)
    }

    #[must_use]
    pub fn get(&self, approval_id: &str) -> Option<GlobalApprovalRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(approval_id)
            .cloned()
    }

    #[must_use]
    pub fn pending(&self) -> Vec<GlobalApprovalRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|request| request.status == GlobalApprovalStatus::Pending)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn list(&self) -> Vec<GlobalApprovalRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn grants(&self) -> Vec<ApprovalGrant> {
        self.grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn active_grants(&self) -> Vec<ApprovalGrant> {
        let now = now_ms();
        self.grants()
            .into_iter()
            .filter(|grant| {
                grant.status == ApprovalGrantStatus::Active
                    && grant.expires_at_ms.is_none_or(|expires| expires > now)
            })
            .collect()
    }

    #[must_use]
    pub fn grant_for_approval(&self, approval_id: &str) -> Option<ApprovalGrant> {
        self.grants()
            .into_iter()
            .find(|grant| grant.approval_id == approval_id)
    }

    /// Match a durable approval grant against the complete current execution
    /// context. A broad command string is never sufficient.
    #[must_use]
    pub fn matching_grant(
        &self,
        context: &ApprovalContext,
        risk: harness_contract::core::TaskRisk,
    ) -> Option<ApprovalGrant> {
        let mut grants = self.active_grants();
        grants.sort_by_key(|grant| match grant.scope {
            ApprovalGrantScope::Once => 0,
            ApprovalGrantScope::Turn => 1,
            ApprovalGrantScope::Task => 2,
            ApprovalGrantScope::Session => 3,
            ApprovalGrantScope::Global => 4,
        });
        grants
            .into_iter()
            .find(|grant| grant_matches(grant, context, risk))
    }

    /// Consume a one-shot grant after the exact invocation has received its
    /// authorization lease. Other grant scopes remain active until revoked or
    /// expired.
    pub fn consume_once_grant(&self, grant_id: &str) -> Result<(), String> {
        let mut grants = self
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let grant = grants
            .get_mut(grant_id)
            .ok_or_else(|| format!("approval grant not found: {grant_id}"))?;
        if grant.scope != ApprovalGrantScope::Once || grant.status != ApprovalGrantStatus::Active {
            return Ok(());
        }
        let stream_id = format!("approval:{}", grant.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let consumed_at_ms = now_ms();
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-grant-consume:{grant_id}"),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.grant_consumed".to_string(),
                    status: Some("expired".to_string()),
                    actor: Some("approval_coordinator".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "approval_grant".to_string(),
                        id: grant_id.to_string(),
                    }],
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "grant_id": grant_id,
                        "consumed_at_ms": consumed_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        grant.status = ApprovalGrantStatus::Expired;
        grant.expires_at_ms = Some(consumed_at_ms);
        Ok(())
    }

    pub fn revoke_grant(
        &self,
        principal: &crate::VerifiedPrincipal,
        grant_id: &str,
        reason: &str,
    ) -> Result<ApprovalGrant, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let mut grants = self
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let grant = grants
            .get_mut(grant_id)
            .ok_or_else(|| format!("approval grant not found: {grant_id}"))?;
        if grant.status != ApprovalGrantStatus::Active {
            return Ok(grant.clone());
        }
        let stream_id = format!("approval:{}", grant.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let revoked_at_ms = now_ms();
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-grant-revoke:{grant_id}"),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.grant_revoked".to_string(),
                    status: Some("revoked".to_string()),
                    actor: Some(principal.claims().principal_id.clone()),
                    refs: vec![RuntimeEventRef {
                        kind: "approval_grant".to_string(),
                        id: grant_id.to_string(),
                    }],
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "grant_id": grant_id,
                        "reason": reason,
                        "revoked_at_ms": revoked_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        grant.status = ApprovalGrantStatus::Revoked;
        grant.revoked_at_ms = Some(revoked_at_ms);
        grant.revoke_reason = Some(reason.to_string());
        Ok(grant.clone())
    }

    pub fn cancel(
        &self,
        approval_id: &str,
        reason: &str,
        superseded: bool,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = requests
            .get_mut(approval_id)
            .ok_or_else(|| format!("approval request not found: {approval_id}"))?;
        if request.status != GlobalApprovalStatus::Pending {
            return Ok(GlobalApprovalDecisionReceipt {
                approval_id: request.approval_id.clone(),
                status: request.status,
                route_back: request.source.clone(),
                message: format!("approval already {}", status_label(request.status)),
                grant_id: None,
            });
        }
        let status = if superseded {
            GlobalApprovalStatus::Superseded
        } else {
            GlobalApprovalStatus::Cancelled
        };
        let resolved_at_ms = now_ms();
        let actor = ApprovalDecisionActor {
            kind: ApprovalDecisionActorKind::Policy,
            actor_id: "session-control".to_string(),
        };
        let decision = ApprovalDecision {
            approved: false,
            reason: reason.to_string(),
            scope: ApprovalGrantScope::Once,
            actor: actor.clone(),
            evidence_refs: vec!["session.control".to_string()],
            decided_at_ms: resolved_at_ms,
        };
        let stream_id = format!("approval:{}", request.approval_id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("approval-cancel:{}", request.approval_id),
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: if superseded {
                        "approval.superseded".to_string()
                    } else {
                        "approval.cancelled".to_string()
                    },
                    status: Some(status.as_str().to_string()),
                    actor: Some(actor.actor_id),
                    refs: approval_source_refs(&request.source),
                    payload: serde_json::json!({
                        "schema_version": 2,
                        "decision": decision,
                        "reason": reason,
                        "resolved_at_ms": resolved_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        request.status = status;
        request.resolved_at_ms = Some(resolved_at_ms);
        request.decision = Some(decision);
        Ok(GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status,
            route_back: request.source.clone(),
            message: reason.to_string(),
            grant_id: None,
        })
    }

    pub fn projection(&self) -> serde_json::Value {
        let requests = self.list();
        let pending_count = requests
            .iter()
            .filter(|request| request.status == GlobalApprovalStatus::Pending)
            .count();
        serde_json::json!({
            "kind": "runtime.global_approvals",
            "count": requests.len(),
            "pending_count": pending_count,
            "requests": requests,
            "grants": self.grants(),
            "active_grant_count": self.active_grants().len(),
        })
    }
}

fn status_label(status: GlobalApprovalStatus) -> &'static str {
    status.as_str()
}

fn approval_source_refs(source: &ApprovalSource) -> Vec<RuntimeEventRef> {
    let mut refs = Vec::new();
    if let Some(id) = &source.session_id {
        refs.push(RuntimeEventRef {
            kind: "session".to_string(),
            id: id.clone(),
        });
    }
    if let Some(id) = &source.agent_id {
        refs.push(RuntimeEventRef {
            kind: "agent".to_string(),
            id: id.clone(),
        });
    }
    if let Some(id) = &source.team_id {
        refs.push(RuntimeEventRef {
            kind: "team".to_string(),
            id: id.clone(),
        });
    }
    if let Some(id) = &source.mission_id {
        refs.push(RuntimeEventRef {
            kind: "mission".to_string(),
            id: id.clone(),
        });
    }
    if let Some(id) = &source.resource_ref {
        refs.push(RuntimeEventRef {
            kind: "resource".to_string(),
            id: id.clone(),
        });
    }
    if let Some(id) = &source.review_ref {
        refs.push(RuntimeEventRef {
            kind: "review".to_string(),
            id: id.clone(),
        });
    }
    if let Some(application) = &source.application {
        refs.push(RuntimeEventRef {
            kind: "application".to_string(),
            id: application.app_id.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "approval_correlation_schema".to_string(),
            id: application.correlation_schema.clone(),
        });
    }
    refs
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_approval_context(context: &ApprovalContext) -> Result<(), String> {
    if context.principal_id.trim().is_empty()
        || context.profile_id.trim().is_empty()
        || context.workspace_key.trim().is_empty()
        || context.capability.trim().is_empty()
    {
        return Err("approval context is incomplete".to_string());
    }
    if let Some(effect) = context.effect.as_ref() {
        if effect.tool_id.trim().is_empty() || effect.descriptor_hash.trim().is_empty() {
            return Err("approval effect descriptor is incomplete".to_string());
        }
    }
    Ok(())
}

fn approval_grant_from_request(
    request: &GlobalApprovalRequest,
    scope: ApprovalGrantScope,
    actor: ApprovalDecisionActor,
    created_at_ms: u64,
) -> ApprovalGrant {
    ApprovalGrant {
        grant_id: format!("approval-grant:{}", request.approval_id),
        approval_id: request.approval_id.clone(),
        scope,
        principal_id: request.context.principal_id.clone(),
        profile_id: request.context.profile_id.clone(),
        workspace_key: request.context.workspace_key.clone(),
        capability: request.context.capability.clone(),
        session_id: request.context.session_id.clone(),
        turn_id: request.context.turn_id.clone(),
        task_id: request.context.task_id.clone(),
        invocation_id: request.context.invocation_id.clone(),
        resource_targets: request.context.resource_targets.clone(),
        effect_descriptor_hash: request
            .context
            .effect
            .as_ref()
            .map(|effect| effect.descriptor_hash.clone()),
        risk_ceiling: request.risk,
        status: ApprovalGrantStatus::Active,
        issued_by: actor,
        created_at_ms,
        expires_at_ms: None,
        revoked_at_ms: None,
        revoke_reason: None,
    }
}

fn grant_matches(
    grant: &ApprovalGrant,
    context: &ApprovalContext,
    risk: harness_contract::core::TaskRisk,
) -> bool {
    if grant.status != ApprovalGrantStatus::Active
        || grant.principal_id != context.principal_id
        || grant.profile_id != context.profile_id
        || grant.workspace_key != context.workspace_key
        || grant.capability != context.capability
        || risk_rank(risk) > risk_rank(grant.risk_ceiling)
    {
        return false;
    }
    if grant
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= now_ms())
    {
        return false;
    }
    if grant.effect_descriptor_hash.as_ref()
        != context
            .effect
            .as_ref()
            .map(|effect| &effect.descriptor_hash)
    {
        return false;
    }
    if !grant.resource_targets.is_empty()
        && !context.resource_targets.iter().all(|target| {
            grant
                .resource_targets
                .iter()
                .any(|allowed| target == allowed)
        })
    {
        return false;
    }
    match grant.scope {
        ApprovalGrantScope::Once => {
            grant.invocation_id.is_some() && grant.invocation_id == context.invocation_id
        }
        ApprovalGrantScope::Turn => {
            grant.session_id.is_some()
                && grant.session_id == context.session_id
                && grant.turn_id.is_some()
                && grant.turn_id == context.turn_id
        }
        ApprovalGrantScope::Task => {
            grant.session_id.is_some()
                && grant.session_id == context.session_id
                && grant.task_id.is_some()
                && grant.task_id == context.task_id
        }
        ApprovalGrantScope::Session => {
            grant.session_id.is_some() && grant.session_id == context.session_id
        }
        ApprovalGrantScope::Global => true,
    }
}

const fn risk_rank(risk: harness_contract::core::TaskRisk) -> u8 {
    match risk {
        harness_contract::core::TaskRisk::Low => 0,
        harness_contract::core::TaskRisk::Medium => 1,
        harness_contract::core::TaskRisk::High => 2,
        harness_contract::core::TaskRisk::Critical => 3,
    }
}

fn restore_approval_state(
    event_store: &RuntimeEventStore,
) -> (
    BTreeMap<String, GlobalApprovalRequest>,
    BTreeMap<String, ApprovalGrant>,
) {
    let mut requests = BTreeMap::new();
    let mut grants = BTreeMap::new();
    let Ok(events) = event_store.replay_scope(RuntimeEventScope::Approval) else {
        return (requests, grants);
    };
    for event in events {
        match event.kind.as_str() {
            "approval.submitted" => {
                if let Some(request) = event
                    .payload
                    .get("request")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                {
                    let request: GlobalApprovalRequest = request;
                    requests.insert(request.approval_id.clone(), request);
                }
            }
            "approval.decided"
            | "approval.decided.application"
            | "approval.timed_out"
            | "approval.cancelled"
            | "approval.superseded" => {
                let Some(approval_id) = event.stream_id.strip_prefix("approval:") else {
                    continue;
                };
                let Some(request) = requests.get_mut(approval_id) else {
                    continue;
                };
                request.status = match event.status.as_deref() {
                    Some("approved") => GlobalApprovalStatus::Approved,
                    Some("denied") => GlobalApprovalStatus::Denied,
                    Some("timed_out") => GlobalApprovalStatus::TimedOut,
                    Some("cancelled") => GlobalApprovalStatus::Cancelled,
                    Some("superseded") => GlobalApprovalStatus::Superseded,
                    _ => request.status,
                };
                request.decision = event
                    .payload
                    .get("decision")
                    .or_else(|| event.payload.get("decision_record"))
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
                request.resolved_at_ms = if request.status == GlobalApprovalStatus::Pending {
                    None
                } else {
                    event
                        .payload
                        .get("resolved_at_ms")
                        .and_then(serde_json::Value::as_u64)
                        .or(Some(event.created_at_ms))
                };
            }
            "approval.grant_issued" => {
                if let Some(grant) = event
                    .payload
                    .get("grant")
                    .and_then(|value| serde_json::from_value::<ApprovalGrant>(value.clone()).ok())
                {
                    grants.insert(grant.grant_id.clone(), grant);
                }
            }
            "approval.grant_revoked" | "approval.grant_consumed" => {
                let Some(grant_id) = event
                    .payload
                    .get("grant_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(grant) = grants.get_mut(grant_id) else {
                    continue;
                };
                if event.kind == "approval.grant_revoked" {
                    grant.status = ApprovalGrantStatus::Revoked;
                    grant.revoked_at_ms = event
                        .payload
                        .get("revoked_at_ms")
                        .and_then(serde_json::Value::as_u64)
                        .or(Some(event.created_at_ms));
                    grant.revoke_reason = event
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string);
                } else {
                    grant.status = ApprovalGrantStatus::Expired;
                    grant.expires_at_ms = event
                        .payload
                        .get("consumed_at_ms")
                        .and_then(serde_json::Value::as_u64)
                        .or(Some(event.created_at_ms));
                }
            }
            _ => {}
        }
    }
    (requests, grants)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::core::TaskRisk;

    fn queue() -> ApprovalQueue {
        ApprovalQueue::new(Arc::new(
            RuntimeEventStore::try_open_in_memory().expect("event store"),
        ))
    }

    fn session_source() -> ApprovalSource {
        ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("session-approval".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        }
    }

    fn approval_context() -> ApprovalContext {
        ApprovalContext {
            principal_id: "principal:test".to_string(),
            profile_id: "balanced".to_string(),
            workspace_key: "workspace:test".to_string(),
            session_id: Some("session-approval".to_string()),
            turn_id: Some("turn-approval".to_string()),
            task_id: Some("task-approval".to_string()),
            capability: "test.approval".to_string(),
            invocation_id: Some("invocation-approval".to_string()),
            execution_id: Some("execution-approval".to_string()),
            strategy_decision_ref: Some("strategy-approval".to_string()),
            source_surface: Some("test".to_string()),
            resource_targets: Vec::new(),
            effect: None,
            explicit_ask: true,
        }
    }

    fn human_decision(
        approval_id: impl Into<String>,
        approved: bool,
        reason: impl Into<String>,
    ) -> ApprovalDecisionCommand {
        ApprovalDecisionCommand {
            approval_id: approval_id.into(),
            approved,
            reason: reason.into(),
            scope: ApprovalGrantScope::Once,
            actor: ApprovalDecisionActor {
                kind: ApprovalDecisionActorKind::Human,
                actor_id: "principal:test".to_string(),
            },
            evidence_refs: vec!["test.approval.decision".to_string()],
        }
    }

    #[test]
    fn approval_queue_routes_decisions_back_to_source() {
        let queue = queue();
        let request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "apply_patch".to_string(),
                summary: "modify runtime file".to_string(),
                risk: TaskRisk::Medium,
                evidence_refs: vec!["trace:1".to_string()],
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval submitted");

        assert_eq!(queue.pending().len(), 1);
        let principal = crate::security::test_human_interactive_principal();
        let receipt = queue
            .decide(
                &principal,
                human_decision(request.approval_id.clone(), true, "looks safe"),
            )
            .expect("approval decided");

        assert_eq!(receipt.status, GlobalApprovalStatus::Approved);
        assert_eq!(
            receipt.route_back.session_id.as_deref(),
            Some("session-approval")
        );
        assert!(queue.pending().is_empty());
        assert_eq!(queue.projection()["pending_count"], 0);
    }

    #[test]
    fn refresh_preserves_durable_approved_status() {
        let queue = queue();
        let request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "apply_patch".to_string(),
                summary: "modify runtime file".to_string(),
                risk: TaskRisk::Medium,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval submitted");
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                human_decision(request.approval_id.clone(), true, "reviewed"),
            )
            .expect("approval decided");

        queue.refresh();

        assert_eq!(
            queue.get(&request.approval_id).map(|value| value.status),
            Some(GlobalApprovalStatus::Approved)
        );
    }

    #[test]
    fn timeout_policy_can_hold_or_release_pending_work() {
        let queue = queue();
        let held = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "critical-command".to_string(),
                summary: "needs human".to_string(),
                risk: TaskRisk::Critical,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("held approval");
        let receipt = queue.timeout(&held.approval_id).expect("timeout held");
        assert_eq!(receipt.status, GlobalApprovalStatus::Pending);

        let alternative = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "optional-command".to_string(),
                summary: "can do something else".to_string(),
                risk: TaskRisk::Medium,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::ContinueAlternative,
            })
            .expect("alternative approval");
        let receipt = queue
            .timeout(&alternative.approval_id)
            .expect("timeout alternative");
        assert_eq!(receipt.status, GlobalApprovalStatus::TimedOut);
        assert!(receipt.message.contains("alternative"));
    }

    #[test]
    fn application_source_rejects_generic_decision_and_accepts_only_typed_correlated_fact() {
        let queue = queue();
        let application = ApprovalApplicationSource {
            app_id: "fulfillment".to_string(),
            correlation_schema: "fulfillment.review.v1".to_string(),
            decision_capability: "fulfillment.review".to_string(),
        };
        let blank_review = queue
            .submit(SubmitGlobalApprovalRequest {
                source: ApprovalSource {
                    kind: ApprovalSourceKind::Application,
                    session_id: None,
                    agent_id: None,
                    team_id: None,
                    mission_id: None,
                    resource_ref: Some("application:report:blank-review".to_string()),
                    review_ref: Some("   ".to_string()),
                    application: Some(application.clone()),
                },
                context: approval_context(),
                action: "fulfillment.review".to_string(),
                summary: "blank review must be rejected".to_string(),
                risk: TaskRisk::High,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap_err();
        assert_eq!(blank_review, "application_approval_source_is_incomplete");
        let request = queue
            .submit_scoped(
                "application-approval:review-1",
                SubmitGlobalApprovalRequest {
                    source: ApprovalSource {
                        kind: ApprovalSourceKind::Application,
                        session_id: None,
                        agent_id: None,
                        team_id: None,
                        mission_id: None,
                        resource_ref: Some("application:report:report-1".to_string()),
                        review_ref: Some("review-1".to_string()),
                        application: Some(application.clone()),
                    },
                    context: approval_context(),
                    action: "fulfillment.review.typed_decision".to_string(),
                    summary: "review failed report delivery".to_string(),
                    risk: TaskRisk::High,
                    evidence_refs: vec!["digest:dead-letter".to_string()],
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .unwrap();
        assert_eq!(
            queue
                .decide(
                    &crate::security::test_human_interactive_principal(),
                    human_decision(request.approval_id.clone(), true, "generic bypass"),
                )
                .unwrap_err(),
            "application_review_requires_typed_decision_service"
        );
        let receipt = queue
            .record_application_decision_fact(
                &request.approval_id,
                &application,
                "review-1",
                "principal:reviewer",
                true,
                "force_retry",
                "reviewed",
                "lease:review-1",
            )
            .unwrap();
        assert_eq!(receipt.status, GlobalApprovalStatus::Approved);
        queue.refresh();
        assert_eq!(
            queue.get(&request.approval_id).unwrap().status,
            GlobalApprovalStatus::Approved
        );
    }

    #[test]
    fn application_source_rejects_malformed_metadata_mismatch_and_conflicting_decision() {
        let queue = queue();
        let malformed = queue
            .submit(SubmitGlobalApprovalRequest {
                source: ApprovalSource {
                    kind: ApprovalSourceKind::Application,
                    session_id: None,
                    agent_id: None,
                    team_id: None,
                    mission_id: None,
                    resource_ref: Some("application:report:malformed".to_string()),
                    review_ref: Some("review-malformed".to_string()),
                    application: None,
                },
                context: approval_context(),
                action: "fulfillment.review".to_string(),
                summary: "malformed source must be rejected".to_string(),
                risk: TaskRisk::High,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap_err();
        assert_eq!(malformed, "application_approval_source_is_incomplete");

        let application = ApprovalApplicationSource {
            app_id: "fulfillment".to_string(),
            correlation_schema: "fulfillment.review.v1".to_string(),
            decision_capability: "fulfillment.review".to_string(),
        };
        let request = queue
            .submit_scoped(
                "application-approval:conflict",
                SubmitGlobalApprovalRequest {
                    source: ApprovalSource {
                        kind: ApprovalSourceKind::Application,
                        session_id: None,
                        agent_id: None,
                        team_id: None,
                        mission_id: None,
                        resource_ref: Some("application:report:conflict".to_string()),
                        review_ref: Some("review-conflict".to_string()),
                        application: Some(application.clone()),
                    },
                    context: approval_context(),
                    action: "fulfillment.review.typed_decision".to_string(),
                    summary: "typed review".to_string(),
                    risk: TaskRisk::High,
                    evidence_refs: Vec::new(),
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .unwrap();
        let wrong_application = ApprovalApplicationSource {
            correlation_schema: "inventory.review.v1".to_string(),
            ..application.clone()
        };
        assert_eq!(
            queue
                .record_application_decision_fact(
                    &request.approval_id,
                    &wrong_application,
                    "review-conflict",
                    "principal:reviewer",
                    true,
                    "resolve",
                    "wrong schema",
                    "lease:wrong",
                )
                .unwrap_err(),
            "application_approval_correlation_mismatch"
        );
        queue
            .record_application_decision_fact(
                &request.approval_id,
                &application,
                "review-conflict",
                "principal:reviewer",
                true,
                "resolve",
                "approved",
                "lease:approved",
            )
            .unwrap();
        assert_eq!(
            queue
                .record_application_decision_fact(
                    &request.approval_id,
                    &application,
                    "review-conflict",
                    "principal:reviewer",
                    false,
                    "reject",
                    "conflict",
                    "lease:conflict",
                )
                .unwrap_err(),
            "application_approval_decision_conflict"
        );
    }

    #[test]
    fn decided_approval_is_restored_after_restart() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = ApprovalQueue::new(Arc::clone(&store));
        let request = queue
            .submit_scoped(
                "approval:graph:node",
                SubmitGlobalApprovalRequest {
                    source: session_source(),
                    context: approval_context(),
                    action: "send".to_string(),
                    summary: "send after approval".to_string(),
                    risk: TaskRisk::High,
                    evidence_refs: Vec::new(),
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .unwrap();
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                human_decision(request.approval_id.clone(), true, "approved"),
            )
            .unwrap();

        let restarted = ApprovalQueue::new(store);
        assert_eq!(
            restarted.get(&request.approval_id).unwrap().status,
            GlobalApprovalStatus::Approved
        );
        assert!(restarted.pending().is_empty());
        assert_eq!(restarted.active_grants().len(), 1);
        assert_eq!(
            restarted.active_grants()[0].approval_id,
            request.approval_id
        );
    }

    #[test]
    fn once_grant_is_consumed_and_session_grant_is_context_bound_and_revocable() {
        let queue = queue();
        let once_request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "read".to_string(),
                summary: "read once".to_string(),
                risk: TaskRisk::Low,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                human_decision(once_request.approval_id.clone(), true, "approved once"),
            )
            .unwrap();
        let once = queue
            .matching_grant(&approval_context(), TaskRisk::Low)
            .expect("once grant matches exact invocation");
        queue.consume_once_grant(&once.grant_id).unwrap();
        assert!(queue
            .matching_grant(&approval_context(), TaskRisk::Low)
            .is_none());

        let session_request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "read".to_string(),
                summary: "read in session".to_string(),
                risk: TaskRisk::Medium,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();
        let mut decision = human_decision(
            session_request.approval_id.clone(),
            true,
            "approved session",
        );
        decision.scope = ApprovalGrantScope::Session;
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                decision,
            )
            .unwrap();
        let session_grant = queue
            .matching_grant(&approval_context(), TaskRisk::Low)
            .expect("session grant matches within the same session");
        let mut other_session = approval_context();
        other_session.session_id = Some("session-other".to_string());
        assert!(queue
            .matching_grant(&other_session, TaskRisk::Low)
            .is_none());
        queue
            .revoke_grant(
                &crate::security::test_human_interactive_principal(),
                &session_grant.grant_id,
                "no longer needed",
            )
            .unwrap();
        assert!(queue
            .matching_grant(&approval_context(), TaskRisk::Low)
            .is_none());
    }

    #[test]
    fn external_surface_cannot_create_global_grants() {
        let queue = queue();
        let request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "read".to_string(),
                summary: "external approval".to_string(),
                risk: TaskRisk::Low,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();
        let mut decision = human_decision(request.approval_id, true, "external approval");
        decision.scope = ApprovalGrantScope::Global;
        assert_eq!(
            queue
                .decide_surface_human("surface:user", decision)
                .unwrap_err(),
            "global_approval_requires_interactive_control_surface"
        );
    }

    #[test]
    fn turn_task_and_global_grants_enforce_their_complete_boundaries() {
        for scope in [
            ApprovalGrantScope::Turn,
            ApprovalGrantScope::Task,
            ApprovalGrantScope::Global,
        ] {
            let queue = queue();
            let request = queue
                .submit(SubmitGlobalApprovalRequest {
                    source: session_source(),
                    context: approval_context(),
                    action: "read".to_string(),
                    summary: format!("approve {scope:?}"),
                    risk: TaskRisk::Medium,
                    evidence_refs: Vec::new(),
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                })
                .unwrap();
            let mut decision = human_decision(
                request.approval_id.clone(),
                true,
                format!("approve {scope:?}"),
            );
            decision.scope = scope;
            queue
                .decide(
                    &crate::security::test_human_interactive_principal(),
                    decision,
                )
                .unwrap();

            assert_eq!(
                queue
                    .matching_grant(&approval_context(), TaskRisk::Low)
                    .map(|grant| grant.scope),
                Some(scope)
            );

            let mut changed_session = approval_context();
            changed_session.session_id = Some("session-other".to_string());
            let mut changed_turn = approval_context();
            changed_turn.turn_id = Some("turn-other".to_string());
            let mut changed_task = approval_context();
            changed_task.task_id = Some("task-other".to_string());

            match scope {
                ApprovalGrantScope::Turn => {
                    assert!(queue
                        .matching_grant(&changed_session, TaskRisk::Low)
                        .is_none());
                    assert!(queue.matching_grant(&changed_turn, TaskRisk::Low).is_none());
                }
                ApprovalGrantScope::Task => {
                    assert!(queue
                        .matching_grant(&changed_session, TaskRisk::Low)
                        .is_none());
                    assert!(queue.matching_grant(&changed_task, TaskRisk::Low).is_none());
                }
                ApprovalGrantScope::Global => {
                    assert!(queue
                        .matching_grant(&changed_session, TaskRisk::Low)
                        .is_some());
                    let mut changed_workspace = approval_context();
                    changed_workspace.workspace_key = "workspace:other".to_string();
                    assert!(queue
                        .matching_grant(&changed_workspace, TaskRisk::Low)
                        .is_none());
                    let mut changed_principal = approval_context();
                    changed_principal.principal_id = "principal:other".to_string();
                    assert!(queue
                        .matching_grant(&changed_principal, TaskRisk::Low)
                        .is_none());
                }
                ApprovalGrantScope::Once | ApprovalGrantScope::Session => unreachable!(),
            }
        }
    }

    #[test]
    fn global_grant_restores_with_the_same_scope_boundaries() {
        let event_store =
            Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("event store"));
        let queue = ApprovalQueue::new(Arc::clone(&event_store));
        let request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
                context: approval_context(),
                action: "read".to_string(),
                summary: "restore global grant".to_string(),
                risk: TaskRisk::Medium,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();
        let mut decision = human_decision(request.approval_id, true, "approved global");
        decision.scope = ApprovalGrantScope::Global;
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                decision,
            )
            .unwrap();

        let restarted = ApprovalQueue::new(event_store);
        let mut other_session = approval_context();
        other_session.session_id = Some("session-after-restart".to_string());
        assert_eq!(
            restarted
                .matching_grant(&other_session, TaskRisk::Low)
                .map(|grant| grant.scope),
            Some(ApprovalGrantScope::Global)
        );
        other_session.workspace_key = "workspace:other".to_string();
        assert!(restarted
            .matching_grant(&other_session, TaskRisk::Low)
            .is_none());
    }
}
