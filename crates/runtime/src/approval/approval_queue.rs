//! Runtime-owned global approval queue.
//!
//! This queue is the common routing point for approvals raised by sessions,
//! agents, teams, and future steward agents. It records pending requests,
//! decisions, timeout policy, and the source that should receive the result.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use harness_contract::core::TaskRisk;
use serde::{Deserialize, Serialize};

use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSourceKind {
    Session,
    Agent,
    Team,
    Mission,
    Steward,
    /// A Runtime-governed release decision for an evolution candidate.
    Evolution,
    /// An APP-owned typed business review. Runtime stores only a correlated
    /// approval fact; the application remains the review/effect state owner.
    Application,
}

/// Immutable application metadata that binds a typed review to its owning
/// application. Runtime deliberately treats the strings as opaque: their
/// schema and business DTO belong to the APP, not to Cowd core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalApplicationSource {
    pub app_id: String,
    pub correlation_schema: String,
    pub decision_capability: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSource {
    pub kind: ApprovalSourceKind,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub team_id: Option<String>,
    pub mission_id: Option<String>,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub review_ref: Option<String>,
    /// Present only for an [`ApprovalSourceKind::Application`] request.
    #[serde(default)]
    pub application: Option<ApprovalApplicationSource>,
}

impl ApprovalSource {
    fn validate(&self) -> Result<(), String> {
        match self.kind {
            ApprovalSourceKind::Application => {
                let application = self
                    .application
                    .as_ref()
                    .ok_or_else(|| "application_approval_source_is_incomplete".to_string())?;
                if application.app_id.trim().is_empty()
                    || application.correlation_schema.trim().is_empty()
                    || application.decision_capability.trim().is_empty()
                    || self
                        .review_ref
                        .as_deref()
                        .is_none_or(|review_ref| review_ref.trim().is_empty())
                {
                    return Err("application_approval_source_is_incomplete".to_string());
                }
            }
            _ if self.application.is_some() => {
                return Err("non_application_approval_cannot_include_application_metadata".into());
            }
            _ => {}
        }
        Ok(())
    }

    #[must_use]
    pub fn typed_application(&self) -> Option<&ApprovalApplicationSource> {
        (self.kind == ApprovalSourceKind::Application)
            .then_some(self.application.as_ref())
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalTimeoutPolicy {
    Pending,
    AutoDeny,
    ContinueAlternative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalApprovalStatus {
    Pending,
    Approved,
    Denied,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalApprovalRequest {
    pub approval_id: String,
    pub source: ApprovalSource,
    pub action: String,
    pub summary: String,
    pub risk: TaskRisk,
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
    pub status: GlobalApprovalStatus,
    pub created_at_ms: u64,
    pub resolved_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitGlobalApprovalRequest {
    pub source: ApprovalSource,
    pub action: String,
    pub summary: String,
    pub risk: TaskRisk,
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecisionCommand {
    pub approval_id: String,
    pub approved: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalApprovalDecisionReceipt {
    pub approval_id: String,
    pub status: GlobalApprovalStatus,
    pub route_back: ApprovalSource,
    pub message: String,
}

#[derive(Debug)]
pub struct ApprovalQueue {
    requests: Mutex<BTreeMap<String, GlobalApprovalRequest>>,
    event_store: Arc<RuntimeEventStore>,
}

impl ApprovalQueue {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let requests = restore_requests(&event_store);
        Self {
            requests: Mutex::new(requests),
            event_store,
        }
    }

    /// Rebuild the in-memory read model after another commit owner appended
    /// approval events as part of a larger transaction.
    pub fn refresh(&self) {
        let restored = restore_requests(&self.event_store);
        *self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = restored;
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
            action: request.action,
            summary: request.summary,
            risk: request.risk,
            evidence_refs: request.evidence_refs,
            timeout_policy: request.timeout_policy,
            status: GlobalApprovalStatus::Pending,
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
        decision: ApprovalDecisionCommand,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let decided_by = principal.claims().principal_id.clone();
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
        };
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
                vec![RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.decided".to_string(),
                    status: Some(next_status.as_str().to_string()),
                    actor: Some(decided_by),
                    refs: approval_source_refs(&request.source),
                    payload: serde_json::json!({
                        "approved": decision.approved,
                        "reason": decision.reason,
                        "message": receipt.message,
                        "resolved_at_ms": resolved_at_ms,
                    }),
                }
                .into()],
            )
            .map_err(|e| e.to_string())?;
        request.status = next_status;
        request.resolved_at_ms = Some(resolved_at_ms);
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
        if request.status != GlobalApprovalStatus::Pending {
            return Ok(GlobalApprovalDecisionReceipt {
                approval_id: request.approval_id.clone(),
                status: request.status,
                route_back: request.source.clone(),
                message: format!("approval already {}", status_label(request.status)),
            });
        }
        let next_status = match request.timeout_policy {
            ApprovalTimeoutPolicy::Pending => GlobalApprovalStatus::Pending,
            ApprovalTimeoutPolicy::AutoDeny | ApprovalTimeoutPolicy::ContinueAlternative => {
                GlobalApprovalStatus::TimedOut
            }
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
            },
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
        })
    }
}

fn status_label(status: GlobalApprovalStatus) -> &'static str {
    match status {
        GlobalApprovalStatus::Pending => "pending",
        GlobalApprovalStatus::Approved => "approved",
        GlobalApprovalStatus::Denied => "denied",
        GlobalApprovalStatus::TimedOut => "timed_out",
    }
}

impl GlobalApprovalStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        status_label(self)
    }
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

fn restore_requests(event_store: &RuntimeEventStore) -> BTreeMap<String, GlobalApprovalRequest> {
    let mut requests = BTreeMap::new();
    let Ok(events) = event_store.list_scope(RuntimeEventScope::Approval, 100_000) else {
        return requests;
    };
    // `list_scope` is newest-first for query consumers; replay in the
    // append-only commit order so a submission is materialized before its
    // decision.
    for event in events.into_iter().rev() {
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
            "approval.decided" | "approval.decided.application" | "approval.timed_out" => {
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
                    _ => request.status,
                };
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
            _ => {}
        }
    }
    requests
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn approval_queue_routes_decisions_back_to_source() {
        let queue = queue();
        let request = queue
            .submit(SubmitGlobalApprovalRequest {
                source: session_source(),
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
                ApprovalDecisionCommand {
                    approval_id: request.approval_id.clone(),
                    approved: true,
                    reason: "looks safe".to_string(),
                },
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
                ApprovalDecisionCommand {
                    approval_id: request.approval_id.clone(),
                    approved: true,
                    reason: "reviewed".to_string(),
                },
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
                    ApprovalDecisionCommand {
                        approval_id: request.approval_id.clone(),
                        approved: true,
                        reason: "generic bypass".to_string(),
                    },
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
                ApprovalDecisionCommand {
                    approval_id: request.approval_id.clone(),
                    approved: true,
                    reason: "approved".to_string(),
                },
            )
            .unwrap();

        let restarted = ApprovalQueue::new(store);
        assert_eq!(
            restarted.get(&request.approval_id).unwrap().status,
            GlobalApprovalStatus::Approved
        );
        assert!(restarted.pending().is_empty());
    }
}
