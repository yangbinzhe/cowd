//! Runtime-owned global approval queue.
//!
//! This queue is the common routing point for approvals raised by sessions,
//! agents, teams, and future steward agents. It records pending requests,
//! decisions, timeout policy, and the source that should receive the result.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use ai_kernel::core::TaskRisk;
use serde::{Deserialize, Serialize};

use crate::{record_runtime_event, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSourceKind {
    Session,
    Agent,
    Team,
    Mission,
    Steward,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSource {
    pub kind: ApprovalSourceKind,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub team_id: Option<String>,
    pub mission_id: Option<String>,
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
pub struct GlobalApprovalDecision {
    pub approval_id: String,
    pub approved: bool,
    pub decided_by: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalApprovalDecisionReceipt {
    pub approval_id: String,
    pub status: GlobalApprovalStatus,
    pub route_back: ApprovalSource,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct GlobalApprovalQueue {
    requests: Mutex<BTreeMap<String, GlobalApprovalRequest>>,
}

impl GlobalApprovalQueue {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(
        &self,
        request: SubmitGlobalApprovalRequest,
    ) -> Result<GlobalApprovalRequest, String> {
        if request.action.trim().is_empty() {
            return Err("approval action must not be empty".to_string());
        }
        if request.summary.trim().is_empty() {
            return Err("approval summary must not be empty".to_string());
        }
        let approval_id = format!("approval-{}", uuid::Uuid::new_v4());
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
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(approval_id, approval.clone());
        let _ = record_runtime_event(RuntimeEventInput {
            stream_id: format!("approval:{}", approval.approval_id),
            scope: RuntimeEventScope::Approval,
            kind: "approval.submitted".to_string(),
            status: Some(approval.status.as_str().to_string()),
            actor: Some("global_approval_queue".to_string()),
            refs: approval_source_refs(&approval.source),
            payload: serde_json::json!({
                "action": approval.action,
                "summary": approval.summary,
                "risk": approval.risk,
                "timeout_policy": approval.timeout_policy,
            }),
        });
        Ok(approval)
    }

    pub fn decide(
        &self,
        decision: GlobalApprovalDecision,
    ) -> Result<GlobalApprovalDecisionReceipt, String> {
        if decision.decided_by.trim().is_empty() {
            return Err("decided_by must not be empty".to_string());
        }
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = requests
            .get_mut(&decision.approval_id)
            .ok_or_else(|| format!("approval request not found: {}", decision.approval_id))?;
        if request.status != GlobalApprovalStatus::Pending {
            return Ok(GlobalApprovalDecisionReceipt {
                approval_id: request.approval_id.clone(),
                status: request.status,
                route_back: request.source.clone(),
                message: format!("approval already {}", status_label(request.status)),
            });
        }
        request.status = if decision.approved {
            GlobalApprovalStatus::Approved
        } else {
            GlobalApprovalStatus::Denied
        };
        request.resolved_at_ms = Some(now_ms());
        let receipt = GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status: request.status,
            route_back: request.source.clone(),
            message: if decision.approved {
                format!("approved by {}", decision.decided_by)
            } else {
                format!("denied by {}: {}", decision.decided_by, decision.reason)
            },
        };
        let _ = record_runtime_event(RuntimeEventInput {
            stream_id: format!("approval:{}", request.approval_id),
            scope: RuntimeEventScope::Approval,
            kind: "approval.decided".to_string(),
            status: Some(request.status.as_str().to_string()),
            actor: Some(decision.decided_by),
            refs: approval_source_refs(&request.source),
            payload: serde_json::json!({
                "approved": decision.approved,
                "reason": decision.reason,
                "message": receipt.message,
            }),
        });
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
        request.status = match request.timeout_policy {
            ApprovalTimeoutPolicy::Pending => GlobalApprovalStatus::Pending,
            ApprovalTimeoutPolicy::AutoDeny | ApprovalTimeoutPolicy::ContinueAlternative => {
                GlobalApprovalStatus::TimedOut
            }
        };
        if request.status == GlobalApprovalStatus::TimedOut {
            request.resolved_at_ms = Some(now_ms());
        }
        let receipt = GlobalApprovalDecisionReceipt {
            approval_id: request.approval_id.clone(),
            status: request.status,
            route_back: request.source.clone(),
            message: match request.timeout_policy {
                ApprovalTimeoutPolicy::Pending => "approval remains pending".to_string(),
                ApprovalTimeoutPolicy::AutoDeny => "approval timed out and must deny".to_string(),
                ApprovalTimeoutPolicy::ContinueAlternative => {
                    "approval timed out; source should continue alternative path".to_string()
                }
            },
        };
        let _ = record_runtime_event(RuntimeEventInput {
            stream_id: format!("approval:{}", request.approval_id),
            scope: RuntimeEventScope::Approval,
            kind: "approval.timed_out".to_string(),
            status: Some(request.status.as_str().to_string()),
            actor: Some("global_approval_queue".to_string()),
            refs: approval_source_refs(&request.source),
            payload: serde_json::json!({
                "timeout_policy": request.timeout_policy,
                "message": receipt.message,
            }),
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

pub fn global_approval_queue() -> &'static GlobalApprovalQueue {
    static QUEUE: OnceLock<GlobalApprovalQueue> = OnceLock::new();
    QUEUE.get_or_init(GlobalApprovalQueue::new)
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
    refs
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_source() -> ApprovalSource {
        ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("session-approval".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
        }
    }

    #[test]
    fn global_approval_queue_routes_decisions_back_to_source() {
        let queue = GlobalApprovalQueue::new();
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
        let receipt = queue
            .decide(GlobalApprovalDecision {
                approval_id: request.approval_id.clone(),
                approved: true,
                decided_by: "human".to_string(),
                reason: "looks safe".to_string(),
            })
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
    fn timeout_policy_can_hold_or_release_pending_work() {
        let queue = GlobalApprovalQueue::new();
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
}
