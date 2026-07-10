//! Steward Agent policy-bound delegated decision loop.
//!
//! The steward never bypasses autonomy profiles or the global approval queue.
//! It evaluates an intended action, either delegates it, denies it, or submits
//! an approval request for human/global resolution.

use harness_contract::core::TaskRisk;
use serde::{Deserialize, Serialize};

use crate::{
    global_approval_queue, ApprovalSource, ApprovalTimeoutPolicy, AutonomyDecisionInput,
    AutonomyDecisionKind, AutonomyProfileCatalog, AutonomyProfileId, CollaborationTemplateId,
    GlobalApprovalRequest, SubmitGlobalApprovalRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardActionRequest {
    pub steward_id: String,
    pub profile_id: AutonomyProfileId,
    pub source: ApprovalSource,
    pub action: String,
    pub summary: String,
    pub risk: TaskRisk,
    pub requested_tool: Option<String>,
    pub template_id: Option<CollaborationTemplateId>,
    pub requires_write: bool,
    pub is_critical_operation: bool,
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StewardActionStatus {
    Delegated,
    ApprovalSubmitted,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StewardDecisionRecord {
    pub steward_id: String,
    pub status: StewardActionStatus,
    pub action: String,
    pub reason: String,
    pub risk: TaskRisk,
    pub policy_basis: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub approval_id: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StewardAgent;

impl StewardAgent {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn evaluate_action(
        &self,
        request: StewardActionRequest,
    ) -> Result<StewardDecisionRecord, String> {
        if request.steward_id.trim().is_empty() {
            return Err("steward_id must not be empty".to_string());
        }
        if request.action.trim().is_empty() {
            return Err("action must not be empty".to_string());
        }
        let autonomy = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: request.profile_id,
            requested_risk: request.risk,
            requested_tool: request.requested_tool.clone(),
            template_id: request.template_id,
            requires_write: request.requires_write,
            is_critical_operation: request.is_critical_operation,
        });
        match autonomy.decision {
            AutonomyDecisionKind::Allow => Ok(StewardDecisionRecord {
                steward_id: request.steward_id,
                status: StewardActionStatus::Delegated,
                action: request.action,
                reason: autonomy.reason,
                risk: request.risk,
                policy_basis: autonomy.policy_basis,
                evidence_refs: request.evidence_refs,
                approval_id: None,
                created_at_ms: now_ms(),
            }),
            AutonomyDecisionKind::Deny => Ok(StewardDecisionRecord {
                steward_id: request.steward_id,
                status: StewardActionStatus::Denied,
                action: request.action,
                reason: autonomy.reason,
                risk: request.risk,
                policy_basis: autonomy.policy_basis,
                evidence_refs: request.evidence_refs,
                approval_id: None,
                created_at_ms: now_ms(),
            }),
            AutonomyDecisionKind::RequireApproval | AutonomyDecisionKind::EscalateToHuman => {
                let approval = global_approval_queue().submit(SubmitGlobalApprovalRequest {
                    source: request.source.clone(),
                    action: request.action.clone(),
                    summary: request.summary.clone(),
                    risk: request.risk,
                    evidence_refs: request.evidence_refs.clone(),
                    timeout_policy: request.timeout_policy,
                })?;
                Ok(steward_record_from_approval(
                    request,
                    autonomy.reason,
                    autonomy.policy_basis,
                    approval,
                ))
            }
        }
    }
}

fn steward_record_from_approval(
    request: StewardActionRequest,
    reason: String,
    policy_basis: Vec<String>,
    approval: GlobalApprovalRequest,
) -> StewardDecisionRecord {
    StewardDecisionRecord {
        steward_id: request.steward_id,
        status: StewardActionStatus::ApprovalSubmitted,
        action: request.action,
        reason,
        risk: request.risk,
        policy_basis,
        evidence_refs: request.evidence_refs,
        approval_id: Some(approval.approval_id),
        created_at_ms: now_ms(),
    }
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
    use crate::ApprovalSourceKind;

    fn source() -> ApprovalSource {
        ApprovalSource {
            kind: ApprovalSourceKind::Steward,
            session_id: Some("session-steward".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: Some("mission-runtime".to_string()),
        }
    }

    #[test]
    fn steward_delegates_low_risk_actions_allowed_by_profile() {
        let record = StewardAgent::new()
            .evaluate_action(StewardActionRequest {
                steward_id: "steward-1".to_string(),
                profile_id: AutonomyProfileId::Stewarded,
                source: source(),
                action: "read evidence".to_string(),
                summary: "read local evidence".to_string(),
                risk: TaskRisk::Low,
                requested_tool: Some("read_file".to_string()),
                template_id: Some(CollaborationTemplateId::ExecuteReview),
                requires_write: false,
                is_critical_operation: false,
                evidence_refs: vec!["trace:low".to_string()],
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("steward action");

        assert_eq!(record.status, StewardActionStatus::Delegated);
        assert_eq!(record.approval_id, None);
    }

    #[test]
    fn steward_submits_global_approval_for_risky_actions() {
        let record = StewardAgent::new()
            .evaluate_action(StewardActionRequest {
                steward_id: "steward-2".to_string(),
                profile_id: AutonomyProfileId::Stewarded,
                source: source(),
                action: "apply patch".to_string(),
                summary: "write runtime changes".to_string(),
                risk: TaskRisk::High,
                requested_tool: Some("apply_patch".to_string()),
                template_id: Some(CollaborationTemplateId::ImplementationReviewFix),
                requires_write: true,
                is_critical_operation: false,
                evidence_refs: vec!["trace:risky".to_string()],
                timeout_policy: ApprovalTimeoutPolicy::ContinueAlternative,
            })
            .expect("steward approval action");

        assert_eq!(record.status, StewardActionStatus::ApprovalSubmitted);
        assert!(record.approval_id.is_some());
    }
}
