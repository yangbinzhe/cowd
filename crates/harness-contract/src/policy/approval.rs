//! Unified AI policy decision and receipt contracts.
//!
//! This crate normalizes policy outcomes. It does not replace existing
//! cross-plane or approval engines; adapters should convert those decisions
//! into receipts from this crate.

use super::PolicyDecisionKind;
use crate::agent::{AgentPolicyRequirement, AgentSpec};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyScope {
    Global,
    Session,
    Agent,
    Harness,
    Tool,
    Connector,
    Memory,
    Matrix,
    ExecutionGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReceipt {
    pub id: String,
    pub scope: PolicyScope,
    pub decision: PolicyDecisionKind,
    pub reasons: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub source_policy: String,
    pub created_at: DateTime<Utc>,
}

impl PolicyReceipt {
    #[must_use]
    pub fn new(
        scope: PolicyScope,
        decision: PolicyDecisionKind,
        source_policy: impl Into<String>,
    ) -> Self {
        Self {
            id: format!("policy-receipt-{}", uuid::Uuid::new_v4()),
            scope,
            decision,
            reasons: Vec::new(),
            evidence_refs: Vec::new(),
            source_policy: source_policy.into(),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reasons.push(reason.into());
        self
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, reference: impl Into<String>) -> Self {
        self.evidence_refs.push(reference.into());
        self
    }
}

#[must_use]
pub fn governed_tool_policy_receipts(
    plan_ids: &[String],
    requires_checkpoint: bool,
    requires_human_confirm: bool,
) -> Vec<PolicyReceipt> {
    let mut receipts = Vec::new();
    let decision = if requires_human_confirm {
        PolicyDecisionKind::Ask
    } else {
        PolicyDecisionKind::Allow
    };
    let mut receipt = PolicyReceipt::new(
        PolicyScope::Tool,
        decision,
        "governed_tool_execution_policy",
    );
    for plan_id in plan_ids {
        receipt = receipt.with_evidence_ref(format!("governed_tool_plan:{plan_id}"));
    }
    if requires_human_confirm {
        receipt = receipt.with_reason("critical tool path requires human confirmation");
    } else if requires_checkpoint {
        receipt = receipt.with_reason("write path requires checkpoint receipt");
    } else {
        receipt = receipt.with_reason("governed tool execution is allowed by current policy");
    }
    receipts.push(receipt);
    receipts
}

#[must_use]
pub fn agent_spec_policy_receipts(agent_spec: &AgentSpec) -> Vec<PolicyReceipt> {
    let mut receipts = Vec::new();
    for requirement in &agent_spec.policies {
        let (scope, decision, reason) = match requirement {
            AgentPolicyRequirement::RequiresApproval => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires approval",
            ),
            AgentPolicyRequirement::RequiresMatrixEvidence => (
                PolicyScope::Matrix,
                PolicyDecisionKind::Allow,
                "agent contract requires matrix evidence",
            ),
            AgentPolicyRequirement::RequiresVerification => (
                PolicyScope::Harness,
                PolicyDecisionKind::Allow,
                "agent contract requires verification",
            ),
            AgentPolicyRequirement::RequiresWorktreeIsolation => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires worktree isolation",
            ),
            AgentPolicyRequirement::RequiresHumanReview => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires human review",
            ),
        };
        receipts.push(
            PolicyReceipt::new(scope, decision, "agent_spec_policy")
                .with_reason(reason)
                .with_evidence_ref(format!("agent_spec:{}", agent_spec.id)),
        );
    }
    receipts
}

#[must_use]
pub fn behavior_policy_receipt(
    allow_execution: bool,
    requires_scope_downgrade: bool,
    requires_human_review: bool,
    risks: &[String],
) -> PolicyReceipt {
    let decision = if !allow_execution {
        PolicyDecisionKind::Deny
    } else if requires_human_review || requires_scope_downgrade {
        PolicyDecisionKind::Ask
    } else {
        PolicyDecisionKind::Allow
    };
    let mut receipt = PolicyReceipt::new(PolicyScope::Global, decision, "behavior_policy");
    if risks.is_empty() {
        receipt = receipt.with_reason("behavior policy permits current execution scope");
    } else {
        for risk in risks {
            receipt = receipt.with_reason(risk.clone());
        }
    }
    if requires_scope_downgrade {
        receipt = receipt.with_reason("execution scope should be downgraded before expansion");
    }
    if requires_human_review {
        receipt = receipt.with_reason("behavior policy requires human review");
    }
    receipt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_governed_tool_plan_maps_to_ask() {
        let receipts = governed_tool_policy_receipts(&["plan-1".to_string()], true, true);
        assert_eq!(receipts[0].decision, PolicyDecisionKind::Ask);
        assert!(receipts[0].evidence_refs[0].contains("plan-1"));
    }

    #[test]
    fn agent_spec_policy_maps_review_to_ask() {
        let receipts = agent_spec_policy_receipts(&AgentSpec::reviewer());
        assert!(receipts
            .iter()
            .any(|receipt| receipt.decision == PolicyDecisionKind::Ask));
    }
}
