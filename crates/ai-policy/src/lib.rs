//! Unified AI policy decision and receipt contracts.
//!
//! This crate normalizes policy outcomes. It does not replace existing
//! cross-plane or approval engines; adapters should convert those decisions
//! into receipts from this crate.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionKind {
    Allow,
    Deny,
    Ask,
}

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
    WorkGraph,
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
pub fn tool_transaction_policy_receipts(
    transaction_id: Option<&str>,
    requires_checkpoint: bool,
    requires_human_confirm: bool,
) -> Vec<PolicyReceipt> {
    let mut receipts = Vec::new();
    let decision = if requires_human_confirm {
        PolicyDecisionKind::Ask
    } else {
        PolicyDecisionKind::Allow
    };
    let mut receipt = PolicyReceipt::new(PolicyScope::Tool, decision, "tool_transaction_policy");
    if let Some(transaction_id) = transaction_id {
        receipt = receipt.with_evidence_ref(format!("tool_transaction:{transaction_id}"));
    }
    if requires_human_confirm {
        receipt = receipt.with_reason("critical tool path requires human confirmation");
    } else if requires_checkpoint {
        receipt = receipt.with_reason("write path requires checkpoint receipt");
    } else {
        receipt = receipt.with_reason("tool transaction is allowed by current policy");
    }
    receipts.push(receipt);
    receipts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_tool_transaction_maps_to_ask() {
        let receipts = tool_transaction_policy_receipts(Some("tx-1"), true, true);
        assert_eq!(receipts[0].decision, PolicyDecisionKind::Ask);
        assert!(receipts[0].evidence_refs[0].contains("tx-1"));
    }
}
