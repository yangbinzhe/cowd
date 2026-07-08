use serde::{Deserialize, Serialize};

use super::{RuntimeAdaptiveDecision, RuntimeAdaptiveDecisionKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryBridgeAction {
    pub reason_code: String,
    pub action: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryBridgePlan {
    pub actions: Vec<RecoveryBridgeAction>,
}

impl RecoveryBridgePlan {
    #[must_use]
    pub fn from_decision(decision: &RuntimeAdaptiveDecision) -> Self {
        if !matches!(
            decision.kind,
            RuntimeAdaptiveDecisionKind::RequestRecovery
                | RuntimeAdaptiveDecisionKind::CompactContext
        ) {
            return Self::default();
        }
        let action = decision
            .recommended_action()
            .unwrap_or("runtime_recovery")
            .to_string();
        let reason_code = decision
            .reason_code()
            .unwrap_or("runtime_self_regulation")
            .to_string();
        Self {
            actions: vec![RecoveryBridgeAction {
                reason_code,
                action,
            }],
        }
    }
}
