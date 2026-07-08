use serde::{Deserialize, Serialize};

use super::{RuntimeAdaptiveDecision, RuntimeAdaptiveDecisionKind, RuntimeSelfRegulationPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPressureObservation {
    pub used_tokens: usize,
    pub context_window: usize,
    pub percent: u8,
}

#[derive(Debug, Clone, Default)]
pub struct ContextPressurePolicy {
    policy: RuntimeSelfRegulationPolicy,
}

impl ContextPressurePolicy {
    #[must_use]
    pub fn new(policy: RuntimeSelfRegulationPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn observe(&self, used_tokens: usize, context_window: usize) -> RuntimeAdaptiveDecision {
        if context_window == 0 {
            return RuntimeAdaptiveDecision::continue_now();
        }
        let percent = used_tokens.saturating_mul(100) / context_window;
        if percent >= usize::from(self.policy.context_pressure_critical_percent) {
            return RuntimeAdaptiveDecision::with_action(
                RuntimeAdaptiveDecisionKind::CompactContext,
                format!(
                    "context pressure critical used_tokens={used_tokens} context_window={context_window}"
                ),
                "compact_runtime_context",
                "context_pressure_critical",
                None,
                Some(
                    "Runtime self-regulation: context pressure is critical. Compact the active runtime memory before adding more evidence, preserve decisions and unresolved risks, then continue from the compacted receipt.".to_string(),
                ),
            );
        }
        if percent >= usize::from(self.policy.context_pressure_soft_percent) {
            return RuntimeAdaptiveDecision::with_action(
                RuntimeAdaptiveDecisionKind::NudgeModel,
                format!(
                    "context pressure elevated used_tokens={used_tokens} context_window={context_window}"
                ),
                "prefer_summarized_evidence",
                "context_pressure_soft",
                None,
                Some(
                    "Runtime self-regulation: context pressure is elevated. Prefer summarized evidence, batch reads, and explicit fact receipts instead of expanding raw context.".to_string(),
                ),
            );
        }
        RuntimeAdaptiveDecision::continue_now()
    }
}
