//! Runtime self-regulation loop.
//!
//! This module owns hot-path adaptive control for a turn.  The model remains
//! the decision maker, while the runtime observes stalled progress, context
//! pressure, and repeated failures, then injects concise guidance and durable
//! evidence instead of silently spinning.

pub mod context_pressure;
pub mod controller;
pub mod decision;
pub mod event;
pub mod observation;
pub mod progress;
pub mod recovery_bridge;
pub mod tool_progress;

pub use context_pressure::{ContextPressureObservation, ContextPressurePolicy};
pub use controller::AdaptiveController;
pub use decision::{
    RuntimeAdaptiveDecision, RuntimeAdaptiveDecisionKind, RuntimeCorrectiveAction,
    RuntimeSelfRegulationPolicy,
};
pub use event::{RuntimeSelfRegulationEvent, RuntimeSelfRegulationEventInput};
pub use observation::{RuntimeObservationKind, RuntimeStepObservation};
pub use progress::RuntimeProgressLedger;
pub use recovery_bridge::{RecoveryBridgeAction, RecoveryBridgePlan};
pub use tool_progress::{ToolProgressAdapter, ToolProgressObservation};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_tool_progress_triggers_self_regulation_decision() {
        let mut controller = AdaptiveController::new();
        let mut last = RuntimeAdaptiveDecision::continue_now();
        for _ in 0..4 {
            let (_, decision) = controller.observe_tool_result(
                "read_file",
                r#"{"path":"README.md","offset":0,"limit":80}"#,
                "same README evidence",
                false,
            );
            last = decision;
        }

        assert_eq!(last.kind, RuntimeAdaptiveDecisionKind::NudgeModel);
        assert!(last
            .recommended_action()
            .is_some_and(|action| action.contains("request_reflexion_retry")));
        assert!(controller
            .progress_ledger()
            .compact_summary()
            .contains("tool=4"));
    }

    #[test]
    fn context_pressure_uses_configurable_policy() {
        let mut controller = AdaptiveController::with_policy(RuntimeSelfRegulationPolicy {
            context_pressure_soft_percent: 60,
            context_pressure_critical_percent: 80,
            repeated_decision_fallback_after: 2,
            evolution_signal_after_same_reason: 2,
        });

        let decision = controller.observe_context_pressure(820, 1000);

        assert_eq!(decision.kind, RuntimeAdaptiveDecisionKind::CompactContext);
        assert_eq!(decision.reason_code(), Some("context_pressure_critical"));
    }

    #[test]
    fn event_payload_uses_self_regulation_source() {
        let mut controller = AdaptiveController::new();
        let (observation, decision) = controller.observe_tool_result(
            "read_file",
            r#"{"path":"README.md","offset":0,"limit":80}"#,
            "same README evidence",
            false,
        );
        let event = RuntimeSelfRegulationEvent::from_tool_decision(&observation, &decision);

        assert_eq!(event.event_type, "runtime.self_regulation.decision");
        assert_eq!(event.source, "runtime.self_regulation");
        assert_eq!(event.payload["source"], "runtime.self_regulation");
    }
}
