use serde_json::json;

use super::{
    ContextPressurePolicy, RuntimeAdaptiveDecision, RuntimeObservationKind, RuntimeProgressLedger,
    RuntimeSelfRegulationPolicy, RuntimeStepObservation, ToolProgressAdapter,
    ToolProgressObservation,
};

#[derive(Debug)]
pub struct AdaptiveController {
    tool_progress: ToolProgressAdapter,
    context_pressure: ContextPressurePolicy,
    ledger: RuntimeProgressLedger,
}

impl AdaptiveController {
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(RuntimeSelfRegulationPolicy::default())
    }

    #[must_use]
    pub fn with_policy(policy: RuntimeSelfRegulationPolicy) -> Self {
        Self {
            tool_progress: ToolProgressAdapter::new(),
            context_pressure: ContextPressurePolicy::new(policy),
            ledger: RuntimeProgressLedger::default(),
        }
    }

    pub fn observe_tool_result(
        &mut self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> (ToolProgressObservation, RuntimeAdaptiveDecision) {
        let (observation, decision) = self
            .tool_progress
            .observe_tool_result(tool_name, input, output, is_error);
        self.ledger
            .push_observation(observation.to_runtime_observation(&decision));
        self.ledger.push_decision(decision.clone());
        (observation, decision)
    }

    pub fn observe_context_pressure(
        &mut self,
        used_tokens: usize,
        context_window: usize,
    ) -> RuntimeAdaptiveDecision {
        let decision = self.context_pressure.observe(used_tokens, context_window);
        self.ledger.push_observation(
            RuntimeStepObservation::new(
                RuntimeObservationKind::ContextPressure,
                "runtime.self_regulation.context_pressure",
                format!("used_tokens={used_tokens} context_window={context_window}"),
            )
            .with_payload(json!({
                "used_tokens": used_tokens,
                "context_window": context_window,
                "decision": decision.kind_str(),
            })),
        );
        self.ledger.push_decision(decision.clone());
        decision
    }

    #[must_use]
    pub fn tool_ledger(&self) -> &crate::turn_supervisor::ToolCallLedger {
        self.tool_progress.ledger()
    }

    #[must_use]
    pub fn progress_ledger(&self) -> &RuntimeProgressLedger {
        &self.ledger
    }

    #[must_use]
    pub fn partial_answer_text(&self, reason: &str) -> String {
        self.tool_progress.partial_answer_text(reason)
    }
}

impl Default for AdaptiveController {
    fn default() -> Self {
        Self::new()
    }
}
