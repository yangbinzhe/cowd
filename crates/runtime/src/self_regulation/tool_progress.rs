use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::turn_supervisor::{SupervisorDecision, ToolCallLedger, TurnSupervisor};

use super::{
    RuntimeAdaptiveDecision, RuntimeAdaptiveDecisionKind, RuntimeObservationKind,
    RuntimeStepObservation,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolProgressObservation {
    pub legacy: crate::turn_supervisor::ToolProgressObservation,
}

#[derive(Debug, Default)]
pub struct ToolProgressAdapter {
    supervisor: TurnSupervisor,
}

impl ToolProgressAdapter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            supervisor: TurnSupervisor::new(),
        }
    }

    pub fn observe_tool_result(
        &mut self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> (ToolProgressObservation, RuntimeAdaptiveDecision) {
        let (legacy, decision) = self
            .supervisor
            .observe_tool_result(tool_name, input, output, is_error);
        (
            ToolProgressObservation { legacy },
            runtime_decision_from_supervisor(decision),
        )
    }

    #[must_use]
    pub fn ledger(&self) -> &ToolCallLedger {
        self.supervisor.ledger()
    }

    #[must_use]
    pub fn partial_answer_text(&self, reason: &str) -> String {
        self.supervisor.partial_answer_text(reason)
    }
}

impl ToolProgressObservation {
    #[must_use]
    pub fn fingerprint(&self) -> &crate::turn_supervisor::ToolCallFingerprint {
        &self.legacy.fingerprint
    }

    #[must_use]
    pub fn is_error(&self) -> bool {
        self.legacy.is_error
    }

    #[must_use]
    pub fn to_runtime_observation(
        &self,
        decision: &RuntimeAdaptiveDecision,
    ) -> RuntimeStepObservation {
        RuntimeStepObservation::new(
            RuntimeObservationKind::ToolProgress,
            "runtime.self_regulation.tool_progress",
            format!(
                "{}: {}",
                decision.kind_str(),
                decision.reason().unwrap_or("tool progress observed")
            ),
        )
        .with_payload(json!({
            "tool": {
                "name": self.legacy.fingerprint.tool_name,
                "target": self.legacy.fingerprint.target,
                "range": self.legacy.fingerprint.range,
                "input_hash": self.legacy.fingerprint.input_hash,
                "output_hash": self.legacy.fingerprint.output_hash,
                "is_error": self.legacy.is_error,
            },
            "decision": decision.kind_str(),
            "reason": decision.reason(),
            "recommended_action": decision.recommended_action(),
            "recommended_pattern": decision.recommended_pattern().map(|mode| format!("{mode:?}")),
        }))
    }
}

fn runtime_decision_from_supervisor(decision: SupervisorDecision) -> RuntimeAdaptiveDecision {
    match decision {
        SupervisorDecision::Continue => RuntimeAdaptiveDecision::continue_now(),
        SupervisorDecision::Nudge {
            reason,
            prompt,
            reason_code,
            recommended_pattern,
            recommended_action,
        } => RuntimeAdaptiveDecision::with_action(
            RuntimeAdaptiveDecisionKind::NudgeModel,
            reason,
            recommended_action,
            reason_code,
            Some(recommended_pattern),
            Some(prompt),
        ),
        SupervisorDecision::Replan {
            reason,
            prompt,
            reason_code,
            recommended_pattern,
            recommended_action,
        } => RuntimeAdaptiveDecision::with_action(
            RuntimeAdaptiveDecisionKind::ReplanExecution,
            reason,
            recommended_action,
            reason_code,
            Some(recommended_pattern),
            Some(prompt),
        ),
        SupervisorDecision::FallbackAnswer {
            reason,
            prompt,
            reason_code,
            recommended_pattern,
            recommended_action,
        } => RuntimeAdaptiveDecision::with_action(
            RuntimeAdaptiveDecisionKind::FallbackAnswerWithCheckedEvidence,
            reason,
            recommended_action,
            reason_code,
            Some(recommended_pattern),
            Some(prompt),
        ),
    }
}
