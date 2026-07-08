use serde::{Deserialize, Serialize};

use harness_contract::core::ExecutionMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAdaptiveDecisionKind {
    Continue,
    NudgeModel,
    ReplanExecution,
    FallbackAnswerWithCheckedEvidence,
    CompactContext,
    RequestRecovery,
    EmitEvolutionSignal,
}

impl RuntimeAdaptiveDecisionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::NudgeModel => "nudge_model",
            Self::ReplanExecution => "replan_execution",
            Self::FallbackAnswerWithCheckedEvidence => "fallback_answer_with_checked_evidence",
            Self::CompactContext => "compact_context",
            Self::RequestRecovery => "request_recovery",
            Self::EmitEvolutionSignal => "emit_evolution_signal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCorrectiveAction {
    pub name: String,
    pub mode: Option<ExecutionMode>,
    pub reason_code: String,
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAdaptiveDecision {
    pub kind: RuntimeAdaptiveDecisionKind,
    pub reason: Option<String>,
    pub corrective_action: Option<RuntimeCorrectiveAction>,
}

impl RuntimeAdaptiveDecision {
    #[must_use]
    pub fn continue_now() -> Self {
        Self {
            kind: RuntimeAdaptiveDecisionKind::Continue,
            reason: None,
            corrective_action: None,
        }
    }

    #[must_use]
    pub fn with_action(
        kind: RuntimeAdaptiveDecisionKind,
        reason: impl Into<String>,
        action_name: impl Into<String>,
        reason_code: impl Into<String>,
        mode: Option<ExecutionMode>,
        prompt: Option<String>,
    ) -> Self {
        Self {
            kind,
            reason: Some(reason.into()),
            corrective_action: Some(RuntimeCorrectiveAction {
                name: action_name.into(),
                mode,
                reason_code: reason_code.into(),
                prompt,
            }),
        }
    }

    #[must_use]
    pub fn should_inject(&self) -> bool {
        !matches!(self.kind, RuntimeAdaptiveDecisionKind::Continue)
            && self
                .corrective_action
                .as_ref()
                .and_then(|action| action.prompt.as_deref())
                .is_some()
    }

    #[must_use]
    pub fn prompt(&self) -> Option<&str> {
        self.corrective_action
            .as_ref()
            .and_then(|action| action.prompt.as_deref())
    }

    #[must_use]
    pub fn kind_str(&self) -> &'static str {
        self.kind.as_str()
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    #[must_use]
    pub fn recommended_action(&self) -> Option<&str> {
        self.corrective_action
            .as_ref()
            .map(|action| action.name.as_str())
    }

    #[must_use]
    pub fn recommended_mode(&self) -> Option<ExecutionMode> {
        self.corrective_action
            .as_ref()
            .and_then(|action| action.mode)
    }

    #[must_use]
    pub fn reason_code(&self) -> Option<&str> {
        self.corrective_action
            .as_ref()
            .map(|action| action.reason_code.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSelfRegulationPolicy {
    pub context_pressure_soft_percent: u8,
    pub context_pressure_critical_percent: u8,
    pub repeated_decision_fallback_after: usize,
    pub evolution_signal_after_same_reason: usize,
}

impl Default for RuntimeSelfRegulationPolicy {
    fn default() -> Self {
        Self {
            context_pressure_soft_percent: 70,
            context_pressure_critical_percent: 85,
            repeated_decision_fallback_after: 2,
            evolution_signal_after_same_reason: 2,
        }
    }
}
