//! Controlled growth signals for the Cowd AI harness.
//!
//! This crate does not mutate strategy policy directly. It converts execution
//! traces into structured learning records that later policy layers may inspect.

use ai_core::{ExecutionMode, TaskComplexity, TaskRisk};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthSignalKind {
    StrategyFit,
    ContextPressure,
    ToolRisk,
    VerificationGap,
    EvaluationRegression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthSeverity {
    Info,
    Watch,
    Improve,
    Blocker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthSignal {
    pub kind: GrowthSignalKind,
    pub severity: GrowthSeverity,
    pub summary: String,
}

impl GrowthSignal {
    #[must_use]
    pub fn new(
        kind: GrowthSignalKind,
        severity: GrowthSeverity,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            severity,
            summary: summary.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthInput {
    pub selected_mode: ExecutionMode,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub context_omitted: usize,
    pub tool_requires_checkpoint: bool,
    pub tool_requires_human_confirm: bool,
    pub verification_can_finalize: bool,
    pub bench_passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearningRecord {
    pub id: String,
    pub policy: String,
    pub signals: Vec<GrowthSignal>,
    pub next_strategy_hints: Vec<String>,
}

impl LearningRecord {
    #[must_use]
    pub fn from_input(input: GrowthInput) -> Self {
        let mut signals = Vec::new();
        let mut hints = Vec::new();

        if matches!(
            input.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        ) && matches!(
            input.selected_mode,
            ExecutionMode::DirectAnswer | ExecutionMode::FastEdit
        ) {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::StrategyFit,
                GrowthSeverity::Improve,
                "complex task used an underspecified execution mode",
            ));
            hints.push(
                "prefer plan_execute or supervisor_subagents for comparable tasks".to_string(),
            );
        }

        if input.context_omitted > 0 {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::ContextPressure,
                GrowthSeverity::Watch,
                format!("{} context items were omitted", input.context_omitted),
            ));
            hints.push(
                "inspect omitted context before treating answer drift as model error".to_string(),
            );
        }

        if input.tool_requires_human_confirm {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::ToolRisk,
                GrowthSeverity::Blocker,
                "critical tool path requires human confirmation",
            ));
            hints.push("do not auto-advance critical tool operations".to_string());
        } else if input.tool_requires_checkpoint {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::ToolRisk,
                GrowthSeverity::Improve,
                "write path requires checkpoint discipline",
            ));
            hints.push("prefer checkpointed write batches for comparable changes".to_string());
        }

        if !input.verification_can_finalize {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::VerificationGap,
                GrowthSeverity::Blocker,
                "verification ledger blocked finalization",
            ));
            hints.push("collect supporting evidence before final synthesis".to_string());
        }

        if !input.bench_passed {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::EvaluationRegression,
                GrowthSeverity::Improve,
                "trajectory missed required benchmark checks",
            ));
            hints
                .push("review trajectory requirements before completing similar tasks".to_string());
        }

        if signals.is_empty() {
            signals.push(GrowthSignal::new(
                GrowthSignalKind::StrategyFit,
                GrowthSeverity::Info,
                "execution trace matched current policy expectations",
            ));
        }

        Self {
            id: format!("learning-record-{}", uuid::Uuid::new_v4()),
            policy: "growth-policy-v1".to_string(),
            signals,
            next_strategy_hints: hints,
        }
    }

    #[must_use]
    pub fn has_blocker(&self) -> bool {
        self.signals
            .iter()
            .any(|signal| signal.severity == GrowthSeverity::Blocker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_verification_creates_blocker_signal() {
        let record = LearningRecord::from_input(GrowthInput {
            selected_mode: ExecutionMode::PlanExecute,
            complexity: TaskComplexity::Complex,
            risk: TaskRisk::Medium,
            context_omitted: 0,
            tool_requires_checkpoint: false,
            tool_requires_human_confirm: false,
            verification_can_finalize: false,
            bench_passed: true,
        });

        assert!(record.has_blocker());
        assert!(record
            .next_strategy_hints
            .iter()
            .any(|hint| hint.contains("supporting evidence")));
    }

    #[test]
    fn clean_trace_records_positive_policy_signal() {
        let record = LearningRecord::from_input(GrowthInput {
            selected_mode: ExecutionMode::DirectAnswer,
            complexity: TaskComplexity::Simple,
            risk: TaskRisk::Low,
            context_omitted: 0,
            tool_requires_checkpoint: false,
            tool_requires_human_confirm: false,
            verification_can_finalize: true,
            bench_passed: true,
        });

        assert!(!record.has_blocker());
        assert_eq!(record.signals.len(), 1);
    }
}
