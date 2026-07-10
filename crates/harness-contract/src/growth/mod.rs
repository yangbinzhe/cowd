//! Controlled growth signals for the Cowd AI harness.
//!
//! This crate does not mutate strategy policy directly. It converts execution
//! traces into structured learning records that later policy layers may inspect.

use crate::core::{ExecutionPattern, TaskComplexity, TaskRisk};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthSignalKind {
    StrategyFit,
    ContextPressure,
    ToolRisk,
    VerificationGap,
    EvaluationRegression,
    MatrixQualityGate,
    MultiAgentValue,
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

    #[must_use]
    pub fn from_matrix_quality_gate(allowed: bool, score_bp: u16, reasons: &[String]) -> Self {
        let severity = if allowed {
            GrowthSeverity::Info
        } else if score_bp < 5000 {
            GrowthSeverity::Blocker
        } else {
            GrowthSeverity::Improve
        };
        let detail = if reasons.is_empty() {
            "quality gate accepted evidence packet".to_string()
        } else {
            reasons.join("; ")
        };
        Self::new(
            GrowthSignalKind::MatrixQualityGate,
            severity,
            format!("matrix quality gate score={score_bp} allowed={allowed}: {detail}"),
        )
    }

    #[must_use]
    pub fn from_multi_agent_value(
        positive_lift: bool,
        continue_multi_agent: bool,
        value_score: u16,
        reasons: &[String],
    ) -> Self {
        let severity = if positive_lift {
            GrowthSeverity::Info
        } else if continue_multi_agent {
            GrowthSeverity::Watch
        } else {
            GrowthSeverity::Improve
        };
        let detail = if reasons.is_empty() {
            "multi-agent value assessed".to_string()
        } else {
            reasons.join("; ")
        };
        Self::new(
            GrowthSignalKind::MultiAgentValue,
            severity,
            format!(
                "multi-agent value score={value_score} positive_lift={positive_lift} continue={continue_multi_agent}: {detail}"
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthInput {
    pub selected_pattern: ExecutionPattern,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthMemoryCandidateKind {
    Conflict,
    Stale,
    AuthorityPromotion,
    RelationshipRefresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthEvidenceRef {
    pub kind: String,
    pub reference: String,
    pub summary: String,
}

impl GrowthEvidenceRef {
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        reference: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            reference: reference.into(),
            summary: summary.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthMemoryCandidate {
    pub id: String,
    pub kind: GrowthMemoryCandidateKind,
    pub summary: String,
    pub reason: String,
    pub confidence_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthMatrixSignal {
    pub fact_type: String,
    pub dimensions: serde_json::Value,
    pub measures: serde_json::Value,
    pub confidence_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthEvent {
    pub id: String,
    pub session_id: String,
    pub source_event_kind: String,
    pub strategy_pattern: ExecutionPattern,
    pub learning_record_id: String,
    pub signals: Vec<GrowthSignal>,
    pub evidence_refs: Vec<GrowthEvidenceRef>,
    pub memory_candidates: Vec<GrowthMemoryCandidate>,
    pub matrix_signals: Vec<GrowthMatrixSignal>,
    pub confidence_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthEventInput {
    pub session_id: String,
    pub source_event_kind: String,
    pub strategy_pattern: ExecutionPattern,
    pub learning_record: LearningRecord,
    pub evidence_refs: Vec<GrowthEvidenceRef>,
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
            input.selected_pattern,
            ExecutionPattern::Direct | ExecutionPattern::Execute
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

impl GrowthEvent {
    #[must_use]
    pub fn from_input(input: GrowthEventInput) -> Self {
        let memory_candidates = input
            .learning_record
            .signals
            .iter()
            .filter_map(growth_memory_candidate)
            .collect::<Vec<_>>();
        let matrix_signals = input
            .learning_record
            .signals
            .iter()
            .map(|signal| GrowthMatrixSignal {
                fact_type: format!("ai_growth_{:?}", signal.kind).to_ascii_lowercase(),
                dimensions: serde_json::json!({
                    "kind": format!("{:?}", signal.kind),
                    "severity": format!("{:?}", signal.severity),
                }),
                measures: serde_json::json!({
                    "blocking": signal.severity == GrowthSeverity::Blocker,
                }),
                confidence_bp: confidence_bp_for(signal.severity),
            })
            .collect::<Vec<_>>();
        let confidence_bp = if input.learning_record.has_blocker() {
            9000
        } else {
            7000
        };

        Self {
            id: format!("growth-event-{}", uuid::Uuid::new_v4()),
            session_id: input.session_id,
            source_event_kind: input.source_event_kind,
            strategy_pattern: input.strategy_pattern,
            learning_record_id: input.learning_record.id,
            signals: input.learning_record.signals,
            evidence_refs: input.evidence_refs,
            memory_candidates,
            matrix_signals,
            confidence_bp,
        }
    }
}

fn growth_memory_candidate(signal: &GrowthSignal) -> Option<GrowthMemoryCandidate> {
    let kind = match signal.kind {
        GrowthSignalKind::StrategyFit if signal.severity == GrowthSeverity::Info => return None,
        GrowthSignalKind::StrategyFit => GrowthMemoryCandidateKind::RelationshipRefresh,
        GrowthSignalKind::ContextPressure => GrowthMemoryCandidateKind::RelationshipRefresh,
        GrowthSignalKind::ToolRisk => GrowthMemoryCandidateKind::AuthorityPromotion,
        GrowthSignalKind::VerificationGap => GrowthMemoryCandidateKind::Conflict,
        GrowthSignalKind::EvaluationRegression => GrowthMemoryCandidateKind::Stale,
        GrowthSignalKind::MatrixQualityGate if signal.severity == GrowthSeverity::Info => {
            return None;
        }
        GrowthSignalKind::MatrixQualityGate => GrowthMemoryCandidateKind::AuthorityPromotion,
        GrowthSignalKind::MultiAgentValue if signal.severity == GrowthSeverity::Info => {
            return None;
        }
        GrowthSignalKind::MultiAgentValue => GrowthMemoryCandidateKind::RelationshipRefresh,
    };
    Some(GrowthMemoryCandidate {
        id: format!("growth-candidate-{}", uuid::Uuid::new_v4()),
        kind,
        summary: signal.summary.clone(),
        reason: format!("{:?}:{:?}", signal.kind, signal.severity),
        confidence_bp: confidence_bp_for(signal.severity),
    })
}

fn confidence_bp_for(severity: GrowthSeverity) -> u16 {
    match severity {
        GrowthSeverity::Info => 4500,
        GrowthSeverity::Watch => 6000,
        GrowthSeverity::Improve => 7600,
        GrowthSeverity::Blocker => 9200,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_verification_creates_blocker_signal() {
        let record = LearningRecord::from_input(GrowthInput {
            selected_pattern: ExecutionPattern::Execute,
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
            selected_pattern: ExecutionPattern::Direct,
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

    #[test]
    fn growth_event_turns_blocker_into_memory_and_matrix_signals() {
        let record = LearningRecord::from_input(GrowthInput {
            selected_pattern: ExecutionPattern::Execute,
            complexity: TaskComplexity::Complex,
            risk: TaskRisk::Medium,
            context_omitted: 0,
            tool_requires_checkpoint: false,
            tool_requires_human_confirm: false,
            verification_can_finalize: false,
            bench_passed: false,
        });

        let event = GrowthEvent::from_input(GrowthEventInput {
            session_id: "session-1".to_string(),
            source_event_kind: "runtime.harness_contract.trace".to_string(),
            strategy_pattern: ExecutionPattern::Execute,
            learning_record: record,
            evidence_refs: vec![GrowthEvidenceRef::new(
                "runtime_event",
                "event-1",
                "AI kernel trace",
            )],
        });

        assert!(!event.memory_candidates.is_empty());
        assert!(!event.matrix_signals.is_empty());
        assert!(event.confidence_bp >= 9000);
    }

    #[test]
    fn matrix_quality_gate_signal_can_block_growth() {
        let signal = GrowthSignal::from_matrix_quality_gate(
            false,
            4200,
            &["missing high-authority source".to_string()],
        );

        assert_eq!(signal.kind, GrowthSignalKind::MatrixQualityGate);
        assert_eq!(signal.severity, GrowthSeverity::Blocker);
        assert!(signal.summary.contains("missing high-authority source"));
    }

    #[test]
    fn multi_agent_value_signal_marks_low_lift_as_improvement() {
        let signal = GrowthSignal::from_multi_agent_value(
            false,
            false,
            35,
            &["no_synthesis_lift".to_string()],
        );

        assert_eq!(signal.kind, GrowthSignalKind::MultiAgentValue);
        assert_eq!(signal.severity, GrowthSeverity::Improve);
        assert!(signal.summary.contains("no_synthesis_lift"));
    }
}
