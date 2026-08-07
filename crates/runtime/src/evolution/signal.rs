use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionSignalType {
    LowNoveltyToolLoop,
    MissingToolCapability,
    MemoryNoise,
    AgentFailurePattern,
    RecoveryGap,
    EvalFailure,
    SlowProgress,
    ContextPressure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionSignalSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSignalSource {
    pub owner: String,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub team_id: Option<String>,
    pub run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSignalInput {
    pub signal_type: EvolutionSignalType,
    pub source: EvolutionSignalSource,
    pub evidence_refs: Vec<EvidenceRef>,
    pub severity: EvolutionSignalSeverity,
    pub summary: String,
    pub suggested_action: String,
    pub immediate_task_can_continue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSignal {
    pub signal_id: String,
    pub signal_type: EvolutionSignalType,
    pub source: EvolutionSignalSource,
    pub evidence_refs: Vec<EvidenceRef>,
    pub severity: EvolutionSignalSeverity,
    pub summary: String,
    pub suggested_action: String,
    pub immediate_task_can_continue: bool,
    #[serde(default)]
    pub scope: EvolutionSignalScope,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSignalScope {
    pub workspace_identity: String,
    pub affected_subject: String,
    pub workload_fingerprint: String,
    pub config_definition_revision: String,
    pub provider: String,
    pub model: String,
    pub evaluation_environment: String,
}

impl EvolutionSignal {
    #[must_use]
    pub fn new(input: EvolutionSignalInput) -> Self {
        Self {
            signal_id: format!("evo-signal-{}", Uuid::new_v4()),
            signal_type: input.signal_type,
            source: input.source,
            evidence_refs: input.evidence_refs,
            severity: input.severity,
            summary: input.summary,
            suggested_action: input.suggested_action,
            immediate_task_can_continue: input.immediate_task_can_continue,
            scope: EvolutionSignalScope::default(),
            created_at_ms: now_ms(),
        }
    }

    #[must_use]
    pub fn low_novelty_tool_loop(
        owner: impl Into<String>,
        session_id: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Self {
        Self::new(EvolutionSignalInput {
            signal_type: EvolutionSignalType::LowNoveltyToolLoop,
            source: EvolutionSignalSource {
                owner: owner.into(),
                session_id: Some(session_id.into()),
                agent_id: None,
                team_id: None,
                run_id: None,
            },
            evidence_refs,
            severity: EvolutionSignalSeverity::Warning,
            summary: "Repeated low-novelty tool calls slowed task progress".to_string(),
            suggested_action:
                "Plan a batch-read, Tool DAG, or team delegation strategy before repeating reads"
                    .to_string(),
            immediate_task_can_continue: true,
        })
    }

    #[must_use]
    pub fn memory_noise(
        owner: impl Into<String>,
        session_id: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Self {
        Self::new(EvolutionSignalInput {
            signal_type: EvolutionSignalType::MemoryNoise,
            source: EvolutionSignalSource {
                owner: owner.into(),
                session_id: Some(session_id.into()),
                agent_id: None,
                team_id: None,
                run_id: None,
            },
            evidence_refs,
            severity: EvolutionSignalSeverity::Warning,
            summary: "Recall packet included low-relevance or cross-scope memory".to_string(),
            suggested_action:
                "Create a memory governance adjustment proposal with scope and salience gates"
                    .to_string(),
            immediate_task_can_continue: true,
        })
    }

    #[must_use]
    pub fn eval_failure(run_id: impl Into<String>, evidence_refs: Vec<EvidenceRef>) -> Self {
        Self::new(EvolutionSignalInput {
            signal_type: EvolutionSignalType::EvalFailure,
            source: EvolutionSignalSource {
                owner: "harness_eval".to_string(),
                session_id: None,
                agent_id: None,
                team_id: None,
                run_id: Some(run_id.into()),
            },
            evidence_refs,
            severity: EvolutionSignalSeverity::Critical,
            summary: "Harness evaluation found a capability or evidence gap".to_string(),
            suggested_action: "Generate a test scenario proposal and sandbox evaluation"
                .to_string(),
            immediate_task_can_continue: false,
        })
    }

    #[must_use]
    pub fn signal_type_label(&self) -> &'static str {
        match self.signal_type {
            EvolutionSignalType::LowNoveltyToolLoop => "low_novelty_tool_loop",
            EvolutionSignalType::MissingToolCapability => "missing_tool_capability",
            EvolutionSignalType::MemoryNoise => "memory_noise",
            EvolutionSignalType::AgentFailurePattern => "agent_failure_pattern",
            EvolutionSignalType::RecoveryGap => "recovery_gap",
            EvolutionSignalType::EvalFailure => "eval_failure",
            EvolutionSignalType::SlowProgress => "slow_progress",
            EvolutionSignalType::ContextPressure => "context_pressure",
        }
    }

    #[must_use]
    pub fn severity_label(&self) -> &'static str {
        match self.severity {
            EvolutionSignalSeverity::Info => "info",
            EvolutionSignalSeverity::Warning => "warning",
            EvolutionSignalSeverity::Critical => "critical",
        }
    }

    #[must_use]
    pub fn aggregate_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.source.owner,
            self.source
                .session_id
                .as_deref()
                .or(self.source.run_id.as_deref())
                .unwrap_or("global"),
            self.signal_type_label()
        )
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_signal_supports_three_signal_classes() {
        let signals = [
            EvolutionSignal::low_novelty_tool_loop(
                "runtime",
                "session-1",
                vec![EvidenceRef::observed("tool", "tool:read:1")],
            ),
            EvolutionSignal::memory_noise(
                "runtime",
                "session-1",
                vec![EvidenceRef::observed("memory", "memory:packet:noise")],
            ),
            EvolutionSignal::eval_failure(
                "run-1",
                vec![EvidenceRef::observed("evaluation_report", "report:gate")],
            ),
        ];
        assert_eq!(signals.len(), 3);
        assert!(signals
            .iter()
            .any(|signal| signal.signal_type == EvolutionSignalType::LowNoveltyToolLoop));
        assert!(signals
            .iter()
            .any(|signal| signal.signal_type == EvolutionSignalType::MemoryNoise));
        assert!(signals
            .iter()
            .any(|signal| signal.signal_type == EvolutionSignalType::EvalFailure));
    }
}
