use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

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
    pub evidence_refs: Vec<String>,
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
    pub evidence_refs: Vec<String>,
    pub severity: EvolutionSignalSeverity,
    pub summary: String,
    pub suggested_action: String,
    pub immediate_task_can_continue: bool,
    pub created_at_ms: u128,
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
            created_at_ms: now_ms(),
        }
    }

    #[must_use]
    pub fn low_novelty_tool_loop(
        owner: impl Into<String>,
        session_id: impl Into<String>,
        evidence_refs: Vec<String>,
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
        evidence_refs: Vec<String>,
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
    pub fn eval_failure(run_id: impl Into<String>, evidence_refs: Vec<String>) -> Self {
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
}

#[derive(Debug, Clone)]
pub struct EvolutionSignalStore {
    path: PathBuf,
}

impl EvolutionSignalStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("signals.jsonl"),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, signal: &EvolutionSignal) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(signal).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionSignal>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut signals = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            let signal = serde_json::from_str::<EvolutionSignal>(&line)
                .map_err(|error| error.to_string())?;
            signals.push(signal);
        }
        signals.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(signals)
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
    fn evolution_signal_store_persists_three_signal_classes() {
        let root = std::env::temp_dir().join(format!("cowd-evolution-{}", Uuid::new_v4()));
        let store = EvolutionSignalStore::new(&root);
        for signal in [
            EvolutionSignal::low_novelty_tool_loop(
                "runtime",
                "session-1",
                vec!["tool:read:1".to_string()],
            ),
            EvolutionSignal::memory_noise(
                "runtime",
                "session-1",
                vec!["memory:packet:noise".to_string()],
            ),
            EvolutionSignal::eval_failure("run-1", vec!["report:gate".to_string()]),
        ] {
            store.append(&signal).expect("append");
        }

        let signals = store.list().expect("list");
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
        let _ = fs::remove_dir_all(root);
    }
}
