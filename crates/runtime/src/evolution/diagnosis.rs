use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::signal::{EvolutionSignal, EvolutionSignalSeverity, EvolutionSignalType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionRootCauseKind {
    RuntimeControlPolicyGap,
    ToolContractGap,
    ContextPolicyGap,
    MemoryGovernanceGap,
    TeamLifecycleGap,
    EvalCoverageGap,
    SurfaceProjectionGap,
    ProviderModelAffordanceGap,
}

impl EvolutionRootCauseKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RuntimeControlPolicyGap => "runtime_control_policy_gap",
            Self::ToolContractGap => "tool_contract_gap",
            Self::ContextPolicyGap => "context_policy_gap",
            Self::MemoryGovernanceGap => "memory_governance_gap",
            Self::TeamLifecycleGap => "team_lifecycle_gap",
            Self::EvalCoverageGap => "eval_coverage_gap",
            Self::SurfaceProjectionGap => "surface_projection_gap",
            Self::ProviderModelAffordanceGap => "provider_model_affordance_gap",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionDiagnosis {
    pub diagnosis_id: String,
    pub root_cause_kind: EvolutionRootCauseKind,
    pub affected_owner: String,
    pub affected_files_or_modules: Vec<String>,
    pub symptoms: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub source_signal_ids: Vec<String>,
    pub impact: String,
    pub recurrence: usize,
    pub recommended_candidate_kind: String,
    pub acceptance_gates: Vec<String>,
    pub risk_boundaries: Vec<String>,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionDiagnosisEngine;

impl EvolutionDiagnosisEngine {
    #[must_use]
    pub fn diagnose(signals: &[EvolutionSignal]) -> EvolutionDiagnosis {
        let root_cause_kind = dominant_root_cause(signals);
        let affected_owner = affected_owner(&root_cause_kind).to_string();
        let affected_files_or_modules = affected_files(&root_cause_kind);
        let symptoms = signals
            .iter()
            .map(|signal| {
                format!(
                    "{}:{}:{}",
                    signal.signal_type_label(),
                    signal.severity_label(),
                    signal.summary
                )
            })
            .collect::<Vec<_>>();
        let evidence_refs = signals
            .iter()
            .flat_map(|signal| signal.evidence_refs.clone())
            .collect::<Vec<_>>();
        let source_signal_ids = signals
            .iter()
            .map(|signal| signal.signal_id.clone())
            .collect::<Vec<_>>();
        let recurrence = signals.len().max(1);
        EvolutionDiagnosis {
            diagnosis_id: format!("evo-diagnosis-{}", Uuid::new_v4()),
            root_cause_kind: root_cause_kind.clone(),
            affected_owner,
            affected_files_or_modules,
            symptoms,
            evidence_refs,
            source_signal_ids,
            impact: impact_for(&root_cause_kind, signals),
            recurrence,
            recommended_candidate_kind: recommended_candidate_kind(&root_cause_kind).to_string(),
            acceptance_gates: acceptance_gates_for(&root_cause_kind),
            risk_boundaries: vec![
                "no_mainline_auto_write".to_string(),
                "sandbox_artifacts_required".to_string(),
                "human_approval_before_adoption".to_string(),
                "rollback_metadata_required".to_string(),
            ],
            created_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionDiagnosisStore {
    path: PathBuf,
}

impl EvolutionDiagnosisStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("diagnoses.jsonl"),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, diagnosis: &EvolutionDiagnosis) -> Result<(), String> {
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
            serde_json::to_string(diagnosis).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionDiagnosis>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut diagnoses = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            diagnoses.push(
                serde_json::from_str::<EvolutionDiagnosis>(&line)
                    .map_err(|error| error.to_string())?,
            );
        }
        diagnoses.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(diagnoses)
    }
}

fn dominant_root_cause(signals: &[EvolutionSignal]) -> EvolutionRootCauseKind {
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::MemoryNoise)
    {
        return EvolutionRootCauseKind::MemoryGovernanceGap;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::ContextPressure)
    {
        return EvolutionRootCauseKind::ContextPolicyGap;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::MissingToolCapability)
    {
        return EvolutionRootCauseKind::ToolContractGap;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::AgentFailurePattern)
    {
        return EvolutionRootCauseKind::TeamLifecycleGap;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::EvalFailure)
    {
        return EvolutionRootCauseKind::EvalCoverageGap;
    }
    if signals.iter().any(|signal| {
        matches!(
            signal.signal_type,
            EvolutionSignalType::LowNoveltyToolLoop
                | EvolutionSignalType::RecoveryGap
                | EvolutionSignalType::SlowProgress
        )
    }) {
        return EvolutionRootCauseKind::RuntimeControlPolicyGap;
    }
    EvolutionRootCauseKind::ProviderModelAffordanceGap
}

fn affected_owner(kind: &EvolutionRootCauseKind) -> &'static str {
    match kind {
        EvolutionRootCauseKind::RuntimeControlPolicyGap
        | EvolutionRootCauseKind::ContextPolicyGap
        | EvolutionRootCauseKind::TeamLifecycleGap
        | EvolutionRootCauseKind::ProviderModelAffordanceGap => "runtime",
        EvolutionRootCauseKind::ToolContractGap => "tools",
        EvolutionRootCauseKind::MemoryGovernanceGap => "reality_core",
        EvolutionRootCauseKind::EvalCoverageGap => "harness_eval",
        EvolutionRootCauseKind::SurfaceProjectionGap => "surface",
    }
}

fn affected_files(kind: &EvolutionRootCauseKind) -> Vec<String> {
    match kind {
        EvolutionRootCauseKind::RuntimeControlPolicyGap => vec![
            "crates/runtime/src/self_regulation".to_string(),
            "crates/runtime/src/conversation".to_string(),
        ],
        EvolutionRootCauseKind::ContextPolicyGap => vec![
            "crates/runtime/src/context".to_string(),
            "crates/runtime/src/self_regulation".to_string(),
        ],
        EvolutionRootCauseKind::MemoryGovernanceGap => vec![
            "crates/runtime/src/context".to_string(),
            "crates/runtime/src/evolution".to_string(),
        ],
        EvolutionRootCauseKind::ToolContractGap => vec![
            "crates/tools".to_string(),
            "crates/runtime/src/tool_host".to_string(),
        ],
        EvolutionRootCauseKind::TeamLifecycleGap => vec![
            "crates/runtime/src/agent".to_string(),
            "crates/runtime/src/mission".to_string(),
        ],
        EvolutionRootCauseKind::EvalCoverageGap => vec!["crates/harness-eval/src".to_string()],
        EvolutionRootCauseKind::SurfaceProjectionGap => vec![
            "crates/tui/src".to_string(),
            "cowd-edge/surfaces/webui/src".to_string(),
        ],
        EvolutionRootCauseKind::ProviderModelAffordanceGap => {
            vec!["crates/runtime/src/provider".to_string()]
        }
    }
}

fn impact_for(kind: &EvolutionRootCauseKind, signals: &[EvolutionSignal]) -> String {
    let critical = signals
        .iter()
        .any(|signal| signal.severity == EvolutionSignalSeverity::Critical);
    let severity = if critical { "critical" } else { "warning" };
    format!(
        "{severity} impact on {}; recurrence={}",
        kind.as_str(),
        signals.len().max(1)
    )
}

fn recommended_candidate_kind(kind: &EvolutionRootCauseKind) -> &'static str {
    match kind {
        EvolutionRootCauseKind::MemoryGovernanceGap => "memory_governance_rule",
        EvolutionRootCauseKind::EvalCoverageGap => "eval_scenario",
        EvolutionRootCauseKind::ToolContractGap => "tool_contract_patch",
        EvolutionRootCauseKind::ContextPolicyGap => "runtime_policy",
        EvolutionRootCauseKind::RuntimeControlPolicyGap => "runtime_policy",
        EvolutionRootCauseKind::TeamLifecycleGap => "runtime_policy",
        EvolutionRootCauseKind::SurfaceProjectionGap => "documentation_update",
        EvolutionRootCauseKind::ProviderModelAffordanceGap => "policy_patch",
    }
}

fn acceptance_gates_for(kind: &EvolutionRootCauseKind) -> Vec<String> {
    let mut gates = vec![
        "diagnosis evidence references at least one signal".to_string(),
        "candidate artifact is written under evolution sandbox root".to_string(),
        "sandbox eval produces baseline and candidate verification artifacts".to_string(),
        "adoption requires human approval boundary".to_string(),
    ];
    match kind {
        EvolutionRootCauseKind::MemoryGovernanceGap => {
            gates.push("memory recall contamination scenario is covered".to_string());
        }
        EvolutionRootCauseKind::EvalCoverageGap => {
            gates.push("harness eval report records regression gate".to_string());
        }
        EvolutionRootCauseKind::ToolContractGap => {
            gates.push("tool contract includes input/output boundary".to_string());
        }
        EvolutionRootCauseKind::ContextPolicyGap => {
            gates.push("context budget and compression policy are explicit".to_string());
        }
        _ => {
            gates.push("runtime behavior change remains side-effect guarded".to_string());
        }
    }
    gates
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
    use crate::evolution::EvolutionSignal;

    #[test]
    fn diagnosis_classifies_memory_noise_and_sets_gates() {
        let signal =
            EvolutionSignal::memory_noise("runtime", "session-1", vec!["memory:noise".into()]);
        let diagnosis = EvolutionDiagnosisEngine::diagnose(&[signal]);

        assert_eq!(
            diagnosis.root_cause_kind,
            EvolutionRootCauseKind::MemoryGovernanceGap
        );
        assert_eq!(diagnosis.affected_owner, "reality_core");
        assert!(diagnosis
            .acceptance_gates
            .iter()
            .any(|gate| gate.contains("memory recall contamination")));
        assert_eq!(diagnosis.recurrence, 1);
    }
}
