use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{artifact_builder::EvolutionGeneratedArtifact, candidate_kind::EvolutionCandidateKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCandidatePlan {
    pub target_owner: String,
    pub target_files_or_modules: Vec<String>,
    pub artifact_root: String,
    pub baseline_command: String,
    pub candidate_command: String,
    pub verification_command: String,
    pub acceptance_gates: Vec<String>,
    pub rollback_plan: String,
    pub mainline_write_allowed: bool,
    pub approval_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCandidateStatus {
    Draft,
    SandboxReady,
    Evaluated,
    ApprovedForAdoption,
    Rejected,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCandidate {
    pub candidate_id: String,
    #[serde(default)]
    pub mission_id: Option<String>,
    pub proposal_id: String,
    #[serde(default)]
    pub goal_ids: Vec<String>,
    pub kind: EvolutionCandidateKind,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub scope: Vec<String>,
    #[serde(default)]
    pub trigger_signal_ids: Vec<String>,
    #[serde(default)]
    pub affected_files_or_modules: Vec<String>,
    #[serde(default)]
    pub generated_artifacts: Vec<EvolutionGeneratedArtifact>,
    #[serde(default)]
    pub eval_scenario_ids: Vec<String>,
    #[serde(default)]
    pub promotion_adapter: String,
    #[serde(default)]
    pub autonomy_level: String,
    #[serde(default)]
    pub risk_boundaries: Vec<String>,
    #[serde(default)]
    pub approval_required: bool,
    pub baseline_ref: String,
    pub candidate_ref: String,
    #[serde(default)]
    pub target_owner: String,
    #[serde(default)]
    pub target_files_or_modules: Vec<String>,
    #[serde(default)]
    pub artifact_root: Option<String>,
    #[serde(default)]
    pub baseline_command: String,
    #[serde(default)]
    pub candidate_command: String,
    #[serde(default)]
    pub verification_command: String,
    pub artifact_path: Option<String>,
    pub expected_change: String,
    pub adoption_gate: Vec<String>,
    pub rollback_strategy: String,
    pub status: EvolutionCandidateStatus,
    pub mainline_modified: bool,
    pub human_approval_required: bool,
    #[serde(default)]
    pub comparison_report_ref: Option<String>,
    #[serde(default)]
    pub version_record_ref: Option<String>,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

impl EvolutionCandidate {
    #[must_use]
    pub fn with_artifact(mut self, artifact_path: impl Into<String>) -> Self {
        let artifact_path = artifact_path.into();
        self.artifact_path = Some(artifact_path.clone());
        self.artifact_root = Some(artifact_path);
        self.status = EvolutionCandidateStatus::SandboxReady;
        self.updated_at_ms = now_ms();
        self
    }

    #[must_use]
    pub fn plan(&self) -> EvolutionCandidatePlan {
        EvolutionCandidatePlan {
            target_owner: self.target_owner.clone(),
            target_files_or_modules: self.target_files_or_modules.clone(),
            artifact_root: self
                .artifact_root
                .clone()
                .unwrap_or_else(|| format!("evolution/sandboxes/{}", self.candidate_id)),
            baseline_command: self.baseline_command.clone(),
            candidate_command: self.candidate_command.clone(),
            verification_command: self.verification_command.clone(),
            acceptance_gates: self.adoption_gate.clone(),
            rollback_plan: self.rollback_strategy.clone(),
            mainline_write_allowed: false,
            approval_required: self.human_approval_required,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionCandidateStore {
    path: PathBuf,
}

impl EvolutionCandidateStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("candidates.jsonl"),
        }
    }

    pub fn append(&self, candidate: &EvolutionCandidate) -> Result<(), String> {
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
            serde_json::to_string(candidate).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionCandidate>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut candidates = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            candidates.push(
                serde_json::from_str::<EvolutionCandidate>(&line)
                    .map_err(|error| error.to_string())?,
            );
        }
        candidates.sort_by(|left, right| right.updated_at_ms.cmp(&left.updated_at_ms));
        Ok(candidates)
    }

    pub fn update_status(
        &self,
        candidate_id: &str,
        status: EvolutionCandidateStatus,
    ) -> Result<EvolutionCandidate, String> {
        self.update_candidate(candidate_id, |candidate| {
            candidate.status = status;
        })
    }

    pub fn update_candidate(
        &self,
        candidate_id: &str,
        update: impl FnOnce(&mut EvolutionCandidate),
    ) -> Result<EvolutionCandidate, String> {
        let mut candidates = self.list()?;
        let Some(candidate) = candidates
            .iter_mut()
            .find(|candidate| candidate.candidate_id == candidate_id)
        else {
            return Err("evolution candidate not found".to_string());
        };
        update(candidate);
        candidate.updated_at_ms = now_ms();
        let updated = candidate.clone();
        candidates.sort_by(|left, right| left.created_at_ms.cmp(&right.created_at_ms));
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = fs::File::create(&self.path).map_err(|error| error.to_string())?;
        for candidate in &candidates {
            writeln!(
                file,
                "{}",
                serde_json::to_string(candidate).map_err(|error| error.to_string())?
            )
            .map_err(|error| error.to_string())?;
        }
        Ok(updated)
    }

    pub fn find(&self, candidate_id: &str) -> Result<Option<EvolutionCandidate>, String> {
        Ok(self
            .list()?
            .into_iter()
            .find(|candidate| candidate.candidate_id == candidate_id))
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
    use crate::evolution::{EvolutionCandidateGenerator, EvolutionProposal, EvolutionSignal};

    #[test]
    fn candidate_starts_sandboxed_without_mainline_write() {
        let signal =
            EvolutionSignal::memory_noise("runtime", "session-1", vec!["memory:noise".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let candidate =
            EvolutionCandidateGenerator::generate(&proposal, "baseline:main", "candidate:sandbox");

        assert_eq!(candidate.kind, EvolutionCandidateKind::MemoryGovernance);
        assert_eq!(candidate.status, EvolutionCandidateStatus::Draft);
        assert_eq!(candidate.target_owner, "reality_core");
        for command in [
            &candidate.baseline_command,
            &candidate.candidate_command,
            &candidate.verification_command,
        ] {
            assert!(command.starts_with("cargo metadata "));
            assert!(!command.contains("deterministic-artifact-check"));
            assert_ne!(command.trim(), "true");
        }
        assert!(!candidate.mainline_modified);
        assert!(candidate.human_approval_required);
        assert!(candidate
            .adoption_gate
            .iter()
            .any(|gate| gate.contains("candidate artifact")));
    }

    #[test]
    fn adoption_policy_blocks_adoption_without_sandbox_eval() {
        let signal =
            EvolutionSignal::memory_noise("runtime", "session-1", vec!["memory:noise".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let candidate =
            EvolutionCandidateGenerator::generate(&proposal, "baseline:main", "candidate:sandbox");

        let receipt = crate::evolution::EvolutionAdoptionManager::evaluate(
            &candidate,
            EvolutionCandidateStatus::ApprovedForAdoption,
            None,
        );

        assert!(!receipt.accepted);
        assert!(receipt.reason.contains("requires comparison report"));
    }

    #[test]
    fn all_candidate_kinds_have_artifact_mapping() {
        let signal = EvolutionSignal::eval_failure("run-1", vec!["harness".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        for kind in EvolutionCandidateKind::ALL {
            let candidate = crate::evolution::EvolutionCandidateGenerator::generate_kind(
                &proposal,
                kind,
                "baseline",
                "candidate",
            );
            assert_eq!(candidate.kind, kind);
            assert_eq!(candidate.promotion_adapter, kind.promotion_adapter());
            assert!(!candidate.eval_scenario_ids.is_empty());
        }
    }
}
