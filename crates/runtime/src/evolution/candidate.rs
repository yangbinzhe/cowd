use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::planner::{EvolutionProposal, EvolutionProposalKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCandidateKind {
    Plan,
    Skill,
    ToolContract,
    ConnectorContract,
    MemoryGovernance,
    RuntimePolicy,
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
    pub proposal_id: String,
    pub kind: EvolutionCandidateKind,
    pub baseline_ref: String,
    pub candidate_ref: String,
    pub artifact_path: Option<String>,
    pub expected_change: String,
    pub adoption_gate: Vec<String>,
    pub rollback_strategy: String,
    pub status: EvolutionCandidateStatus,
    pub mainline_modified: bool,
    pub human_approval_required: bool,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

impl EvolutionCandidate {
    #[must_use]
    pub fn from_proposal(
        proposal: &EvolutionProposal,
        baseline_ref: impl Into<String>,
        candidate_ref: impl Into<String>,
    ) -> Self {
        let now = now_ms();
        Self {
            candidate_id: format!("evo-candidate-{}", Uuid::new_v4()),
            proposal_id: proposal.proposal_id.clone(),
            kind: candidate_kind(&proposal.kind),
            baseline_ref: baseline_ref.into(),
            candidate_ref: candidate_ref.into(),
            artifact_path: None,
            expected_change: proposal.target_improvement.clone(),
            adoption_gate: vec![
                "sandbox evaluation passed".to_string(),
                "mainline was not modified by candidate generation".to_string(),
                "human approval granted before adoption".to_string(),
                "rollback strategy recorded".to_string(),
            ],
            rollback_strategy: proposal.rollback_strategy.clone(),
            status: EvolutionCandidateStatus::Draft,
            mainline_modified: false,
            human_approval_required: true,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    #[must_use]
    pub fn with_artifact(mut self, artifact_path: impl Into<String>) -> Self {
        self.artifact_path = Some(artifact_path.into());
        self.status = EvolutionCandidateStatus::SandboxReady;
        self.updated_at_ms = now_ms();
        self
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
        let mut candidates = self.list()?;
        let Some(candidate) = candidates
            .iter_mut()
            .find(|candidate| candidate.candidate_id == candidate_id)
        else {
            return Err("evolution candidate not found".to_string());
        };
        candidate.status = status;
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
}

fn candidate_kind(kind: &EvolutionProposalKind) -> EvolutionCandidateKind {
    match kind {
        EvolutionProposalKind::PlanDraft | EvolutionProposalKind::TestScenario => {
            EvolutionCandidateKind::Plan
        }
        EvolutionProposalKind::SkillDraft => EvolutionCandidateKind::Skill,
        EvolutionProposalKind::ToolCapabilityRequest => EvolutionCandidateKind::ToolContract,
        EvolutionProposalKind::ConnectorCapabilityRequest => {
            EvolutionCandidateKind::ConnectorContract
        }
        EvolutionProposalKind::MemoryGovernanceAdjustment => {
            EvolutionCandidateKind::MemoryGovernance
        }
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
    use crate::evolution::{EvolutionProposal, EvolutionSignal};

    #[test]
    fn candidate_starts_sandboxed_without_mainline_write() {
        let signal =
            EvolutionSignal::memory_noise("runtime", "session-1", vec!["memory:noise".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let candidate =
            EvolutionCandidate::from_proposal(&proposal, "baseline:main", "candidate:sandbox");

        assert_eq!(candidate.kind, EvolutionCandidateKind::MemoryGovernance);
        assert_eq!(candidate.status, EvolutionCandidateStatus::Draft);
        assert!(!candidate.mainline_modified);
        assert!(candidate.human_approval_required);
        assert!(candidate
            .adoption_gate
            .iter()
            .any(|gate| gate.contains("sandbox")));
    }
}
