use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::planner::EvolutionProposal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionSandboxRecommendation {
    AdoptAfterHumanApproval,
    Revise,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSandboxEval {
    pub eval_id: String,
    pub proposal_id: String,
    pub baseline_ref: String,
    pub candidate_ref: String,
    pub artifact_path: String,
    pub baseline_score: i32,
    pub candidate_score: i32,
    pub recommendation: EvolutionSandboxRecommendation,
    pub mainline_modified: bool,
    pub human_approval_required: bool,
    pub created_at_ms: u128,
}

impl EvolutionSandboxEval {
    #[must_use]
    pub fn compare(
        proposal: &EvolutionProposal,
        baseline_ref: impl Into<String>,
        candidate_ref: impl Into<String>,
        artifact_path: impl Into<String>,
        baseline_score: i32,
        candidate_score: i32,
    ) -> Self {
        let recommendation = if candidate_score > baseline_score {
            EvolutionSandboxRecommendation::AdoptAfterHumanApproval
        } else if candidate_score == baseline_score {
            EvolutionSandboxRecommendation::Revise
        } else {
            EvolutionSandboxRecommendation::Reject
        };
        Self {
            eval_id: format!("evo-sandbox-{}", Uuid::new_v4()),
            proposal_id: proposal.proposal_id.clone(),
            baseline_ref: baseline_ref.into(),
            candidate_ref: candidate_ref.into(),
            artifact_path: artifact_path.into(),
            baseline_score,
            candidate_score,
            recommendation,
            mainline_modified: false,
            human_approval_required: true,
            created_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionSandboxStore {
    path: PathBuf,
}

impl EvolutionSandboxStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("sandbox-evals.jsonl"),
        }
    }

    pub fn append(&self, eval: &EvolutionSandboxEval) -> Result<(), String> {
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
            serde_json::to_string(eval).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionSandboxEval>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut evals = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            evals.push(
                serde_json::from_str::<EvolutionSandboxEval>(&line)
                    .map_err(|error| error.to_string())?,
            );
        }
        evals.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(evals)
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
    use crate::evolution::{planner::EvolutionProposal, signal::EvolutionSignal};

    #[test]
    fn sandbox_eval_compares_without_mainline_write() {
        let signal =
            EvolutionSignal::eval_failure("run-1", vec!["harness:report_gate".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let eval = EvolutionSandboxEval::compare(
            &proposal,
            "baseline:main",
            "candidate:temp-worktree",
            "/tmp/evo/report.json",
            60,
            82,
        );

        assert_eq!(
            eval.recommendation,
            EvolutionSandboxRecommendation::AdoptAfterHumanApproval
        );
        assert!(!eval.mainline_modified);
        assert!(eval.human_approval_required);
    }
}
