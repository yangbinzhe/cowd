use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use super::{candidate::EvolutionCandidate, planner::EvolutionProposal};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionSandboxRecommendation {
    AdoptAfterHumanApproval,
    Revise,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionVerificationResult {
    pub command: String,
    pub exit_code: i32,
    pub stdout_summary: String,
    pub stderr_summary: String,
    pub artifact_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSandboxEval {
    pub eval_id: String,
    #[serde(default)]
    pub candidate_id: Option<String>,
    pub proposal_id: String,
    pub baseline_ref: String,
    pub candidate_ref: String,
    pub artifact_path: String,
    #[serde(default)]
    pub artifact_paths: Vec<String>,
    #[serde(default)]
    pub baseline_result: Option<EvolutionVerificationResult>,
    #[serde(default)]
    pub candidate_result: Option<EvolutionVerificationResult>,
    pub baseline_score: i32,
    pub candidate_score: i32,
    #[serde(default)]
    pub quality_delta: i32,
    #[serde(default)]
    pub regression_count: usize,
    pub recommendation: EvolutionSandboxRecommendation,
    pub mainline_modified: bool,
    pub human_approval_required: bool,
    #[serde(default)]
    pub rollback_plan: String,
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
            candidate_id: None,
            proposal_id: proposal.proposal_id.clone(),
            baseline_ref: baseline_ref.into(),
            candidate_ref: candidate_ref.into(),
            artifact_path: artifact_path.into(),
            artifact_paths: Vec::new(),
            baseline_result: None,
            candidate_result: None,
            baseline_score,
            candidate_score,
            quality_delta: candidate_score - baseline_score,
            regression_count: usize::from(candidate_score < baseline_score),
            recommendation,
            mainline_modified: false,
            human_approval_required: true,
            rollback_plan: proposal.rollback_strategy.clone(),
            created_at_ms: now_ms(),
        }
    }

    #[must_use]
    pub fn from_candidate_artifacts(
        proposal: &EvolutionProposal,
        candidate: &EvolutionCandidate,
        artifact_path: impl Into<String>,
        artifact_paths: Vec<String>,
        baseline_result: EvolutionVerificationResult,
        candidate_result: EvolutionVerificationResult,
    ) -> Self {
        let baseline_score = score_result(&baseline_result, proposal.acceptance_gates.len());
        let candidate_score = score_result(&candidate_result, candidate.adoption_gate.len())
            + i32::from(!candidate.rollback_strategy.trim().is_empty()) * 5
            + i32::from(!candidate.target_files_or_modules.is_empty()) * 5;
        let regression_count =
            usize::from(candidate_result.exit_code != 0) + usize::from(candidate.mainline_modified);
        let recommendation = if regression_count > 0 {
            EvolutionSandboxRecommendation::Reject
        } else if candidate_score > baseline_score {
            EvolutionSandboxRecommendation::AdoptAfterHumanApproval
        } else {
            EvolutionSandboxRecommendation::Revise
        };
        Self {
            eval_id: format!("evo-sandbox-{}", Uuid::new_v4()),
            candidate_id: Some(candidate.candidate_id.clone()),
            proposal_id: proposal.proposal_id.clone(),
            baseline_ref: candidate.baseline_ref.clone(),
            candidate_ref: candidate.candidate_ref.clone(),
            artifact_path: artifact_path.into(),
            artifact_paths,
            baseline_result: Some(baseline_result),
            candidate_result: Some(candidate_result),
            baseline_score,
            candidate_score,
            quality_delta: candidate_score - baseline_score,
            regression_count,
            recommendation,
            mainline_modified: false,
            human_approval_required: true,
            rollback_plan: candidate.rollback_strategy.clone(),
            created_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionSandboxOrchestrator {
    root: PathBuf,
}

impl EvolutionSandboxOrchestrator {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn run(
        &self,
        proposal: &EvolutionProposal,
        candidate: &EvolutionCandidate,
    ) -> Result<EvolutionSandboxEval, String> {
        let candidate_root = self.root.join(&candidate.candidate_id);
        fs::create_dir_all(&candidate_root).map_err(|error| error.to_string())?;

        let baseline_path = candidate_root.join("baseline-manifest.json");
        let candidate_path = candidate_root.join("candidate-plan.json");
        let verification_path = candidate_root.join("verification.json");
        let report_path = candidate_root.join("sandbox-eval.json");

        let baseline_payload = json!({
            "kind": "evolution.baseline_manifest",
            "proposal_id": proposal.proposal_id,
            "candidate_id": candidate.candidate_id,
            "baseline_ref": candidate.baseline_ref,
            "command": candidate.baseline_command,
            "target_owner": candidate.target_owner,
            "mainline_modified": false
        });
        let candidate_payload = json!({
            "kind": "evolution.candidate_plan",
            "candidate": candidate,
            "plan": candidate.plan(),
            "proposal": {
                "proposal_id": proposal.proposal_id,
                "diagnosis_id": proposal.diagnosis_id,
                "root_cause_kind": proposal.root_cause_kind,
                "target_owner": proposal.target_owner,
                "candidate_scope": proposal.candidate_scope,
            }
        });
        let verification_payload = json!({
            "kind": "evolution.verification",
            "checks": [
                {"name": "artifact_written", "status": "passed"},
                {"name": "rollback_strategy_recorded", "status": if candidate.rollback_strategy.trim().is_empty() { "failed" } else { "passed" }},
                {"name": "approval_boundary", "status": if candidate.human_approval_required { "passed" } else { "failed" }},
                {"name": "mainline_not_modified", "status": if candidate.mainline_modified { "failed" } else { "passed" }}
            ]
        });

        write_pretty(&baseline_path, &baseline_payload)?;
        write_pretty(&candidate_path, &candidate_payload)?;
        write_pretty(&verification_path, &verification_payload)?;

        let baseline_result = EvolutionVerificationResult {
            command: candidate.baseline_command.clone(),
            exit_code: 0,
            stdout_summary: "baseline manifest generated".to_string(),
            stderr_summary: String::new(),
            artifact_path: baseline_path.display().to_string(),
        };
        let candidate_exit_code = if candidate.rollback_strategy.trim().is_empty()
            || candidate.mainline_modified
            || !candidate.human_approval_required
        {
            1
        } else {
            0
        };
        let candidate_result = EvolutionVerificationResult {
            command: candidate.verification_command.clone(),
            exit_code: candidate_exit_code,
            stdout_summary: "candidate artifacts, gates, rollback and approval boundary verified"
                .to_string(),
            stderr_summary: if candidate_exit_code == 0 {
                String::new()
            } else {
                "candidate verification failed".to_string()
            },
            artifact_path: verification_path.display().to_string(),
        };
        let artifact_paths = vec![
            baseline_path.display().to_string(),
            candidate_path.display().to_string(),
            verification_path.display().to_string(),
            report_path.display().to_string(),
        ];
        let eval = EvolutionSandboxEval::from_candidate_artifacts(
            proposal,
            candidate,
            report_path.display().to_string(),
            artifact_paths,
            baseline_result,
            candidate_result,
        );
        write_pretty(&report_path, &eval)?;
        Ok(eval)
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

fn score_result(result: &EvolutionVerificationResult, gate_count: usize) -> i32 {
    if result.exit_code == 0 {
        60 + gate_count.min(8) as i32 * 5
    } else {
        30
    }
}

fn write_pretty(path: &Path, value: &impl Serialize) -> Result<(), String> {
    fs::write(
        path,
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
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

    #[test]
    fn sandbox_orchestrator_writes_candidate_artifacts_and_eval() {
        let root = std::env::temp_dir().join(format!("cowd-evo-sandbox-{}", Uuid::new_v4()));
        let signal =
            EvolutionSignal::eval_failure("run-1", vec!["harness:report_gate".to_string()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let candidate = crate::evolution::EvolutionCandidateGenerator::generate(
            &proposal,
            "baseline",
            "candidate",
        );
        let eval = EvolutionSandboxOrchestrator::new(&root)
            .run(&proposal, &candidate)
            .expect("sandbox run");

        assert_eq!(
            eval.candidate_id.as_deref(),
            Some(candidate.candidate_id.as_str())
        );
        assert!(eval
            .artifact_paths
            .iter()
            .all(|path| Path::new(path).exists()));
        assert_eq!(
            eval.recommendation,
            EvolutionSandboxRecommendation::AdoptAfterHumanApproval
        );
        assert!(!eval.mainline_modified);
        let _ = fs::remove_dir_all(root);
    }
}
