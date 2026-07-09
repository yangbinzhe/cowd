use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{candidate::EvolutionCandidate, runner_result::EvolutionRunnerResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionEvaluationRequest {
    pub request_id: String,
    pub candidate_id: String,
    pub mission_id: Option<String>,
    pub kind: String,
    pub baseline_ref: String,
    pub candidate_ref: String,
    pub scenario_ids: Vec<String>,
    pub runner_result_refs: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub created_at_ms: u128,
}

impl EvolutionEvaluationRequest {
    #[must_use]
    pub fn from_candidate(
        candidate: &EvolutionCandidate,
        runner_results: &[EvolutionRunnerResult],
    ) -> Self {
        Self {
            request_id: format!("evo-eval-request-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            mission_id: candidate.mission_id.clone(),
            kind: candidate.kind.as_str().to_string(),
            baseline_ref: candidate.baseline_ref.clone(),
            candidate_ref: candidate.candidate_ref.clone(),
            scenario_ids: candidate.eval_scenario_ids.clone(),
            runner_result_refs: runner_results
                .iter()
                .map(|result| format!("{}:{}", result.mode, result.run_id))
                .collect(),
            artifact_refs: candidate
                .generated_artifacts
                .iter()
                .map(|artifact| artifact.path.clone())
                .collect(),
            created_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvolutionComparisonReport {
    pub comparison_id: String,
    pub candidate_id: String,
    pub baseline_ref: String,
    pub candidate_ref: String,
    pub scenario_ids: Vec<String>,
    pub baseline_metrics: Vec<EvolutionMetric>,
    pub candidate_metrics: Vec<EvolutionMetric>,
    pub quality_delta: f64,
    pub cost_delta: f64,
    pub latency_delta: f64,
    pub regression_count: usize,
    pub recommendation: String,
    pub evidence_package_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvolutionMetric {
    pub name: String,
    pub value: f64,
}

impl EvolutionComparisonReport {
    #[must_use]
    pub fn from_request_and_runner_results(
        request: &EvolutionEvaluationRequest,
        evidence_package_path: impl Into<String>,
        runner_results: &[EvolutionRunnerResult],
    ) -> Self {
        let required_modes = ["artifact", "baseline", "candidate", "verification"];
        let missing_required_modes = required_modes
            .iter()
            .filter(|mode| !runner_results.iter().any(|result| result.mode == **mode))
            .count();
        let failed_required_modes = required_modes
            .iter()
            .filter(|mode| {
                runner_results
                    .iter()
                    .filter(|result| result.mode == **mode)
                    .any(|result| result.exit_code != 0 || !result.policy_violations.is_empty())
            })
            .count();
        let policy_violation_count = runner_results
            .iter()
            .map(|result| result.policy_violations.len())
            .sum::<usize>();
        let baseline_success = success_ratio(runner_results, "baseline");
        let candidate_success = success_ratio(runner_results, "candidate");
        let verification_success = success_ratio(runner_results, "verification");
        let artifact_success = success_ratio(runner_results, "artifact");
        let scenario_coverage = if request.scenario_ids.is_empty() {
            0.0
        } else {
            1.0
        };
        let runner_coverage = required_modes
            .iter()
            .filter(|mode| runner_results.iter().any(|result| result.mode == **mode))
            .count() as f64
            / required_modes.len() as f64;
        let policy_penalty = policy_violation_count as f64 * 8.0;
        let missing_penalty = missing_required_modes as f64 * 12.0;
        let baseline_quality = (baseline_success * 55.0) + (runner_coverage * 15.0);
        let candidate_quality = (candidate_success * 32.0)
            + (verification_success * 26.0)
            + (artifact_success * 24.0)
            + (scenario_coverage * 10.0)
            + (runner_coverage * 8.0)
            - policy_penalty
            - missing_penalty;
        let mut regression_count =
            missing_required_modes + failed_required_modes + policy_violation_count;
        let quality_delta = candidate_quality - baseline_quality;
        if quality_delta < 0.0 {
            regression_count += 1;
        }
        let recommendation = if regression_count == 0 && quality_delta >= 0.0 {
            "promote_after_human_approval"
        } else if policy_violation_count == 0 && missing_required_modes == 0 {
            "revise"
        } else {
            "reject"
        };
        Self {
            comparison_id: format!("evo-comparison-{}", Uuid::new_v4()),
            candidate_id: request.candidate_id.clone(),
            baseline_ref: request.baseline_ref.clone(),
            candidate_ref: request.candidate_ref.clone(),
            scenario_ids: request.scenario_ids.clone(),
            baseline_metrics: vec![
                EvolutionMetric {
                    name: "quality".to_string(),
                    value: baseline_quality,
                },
                EvolutionMetric {
                    name: "runner_coverage".to_string(),
                    value: runner_coverage,
                },
                EvolutionMetric {
                    name: "baseline_success".to_string(),
                    value: baseline_success,
                },
            ],
            candidate_metrics: vec![
                EvolutionMetric {
                    name: "quality".to_string(),
                    value: candidate_quality,
                },
                EvolutionMetric {
                    name: "candidate_success".to_string(),
                    value: candidate_success,
                },
                EvolutionMetric {
                    name: "verification_success".to_string(),
                    value: verification_success,
                },
                EvolutionMetric {
                    name: "artifact_success".to_string(),
                    value: artifact_success,
                },
                EvolutionMetric {
                    name: "policy_violations".to_string(),
                    value: policy_violation_count as f64,
                },
            ],
            quality_delta,
            cost_delta: runner_results.len() as f64 - required_modes.len() as f64,
            latency_delta: total_duration(runner_results, &["candidate", "verification"])
                - total_duration(runner_results, &["baseline"]),
            regression_count,
            recommendation: recommendation.to_string(),
            evidence_package_path: evidence_package_path.into(),
        }
    }
}

fn success_ratio(results: &[EvolutionRunnerResult], mode: &str) -> f64 {
    let matching = results
        .iter()
        .filter(|result| result.mode == mode)
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return 0.0;
    }
    matching
        .iter()
        .filter(|result| result.exit_code == 0 && result.policy_violations.is_empty())
        .count() as f64
        / matching.len() as f64
}

fn total_duration(results: &[EvolutionRunnerResult], modes: &[&str]) -> f64 {
    results
        .iter()
        .filter(|result| modes.contains(&result.mode.as_str()))
        .map(|result| result.duration_ms as f64)
        .sum()
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

    fn request() -> EvolutionEvaluationRequest {
        EvolutionEvaluationRequest {
            request_id: "request-test".to_string(),
            candidate_id: "candidate-test".to_string(),
            mission_id: Some("mission-test".to_string()),
            kind: "runtime_policy".to_string(),
            baseline_ref: "baseline".to_string(),
            candidate_ref: "candidate".to_string(),
            scenario_ids: vec!["scenario:test".to_string()],
            runner_result_refs: Vec::new(),
            artifact_refs: vec!["artifact.json".to_string()],
            created_at_ms: 1,
        }
    }

    fn result(mode: &str, exit_code: i32) -> EvolutionRunnerResult {
        EvolutionRunnerResult {
            run_id: format!("run-{mode}"),
            candidate_id: "candidate-test".to_string(),
            mode: mode.to_string(),
            command: format!("cargo metadata --mode {mode}"),
            exit_code,
            duration_ms: 10,
            stdout_summary: String::new(),
            stderr_summary: String::new(),
            stdout_log_path: format!("{mode}.stdout.log"),
            stderr_log_path: format!("{mode}.stderr.log"),
            artifact_paths: Vec::new(),
            changed_files: Vec::new(),
            mainline_modified: false,
            policy_violations: Vec::new(),
            heartbeat_events: vec![format!("{mode}_completed")],
        }
    }

    #[test]
    fn comparison_requires_artifact_baseline_candidate_and_verification_evidence() {
        let request = request();
        let all_modes = ["artifact", "baseline", "candidate", "verification"]
            .into_iter()
            .map(|mode| result(mode, 0))
            .collect::<Vec<_>>();
        let report = EvolutionComparisonReport::from_request_and_runner_results(
            &request,
            "evidence.json",
            &all_modes,
        );
        assert_eq!(report.regression_count, 0);
        assert_eq!(report.recommendation, "promote_after_human_approval");

        let missing_verification = ["artifact", "baseline", "candidate"]
            .into_iter()
            .map(|mode| result(mode, 0))
            .collect::<Vec<_>>();
        let report = EvolutionComparisonReport::from_request_and_runner_results(
            &request,
            "evidence.json",
            &missing_verification,
        );
        assert!(report.regression_count > 0);
        assert_eq!(report.recommendation, "reject");
    }
}
