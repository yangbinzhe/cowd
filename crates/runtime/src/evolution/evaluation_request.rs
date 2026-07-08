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
        runner_result: Option<&EvolutionRunnerResult>,
    ) -> Self {
        Self {
            request_id: format!("evo-eval-request-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            mission_id: candidate.mission_id.clone(),
            kind: candidate.kind.as_str().to_string(),
            baseline_ref: candidate.baseline_ref.clone(),
            candidate_ref: candidate.candidate_ref.clone(),
            scenario_ids: candidate.eval_scenario_ids.clone(),
            runner_result_refs: runner_result
                .map(|result| vec![result.run_id.clone()])
                .unwrap_or_default(),
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
    pub fn deterministic_from_request(
        request: &EvolutionEvaluationRequest,
        evidence_package_path: impl Into<String>,
        runner_exit_code: i32,
    ) -> Self {
        let baseline_quality = 60.0;
        let candidate_quality = if runner_exit_code == 0 { 78.0 } else { 42.0 };
        let regression_count = usize::from(runner_exit_code != 0);
        let quality_delta = candidate_quality - baseline_quality;
        let recommendation = if regression_count == 0 && quality_delta > 0.0 {
            "promote_after_human_approval"
        } else if regression_count == 0 {
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
            baseline_metrics: vec![EvolutionMetric {
                name: "quality".to_string(),
                value: baseline_quality,
            }],
            candidate_metrics: vec![EvolutionMetric {
                name: "quality".to_string(),
                value: candidate_quality,
            }],
            quality_delta,
            cost_delta: 0.0,
            latency_delta: 0.0,
            regression_count,
            recommendation: recommendation.to_string(),
            evidence_package_path: evidence_package_path.into(),
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
