//! CowdBench evaluation contracts and lightweight scoring.

use ai_core::ExecutionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchCaseKind {
    SimpleAnswer,
    FastEdit,
    ArchitecturePlan,
    ContextAssembly,
    VerificationGuard,
    WorkGraphFanout,
    ToolTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdBenchCase {
    pub id: String,
    pub kind: BenchCaseKind,
    pub prompt: String,
    pub expected_mode: ExecutionMode,
    pub required_checks: Vec<String>,
}

impl CowdBenchCase {
    #[must_use]
    pub fn new(
        kind: BenchCaseKind,
        prompt: impl Into<String>,
        expected_mode: ExecutionMode,
    ) -> Self {
        Self {
            id: format!("cowdbench-{}", uuid::Uuid::new_v4()),
            kind,
            prompt: prompt.into(),
            expected_mode,
            required_checks: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrajectoryEvent {
    pub kind: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trajectory {
    pub case_id: String,
    pub selected_mode: ExecutionMode,
    pub checks_passed: Vec<String>,
    pub checks_failed: Vec<String>,
    pub events: Vec<TrajectoryEvent>,
}

impl Trajectory {
    #[must_use]
    pub fn new(case_id: impl Into<String>, selected_mode: ExecutionMode) -> Self {
        Self {
            case_id: case_id.into(),
            selected_mode,
            checks_passed: Vec::new(),
            checks_failed: Vec::new(),
            events: Vec::new(),
        }
    }

    #[must_use]
    pub fn pass(mut self, check: impl Into<String>) -> Self {
        self.checks_passed.push(check.into());
        self
    }

    #[must_use]
    pub fn fail(mut self, check: impl Into<String>) -> Self {
        self.checks_failed.push(check.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchCaseResult {
    pub case_id: String,
    pub passed: bool,
    pub score: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub average_score: f32,
    pub results: Vec<BenchCaseResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegressionGateVerdict {
    pub allowed: bool,
    pub min_average_score: f32,
    pub require_all_pass: bool,
    pub average_score: f32,
    pub failed: usize,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RegressionGate {
    pub min_average_score: f32,
    pub require_all_pass: bool,
}

impl Default for RegressionGate {
    fn default() -> Self {
        Self {
            min_average_score: 0.8,
            require_all_pass: false,
        }
    }
}

impl RegressionGate {
    #[must_use]
    pub fn allows(self, report: &BenchReport) -> bool {
        report.average_score >= self.min_average_score
            && (!self.require_all_pass || report.failed == 0)
    }

    #[must_use]
    pub fn evaluate(self, report: &BenchReport) -> RegressionGateVerdict {
        let mut reasons = Vec::new();
        if report.average_score < self.min_average_score {
            reasons.push(format!(
                "average score {} below minimum {}",
                report.average_score, self.min_average_score
            ));
        }
        if self.require_all_pass && report.failed > 0 {
            reasons.push(format!("{} benchmark cases failed", report.failed));
        }
        RegressionGateVerdict {
            allowed: reasons.is_empty(),
            min_average_score: self.min_average_score,
            require_all_pass: self.require_all_pass,
            average_score: report.average_score,
            failed: report.failed,
            reasons,
        }
    }
}

#[must_use]
pub fn score_case(case: &CowdBenchCase, trajectory: &Trajectory) -> BenchCaseResult {
    let mut score = 0.0f32;
    let mut reasons = Vec::new();
    if case.expected_mode == trajectory.selected_mode {
        score += 0.5;
    } else {
        reasons.push(format!(
            "mode mismatch: expected {}, got {}",
            case.expected_mode.as_str(),
            trajectory.selected_mode.as_str()
        ));
    }
    let required_count = case.required_checks.len();
    if required_count == 0 {
        score += 0.5;
    } else {
        let passed = case
            .required_checks
            .iter()
            .filter(|check| trajectory.checks_passed.contains(*check))
            .count();
        score += 0.5 * (passed as f32 / required_count as f32);
        for check in &case.required_checks {
            if !trajectory.checks_passed.contains(check) {
                reasons.push(format!("required check not passed: {check}"));
            }
        }
    }
    if !trajectory.checks_failed.is_empty() {
        score *= 0.75;
        reasons.push("trajectory contains failed checks".to_string());
    }
    BenchCaseResult {
        case_id: case.id.clone(),
        passed: score >= 0.8 && trajectory.checks_failed.is_empty(),
        score,
        reasons,
    }
}

#[must_use]
pub fn score_report(cases: &[CowdBenchCase], trajectories: &[Trajectory]) -> BenchReport {
    let mut results = Vec::new();
    for case in cases {
        if let Some(trajectory) = trajectories.iter().find(|item| item.case_id == case.id) {
            results.push(score_case(case, trajectory));
        } else {
            results.push(BenchCaseResult {
                case_id: case.id.clone(),
                passed: false,
                score: 0.0,
                reasons: vec!["missing trajectory".to_string()],
            });
        }
    }
    let total = results.len();
    let passed = results.iter().filter(|result| result.passed).count();
    let failed = total.saturating_sub(passed);
    let average_score = if total == 0 {
        0.0
    } else {
        results.iter().map(|result| result.score).sum::<f32>() / total as f32
    };
    BenchReport {
        total,
        passed,
        failed,
        average_score,
        results,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_case_scores_one() {
        let mut case = CowdBenchCase::new(
            BenchCaseKind::SimpleAnswer,
            "explain this",
            ExecutionMode::DirectAnswer,
        );
        case.required_checks.push("answered".to_string());
        let trajectory =
            Trajectory::new(case.id.clone(), ExecutionMode::DirectAnswer).pass("answered");

        let result = score_case(&case, &trajectory);

        assert!(result.passed);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn mode_mismatch_is_penalized() {
        let case = CowdBenchCase::new(
            BenchCaseKind::FastEdit,
            "small edit",
            ExecutionMode::FastEdit,
        );
        let trajectory = Trajectory::new(case.id.clone(), ExecutionMode::PlanExecute);

        let result = score_case(&case, &trajectory);

        assert!(!result.passed);
        assert!(result.reasons[0].contains("mode mismatch"));
    }

    #[test]
    fn regression_gate_blocks_low_average() {
        let case = CowdBenchCase::new(
            BenchCaseKind::ToolTransaction,
            "write safely",
            ExecutionMode::RiskGate,
        );
        let report = score_report(&[case], &[]);

        assert!(!RegressionGate::default().allows(&report));
    }
}
