//! CowdBench evaluation contracts and lightweight scoring.

use harness_contract::core::{ExecutionModifier, ExecutionPattern};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchCaseKind {
    SimpleAnswer,
    BoundedChange,
    ArchitecturePlan,
    ContextAssembly,
    VerificationGuard,
    ExecutionGraphFanout,
    ToolTransaction,
    BehaviorMinimalScope,
    MemoryGrowthLoop,
    MatrixEvidenceSignal,
    HarnessReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdBenchCase {
    pub id: String,
    pub kind: BenchCaseKind,
    pub prompt: String,
    pub expected_mode: ExecutionPattern,
    #[serde(default)]
    pub expected_modifiers: Vec<ExecutionModifier>,
    pub required_checks: Vec<String>,
}

impl CowdBenchCase {
    #[must_use]
    pub fn new(
        kind: BenchCaseKind,
        prompt: impl Into<String>,
        expected_mode: ExecutionPattern,
    ) -> Self {
        Self {
            id: format!("cowdbench-{}", uuid::Uuid::new_v4()),
            kind,
            prompt: prompt.into(),
            expected_mode,
            expected_modifiers: Vec::new(),
            required_checks: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_expected_modifier(mut self, modifier: ExecutionModifier) -> Self {
        if !self.expected_modifiers.contains(&modifier) {
            self.expected_modifiers.push(modifier);
        }
        self
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
    pub selected_pattern: ExecutionPattern,
    #[serde(default)]
    pub selected_modifiers: Vec<ExecutionModifier>,
    pub checks_passed: Vec<String>,
    pub checks_failed: Vec<String>,
    pub events: Vec<TrajectoryEvent>,
}

impl Trajectory {
    #[must_use]
    pub fn new(case_id: impl Into<String>, selected_pattern: ExecutionPattern) -> Self {
        Self {
            case_id: case_id.into(),
            selected_pattern,
            selected_modifiers: Vec::new(),
            checks_passed: Vec::new(),
            checks_failed: Vec::new(),
            events: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_modifier(mut self, modifier: ExecutionModifier) -> Self {
        if !self.selected_modifiers.contains(&modifier) {
            self.selected_modifiers.push(modifier);
        }
        self
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgenticEvalGateReport {
    pub verdict: RegressionGateVerdict,
    pub required_capabilities: Vec<String>,
    pub covered_capabilities: Vec<String>,
    pub missing_capabilities: Vec<String>,
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
    pub const fn strict() -> Self {
        Self {
            min_average_score: 0.9,
            require_all_pass: true,
        }
    }

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdBenchSmokeSuite {
    pub cases: Vec<CowdBenchCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioSpec {
    pub id: String,
    pub prompt: String,
    pub expected_mode: Option<ExecutionPattern>,
    pub required_checks: Vec<ScenarioCheck>,
}

impl ScenarioSpec {
    #[must_use]
    pub fn new(id: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            prompt: prompt.into(),
            expected_mode: None,
            required_checks: Vec::new(),
        }
    }

    #[must_use]
    pub const fn expect_mode(mut self, mode: ExecutionPattern) -> Self {
        self.expected_mode = Some(mode);
        self
    }

    #[must_use]
    pub fn require(mut self, check: ScenarioCheck) -> Self {
        self.required_checks.push(check);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioCheckKind {
    FinalizationBlocked,
    RegressionAllowed,
    ExecutionGraphPresent,
    ExecutionGraphQualityOk,
    GrowthBlocker,
    GrowthSignal,
    MemoryCandidateCount,
    MatrixSignalCount,
    AssistantTextContains,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioCheck {
    pub id: String,
    pub kind: ScenarioCheckKind,
    pub expected_bool: Option<bool>,
    pub expected_min_count: Option<usize>,
    pub expected_text: Option<String>,
    pub owner: String,
    pub repair_hint: String,
}

impl ScenarioCheck {
    #[must_use]
    pub fn bool(
        id: impl Into<String>,
        kind: ScenarioCheckKind,
        expected: bool,
        owner: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            expected_bool: Some(expected),
            expected_min_count: None,
            expected_text: None,
            owner: owner.into(),
            repair_hint: repair_hint.into(),
        }
    }

    #[must_use]
    pub fn min_count(
        id: impl Into<String>,
        kind: ScenarioCheckKind,
        expected_min_count: usize,
        owner: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            expected_bool: None,
            expected_min_count: Some(expected_min_count),
            expected_text: None,
            owner: owner.into(),
            repair_hint: repair_hint.into(),
        }
    }

    #[must_use]
    pub fn text_contains(
        id: impl Into<String>,
        expected_text: impl Into<String>,
        owner: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind: ScenarioCheckKind::AssistantTextContains,
            expected_bool: None,
            expected_min_count: None,
            expected_text: Some(expected_text.into()),
            owner: owner.into(),
            repair_hint: repair_hint.into(),
        }
    }

    #[must_use]
    pub fn growth_signal(
        id: impl Into<String>,
        kind: impl Into<String>,
        owner: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind: ScenarioCheckKind::GrowthSignal,
            expected_bool: None,
            expected_min_count: None,
            expected_text: Some(kind.into()),
            owner: owner.into(),
            repair_hint: repair_hint.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioObservation {
    pub scenario_id: String,
    pub strategy_pattern: ExecutionPattern,
    pub finalization_blocked: bool,
    pub regression_allowed: bool,
    pub has_execution_graph: bool,
    pub execution_graph_quality_ok: bool,
    pub growth_has_blocker: bool,
    pub growth_signal_kinds: Vec<String>,
    pub memory_candidate_count: usize,
    pub matrix_signal_count: usize,
    pub assistant_text: String,
}

impl ScenarioObservation {
    #[must_use]
    pub fn has_growth_signal(&self, kind: &str) -> bool {
        self.growth_signal_kinds
            .iter()
            .any(|item| item == kind || item.eq_ignore_ascii_case(kind))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedScenarioCheck {
    pub check_id: String,
    pub owner: String,
    pub expected: String,
    pub actual: String,
    pub repair_hint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioVerdict {
    pub scenario_id: String,
    pub passed: bool,
    pub score: f32,
    pub failed_checks: Vec<FailedScenarioCheck>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioSuiteReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub average_score: f32,
    pub verdicts: Vec<ScenarioVerdict>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioSuite {
    pub specs: Vec<ScenarioSpec>,
}

impl ScenarioSuite {
    #[must_use]
    pub fn new(specs: Vec<ScenarioSpec>) -> Self {
        Self { specs }
    }

    #[must_use]
    pub fn evaluate(&self, observations: &[ScenarioObservation]) -> ScenarioSuiteReport {
        let verdicts = self
            .specs
            .iter()
            .map(|spec| {
                observations
                    .iter()
                    .find(|observation| observation.scenario_id == spec.id)
                    .map_or_else(
                        || missing_observation_verdict(spec),
                        |observation| evaluate_scenario(spec, observation),
                    )
            })
            .collect::<Vec<_>>();
        let total = verdicts.len();
        let passed = verdicts.iter().filter(|verdict| verdict.passed).count();
        let failed = total.saturating_sub(passed);
        let average_score = if total == 0 {
            0.0
        } else {
            verdicts.iter().map(|verdict| verdict.score).sum::<f32>() / total as f32
        };
        ScenarioSuiteReport {
            total,
            passed,
            failed,
            average_score,
            verdicts,
        }
    }
}

#[must_use]
pub fn evaluate_scenario(
    spec: &ScenarioSpec,
    observation: &ScenarioObservation,
) -> ScenarioVerdict {
    let mut failed_checks = Vec::new();
    if let Some(expected_mode) = spec.expected_mode {
        if observation.strategy_pattern != expected_mode {
            failed_checks.push(FailedScenarioCheck {
                check_id: "strategy.pattern".to_string(),
                owner: "ai-strategy".to_string(),
                expected: expected_mode.as_str().to_string(),
                actual: observation.strategy_pattern.as_str().to_string(),
                repair_hint: "inspect strategy classifier and experience adapter".to_string(),
            });
        }
    }
    for check in &spec.required_checks {
        if let Some(failure) = evaluate_check(check, observation) {
            failed_checks.push(failure);
        }
    }
    let total_checks = spec.required_checks.len() + usize::from(spec.expected_mode.is_some());
    let passed_checks = total_checks.saturating_sub(failed_checks.len());
    let score = if total_checks == 0 {
        1.0
    } else {
        passed_checks as f32 / total_checks as f32
    };
    ScenarioVerdict {
        scenario_id: spec.id.clone(),
        passed: failed_checks.is_empty(),
        score,
        failed_checks,
    }
}

fn evaluate_check(
    check: &ScenarioCheck,
    observation: &ScenarioObservation,
) -> Option<FailedScenarioCheck> {
    match check.kind {
        ScenarioCheckKind::FinalizationBlocked => {
            compare_bool(check, observation.finalization_blocked)
        }
        ScenarioCheckKind::RegressionAllowed => compare_bool(check, observation.regression_allowed),
        ScenarioCheckKind::ExecutionGraphPresent => {
            compare_bool(check, observation.has_execution_graph)
        }
        ScenarioCheckKind::ExecutionGraphQualityOk => {
            compare_bool(check, observation.execution_graph_quality_ok)
        }
        ScenarioCheckKind::GrowthBlocker => compare_bool(check, observation.growth_has_blocker),
        ScenarioCheckKind::GrowthSignal => match check.expected_text.as_ref() {
            Some(kind) => {
                if observation.has_growth_signal(kind) {
                    None
                } else {
                    Some(failed_check(
                        check,
                        format!("growth signal {kind}"),
                        format!("{:?}", observation.growth_signal_kinds),
                    ))
                }
            }
            None => Some(missing_expectation(check, "expected_text")),
        },
        ScenarioCheckKind::MemoryCandidateCount => {
            compare_min_count(check, observation.memory_candidate_count)
        }
        ScenarioCheckKind::MatrixSignalCount => {
            compare_min_count(check, observation.matrix_signal_count)
        }
        ScenarioCheckKind::AssistantTextContains => match check.expected_text.as_ref() {
            Some(text) => {
                if observation.assistant_text.contains(text) {
                    None
                } else {
                    Some(failed_check(
                        check,
                        format!("assistant text contains {text}"),
                        observation.assistant_text.clone(),
                    ))
                }
            }
            None => Some(missing_expectation(check, "expected_text")),
        },
    }
}

fn compare_bool(check: &ScenarioCheck, actual: bool) -> Option<FailedScenarioCheck> {
    match check.expected_bool {
        Some(expected) => {
            if expected == actual {
                None
            } else {
                Some(failed_check(
                    check,
                    expected.to_string(),
                    actual.to_string(),
                ))
            }
        }
        None => Some(missing_expectation(check, "expected_bool")),
    }
}

fn compare_min_count(check: &ScenarioCheck, actual: usize) -> Option<FailedScenarioCheck> {
    match check.expected_min_count {
        Some(expected) => {
            if actual >= expected {
                None
            } else {
                Some(failed_check(
                    check,
                    format!(">= {expected}"),
                    actual.to_string(),
                ))
            }
        }
        None => Some(missing_expectation(check, "expected_min_count")),
    }
}

fn failed_check(check: &ScenarioCheck, expected: String, actual: String) -> FailedScenarioCheck {
    FailedScenarioCheck {
        check_id: check.id.clone(),
        owner: check.owner.clone(),
        expected,
        actual,
        repair_hint: check.repair_hint.clone(),
    }
}

fn missing_expectation(check: &ScenarioCheck, field: &str) -> FailedScenarioCheck {
    failed_check(check, format!("{field} configured"), "missing".to_string())
}

fn missing_observation_verdict(spec: &ScenarioSpec) -> ScenarioVerdict {
    ScenarioVerdict {
        scenario_id: spec.id.clone(),
        passed: false,
        score: 0.0,
        failed_checks: vec![FailedScenarioCheck {
            check_id: "scenario.observation".to_string(),
            owner: "harness-eval".to_string(),
            expected: "observation present".to_string(),
            actual: "missing".to_string(),
            repair_hint: "ensure scenario runner emits HarnessObservation".to_string(),
        }],
    }
}

impl Default for CowdBenchSmokeSuite {
    fn default() -> Self {
        Self {
            cases: cowdbench_smoke_cases(),
        }
    }
}

impl CowdBenchSmokeSuite {
    #[must_use]
    pub fn score(&self, trajectories: &[Trajectory]) -> BenchReport {
        score_report(&self.cases, trajectories)
    }

    #[must_use]
    pub fn evaluate(
        &self,
        trajectories: &[Trajectory],
        gate: RegressionGate,
    ) -> RegressionGateVerdict {
        gate.evaluate(&self.score(trajectories))
    }

    #[must_use]
    pub fn evaluate_agentic_gate(
        &self,
        trajectories: &[Trajectory],
        gate: RegressionGate,
    ) -> AgenticEvalGateReport {
        let verdict = self.evaluate(trajectories, gate);
        let required_capabilities = vec![
            "strategy".to_string(),
            "context".to_string(),
            "verification".to_string(),
            "execution_graph".to_string(),
            "tool_transaction".to_string(),
            "policy".to_string(),
            "behavior".to_string(),
            "memory".to_string(),
            "matrix".to_string(),
            "harness".to_string(),
        ];
        let covered_capabilities = self
            .cases
            .iter()
            .flat_map(|case| case.required_checks.iter().cloned())
            .filter_map(|check| capability_for_check(&check).map(ToString::to_string))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let missing_capabilities = required_capabilities
            .iter()
            .filter(|capability| !covered_capabilities.contains(capability))
            .cloned()
            .collect::<Vec<_>>();
        AgenticEvalGateReport {
            verdict,
            required_capabilities,
            covered_capabilities,
            missing_capabilities,
        }
    }
}

#[must_use]
pub fn cowdbench_smoke_cases() -> Vec<CowdBenchCase> {
    let specs = [
        (
            BenchCaseKind::SimpleAnswer,
            "explain this function",
            ExecutionPattern::Direct,
            "answered",
        ),
        (
            BenchCaseKind::BoundedChange,
            "fix one small file",
            ExecutionPattern::Execute,
            "guardrails",
        ),
        (
            BenchCaseKind::ArchitecturePlan,
            "plan a multi-crate architecture change",
            ExecutionPattern::Execute,
            "execution_graph",
        ),
        (
            BenchCaseKind::ContextAssembly,
            "assemble relevant memory and workspace context",
            ExecutionPattern::Execute,
            "context_epoch",
        ),
        (
            BenchCaseKind::VerificationGuard,
            "verify claims before final answer",
            ExecutionPattern::Execute,
            "verification_report",
        ),
        (
            BenchCaseKind::ExecutionGraphFanout,
            "parallel multi-agent implementation",
            ExecutionPattern::Collaborate,
            "value_verdict",
        ),
        (
            BenchCaseKind::ToolTransaction,
            "write files safely with rollback discipline",
            ExecutionPattern::Execute,
            "tool_transaction",
        ),
        (
            BenchCaseKind::BehaviorMinimalScope,
            "avoid unnecessary abstractions while preserving safety",
            ExecutionPattern::Execute,
            "minimal_scope",
        ),
        (
            BenchCaseKind::MemoryGrowthLoop,
            "turn runtime learning into reviewable memory candidates",
            ExecutionPattern::Execute,
            "memory_candidate",
        ),
        (
            BenchCaseKind::MatrixEvidenceSignal,
            "emit matrix-compatible evidence and quality signals",
            ExecutionPattern::Execute,
            "matrix_signal",
        ),
        (
            BenchCaseKind::HarnessReceipt,
            "produce a harness receipt for the completed AI turn",
            ExecutionPattern::Execute,
            "harness_receipt",
        ),
    ];
    specs
        .into_iter()
        .map(|(kind, prompt, expected_mode, required_check)| {
            let mut case = CowdBenchCase::new(kind, prompt, expected_mode);
            case.required_checks.push(required_check.to_string());
            case
        })
        .collect()
}

fn capability_for_check(check: &str) -> Option<&'static str> {
    match check {
        "answered" => Some("strategy"),
        "guardrails" => Some("policy"),
        "execution_graph" | "value_verdict" => Some("execution_graph"),
        "context_epoch" => Some("context"),
        "verification_report" => Some("verification"),
        "tool_transaction" => Some("tool_transaction"),
        "minimal_scope" | "reuse_existing" | "safety_preserved" => Some("behavior"),
        "memory_candidate" => Some("memory"),
        "matrix_signal" => Some("matrix"),
        "harness_receipt" => Some("harness"),
        _ => None,
    }
}

#[must_use]
pub fn score_case(case: &CowdBenchCase, trajectory: &Trajectory) -> BenchCaseResult {
    let mut score = 0.0f32;
    let mut reasons = Vec::new();
    if case.expected_mode == trajectory.selected_pattern {
        score += 0.4;
    } else {
        reasons.push(format!(
            "mode mismatch: expected {}, got {}",
            case.expected_mode.as_str(),
            trajectory.selected_pattern.as_str()
        ));
    }
    let missing_modifiers = case
        .expected_modifiers
        .iter()
        .filter(|modifier| !trajectory.selected_modifiers.contains(modifier))
        .collect::<Vec<_>>();
    let modifiers_satisfied = missing_modifiers.is_empty();
    if modifiers_satisfied {
        score += 0.1;
    } else {
        reasons.push(format!(
            "missing execution modifiers: {missing_modifiers:?}"
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
        passed: score >= 0.8 && trajectory.checks_failed.is_empty() && modifiers_satisfied,
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
            ExecutionPattern::Direct,
        );
        case.required_checks.push("answered".to_string());
        let trajectory =
            Trajectory::new(case.id.clone(), ExecutionPattern::Direct).pass("answered");

        let result = score_case(&case, &trajectory);

        assert!(result.passed);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn missing_modifier_is_penalized() {
        let case = CowdBenchCase::new(
            BenchCaseKind::BoundedChange,
            "small edit",
            ExecutionPattern::Execute,
        )
        .with_expected_modifier(ExecutionModifier::BoundedChange);
        let trajectory = Trajectory::new(case.id.clone(), ExecutionPattern::Execute);

        let result = score_case(&case, &trajectory);

        assert!(!result.passed);
        assert!(result.reasons[0].contains("missing execution modifiers"));
    }

    #[test]
    fn regression_gate_blocks_low_average() {
        let case = CowdBenchCase::new(
            BenchCaseKind::ToolTransaction,
            "write safely",
            ExecutionPattern::Execute,
        );
        let report = score_report(&[case], &[]);

        assert!(!RegressionGate::default().allows(&report));
    }

    #[test]
    fn smoke_suite_passes_when_all_required_checks_are_present() {
        let suite = CowdBenchSmokeSuite::default();
        let trajectories = suite
            .cases
            .iter()
            .map(|case| {
                case.required_checks.iter().fold(
                    Trajectory::new(case.id.clone(), case.expected_mode),
                    |trajectory, check| trajectory.pass(check.clone()),
                )
            })
            .collect::<Vec<_>>();

        let verdict = suite.evaluate(&trajectories, RegressionGate::strict());

        assert!(verdict.allowed);
        assert_eq!(suite.cases.len(), 11);
    }

    #[test]
    fn smoke_suite_strict_gate_blocks_missing_path() {
        let suite = CowdBenchSmokeSuite::default();
        let verdict = suite.evaluate(&[], RegressionGate::strict());

        assert!(!verdict.allowed);
        assert_eq!(verdict.failed, suite.cases.len());
    }

    #[test]
    fn scenario_suite_reports_owner_and_repair_hint() {
        let spec = ScenarioSpec::new("empty_answer", "answer this")
            .expect_mode(ExecutionPattern::Direct)
            .require(ScenarioCheck::bool(
                "verification.finalization_blocked",
                ScenarioCheckKind::FinalizationBlocked,
                true,
                "ai-verification/runtime-conversation",
                "ensure finalization gate appends limitation message",
            ));
        let observation = ScenarioObservation {
            scenario_id: "empty_answer".to_string(),
            strategy_pattern: ExecutionPattern::Direct,
            finalization_blocked: false,
            regression_allowed: true,
            has_execution_graph: false,
            execution_graph_quality_ok: false,
            growth_has_blocker: false,
            growth_signal_kinds: Vec::new(),
            memory_candidate_count: 0,
            matrix_signal_count: 0,
            assistant_text: String::new(),
        };

        let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

        assert_eq!(report.total, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(
            report.verdicts[0].failed_checks[0].owner,
            "ai-verification/runtime-conversation"
        );
        assert!(report.verdicts[0].failed_checks[0]
            .repair_hint
            .contains("finalization gate"));
    }

    #[test]
    fn scenario_suite_accepts_growth_signal_checks() {
        let spec = ScenarioSpec::new("matrix_quality", "quality gate")
            .expect_mode(ExecutionPattern::Execute)
            .require(ScenarioCheck::growth_signal(
                "growth.matrix_quality_gate",
                "MatrixQualityGate",
                "ai-growth",
                "map matrix quality gate into growth signal",
            ));
        let observation = ScenarioObservation {
            scenario_id: "matrix_quality".to_string(),
            strategy_pattern: ExecutionPattern::Execute,
            finalization_blocked: false,
            regression_allowed: false,
            has_execution_graph: false,
            execution_graph_quality_ok: false,
            growth_has_blocker: true,
            growth_signal_kinds: vec!["MatrixQualityGate".to_string()],
            memory_candidate_count: 1,
            matrix_signal_count: 1,
            assistant_text: String::new(),
        };

        let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

        assert_eq!(report.failed, 0);
        assert!(report.verdicts[0].passed);
    }

    #[test]
    fn agentic_gate_reports_capability_coverage() {
        let suite = CowdBenchSmokeSuite::default();
        let report = suite.evaluate_agentic_gate(&[], RegressionGate::strict());

        assert!(report
            .covered_capabilities
            .contains(&"behavior".to_string()));
        assert!(report.required_capabilities.contains(&"policy".to_string()));
    }
}
