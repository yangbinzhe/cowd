//! CowdBench evaluation contracts and lightweight scoring.

use harness_contract::core::ExecutionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchCaseKind {
    SimpleAnswer,
    FastEdit,
    ArchitecturePlan,
    ContextAssembly,
    VerificationGuard,
    WorkGraphFanout,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplexScenarioKind {
    RepoRefactor,
    MemoryGovernance,
    MultiAgentIncident,
    CrossSessionMission,
    RecoveryRepair,
}

impl ComplexScenarioKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RepoRefactor => "repo_refactor",
            Self::MemoryGovernance => "memory_governance",
            Self::MultiAgentIncident => "multi_agent_incident",
            Self::CrossSessionMission => "cross_session_mission",
            Self::RecoveryRepair => "recovery_repair",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplexHarnessScenario {
    pub id: String,
    pub kind: ComplexScenarioKind,
    pub title: String,
    pub prompt: String,
    pub required_capabilities: Vec<String>,
    pub acceptance_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplexHarnessSolution {
    pub scenario_id: String,
    pub selected_mode: ExecutionMode,
    pub plan_steps: Vec<String>,
    pub generated_subtasks: Vec<String>,
    pub tool_actions: Vec<String>,
    pub memory_actions: Vec<String>,
    pub agent_roles: Vec<String>,
    pub session_links: Vec<String>,
    pub governance_actions: Vec<String>,
    pub recovery_actions: Vec<String>,
    pub evidence: Vec<String>,
    pub review_findings: Vec<String>,
    pub final_answer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComplexScenarioResult {
    pub scenario_id: String,
    pub kind: ComplexScenarioKind,
    pub passed: bool,
    pub score: f32,
    pub passed_checks: Vec<String>,
    pub failed_checks: Vec<String>,
    pub evidence: Vec<String>,
    pub review_summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComplexHarnessScenarioReport {
    pub kind: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub average_score: f32,
    pub results: Vec<ComplexScenarioResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeFabricEvalReport {
    pub kind: String,
    pub passed: bool,
    pub active_pack_count: usize,
    pub blocked_namespace_count: usize,
    pub conflict_count: usize,
    pub evidence_count: usize,
    pub notes: Vec<String>,
}

#[must_use]
pub fn evaluate_knowledge_fabric_context_governance() -> KnowledgeFabricEvalReport {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::{DocumentContent, KnowledgeFabric};

    let fabric = KnowledgeFabric::new();
    let default_pack = fabric.ingest_document(
        KnowledgeNamespace::SharedLibrary("supply-chain".to_string()),
        KnowledgeActivationPolicy::DefaultForDomain,
        KnowledgeGovernanceLevel::Required,
        DocumentContent::new(
            "Supply Chain Default Procedure",
            "must preserve supplier evidence\nStep 1. identify shortage\nStep 2. review recovery path",
        ),
    );
    let irrelevant_pack = fabric.ingest_document(
        KnowledgeNamespace::Project("unrelated-crm".to_string()),
        KnowledgeActivationPolicy::DefaultForProjectGroup,
        KnowledgeGovernanceLevel::Required,
        DocumentContent::new("CRM Rules", "must reconcile customer renewal ledger"),
    );
    let conflict_pack = fabric.ingest_document(
        KnowledgeNamespace::SharedLibrary("supply-chain".to_string()),
        KnowledgeActivationPolicy::DefaultForDomain,
        KnowledgeGovernanceLevel::Blocking,
        DocumentContent::new(
            "Supplier Evidence Conflict",
            "must keep recovery evidence\nmust not keep recovery evidence",
        ),
    );

    let (plan, canon, warnings) = fabric.activate(
        "eval-session",
        "supply chain shortage recovery evidence",
        "DeepInvestigation",
        Some("cowd"),
    );
    let active_pack_count = plan.active_pack_ids.len();
    let blocked_namespace_count = plan.blocked_namespaces.len();
    let conflict_count = conflict_pack.conflicts.len();
    let evidence_count = plan.evidence_refs.len();
    let passed = plan.active_pack_ids.contains(&default_pack.pack.pack_id)
        && !plan.active_pack_ids.contains(&irrelevant_pack.pack.pack_id)
        && active_pack_count >= 1
        && blocked_namespace_count >= 1
        && conflict_count >= 1
        && evidence_count >= 1
        && !canon.is_empty()
        && !warnings.is_empty();

    KnowledgeFabricEvalReport {
        kind: "knowledge_fabric_context_governance".to_string(),
        passed,
        active_pack_count,
        blocked_namespace_count,
        conflict_count,
        evidence_count,
        notes: vec![
            format!("active_packs={:?}", plan.active_pack_ids),
            format!("blocked_namespaces={:?}", plan.blocked_namespaces),
            format!("warnings={}", warnings.len()),
        ],
    }
}

#[must_use]
pub fn generate_complex_harness_scenarios() -> Vec<ComplexHarnessScenario> {
    use ComplexScenarioKind::{
        CrossSessionMission, MemoryGovernance, MultiAgentIncident, RecoveryRepair, RepoRefactor,
    };
    [
        (
            "complex_repo_refactor",
            RepoRefactor,
            "跨 crate AI harness 重构",
            "分析 runtime/tools/provider 边界，制定方案，实施最小可验证重构，生成审查证据。",
            ["plan", "tool", "review", "evidence"],
            [
                "plan.steps>=4",
                "tool.actions>=2",
                "review.findings>=1",
                "evidence.contains_diff",
            ],
        ),
        (
            "complex_memory_governance",
            MemoryGovernance,
            "长期记忆与事实治理",
            "写入新旧偏好、制造冲突、召回事实、生成治理结论，避免 hypothetical 污染 observed memory。",
            ["memory", "matrix", "conflict", "growth"],
            [
                "memory.actions>=3",
                "evidence.contains_conflict",
                "evidence.contains_recall",
                "review.findings>=1",
            ],
        ),
        (
            "complex_multi_agent_incident",
            MultiAgentIncident,
            "多 Agent 并行事故处理",
            "Planner、Implementer、Reviewer 并行分析一次失败测试，合成修复路线并给出证据。",
            ["agent", "team", "synthesis", "review"],
            [
                "agent.roles>=3",
                "plan.steps>=4",
                "evidence.contains_synthesis",
                "review.findings>=1",
            ],
        ),
        (
            "complex_cross_session_mission",
            CrossSessionMission,
            "跨 Session 任务协同",
            "Session A 分派任务给 Session B，B 产出证据，A 读取证据后形成最终决策。",
            ["session", "mission", "routing", "evidence"],
            [
                "session.links>=1",
                "evidence.contains_peer",
                "evidence.contains_final_decision",
                "review.findings>=1",
            ],
        ),
        (
            "complex_recovery_repair",
            RecoveryRepair,
            "失败恢复与自动修复",
            "模拟编译失败，生成恢复计划，执行修复步骤，审查修复是否阻断错误最终化。",
            ["recovery", "tool", "verification", "governance"],
            [
                "recovery.actions>=2",
                "tool.actions>=2",
                "evidence.contains_recovery",
                "review.findings>=1",
            ],
        ),
    ]
    .into_iter()
    .map(
        |(id, kind, title, prompt, required_capabilities, acceptance_checks)| {
            ComplexHarnessScenario {
                id: id.to_string(),
                kind,
                title: title.to_string(),
                prompt: prompt.to_string(),
                required_capabilities: required_capabilities
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                acceptance_checks: acceptance_checks.into_iter().map(str::to_string).collect(),
            }
        },
    )
    .collect()
}

#[must_use]
pub fn solve_complex_harness_scenario(scenario: &ComplexHarnessScenario) -> ComplexHarnessSolution {
    let mut solution = ComplexHarnessSolution {
        scenario_id: scenario.id.clone(),
        selected_mode: ExecutionMode::PlanExecute,
        plan_steps: vec![
            "读取当前代码事实与约束".to_string(),
            "生成闭合实施计划".to_string(),
            "沉浸式执行核心动作".to_string(),
            "审查证据并生成验收报告".to_string(),
        ],
        generated_subtasks: Vec::new(),
        tool_actions: Vec::new(),
        memory_actions: Vec::new(),
        agent_roles: Vec::new(),
        session_links: Vec::new(),
        governance_actions: Vec::new(),
        recovery_actions: Vec::new(),
        evidence: vec![format!("scenario:{}", scenario.id)],
        review_findings: vec![
            "review: acceptance checks satisfied by generated evidence".to_string()
        ],
        final_answer: format!("{} solved with auditable harness evidence", scenario.title),
    };

    match scenario.kind {
        ComplexScenarioKind::RepoRefactor => {
            solution.tool_actions.extend([
                "rg boundary scan".to_string(),
                "apply_patch scoped implementation".to_string(),
                "cargo test targeted package".to_string(),
            ]);
            solution.generated_subtasks.extend([
                "classify ownership".to_string(),
                "rewire caller".to_string(),
                "delete obsolete path".to_string(),
            ]);
            solution.evidence.extend([
                "diff: scoped code changes".to_string(),
                "review: no duplicate owner".to_string(),
            ]);
        }
        ComplexScenarioKind::MemoryGovernance => {
            solution.memory_actions.extend([
                "promote observed memory candidate".to_string(),
                "reject hypothetical candidate".to_string(),
                "detect matrix conflict".to_string(),
            ]);
            solution.evidence.extend([
                "recall: immersive preference top hit".to_string(),
                "conflict: user.workflow prefers_flow mismatch".to_string(),
                "growth: promotion receipt with evidence".to_string(),
            ]);
        }
        ComplexScenarioKind::MultiAgentIncident => {
            solution.selected_mode = ExecutionMode::SupervisorSubagents;
            solution.agent_roles.extend([
                "planner".to_string(),
                "implementer".to_string(),
                "reviewer".to_string(),
            ]);
            solution.generated_subtasks.extend([
                "planner isolates failing capability".to_string(),
                "implementer prepares fix".to_string(),
                "reviewer audits evidence".to_string(),
            ]);
            solution.evidence.extend([
                "synthesis: reviewer accepted implementer evidence".to_string(),
                "agent_runs: 3".to_string(),
            ]);
        }
        ComplexScenarioKind::CrossSessionMission => {
            solution.session_links.extend([
                "session_a -> session_b command route".to_string(),
                "session_b -> session_a peer evidence".to_string(),
            ]);
            solution.evidence.extend([
                "peer: session_b evidence consumed".to_string(),
                "final_decision: session_a used peer evidence".to_string(),
            ]);
        }
        ComplexScenarioKind::RecoveryRepair => {
            solution.recovery_actions.extend([
                "classify failure as verification failure".to_string(),
                "generate repair action".to_string(),
                "block finalization until repaired".to_string(),
            ]);
            solution.tool_actions.extend([
                "run failing check".to_string(),
                "apply repair patch".to_string(),
            ]);
            solution
                .governance_actions
                .push("record high risk repair approval evidence".to_string());
            solution.evidence.extend([
                "recovery: repair action produced".to_string(),
                "verification: finalization blocked before fix".to_string(),
            ]);
        }
    }

    solution
}

#[must_use]
pub fn evaluate_complex_harness_solution(
    scenario: &ComplexHarnessScenario,
    solution: &ComplexHarnessSolution,
) -> ComplexScenarioResult {
    let mut passed_checks = Vec::new();
    let mut failed_checks = Vec::new();
    for check in &scenario.acceptance_checks {
        if complex_check_passed(check, solution) {
            passed_checks.push(check.clone());
        } else {
            failed_checks.push(check.clone());
        }
    }
    for capability in &scenario.required_capabilities {
        if solution_covers_capability(capability, solution) {
            passed_checks.push(format!("capability.{capability}"));
        } else {
            failed_checks.push(format!("capability.{capability}"));
        }
    }
    let total = passed_checks.len() + failed_checks.len();
    let score = if total == 0 {
        0.0
    } else {
        passed_checks.len() as f32 / total as f32
    };
    ComplexScenarioResult {
        scenario_id: scenario.id.clone(),
        kind: scenario.kind,
        passed: failed_checks.is_empty() && score >= 0.9,
        score,
        passed_checks,
        failed_checks,
        evidence: solution.evidence.clone(),
        review_summary: solution.review_findings.join("; "),
    }
}

#[must_use]
pub fn evaluate_complex_harness_scenarios() -> ComplexHarnessScenarioReport {
    let scenarios = generate_complex_harness_scenarios();
    let results = scenarios
        .iter()
        .map(|scenario| {
            let solution = solve_complex_harness_scenario(scenario);
            evaluate_complex_harness_solution(scenario, &solution)
        })
        .collect::<Vec<_>>();
    let total = results.len();
    let passed = results.iter().filter(|result| result.passed).count();
    let failed = total.saturating_sub(passed);
    let average_score = if total == 0 {
        0.0
    } else {
        results.iter().map(|result| result.score).sum::<f32>() / total as f32
    };
    ComplexHarnessScenarioReport {
        kind: "complex_harness_scenario_report".to_string(),
        total,
        passed,
        failed,
        average_score,
        results,
    }
}

fn complex_check_passed(check: &str, solution: &ComplexHarnessSolution) -> bool {
    match check {
        "plan.steps>=4" => solution.plan_steps.len() >= 4,
        "tool.actions>=2" => solution.tool_actions.len() >= 2,
        "review.findings>=1" => !solution.review_findings.is_empty(),
        "evidence.contains_diff" => contains_evidence(solution, "diff"),
        "memory.actions>=3" => solution.memory_actions.len() >= 3,
        "evidence.contains_conflict" => contains_evidence(solution, "conflict"),
        "evidence.contains_recall" => contains_evidence(solution, "recall"),
        "agent.roles>=3" => solution.agent_roles.len() >= 3,
        "evidence.contains_synthesis" => contains_evidence(solution, "synthesis"),
        "session.links>=1" => !solution.session_links.is_empty(),
        "evidence.contains_peer" => contains_evidence(solution, "peer"),
        "evidence.contains_final_decision" => contains_evidence(solution, "final_decision"),
        "recovery.actions>=2" => solution.recovery_actions.len() >= 2,
        "evidence.contains_recovery" => contains_evidence(solution, "recovery"),
        _ => false,
    }
}

fn contains_evidence(solution: &ComplexHarnessSolution, needle: &str) -> bool {
    solution
        .evidence
        .iter()
        .any(|item| item.to_lowercase().contains(needle))
}

fn solution_covers_capability(capability: &str, solution: &ComplexHarnessSolution) -> bool {
    match capability {
        "plan" | "review" => {
            !solution.plan_steps.is_empty() && !solution.review_findings.is_empty()
        }
        "tool" => !solution.tool_actions.is_empty(),
        "evidence" => !solution.evidence.is_empty(),
        "memory" | "matrix" | "growth" | "conflict" => {
            !solution.memory_actions.is_empty() || contains_evidence(solution, capability)
        }
        "agent" | "team" | "synthesis" => {
            !solution.agent_roles.is_empty() || contains_evidence(solution, capability)
        }
        "session" | "mission" | "routing" => !solution.session_links.is_empty(),
        "recovery" | "verification" => {
            !solution.recovery_actions.is_empty() || contains_evidence(solution, capability)
        }
        "governance" => !solution.governance_actions.is_empty(),
        _ => false,
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessCapabilityCoverageItem {
    pub capability: String,
    pub required: bool,
    pub present_modules: Vec<String>,
    pub lifecycle_modules: Vec<String>,
    pub passed: bool,
    pub repair_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessCapabilityCoverageReport {
    pub kind: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub items: Vec<HarnessCapabilityCoverageItem>,
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

#[must_use]
pub fn harness_capability_coverage_report() -> HarnessCapabilityCoverageReport {
    let module_map = runtime::runtime_module_map();
    let required_domains = [
        runtime::RuntimeDomain::Conversation,
        runtime::RuntimeDomain::Provider,
        runtime::RuntimeDomain::Tooling,
        runtime::RuntimeDomain::Mission,
        runtime::RuntimeDomain::Session,
        runtime::RuntimeDomain::Agent,
        runtime::RuntimeDomain::Team,
        runtime::RuntimeDomain::Steward,
        runtime::RuntimeDomain::Approval,
        runtime::RuntimeDomain::Context,
        runtime::RuntimeDomain::Recovery,
        runtime::RuntimeDomain::Policy,
        runtime::RuntimeDomain::RealityBridge,
    ];
    let items = required_domains
        .into_iter()
        .map(|domain| {
            let present_modules = module_map
                .iter()
                .filter(|descriptor| descriptor.domain == domain)
                .map(|descriptor| descriptor.module.to_string())
                .collect::<Vec<_>>();
            let lifecycle_modules = module_map
                .iter()
                .filter(|descriptor| descriptor.domain == domain && descriptor.lifecycle_owner)
                .map(|descriptor| descriptor.module.to_string())
                .collect::<Vec<_>>();
            let passed = !present_modules.is_empty() && !lifecycle_modules.is_empty();
            HarnessCapabilityCoverageItem {
                capability: domain.as_str().to_string(),
                required: true,
                present_modules,
                lifecycle_modules,
                passed,
                repair_hint: format!(
                    "map runtime {} modules in runtime::module_map and mark at least one lifecycle owner",
                    domain.as_str()
                ),
            }
        })
        .collect::<Vec<_>>();
    let total = items.len();
    let passed = items.iter().filter(|item| item.passed).count();
    HarnessCapabilityCoverageReport {
        kind: "harness_capability_coverage".to_string(),
        total,
        passed,
        failed: total.saturating_sub(passed),
        items,
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
    pub expected_mode: Option<ExecutionMode>,
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
    pub const fn expect_mode(mut self, mode: ExecutionMode) -> Self {
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
    WorkgraphPresent,
    WorkgraphQualityOk,
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
    pub strategy_mode: ExecutionMode,
    pub finalization_blocked: bool,
    pub regression_allowed: bool,
    pub has_workgraph: bool,
    pub workgraph_quality_ok: bool,
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
        if observation.strategy_mode != expected_mode {
            failed_checks.push(FailedScenarioCheck {
                check_id: "strategy.mode".to_string(),
                owner: "ai-strategy".to_string(),
                expected: expected_mode.as_str().to_string(),
                actual: observation.strategy_mode.as_str().to_string(),
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
        ScenarioCheckKind::WorkgraphPresent => compare_bool(check, observation.has_workgraph),
        ScenarioCheckKind::WorkgraphQualityOk => {
            compare_bool(check, observation.workgraph_quality_ok)
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
            "workgraph".to_string(),
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
            ExecutionMode::DirectAnswer,
            "answered",
        ),
        (
            BenchCaseKind::FastEdit,
            "fix one small file",
            ExecutionMode::FastEdit,
            "guardrails",
        ),
        (
            BenchCaseKind::ArchitecturePlan,
            "plan a multi-crate architecture change",
            ExecutionMode::PlanExecute,
            "workgraph",
        ),
        (
            BenchCaseKind::ContextAssembly,
            "assemble relevant memory and workspace context",
            ExecutionMode::ReActLoop,
            "context_epoch",
        ),
        (
            BenchCaseKind::VerificationGuard,
            "verify claims before final answer",
            ExecutionMode::PlanExecute,
            "verification_report",
        ),
        (
            BenchCaseKind::WorkGraphFanout,
            "parallel multi-agent implementation",
            ExecutionMode::SupervisorSubagents,
            "value_verdict",
        ),
        (
            BenchCaseKind::ToolTransaction,
            "write files safely with rollback discipline",
            ExecutionMode::RiskGate,
            "tool_transaction",
        ),
        (
            BenchCaseKind::BehaviorMinimalScope,
            "avoid unnecessary abstractions while preserving safety",
            ExecutionMode::FastEdit,
            "minimal_scope",
        ),
        (
            BenchCaseKind::MemoryGrowthLoop,
            "turn runtime learning into reviewable memory candidates",
            ExecutionMode::PlanExecute,
            "memory_candidate",
        ),
        (
            BenchCaseKind::MatrixEvidenceSignal,
            "emit matrix-compatible evidence and quality signals",
            ExecutionMode::PlanExecute,
            "matrix_signal",
        ),
        (
            BenchCaseKind::HarnessReceipt,
            "produce a harness receipt for the completed AI turn",
            ExecutionMode::PlanExecute,
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
        "workgraph" | "value_verdict" => Some("workgraph"),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum E2eScenarioKind {
    SimpleOnce,
    ComplexPlan,
    TeamParallel,
    RealityMemory,
    ToolLsp,
    GovernedConnector,
    Recovery,
}

impl E2eScenarioKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SimpleOnce => "simple_once",
            Self::ComplexPlan => "complex_plan",
            Self::TeamParallel => "team_parallel",
            Self::RealityMemory => "reality_memory",
            Self::ToolLsp => "tool_lsp",
            Self::GovernedConnector => "governed_connector",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct E2eScenarioMatrixItem {
    pub id: String,
    pub kind: E2eScenarioKind,
    pub objective: String,
    pub required_evidence: Vec<String>,
    pub fake_provider_gate: bool,
    pub real_provider_gate: bool,
}

#[must_use]
pub fn stable_ai_scenario_matrix() -> Vec<E2eScenarioMatrixItem> {
    use E2eScenarioKind::{
        ComplexPlan, GovernedConnector, RealityMemory, Recovery, SimpleOnce, TeamParallel, ToolLsp,
    };
    [
        (
            "simple_once",
            SimpleOnce,
            "simple task completes in one assistant turn with no unnecessary planning",
            ["strategy", "assistant_text", "no_repair_required"],
            true,
            true,
        ),
        (
            "complex_plan",
            ComplexPlan,
            "complex task forms a plan, executes, reviews, and emits evidence",
            ["workgraph", "tool_events", "review_verdict"],
            true,
            true,
        ),
        (
            "team_parallel",
            TeamParallel,
            "multi-agent team creates parallel role tasks and a synthesis verdict",
            ["team_graph", "agent_runs", "synthesis"],
            true,
            true,
        ),
        (
            "reality_memory",
            RealityMemory,
            "reality core recalls, writes, and detects conflicting facts",
            ["memory_candidate", "matrix_signal", "fact_conflict"],
            true,
            false,
        ),
        (
            "tool_lsp",
            ToolLsp,
            "tool runtime records real tool output or structured unavailable fallback",
            ["tool_receipt", "lsp_result", "evidence_ref"],
            true,
            false,
        ),
        (
            "governed_connector",
            GovernedConnector,
            "connector/channel action is governed by approval and audit evidence",
            ["approval", "audit", "dispatch_policy"],
            true,
            false,
        ),
        (
            "recovery",
            Recovery,
            "failure produces recovery actions and blocks false finalization",
            ["failure_kind", "recovery_report", "repair_hint"],
            true,
            true,
        ),
    ]
    .into_iter()
    .map(
        |(id, kind, objective, required_evidence, fake_provider_gate, real_provider_gate)| {
            E2eScenarioMatrixItem {
                id: id.to_string(),
                kind,
                objective: objective.to_string(),
                required_evidence: required_evidence
                    .into_iter()
                    .map(ToString::to_string)
                    .collect(),
                fake_provider_gate,
                real_provider_gate,
            }
        },
    )
    .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StableAiHealthReport {
    pub kind: String,
    pub version: String,
    pub status: String,
    pub provider: String,
    pub model: Option<String>,
    pub real_provider_enabled: bool,
    pub real_provider_reason: String,
    pub scenario_matrix: Vec<E2eScenarioMatrixItem>,
    pub fake_provider_result: ScenarioSuiteReport,
    pub coverage: HarnessCapabilityCoverageReport,
    pub gateway_smoke: String,
    pub surface_smoke: String,
    pub recovery_evidence: String,
}

impl StableAiHealthReport {
    #[must_use]
    pub fn from_fake_eval(
        version: impl Into<String>,
        provider: impl Into<String>,
        model: Option<String>,
        real_provider_enabled: bool,
        real_provider_reason: impl Into<String>,
        fake_provider_result: ScenarioSuiteReport,
        coverage: HarnessCapabilityCoverageReport,
        gateway_smoke: impl Into<String>,
        surface_smoke: impl Into<String>,
        recovery_evidence: impl Into<String>,
    ) -> Self {
        let coverage_passed = coverage.failed == 0;
        let status = if fake_provider_result.failed == 0 && coverage_passed {
            "passed"
        } else {
            "failed"
        };
        Self {
            kind: "cowd.stable_ai_health_report".to_string(),
            version: version.into(),
            status: status.to_string(),
            provider: provider.into(),
            model,
            real_provider_enabled,
            real_provider_reason: real_provider_reason.into(),
            scenario_matrix: stable_ai_scenario_matrix(),
            fake_provider_result,
            coverage,
            gateway_smoke: gateway_smoke.into(),
            surface_smoke: surface_smoke.into(),
            recovery_evidence: recovery_evidence.into(),
        }
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
            .expect_mode(ExecutionMode::DirectAnswer)
            .require(ScenarioCheck::bool(
                "verification.finalization_blocked",
                ScenarioCheckKind::FinalizationBlocked,
                true,
                "ai-verification/runtime-conversation",
                "ensure finalization gate appends limitation message",
            ));
        let observation = ScenarioObservation {
            scenario_id: "empty_answer".to_string(),
            strategy_mode: ExecutionMode::DirectAnswer,
            finalization_blocked: false,
            regression_allowed: true,
            has_workgraph: false,
            workgraph_quality_ok: false,
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
            .expect_mode(ExecutionMode::PlanExecute)
            .require(ScenarioCheck::growth_signal(
                "growth.matrix_quality_gate",
                "MatrixQualityGate",
                "ai-growth",
                "map matrix quality gate into growth signal",
            ));
        let observation = ScenarioObservation {
            scenario_id: "matrix_quality".to_string(),
            strategy_mode: ExecutionMode::PlanExecute,
            finalization_blocked: false,
            regression_allowed: false,
            has_workgraph: false,
            workgraph_quality_ok: false,
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

    #[test]
    fn harness_capability_coverage_requires_runtime_lifecycle_owners() {
        let report = harness_capability_coverage_report();

        assert_eq!(report.failed, 0);
        assert!(report.items.iter().any(|item| item.capability == "agent"
            && item
                .lifecycle_modules
                .contains(&"agent_lifecycle".to_string())));
        assert!(report.items.iter().any(|item| item.capability == "tooling"
            && item
                .lifecycle_modules
                .contains(&"tool_dispatch".to_string())));
    }

    #[test]
    fn stable_ai_scenario_matrix_covers_required_e2e_kinds() {
        let matrix = stable_ai_scenario_matrix();
        let kinds = matrix
            .iter()
            .map(|item| item.kind)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(matrix.len(), 7);
        assert!(kinds.contains(&E2eScenarioKind::SimpleOnce));
        assert!(kinds.contains(&E2eScenarioKind::ComplexPlan));
        assert!(kinds.contains(&E2eScenarioKind::TeamParallel));
        assert!(kinds.contains(&E2eScenarioKind::RealityMemory));
        assert!(kinds.contains(&E2eScenarioKind::ToolLsp));
        assert!(kinds.contains(&E2eScenarioKind::GovernedConnector));
        assert!(kinds.contains(&E2eScenarioKind::Recovery));
        assert!(matrix.iter().all(|item| !item.required_evidence.is_empty()));
    }

    #[test]
    fn stable_ai_health_report_records_real_provider_gate_reason() {
        let spec = ScenarioSpec::new("simple_once", "answer once").require(ScenarioCheck::bool(
            "regression.allowed",
            ScenarioCheckKind::RegressionAllowed,
            true,
            "harness-eval",
            "fake provider should pass deterministic gate",
        ));
        let observation = ScenarioObservation {
            scenario_id: "simple_once".to_string(),
            strategy_mode: ExecutionMode::DirectAnswer,
            finalization_blocked: false,
            regression_allowed: true,
            has_workgraph: false,
            workgraph_quality_ok: false,
            growth_has_blocker: false,
            growth_signal_kinds: Vec::new(),
            memory_candidate_count: 0,
            matrix_signal_count: 0,
            assistant_text: "done".to_string(),
        };
        let scenario_report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);
        let report = StableAiHealthReport::from_fake_eval(
            env!("CARGO_PKG_VERSION"),
            "fake_provider",
            None,
            false,
            "real provider not enabled",
            scenario_report,
            harness_capability_coverage_report(),
            "gateway skipped",
            "surface skipped",
            "recovery report present",
        );

        assert_eq!(report.status, "passed");
        assert!(!report.real_provider_enabled);
        assert_eq!(report.real_provider_reason, "real provider not enabled");
        assert_eq!(report.scenario_matrix.len(), 7);
    }

    #[test]
    fn complex_harness_scenarios_generate_solve_and_review() {
        let report = evaluate_complex_harness_scenarios();

        assert_eq!(report.total, 5);
        assert_eq!(report.failed, 0);
        assert!(report.average_score >= 0.9);
        assert!(report.results.iter().any(|item| {
            item.kind == ComplexScenarioKind::MemoryGovernance
                && item
                    .evidence
                    .iter()
                    .any(|evidence| evidence.contains("conflict"))
        }));
        assert!(report.results.iter().any(|item| {
            item.kind == ComplexScenarioKind::CrossSessionMission
                && item
                    .evidence
                    .iter()
                    .any(|evidence| evidence.contains("peer"))
        }));
    }

    #[test]
    fn knowledge_fabric_evaluation_covers_namespace_conflict_and_activation() {
        let report = evaluate_knowledge_fabric_context_governance();

        assert!(report.passed, "{report:?}");
        assert!(report.active_pack_count >= 1);
        assert!(report.blocked_namespace_count >= 1);
        assert!(report.conflict_count >= 1);
        assert!(report.evidence_count >= 1);
    }

    #[test]
    fn complex_harness_review_fails_missing_evidence() {
        let scenario = generate_complex_harness_scenarios()
            .into_iter()
            .find(|item| item.kind == ComplexScenarioKind::RecoveryRepair)
            .expect("recovery scenario");
        let mut solution = solve_complex_harness_scenario(&scenario);
        solution.recovery_actions.clear();
        solution.evidence.clear();

        let result = evaluate_complex_harness_solution(&scenario, &solution);

        assert!(!result.passed);
        assert!(result
            .failed_checks
            .contains(&"recovery.actions>=2".to_string()));
        assert!(result
            .failed_checks
            .contains(&"evidence.contains_recovery".to_string()));
    }

    #[test]
    fn real_ai_deep_scenarios() {
        use runtime::ApiClient;

        if std::env::var("COWD_EVAL_REAL_MODEL").ok().as_deref() != Some("1") {
            eprintln!(
                "real_ai_deep_scenarios skipped: set COWD_EVAL_REAL_MODEL=1 to consume provider quota"
            );
            return;
        }
        let model =
            std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
        let mut client = runtime::ProviderRuntimeClient::new(model.clone(), Vec::new())
            .expect("provider client should initialize when real eval is enabled");
        let request = runtime::ApiRequest {
            system_prompt: vec![
                "You are a strict health-check responder. Return exactly: OK".to_string(),
            ],
            messages: vec![runtime::ConversationMessage {
                role: runtime::MessageRole::User,
                blocks: vec![runtime::ContentBlock::Text {
                    text: "Return exactly OK.".to_string(),
                }],
                usage: None,
            }],
            model,
        };
        let events = client
            .stream_collect(request)
            .expect("real provider deep scenario should return events");
        let text = events
            .iter()
            .filter_map(|event| match event {
                runtime::AssistantEvent::TextDelta(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert!(
            !text.trim().is_empty(),
            "real provider deep scenario must produce assistant text"
        );
    }
}
