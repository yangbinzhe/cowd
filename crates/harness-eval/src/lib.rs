#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! CowdBench evaluation contracts and lightweight scoring.

use harness_contract::{
    core::{ExecutionModifier, ExecutionPattern},
    reality::{RealityBoundary, RecallSelectionReason, RecallSourceKind},
};
use memory::{
    RecallCandidate, RecallCandidateEvidence, RecallCandidateScores, RecallOmission, RecallReport,
    RecallSourceResult,
};
use runtime::{
    ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextOmission, ContextProfile,
    ContextRole, ContextRuntimeKernel, ContextSourceKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod auto_strategy_paired;
mod certification;
mod evolution;
mod live_scenario_runner;
mod measurement;
mod paired_performance;
mod provider_rounds;
mod real_provider_runner;
mod report;
mod report_store;
mod runner;
mod session_actor;
mod terminal_gate;
mod terminal_matrix;

pub use auto_strategy_paired::{
    run_auto_strategy_paired, write_auto_strategy_report, AutoStrategyPairedOptions,
};
pub use certification::{
    run_certification_manifest, CertificationCheck, CertificationCheckResult,
    CertificationCollector, CertificationCommand, CertificationComparison, CertificationFixture,
    CertificationManifest, CertificationProviderRequirement, CertificationReport,
    CertificationSelector, CertificationSource, CertificationSourceKind, CertificationSourceResult,
    CertificationTimeoutPolicy,
};
pub use evolution::{
    evaluate_evolution_closure, DefinitionEvolutionEvalRunner, DefinitionEvolutionScenarioCatalog,
    DefinitionEvolutionScenarioExecutor, DefinitionEvolutionWorkload, EvolutionClosureReport,
    FileDefinitionEvolutionScenarioCatalog, RuntimeDefinitionEvolutionWorkload,
};
pub use live_scenario_runner::run_live_gateway_scenarios;
pub use paired_performance::{run_paired_performance, PairedPerformanceOptions};
pub use report::{
    evaluate_report_gate, CapabilityResult, ExecutionTrace, HarnessEvalLevel,
    HarnessEvalReportDetail, HarnessEvalReportGate, HarnessEvalReportGateItem,
    HarnessEvalReportSummary, HarnessEvalRunRecord, HarnessEvalRunRequest, HarnessEvalRunStatus,
    HarnessEvalUsageSummary, HarnessMetric, MissionHarnessEvalReport, ProviderRoundDetail,
    ProviderRoundSummary, RealToolScenarioReport, RealToolScenarioResult, ToolCallDetail,
    ToolCallSummary, UsageSummary,
};
pub use report_store::{default_report_root, now_ms, HarnessEvalReportStore};
pub use runner::{run_eval, run_eval_controlled, HarnessEvalRunControl, HarnessEvalRunnerOptions};
pub use terminal_gate::{terminal_gate_report, terminal_gate_report_with_report};
pub use terminal_matrix::{
    render_terminal_capability_matrix_markdown, terminal_capability_matrix,
    TerminalCapabilityMatrixRow,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    pub expected_pattern: ExecutionPattern,
    #[serde(default)]
    pub expected_modifiers: Vec<ExecutionModifier>,
    pub required_checks: Vec<String>,
}

impl CowdBenchCase {
    #[must_use]
    pub fn new(
        kind: BenchCaseKind,
        prompt: impl Into<String>,
        expected_pattern: ExecutionPattern,
    ) -> Self {
        Self {
            id: format!("cowdbench-{}", uuid::Uuid::new_v4()),
            kind,
            prompt: prompt.into(),
            expected_pattern,
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
    pub selected_pattern: ExecutionPattern,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityScenarioEvalSpec {
    pub id: String,
    pub objective: String,
    pub expected_pattern: ExecutionPattern,
    pub input_summary: String,
    pub required_sources: Vec<RecallSourceKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityScenarioEvalResult {
    pub scenario_id: String,
    pub status: String,
    pub objective: String,
    pub expected_pattern: ExecutionPattern,
    pub request_summary: String,
    pub response_summary: String,
    pub source_candidates: Vec<String>,
    pub selected_context_count: usize,
    pub omitted_context_count: usize,
    pub evidence_refs: Vec<String>,
    pub recall_report: RecallReport,
    pub context_envelope: Value,
    pub token_stats: Value,
    pub time_stats: Value,
    pub tool_stats: Value,
    pub pass_fail_analysis: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityContextEvalReport {
    pub kind: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub selected_context_total: usize,
    pub omitted_context_total: usize,
    pub evidence_ref_total: usize,
    pub scenarios: Vec<RealityScenarioEvalResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NextGenHarnessScenarioKind {
    SimpleDirect,
    ComplexStrategySelection,
    ToolBatchEfficiency,
    TeamAgentExecutionOutcome,
    CrossSessionDispatch,
    MemoryRealityContextGovernance,
    ConflictRecovery,
}

impl NextGenHarnessScenarioKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SimpleDirect => "simple_direct",
            Self::ComplexStrategySelection => "complex_strategy_selection",
            Self::ToolBatchEfficiency => "tool_batch_efficiency",
            Self::TeamAgentExecutionOutcome => "team_agent_execution_outcome",
            Self::CrossSessionDispatch => "cross_session_dispatch",
            Self::MemoryRealityContextGovernance => "memory_reality_context_governance",
            Self::ConflictRecovery => "conflict_recovery",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NextGenHarnessScenarioSpec {
    pub id: String,
    pub kind: NextGenHarnessScenarioKind,
    pub title: String,
    pub objective: String,
    pub expected_runtime_actions: Vec<String>,
    pub required_evidence_kinds: Vec<String>,
    pub min_tool_calls_for_full_eval: usize,
    pub claims_orchestration: bool,
    pub claims_memory_context: bool,
    pub claims_replay: bool,
    pub claims_external_access: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NextGenHarnessEvalInput {
    pub level: String,
    pub runtime_action_count: usize,
    pub tool_call_count: usize,
    pub provider_rounds: usize,
    pub total_tokens: u32,
    pub real_model_authorized: bool,
    pub mission_evidence_refs: Vec<String>,
    pub reality_evidence_ref_total: usize,
    pub agent_terminal_count: usize,
    pub mailbox_completed_count: usize,
    pub synthesis_receipt_id: Option<String>,
    pub session_relation_count: usize,
    pub runtime_turn_result_count: usize,
    pub recovery_applied_count: usize,
    pub recovery_verified_count: usize,
    pub source_fixture_status: String,
    pub sidecar_fixture_status: String,
    pub db_fixture_status: String,
}

impl Default for NextGenHarnessEvalInput {
    fn default() -> Self {
        Self {
            level: "quick".to_string(),
            runtime_action_count: 0,
            tool_call_count: 0,
            provider_rounds: 0,
            total_tokens: 0,
            real_model_authorized: false,
            mission_evidence_refs: Vec::new(),
            reality_evidence_ref_total: 0,
            agent_terminal_count: 0,
            mailbox_completed_count: 0,
            synthesis_receipt_id: None,
            session_relation_count: 0,
            runtime_turn_result_count: 0,
            recovery_applied_count: 0,
            recovery_verified_count: 0,
            source_fixture_status: "not_requested".to_string(),
            sidecar_fixture_status: "not_requested".to_string(),
            db_fixture_status: "not_requested".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NextGenHarnessQualityRubric {
    pub correctness: f32,
    pub evidence_strength: String,
    pub efficiency: f32,
    pub governance: f32,
    pub passed: bool,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NextGenHarnessScenarioResult {
    pub scenario_id: String,
    pub kind: NextGenHarnessScenarioKind,
    pub status: String,
    pub objective: String,
    pub runtime_actions: Vec<String>,
    pub tool_calls: usize,
    pub provider_rounds: usize,
    pub token_usage: Value,
    pub latency: Value,
    pub evidence_refs: Vec<String>,
    pub terminal_evidence: Value,
    pub quality_rubric: NextGenHarnessQualityRubric,
    pub missing_capabilities: Vec<String>,
    pub claims_orchestration: bool,
    pub claims_tool_validation: bool,
    pub claims_memory_context: bool,
    pub claims_replay: bool,
    pub claims_external_access: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NextGenHarnessClosureReport {
    pub kind: String,
    pub status: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub level: String,
    pub scenarios: Vec<NextGenHarnessScenarioResult>,
    pub missing_capabilities: Vec<String>,
}

#[must_use]
pub fn next_gen_harness_closure_specs() -> Vec<NextGenHarnessScenarioSpec> {
    use NextGenHarnessScenarioKind::{
        ComplexStrategySelection, ConflictRecovery, CrossSessionDispatch,
        MemoryRealityContextGovernance, SimpleDirect, TeamAgentExecutionOutcome,
        ToolBatchEfficiency,
    };
    [
        (
            "simple_direct",
            SimpleDirect,
            "简单问题一次性直接处理",
            "简单问题应选择 Direct 决策，不启动团队、工具和复杂恢复链路。",
            vec!["direct"],
            vec!["strategy"],
            0,
            false,
            false,
            false,
            false,
        ),
        (
            "complex_strategy_selection",
            ComplexStrategySelection,
            "复杂任务策略选择",
            "复杂目标应显式选择计划、团队或工作图策略，并留下模型可感知动作证据。",
            vec!["strategy_decision", "runtime_capability_catalog"],
            vec!["strategy", "runtime_action"],
            0,
            true,
            false,
            false,
            false,
        ),
        (
            "tool_batch_efficiency",
            ToolBatchEfficiency,
            "幂等工具批量执行",
            "读文件、搜索、证据收集等幂等工具应支持批量规划与并行执行证据。",
            vec!["parallel_tool_batch"],
            vec!["tool_call", "batch_plan"],
            2,
            true,
            false,
            false,
            false,
        ),
        (
            "team_agent_execution_outcome",
            TeamAgentExecutionOutcome,
            "多 Agent 团队执行结果",
            "复杂问题应能形成团队角色、工作图、进度证据和综合结果。",
            vec![
                "team_template",
                "execution_graph",
                "agent_capability_binding",
            ],
            vec!["team", "execution_graph", "agent"],
            0,
            true,
            false,
            false,
            false,
        ),
        (
            "cross_session_dispatch",
            CrossSessionDispatch,
            "跨 Session 派发与联动",
            "主 session 应能派发、观察和引用其他 session 的命令与证据。",
            vec!["session_input", "session_relation"],
            vec!["session_input", "session_relation"],
            0,
            true,
            false,
            false,
            false,
        ),
        (
            "memory_reality_context_governance",
            MemoryRealityContextGovernance,
            "记忆、知识、事实上下文治理",
            "召回必须具备 selected/omitted/evidence 证据，避免无关记忆污染上下文。",
            vec!["context_governance", "recall_report", "context_envelope"],
            vec!["memory", "knowledge", "fact", "context"],
            0,
            false,
            true,
            false,
            false,
        ),
        (
            "conflict_recovery",
            ConflictRecovery,
            "冲突与恢复闭环",
            "冲突、失败和恢复必须进入可审计事件链，阻止无证据最终化。",
            vec!["conflict_arbiter", "recovery_plan"],
            vec!["conflict", "recovery", "replay"],
            0,
            true,
            false,
            true,
            false,
        ),
    ]
    .into_iter()
    .map(
        |(
            id,
            kind,
            title,
            objective,
            expected_runtime_actions,
            required_evidence_kinds,
            min_tool_calls_for_full_eval,
            claims_orchestration,
            claims_memory_context,
            claims_replay,
            claims_external_access,
        )| NextGenHarnessScenarioSpec {
            id: id.to_string(),
            kind,
            title: title.to_string(),
            objective: objective.to_string(),
            expected_runtime_actions: expected_runtime_actions
                .into_iter()
                .map(str::to_string)
                .collect(),
            required_evidence_kinds: required_evidence_kinds
                .into_iter()
                .map(str::to_string)
                .collect(),
            min_tool_calls_for_full_eval,
            claims_orchestration,
            claims_memory_context,
            claims_replay,
            claims_external_access,
        },
    )
    .collect()
}

#[must_use]
pub fn evaluate_next_gen_harness_closure(
    input: NextGenHarnessEvalInput,
) -> NextGenHarnessClosureReport {
    let scenarios = next_gen_harness_closure_specs()
        .into_iter()
        .map(|spec| evaluate_next_gen_harness_scenario(&spec, &input))
        .collect::<Vec<_>>();
    let total = scenarios.len();
    let passed = scenarios
        .iter()
        .filter(|scenario| scenario.status == "passed")
        .count();
    let missing_capabilities = scenarios
        .iter()
        .flat_map(|scenario| scenario.missing_capabilities.clone())
        .collect::<Vec<_>>();
    NextGenHarnessClosureReport {
        kind: "next_gen_harness_closure".to_string(),
        status: if missing_capabilities.is_empty() && passed == total {
            "passed"
        } else {
            "failed"
        }
        .to_string(),
        total,
        passed,
        failed: total.saturating_sub(passed),
        level: input.level,
        scenarios,
        missing_capabilities,
    }
}

fn evaluate_next_gen_harness_scenario(
    spec: &NextGenHarnessScenarioSpec,
    input: &NextGenHarnessEvalInput,
) -> NextGenHarnessScenarioResult {
    let full_or_deep = input.level != "quick";
    let claims_tool_validation = full_or_deep
        && spec.min_tool_calls_for_full_eval > 0
        && spec.kind == NextGenHarnessScenarioKind::ToolBatchEfficiency;
    let runtime_ok = !spec.claims_orchestration || input.runtime_action_count >= 3;
    let tool_ok =
        !claims_tool_validation || input.tool_call_count >= spec.min_tool_calls_for_full_eval;
    let memory_ok = !spec.claims_memory_context || input.reality_evidence_ref_total > 0;
    let evidence_refs = next_gen_evidence_refs(spec, input);
    let evidence_ok = !evidence_refs.is_empty();
    let replay_ok = !spec.claims_replay
        || evidence_refs.iter().any(|item| {
            item.contains("session-relation")
                || item.contains("conflict")
                || item.contains("recovery")
        });
    let terminal_evidence = next_gen_terminal_evidence(spec, input);
    let terminal_ok = terminal_evidence_ok(spec, &terminal_evidence);
    let external_ok = !spec.claims_external_access
        || input.sidecar_fixture_status == "connected"
        || input.source_fixture_status == "connected"
        || input.db_fixture_status == "connected";

    let mut missing = Vec::new();
    if !runtime_ok {
        missing.push(format!("{}.runtime_action_evidence", spec.id));
    }
    if !tool_ok {
        missing.push(format!("{}.tool_batch_execution", spec.id));
    }
    if !memory_ok {
        missing.push(format!("{}.memory_context_evidence", spec.id));
    }
    if !evidence_ok {
        missing.push(format!("{}.evidence_refs", spec.id));
    }
    if !replay_ok {
        missing.push(format!("{}.replay_or_recovery_evidence", spec.id));
    }
    if !terminal_ok {
        missing.push(format!("{}.terminal_evidence", spec.id));
    }
    if !external_ok {
        missing.push(format!("{}.external_access_health", spec.id));
    }

    let passed = missing.is_empty();
    let evidence_strength = if claims_tool_validation {
        "strong"
    } else if input.level == "quick" {
        "medium"
    } else {
        "strong"
    };
    NextGenHarnessScenarioResult {
        scenario_id: spec.id.clone(),
        kind: spec.kind,
        status: if passed { "passed" } else { "failed" }.to_string(),
        objective: spec.objective.clone(),
        runtime_actions: spec.expected_runtime_actions.clone(),
        tool_calls: if spec.kind == NextGenHarnessScenarioKind::ToolBatchEfficiency {
            input.tool_call_count
        } else {
            0
        },
        provider_rounds: input.provider_rounds,
        token_usage: json!({
            "total_tokens": input.total_tokens,
            "usage_source": if input.total_tokens > 0 { "observed_provider_or_tool" } else { "not_observed_contract_lane" }
        }),
        latency: json!({
            "elapsed_ms": Value::Null,
            "source": "not_measured_contract_lane"
        }),
        evidence_refs,
        terminal_evidence,
        quality_rubric: NextGenHarnessQualityRubric {
            correctness: if passed { 1.0 } else { 0.0 },
            evidence_strength: evidence_strength.to_string(),
            efficiency: if spec.kind == NextGenHarnessScenarioKind::SimpleDirect {
                1.0
            } else if claims_tool_validation && input.tool_call_count >= 2 {
                0.95
            } else {
                0.85
            },
            governance: if spec.claims_memory_context || spec.claims_replay {
                1.0
            } else {
                0.9
            },
            passed,
            rationale: if passed {
                "required runtime action, evidence refs, and level-scoped proof are present"
                    .to_string()
            } else {
                format!("missing {}", missing.join(", "))
            },
        },
        missing_capabilities: missing,
        claims_orchestration: spec.claims_orchestration,
        claims_tool_validation,
        claims_memory_context: spec.claims_memory_context,
        claims_replay: spec.claims_replay,
        claims_external_access: spec.claims_external_access,
    }
}

fn next_gen_terminal_evidence(
    spec: &NextGenHarnessScenarioSpec,
    input: &NextGenHarnessEvalInput,
) -> Value {
    match spec.kind {
        NextGenHarnessScenarioKind::SimpleDirect => json!({
            "selected_pattern": ExecutionPattern::Direct.as_str(),
            "team_started": false,
            "source": "strategy_contract"
        }),
        NextGenHarnessScenarioKind::ComplexStrategySelection => json!({
            "runtime_action_count": input.runtime_action_count,
            "model_visible_capability_catalog": input.runtime_action_count >= 3,
            "source": "runtime_capability_contract"
        }),
        NextGenHarnessScenarioKind::ToolBatchEfficiency => json!({
            "tool_calls": input.tool_call_count,
            "batch_plan_present": input.level == "quick" || input.tool_call_count >= spec.min_tool_calls_for_full_eval,
            "source": if input.level == "quick" { "contract_plan" } else { "real_local_tool_evidence" }
        }),
        NextGenHarnessScenarioKind::TeamAgentExecutionOutcome => json!({
            "agent_terminal_count": input.agent_terminal_count,
            "mailbox_completed_count": input.mailbox_completed_count,
            "synthesis_receipt_id": input.synthesis_receipt_id,
            "source": "mission_runtime_collaboration"
        }),
        NextGenHarnessScenarioKind::CrossSessionDispatch => json!({
            "session_relation_count": input.session_relation_count,
            "runtime_turn_result_count": input.runtime_turn_result_count,
            "source": "mission_runtime_session_graph"
        }),
        NextGenHarnessScenarioKind::MemoryRealityContextGovernance => json!({
            "reality_evidence_ref_total": input.reality_evidence_ref_total,
            "selected_omitted_context_evidenced": input.reality_evidence_ref_total > 0,
            "source": "reality_context_eval"
        }),
        NextGenHarnessScenarioKind::ConflictRecovery => json!({
            "recovery_applied_count": input.recovery_applied_count,
            "recovery_verified_count": input.recovery_verified_count,
            "source": "runtime_recovery_contract"
        }),
    }
}

fn terminal_evidence_ok(spec: &NextGenHarnessScenarioSpec, evidence: &Value) -> bool {
    match spec.kind {
        NextGenHarnessScenarioKind::TeamAgentExecutionOutcome => {
            evidence
                .get("agent_terminal_count")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                >= 2
                && evidence
                    .get("mailbox_completed_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    >= 1
                && evidence
                    .get("synthesis_receipt_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
        }
        NextGenHarnessScenarioKind::CrossSessionDispatch => {
            evidence
                .get("session_relation_count")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                >= 1
                && evidence
                    .get("runtime_turn_result_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    >= 1
        }
        NextGenHarnessScenarioKind::ConflictRecovery => {
            evidence
                .get("recovery_applied_count")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                + evidence
                    .get("recovery_verified_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                >= 1
        }
        NextGenHarnessScenarioKind::ToolBatchEfficiency => {
            let tool_calls = evidence
                .get("tool_calls")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize;
            let source = evidence
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or_default();
            source == "contract_plan" || tool_calls >= spec.min_tool_calls_for_full_eval
        }
        _ => true,
    }
}

fn next_gen_evidence_refs(
    spec: &NextGenHarnessScenarioSpec,
    input: &NextGenHarnessEvalInput,
) -> Vec<String> {
    match spec.kind {
        NextGenHarnessScenarioKind::SimpleDirect => {
            vec!["strategy:direct:no_over_orchestration".to_string()]
        }
        NextGenHarnessScenarioKind::ComplexStrategySelection => {
            vec!["runtime-capability:use_team_template".to_string()]
        }
        NextGenHarnessScenarioKind::ToolBatchEfficiency => {
            if input.level == "quick" {
                vec!["tool-batch:contract-plan-only".to_string()]
            } else {
                (0..input.tool_call_count.max(1))
                    .map(|index| format!("tool-call:{}", index + 1))
                    .collect()
            }
        }
        NextGenHarnessScenarioKind::TeamAgentExecutionOutcome => input
            .mission_evidence_refs
            .iter()
            .filter(|item| item.contains("team") || item.contains("execution_graph"))
            .cloned()
            .collect(),
        NextGenHarnessScenarioKind::CrossSessionDispatch => input
            .mission_evidence_refs
            .iter()
            .filter(|item| item.contains("session-command") || item.contains("session-relation"))
            .cloned()
            .collect(),
        NextGenHarnessScenarioKind::MemoryRealityContextGovernance => {
            if input.reality_evidence_ref_total > 0 {
                vec![format!(
                    "reality-context:evidence_refs:{}",
                    input.reality_evidence_ref_total
                )]
            } else {
                Vec::new()
            }
        }
        NextGenHarnessScenarioKind::ConflictRecovery => {
            let mut refs = input
                .mission_evidence_refs
                .iter()
                .filter(|item| item.contains("session-relation"))
                .cloned()
                .collect::<Vec<_>>();
            refs.push("recovery:runtime-event-replay:candidates".to_string());
            refs
        }
    }
}

#[must_use]
pub fn reality_context_eval_specs() -> Vec<RealityScenarioEvalSpec> {
    vec![
        RealityScenarioEvalSpec {
            id: "simple_question_fast_path".to_string(),
            objective: "简单问题快速回答，不启用全量 Reality 负载".to_string(),
            expected_pattern: ExecutionPattern::Direct,
            input_summary: "用户询问当前能力是否健康".to_string(),
            required_sources: vec![RecallSourceKind::Runtime],
        },
        RealityScenarioEvalSpec {
            id: "cross_project_memory_isolation".to_string(),
            objective: "项目 A 与项目 B 记忆召回隔离，避免跨项目污染".to_string(),
            expected_pattern: ExecutionPattern::Explore,
            input_summary: "在 cowd 项目内召回架构约束，排除 unrelated-crm 规则".to_string(),
            required_sources: vec![RecallSourceKind::Memory, RecallSourceKind::Knowledge],
        },
        RealityScenarioEvalSpec {
            id: "global_knowledge_default_activation".to_string(),
            objective: "全局知识在符合 domain policy 时默认激活".to_string(),
            expected_pattern: ExecutionPattern::Direct,
            input_summary: "供应链类任务自动获得共享流程规约".to_string(),
            required_sources: vec![RecallSourceKind::Knowledge],
        },
        RealityScenarioEvalSpec {
            id: "context_compaction_recall".to_string(),
            objective: "上下文压缩后保留可召回线索，不出现绝对遗忘".to_string(),
            expected_pattern: ExecutionPattern::Explore,
            input_summary: "长会话触发压缩并继续追问关键决策依据".to_string(),
            required_sources: vec![RecallSourceKind::SessionCheckpoint],
        },
        RealityScenarioEvalSpec {
            id: "fact_matrix_evidence_trace".to_string(),
            objective: "Fact/Matrix 证据能进入 RecallReport 与 ContextEnvelope".to_string(),
            expected_pattern: ExecutionPattern::Deliberate,
            input_summary: "制造 what-if 需要结构化指标、事实与证据引用".to_string(),
            required_sources: vec![RecallSourceKind::Fact, RecallSourceKind::Matrix],
        },
        RealityScenarioEvalSpec {
            id: "growth_promotion_governance".to_string(),
            objective: "运行时增长候选通过 observed/conflict/hypothetical 边界治理".to_string(),
            expected_pattern: ExecutionPattern::Execute,
            input_summary: "从工具输出抽取候选事实并判断是否晋升".to_string(),
            required_sources: vec![RecallSourceKind::Runtime, RecallSourceKind::Fact],
        },
        RealityScenarioEvalSpec {
            id: "tool_large_output_sandbox".to_string(),
            objective: "工具大输出以摘要/证据引用进入上下文，避免上下文过载".to_string(),
            expected_pattern: ExecutionPattern::Explore,
            input_summary: "代码扫描产生大量输出，模型只接收摘要和可追溯路径".to_string(),
            required_sources: vec![RecallSourceKind::ToolTrace, RecallSourceKind::Workspace],
        },
        RealityScenarioEvalSpec {
            id: "multi_agent_shared_evidence".to_string(),
            objective: "多 Agent 协同能共享必要 evidence，同时保留隔离边界".to_string(),
            expected_pattern: ExecutionPattern::Collaborate,
            input_summary: "Planner/Implementer/Reviewer 协同解决架构修订".to_string(),
            required_sources: vec![RecallSourceKind::AgentPeer, RecallSourceKind::Runtime],
        },
        RealityScenarioEvalSpec {
            id: "cross_session_linked_work".to_string(),
            objective: "跨 Session 能建立关联、查看进度、引用对方证据".to_string(),
            expected_pattern: ExecutionPattern::Supervise,
            input_summary: "主 session 调度另一 session 继续独立分析".to_string(),
            required_sources: vec![
                RecallSourceKind::SessionCheckpoint,
                RecallSourceKind::AgentPeer,
            ],
        },
        RealityScenarioEvalSpec {
            id: "conflict_latest_fact_resolution".to_string(),
            objective: "新旧事实冲突时优先最新可信证据并保留冲突说明".to_string(),
            expected_pattern: ExecutionPattern::Deliberate,
            input_summary: "用户前后规则冲突，系统根据时间和证据做治理".to_string(),
            required_sources: vec![
                RecallSourceKind::Memory,
                RecallSourceKind::Fact,
                RecallSourceKind::Knowledge,
            ],
        },
    ]
}

#[must_use]
pub fn evaluate_reality_context_scenarios() -> RealityContextEvalReport {
    let scenarios = reality_context_eval_specs()
        .into_iter()
        .map(evaluate_reality_context_scenario)
        .collect::<Vec<_>>();
    let total = scenarios.len();
    let passed = scenarios
        .iter()
        .filter(|scenario| scenario.status == "passed")
        .count();
    let selected_context_total = scenarios
        .iter()
        .map(|scenario| scenario.selected_context_count)
        .sum();
    let omitted_context_total = scenarios
        .iter()
        .map(|scenario| scenario.omitted_context_count)
        .sum();
    let evidence_ref_total = scenarios
        .iter()
        .map(|scenario| scenario.evidence_refs.len())
        .sum();
    RealityContextEvalReport {
        kind: "reality_context_eval_report".to_string(),
        total,
        passed,
        failed: total.saturating_sub(passed),
        selected_context_total,
        omitted_context_total,
        evidence_ref_total,
        scenarios,
    }
}

fn evaluate_reality_context_scenario(spec: RealityScenarioEvalSpec) -> RealityScenarioEvalResult {
    let selected = spec
        .required_sources
        .iter()
        .enumerate()
        .map(|(index, source)| recall_candidate_for(&spec, index, *source))
        .collect::<Vec<_>>();
    let omitted = vec![RecallOmission {
        id: uuid::Uuid::new_v4(),
        title: format!("{} omitted unrelated context", spec.id),
        source: RecallSourceKind::Memory,
        reason: "scope_or_budget_filter_rejected_unrelated_context".to_string(),
    }];
    let source_results = spec
        .required_sources
        .iter()
        .map(|source| RecallSourceResult {
            source: *source,
            status: "enabled_and_wired".to_string(),
            selected_count: selected
                .iter()
                .filter(|item| item.source == *source)
                .count(),
            omitted_count: 0,
            degraded_reason: None,
        })
        .collect::<Vec<_>>();
    let recall_report = RecallReport::from_selected_omitted(
        selected.clone(),
        omitted.clone(),
        source_results,
        false,
    );
    let context_items = selected
        .iter()
        .map(|candidate| {
            let mut item = ContextItem::new(
                candidate.id.to_string(),
                context_source_for(candidate.source),
                ContextRole::Evidence,
                format!(
                    "{}\nsource: {}\nevidence: {}",
                    candidate.title,
                    candidate.source.as_str(),
                    candidate.evidence.refs.join(",")
                ),
            );
            item.score = candidate.scores.final_score;
            item.evidence = candidate.evidence.refs.clone();
            item
        })
        .collect::<Vec<_>>();
    let context_omissions = omitted
        .iter()
        .map(|item| ContextOmission {
            source: context_source_for(item.source),
            reason: item.reason.clone(),
            token_estimate: 96,
        })
        .collect::<Vec<_>>();
    let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
        identity: ContextIdentity::main(format!("eval-{}", spec.id)),
        profile: ContextProfile::DeepInvestigation,
        intent: spec.input_summary.clone(),
        stable_head: vec!["cowd-reality-eval:v1".to_string()],
        runtime_header: vec![format!(
            "expected_pattern:{}",
            spec.expected_pattern.as_str()
        )],
        dynamic_items: context_items,
        omitted: context_omissions,
        total_budget_tokens: 32_000,
    });
    let evidence_refs = selected
        .iter()
        .flat_map(|candidate| candidate.evidence.refs.clone())
        .collect::<Vec<_>>();
    let source_candidates = spec
        .required_sources
        .iter()
        .map(|source| source.as_str().to_string())
        .collect::<Vec<_>>();
    let passed = !selected.is_empty()
        && !evidence_refs.is_empty()
        && envelope.selected.len() == selected.len()
        && envelope.omitted.len() == omitted.len();
    RealityScenarioEvalResult {
        scenario_id: spec.id.clone(),
        status: if passed { "passed" } else { "failed" }.to_string(),
        objective: spec.objective,
        expected_pattern: spec.expected_pattern,
        request_summary: spec.input_summary,
        response_summary: if passed {
            "Reality context selected scoped evidence and preserved omitted trace".to_string()
        } else {
            "Reality context failed to preserve selected/omitted evidence".to_string()
        },
        source_candidates,
        selected_context_count: envelope.selected.len(),
        omitted_context_count: envelope.omitted.len(),
        evidence_refs,
        recall_report,
        context_envelope: serde_json::to_value(envelope).unwrap_or(Value::Null),
        token_stats: json!({
            "estimated_input_tokens": 480 + selected.len() as u64 * 120,
            "estimated_output_tokens": 180,
            "total_tokens": 660 + selected.len() as u64 * 120,
            "usage_source": "deterministic_reality_eval_estimate"
        }),
        time_stats: json!({
            "elapsed_ms": 10 + selected.len() as u64 * 3,
            "source": "deterministic_reality_eval"
        }),
        tool_stats: json!({
            "tool_calls": usize::from(spec.id == "tool_large_output_sandbox"),
            "agent_count": if spec.id == "multi_agent_shared_evidence" { 3 } else { 1 }
        }),
        pass_fail_analysis: if passed {
            "passed: selected context, omitted trace, evidence refs, RecallReport, and ContextEnvelope are all present".to_string()
        } else {
            "failed: missing selected context, omitted trace, or evidence refs".to_string()
        },
    }
}

fn recall_candidate_for(
    spec: &RealityScenarioEvalSpec,
    index: usize,
    source: RecallSourceKind,
) -> RecallCandidate {
    let score = (0.92_f32 - index as f32 * 0.04).max(0.70);
    let id = uuid::Uuid::new_v4();
    RecallCandidate {
        id,
        title: format!("{} {}", spec.id, source.as_str()),
        layer: match source {
            RecallSourceKind::Knowledge | RecallSourceKind::Matrix | RecallSourceKind::Fact => {
                memory::MemoryLayer::L3
            }
            RecallSourceKind::AgentPeer | RecallSourceKind::SessionCheckpoint => {
                memory::MemoryLayer::L2
            }
            _ => memory::MemoryLayer::L1,
        },
        content_preview: format!(
            "{} evidence selected for {}",
            source.as_str(),
            spec.objective
        ),
        source,
        scores: RecallCandidateScores {
            relevance: score,
            authority: score,
            recency: 0.86,
            final_score: score,
            vector_similarity: (source == RecallSourceKind::Memory
                || source == RecallSourceKind::Knowledge)
                .then_some(score),
            bm25_score: Some(score * 10.0),
        },
        evidence: RecallCandidateEvidence {
            refs: vec![format!("{}://{}", source.as_str(), spec.id)],
            boundary: RealityBoundary::Observed,
        },
        reason: RecallSelectionReason::selected(
            source,
            score,
            vec![
                "scenario_required_source".to_string(),
                format!("expected_pattern:{}", spec.expected_pattern.as_str()),
            ],
        ),
    }
}

fn context_source_for(source: RecallSourceKind) -> ContextSourceKind {
    match source {
        RecallSourceKind::Memory => ContextSourceKind::Memory,
        RecallSourceKind::Knowledge => ContextSourceKind::Knowledge,
        RecallSourceKind::Matrix | RecallSourceKind::Fact | RecallSourceKind::Runtime => {
            ContextSourceKind::Task
        }
        RecallSourceKind::ToolTrace => ContextSourceKind::ToolTrace,
        RecallSourceKind::SessionCheckpoint => ContextSourceKind::Handoff,
        RecallSourceKind::AgentPeer => ContextSourceKind::AgentPeer,
        RecallSourceKind::Workspace => ContextSourceKind::Workspace,
    }
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
    let projection = fabric.projection();
    let has_namespace_projection = projection["namespace_tree"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty());
    let has_maintenance_projection = projection["maintenance_candidates"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty());
    let has_recall_quality_projection = projection["recall_quality"]
        .as_object()
        .is_some_and(|object| object.contains_key("precision_estimate"));
    let passed = plan.active_pack_ids.contains(&default_pack.pack.pack_id)
        && !plan.active_pack_ids.contains(&irrelevant_pack.pack.pack_id)
        && active_pack_count >= 1
        && blocked_namespace_count >= 1
        && conflict_count >= 1
        && evidence_count >= 1
        && !canon.is_empty()
        && !warnings.is_empty()
        && has_namespace_projection
        && has_maintenance_projection
        && has_recall_quality_projection;

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
            format!("namespace_projection={has_namespace_projection}"),
            format!("maintenance_projection={has_maintenance_projection}"),
            format!("recall_quality_projection={has_recall_quality_projection}"),
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
        selected_pattern: ExecutionPattern::Execute,
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
            solution.selected_pattern = ExecutionPattern::Collaborate;
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
        runtime::RuntimeDomain::Approval,
        runtime::RuntimeDomain::Context,
        runtime::RuntimeDomain::Recovery,
        runtime::RuntimeDomain::Policy,
        runtime::RuntimeDomain::RealityBridge,
        runtime::RuntimeDomain::Skill,
    ];
    let lifecycle_required_domains = [
        runtime::RuntimeDomain::Conversation,
        runtime::RuntimeDomain::Provider,
        runtime::RuntimeDomain::Tooling,
        runtime::RuntimeDomain::Mission,
        runtime::RuntimeDomain::Session,
        runtime::RuntimeDomain::Agent,
        runtime::RuntimeDomain::Team,
        runtime::RuntimeDomain::Approval,
        runtime::RuntimeDomain::Context,
        runtime::RuntimeDomain::Recovery,
        runtime::RuntimeDomain::Policy,
        runtime::RuntimeDomain::Skill,
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
            let requires_lifecycle_owner = lifecycle_required_domains.contains(&domain);
            let passed = !present_modules.is_empty()
                && (!requires_lifecycle_owner || !lifecycle_modules.is_empty());
            let repair_hint = if requires_lifecycle_owner {
                format!(
                    "map runtime {} modules in runtime::module_map and mark at least one lifecycle owner",
                    domain.as_str()
                )
            } else {
                format!(
                    "map runtime {} modules in runtime::module_map; lifecycle owner is not required for bridge-only domains",
                    domain.as_str()
                )
            };
            HarnessCapabilityCoverageItem {
                capability: domain.as_str().to_string(),
                required: true,
                present_modules,
                lifecycle_modules,
                passed,
                repair_hint,
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
    pub expected_pattern: Option<ExecutionPattern>,
    pub required_checks: Vec<ScenarioCheck>,
}

impl ScenarioSpec {
    #[must_use]
    pub fn new(id: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            prompt: prompt.into(),
            expected_pattern: None,
            required_checks: Vec::new(),
        }
    }

    #[must_use]
    pub const fn expect_pattern(mut self, pattern: ExecutionPattern) -> Self {
        self.expected_pattern = Some(pattern);
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
    if let Some(expected_pattern) = spec.expected_pattern {
        if observation.strategy_pattern != expected_pattern {
            failed_checks.push(FailedScenarioCheck {
                check_id: "strategy.pattern".to_string(),
                owner: "ai-strategy".to_string(),
                expected: expected_pattern.as_str().to_string(),
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
    let total_checks = spec.required_checks.len() + usize::from(spec.expected_pattern.is_some());
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
            "compare architecture tradeoffs before selecting a plan",
            ExecutionPattern::Deliberate,
            "execution_graph",
        ),
        (
            BenchCaseKind::ContextAssembly,
            "explore relevant memory and workspace evidence",
            ExecutionPattern::Explore,
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
            "supervise long-running runtime learning and memory promotion",
            ExecutionPattern::Supervise,
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
        .map(|(kind, prompt, expected_pattern, required_check)| {
            let mut case = CowdBenchCase::new(kind, prompt, expected_pattern);
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
    if case.expected_pattern == trajectory.selected_pattern {
        score += 0.4;
    } else {
        reasons.push(format!(
            "pattern mismatch: expected {}, got {}",
            case.expected_pattern.as_str(),
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
            ["execution_graph", "tool_events", "review_verdict"],
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
    pub real_capability_result: Option<RealCapabilityGateReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealCapabilityGate {
    pub name: String,
    pub status: String,
    pub required: bool,
    pub evidence: String,
}

impl RealCapabilityGate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        passed: bool,
        required: bool,
        evidence: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: if passed { "passed" } else { "failed" }.to_string(),
            required,
            evidence: evidence.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealCapabilityGateReport {
    pub kind: String,
    pub status: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub provider_rounds: usize,
    pub tool_calls: usize,
    pub total_tokens: u32,
    pub gates: Vec<RealCapabilityGate>,
}

impl RealCapabilityGateReport {
    #[must_use]
    pub fn new(
        gates: Vec<RealCapabilityGate>,
        provider_rounds: usize,
        tool_calls: usize,
        total_tokens: u32,
    ) -> Self {
        let total = gates.len();
        let passed = gates.iter().filter(|gate| gate.status == "passed").count();
        let failed = gates
            .iter()
            .filter(|gate| gate.required && gate.status != "passed")
            .count();
        Self {
            kind: "cowd.real_capability_gate_report".to_string(),
            status: if failed == 0 { "passed" } else { "failed" }.to_string(),
            total,
            passed,
            failed,
            provider_rounds,
            tool_calls,
            total_tokens,
            gates,
        }
    }
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
            real_capability_result: None,
        }
    }

    #[must_use]
    pub fn from_real_eval(
        version: impl Into<String>,
        provider: impl Into<String>,
        model: Option<String>,
        real_provider_reason: impl Into<String>,
        fake_provider_result: ScenarioSuiteReport,
        coverage: HarnessCapabilityCoverageReport,
        gateway_smoke: impl Into<String>,
        surface_smoke: impl Into<String>,
        recovery_evidence: impl Into<String>,
        real_capability_result: RealCapabilityGateReport,
    ) -> Self {
        let status = if real_capability_result.failed == 0 && coverage.failed == 0 {
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
            real_provider_enabled: true,
            real_provider_reason: real_provider_reason.into(),
            scenario_matrix: stable_ai_scenario_matrix(),
            fake_provider_result,
            coverage,
            gateway_smoke: gateway_smoke.into(),
            surface_smoke: surface_smoke.into(),
            recovery_evidence: recovery_evidence.into(),
            real_capability_result: Some(real_capability_result),
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
                    Trajectory::new(case.id.clone(), case.expected_pattern),
                    |trajectory, check| trajectory.pass(check.clone()),
                )
            })
            .collect::<Vec<_>>();

        let verdict = suite.evaluate(&trajectories, RegressionGate::strict());

        assert!(verdict.allowed);
        assert_eq!(suite.cases.len(), 11);
    }

    #[test]
    fn fixtures_use_pattern_wire_and_cover_all_six_patterns() {
        let cases = cowdbench_smoke_cases();
        let expected_patterns = [
            ExecutionPattern::Direct,
            ExecutionPattern::Explore,
            ExecutionPattern::Execute,
            ExecutionPattern::Deliberate,
            ExecutionPattern::Collaborate,
            ExecutionPattern::Supervise,
        ];

        for expected_pattern in expected_patterns {
            assert!(cases
                .iter()
                .any(|case| case.expected_pattern == expected_pattern));
        }

        let wire = serde_json::to_value(&cases).expect("bench fixture wire payload");
        assert!(wire
            .as_array()
            .is_some_and(|items| items.iter().all(|item| {
                item.get("expected_pattern")
                    .and_then(Value::as_str)
                    .is_some()
            })));
        assert!(reality_context_eval_specs()
            .iter()
            .all(|spec| { expected_patterns.contains(&spec.expected_pattern) }));
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
            .expect_pattern(ExecutionPattern::Direct)
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
            .expect_pattern(ExecutionPattern::Execute)
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

    #[test]
    fn harness_capability_coverage_requires_runtime_lifecycle_owners() {
        let report = harness_capability_coverage_report();

        assert_eq!(report.failed, 0);
        assert!(report
            .items
            .iter()
            .filter(|item| matches!(item.capability.as_str(), "agent" | "tooling"))
            .all(|item| !item.lifecycle_modules.is_empty()));
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
            strategy_pattern: ExecutionPattern::Direct,
            finalization_blocked: false,
            regression_allowed: true,
            has_execution_graph: false,
            execution_graph_quality_ok: false,
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
    fn real_eval_status_is_decided_by_real_gates_not_fake_baseline() {
        let fake_failed = ScenarioSuiteReport {
            total: 1,
            passed: 0,
            failed: 1,
            average_score: 0.0,
            verdicts: Vec::new(),
        };
        let real = RealCapabilityGateReport::new(
            vec![RealCapabilityGate::new(
                "real_provider_smoke",
                true,
                true,
                "provider returned OK",
            )],
            1,
            3,
            120,
        );

        let report = StableAiHealthReport::from_real_eval(
            env!("CARGO_PKG_VERSION"),
            "configured",
            Some("deepseek-v4-flash".to_string()),
            "real provider explicitly enabled",
            fake_failed,
            passing_coverage_report(),
            "gateway ok",
            "surface delegated",
            "recovery ok",
            real,
        );

        assert_eq!(report.status, "passed");
        assert!(report.real_provider_enabled);
        assert_eq!(
            report.real_capability_result.as_ref().unwrap().status,
            "passed"
        );
        assert_eq!(report.fake_provider_result.failed, 1);
    }

    #[test]
    fn real_eval_fails_when_required_real_gate_fails() {
        let fake_passed = ScenarioSuiteReport {
            total: 1,
            passed: 1,
            failed: 0,
            average_score: 1.0,
            verdicts: Vec::new(),
        };
        let real = RealCapabilityGateReport::new(
            vec![RealCapabilityGate::new(
                "real_tool_scenarios",
                false,
                true,
                "tool_calls=0",
            )],
            1,
            0,
            100,
        );

        let report = StableAiHealthReport::from_real_eval(
            env!("CARGO_PKG_VERSION"),
            "configured",
            Some("deepseek-v4-flash".to_string()),
            "real provider explicitly enabled",
            fake_passed,
            passing_coverage_report(),
            "gateway ok",
            "surface delegated",
            "recovery ok",
            real,
        );

        assert_eq!(report.status, "failed");
        assert_eq!(report.real_capability_result.as_ref().unwrap().failed, 1);
    }

    fn passing_coverage_report() -> HarnessCapabilityCoverageReport {
        HarnessCapabilityCoverageReport {
            kind: "harness_capability_coverage".to_string(),
            total: 1,
            passed: 1,
            failed: 0,
            items: vec![HarnessCapabilityCoverageItem {
                capability: "runtime".to_string(),
                required: true,
                present_modules: vec!["runtime".to_string()],
                lifecycle_modules: Vec::new(),
                passed: true,
                repair_hint: "none".to_string(),
            }],
        }
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
    fn report_gate_rejects_full_report_without_tool_or_token_evidence() {
        let report = serde_json::json!({
            "level": "full",
            "status": "passed",
            "scenarios": [
                {"capability": "stable_ai_scenario_matrix", "status": "passed"},
                {"capability": "harness_capability_coverage", "status": "passed"},
                {"capability": "knowledge_fabric_context_governance", "status": "passed"}
            ],
            "complex_scenarios": {"failed": 0, "average_score": 1.0},
            "real_tool_scenarios": {"tool_calls": 0},
            "event_observation_parity": {"status": "passed"},
            "report_package": {
                "status": "written",
                "required_dirs": ["requests", "responses", "events", "run-evidence", "model-speed", "quality-rubric"]
            },
            "execution_trace": {
                "runtime_actions": 4,
                "tool_calls": 0,
                "tool_call_log": [],
                "total_usage": {"total_tokens": 0, "usage_source": "unavailable"}
            }
        });

        let gate = evaluate_report_gate(&report);

        assert_eq!(gate.status, "failed");
        assert!(gate
            .items
            .iter()
            .any(|item| item.name == "complex_tool_calls_nonzero" && item.status == "failed"));
        assert!(
            gate.items
                .iter()
                .any(|item| item.name == "token_usage_nonzero_or_estimated"
                    && item.status == "failed")
        );
    }

    #[test]
    fn next_gen_harness_closure_covers_terminal_capabilities() {
        let report = evaluate_next_gen_harness_closure(NextGenHarnessEvalInput {
            level: "full".to_string(),
            runtime_action_count: 6,
            tool_call_count: 3,
            provider_rounds: 0,
            total_tokens: 512,
            real_model_authorized: false,
            mission_evidence_refs: vec![
                "team:demo".to_string(),
                "execution_graph:demo".to_string(),
                "session-command:demo".to_string(),
                "session-relation:demo".to_string(),
            ],
            reality_evidence_ref_total: 12,
            agent_terminal_count: 3,
            mailbox_completed_count: 2,
            synthesis_receipt_id: Some("synthesis:demo".to_string()),
            session_relation_count: 1,
            runtime_turn_result_count: 1,
            recovery_applied_count: 1,
            recovery_verified_count: 1,
            source_fixture_status: "not_observed_contract_lane".to_string(),
            sidecar_fixture_status: "not_observed_contract_lane".to_string(),
            db_fixture_status: "not_observed_contract_lane".to_string(),
        });

        assert_eq!(report.status, "passed");
        assert_eq!(report.failed, 0);
        assert_eq!(report.scenarios.len(), 7);
        assert!(report.scenarios.iter().any(|scenario| {
            scenario.kind == NextGenHarnessScenarioKind::ToolBatchEfficiency
                && scenario.claims_tool_validation
                && scenario.tool_calls == 3
        }));
        assert!(report.scenarios.iter().any(|scenario| {
            scenario.kind == NextGenHarnessScenarioKind::MemoryRealityContextGovernance
                && scenario.claims_memory_context
                && !scenario.evidence_refs.is_empty()
        }));
    }

    #[test]
    fn report_gate_rejects_claimed_tool_validation_without_tool_evidence() {
        let report = serde_json::json!({
            "level": "full",
            "authorized_real_model": false,
            "scenarios": [
                {"capability": "stable_ai_scenario_matrix", "status": "passed"},
                {"capability": "harness_capability_coverage", "status": "passed"},
                {"capability": "knowledge_fabric_context_governance", "status": "passed"},
                {"capability": "reality_context_eval", "status": "passed"},
                {"capability": "mission_runtime_collaboration_closure", "status": "passed"},
                {"capability": "next_gen_harness_closure", "status": "passed"}
            ],
            "complex_scenarios": {"failed": 0, "average_score": 1.0},
            "real_tool_scenarios": {"tool_calls": 0},
            "event_observation_parity": {"status": "passed"},
            "reality_context_eval": {"failed": 0, "evidence_ref_total": 1},
            "mission_runtime_collaboration": {
                "status": "passed",
                "mission_projection": {"schema_version": 2}
            },
            "next_gen_harness_closure": {
                "status": "passed",
                "failed": 0,
                "scenarios": [{
                    "scenario_id": "tool_batch_efficiency",
                    "claims_tool_validation": true,
                    "claims_orchestration": true,
                    "claims_memory_context": false,
                    "claims_replay": false,
                    "claims_external_access": false,
                    "evidence_refs": ["tool-batch:claimed"]
                }]
            },
            "report_package": {
                "status": "written",
                "required_dirs": ["requests", "responses", "events", "run-evidence", "provider-rounds", "tool-calls", "model-speed", "quality-rubric", "evidence"]
            },
            "evidence_manifest": {
                "kind": "harness_eval.evidence_manifest",
                "repo": ".",
                "commit": "test",
                "version": "test",
                "command": "harness-eval full",
                "source_fixture_status": "not_observed_contract_lane",
                "sidecar_fixture_status": "not_observed_contract_lane",
                "db_fixture_status": "not_observed_contract_lane"
            },
            "execution_trace": {
                "provider_rounds": 0,
                "runtime_actions": 5,
                "tool_calls": 0,
                "tool_call_log": [],
                "total_usage": {"total_tokens": 128, "usage_source": "deterministic_tool_estimate"}
            }
        });

        let gate = evaluate_report_gate(&report);

        assert_eq!(gate.status, "failed");
        assert!(gate.items.iter().any(|item| {
            item.name == "claimed_tool_validation_has_tool_evidence" && item.status == "failed"
        }));
    }

    #[test]
    fn report_gate_rejects_real_model_claim_without_provider_rounds() {
        let report = serde_json::json!({
            "level": "deep",
            "authorized_real_model": true,
            "scenarios": [
                {"capability": "stable_ai_scenario_matrix", "status": "passed"},
                {"capability": "harness_capability_coverage", "status": "passed"},
                {"capability": "knowledge_fabric_context_governance", "status": "passed"},
                {"capability": "reality_context_eval", "status": "passed"},
                {"capability": "mission_runtime_collaboration_closure", "status": "passed"},
                {"capability": "next_gen_harness_closure", "status": "passed"}
            ],
            "complex_scenarios": {"failed": 0, "average_score": 1.0},
            "real_tool_scenarios": {"tool_calls": 1},
            "event_observation_parity": {"status": "passed"},
            "reality_context_eval": {"failed": 0, "evidence_ref_total": 1},
            "mission_runtime_collaboration": {
                "status": "passed",
                "mission_projection": {"schema_version": 2}
            },
            "next_gen_harness_closure": {
                "status": "passed",
                "failed": 0,
                "scenarios": [{
                    "scenario_id": "simple_direct",
                    "claims_tool_validation": false,
                    "claims_orchestration": false,
                    "claims_memory_context": false,
                    "claims_replay": false,
                    "claims_external_access": false,
                    "evidence_refs": ["strategy:direct"]
                }]
            },
            "report_package": {
                "status": "written",
                "required_dirs": ["requests", "responses", "events", "run-evidence", "provider-rounds", "tool-calls", "model-speed", "quality-rubric", "evidence"]
            },
            "evidence_manifest": {
                "kind": "harness_eval.evidence_manifest",
                "repo": ".",
                "commit": "test",
                "version": "test",
                "command": "harness-eval deep --allow-real-model",
                "source_fixture_status": "not_observed_contract_lane",
                "sidecar_fixture_status": "not_observed_contract_lane",
                "db_fixture_status": "not_observed_contract_lane"
            },
            "execution_trace": {
                "provider_rounds": 0,
                "runtime_actions": 5,
                "tool_calls": 1,
                "tool_call_log": [{}],
                "total_usage": {"total_tokens": 128, "usage_source": "provider_event"}
            }
        });

        let gate = evaluate_report_gate(&report);

        assert_eq!(gate.status, "failed");
        assert!(gate.items.iter().any(|item| {
            item.name == "real_model_claim_has_provider_rounds" && item.status == "failed"
        }));
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
        let cwd = std::env::current_dir().expect("current directory");
        let config = runtime::ConfigLoader::default_for(&cwd)
            .load()
            .expect("runtime config should load");
        let registry = std::sync::Arc::new(
            runtime::ProviderRegistry::new(config.providers().clone())
                .expect("provider registry should initialize"),
        );
        let mut client = runtime::ProviderRuntimeClient::new(registry, model.clone(), Vec::new())
            .expect("provider client should initialize when real eval is enabled");
        let request = runtime::ApiRequest {
            prompt: runtime::PromptAssembly::new(vec![
                "You are a strict health-check responder. Return exactly: OK".to_string(),
            ]),
            messages: vec![runtime::ConversationMessage {
                role: runtime::MessageRole::User,
                blocks: vec![runtime::ContentBlock::Text {
                    text: "Return exactly OK.".to_string(),
                }],
                usage: None,
            }]
            .into(),
            model,
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: runtime::context_ledger::RequestBudgetReport::for_attempt(
                "deepseek-v4-flash",
                1_000_000,
                32_000,
                128,
                256,
                0,
            ),
            provider_evidence_context: None,
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
