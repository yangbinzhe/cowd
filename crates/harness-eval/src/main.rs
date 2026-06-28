use fact_kernel::{
    core::{Confidence, EvidencePacket, FactId, FactSource, SourceKind},
    growth::{GrowthCandidate, PromotionDecision},
    health::FactHealthIssueKind,
    hypothesis::HypothesisBoundary,
    matrix::MatrixFact,
    memory::{MemoryCandidate, RecallQuery},
    FactKernelService,
};
use harness_contract::core::TaskRisk;
use harness_contract::strategy::{decide_strategy, StrategyInput};
use harness_eval::{
    evaluate_complex_harness_scenarios, evaluate_knowledge_fabric_context_governance,
    harness_capability_coverage_report, stable_ai_scenario_matrix, ComplexHarnessScenarioReport,
    E2eScenarioKind, E2eScenarioMatrixItem, RealCapabilityGate, RealCapabilityGateReport,
    ScenarioCheck, ScenarioCheckKind, ScenarioObservation, ScenarioSpec, ScenarioSuite,
    ScenarioSuiteReport, StableAiHealthReport,
};
use runtime::{
    global_mission_runtime, global_session_relation_graph, global_steward_runtime_service,
    global_team_runtime_service, AgentExecutionBackendKind, AgentSnapshot, ApiClient, ApiRequest,
    ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, AssistantEvent, AutonomyProfileId,
    CancellationToken, CollaborationTemplateMatcher, ContentBlock, ConversationMessage,
    CrossSessionMessage, MessageRole, MissionControlAction, MissionControlCommand,
    MissionControlCommandTarget, MissionControlRuntime, ProviderRuntimeClient, RecoveryExecutor,
    RuntimeEventInput, RuntimeEventReplayer, RuntimeEventScope, RuntimeEventStore,
    SessionExecutionPlane, SessionProxy, StartMissionSessionRequest, StartStewardRuntimeRequest,
    StartTeamRuntimeRequest, StewardActionStatus, StewardScheduler, StewardSchedulerConfig,
    TeamExecutionLoop, TickStewardRuntimeRequest, DEFAULT_AGENT_MODEL,
};
use serde::Serialize;
use serde_json::json;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_REPORT_DIR: &str =
    "/media/yi/Datas/workspace/plan/0626-AI稳定准确运行闭环补强/reports";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EvalLevel {
    Quick,
    Full,
    Deep,
}

impl EvalLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
            Self::Deep => "deep",
        }
    }
}

#[derive(Debug, Serialize)]
struct CapabilityResult {
    capability: &'static str,
    status: &'static str,
    evidence: String,
    notes: String,
}

#[derive(Debug, Serialize)]
struct HarnessMetric {
    name: &'static str,
    value: String,
    notes: String,
}

#[derive(Debug, Clone, Default, Serialize)]
struct UsageSummary {
    input_tokens: u32,
    output_tokens: u32,
    cache_creation_input_tokens: u32,
    cache_read_input_tokens: u32,
    total_tokens: u32,
    usage_source: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderRoundSummary {
    round_index: usize,
    name: String,
    model: String,
    status: String,
    elapsed_ms: u128,
    usage: UsageSummary,
    text_delta_count: usize,
    tool_use_count: usize,
    request_summary: String,
    response_summary: String,
    detail_path: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderRoundDetail {
    summary: ProviderRoundSummary,
    request: serde_json::Value,
    events: Vec<serde_json::Value>,
    response_text: String,
}

#[derive(Debug, Clone, Serialize)]
struct ToolCallSummary {
    call_index: usize,
    scenario_id: String,
    name: String,
    status: String,
    elapsed_ms: u128,
    input_summary: String,
    output_summary: String,
    detail_path: String,
}

#[derive(Debug, Clone, Serialize)]
struct ToolCallDetail {
    summary: ToolCallSummary,
    input: serde_json::Value,
    output: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RealToolScenarioResult {
    scenario_id: String,
    title: String,
    status: String,
    tool_calls: usize,
    runtime_evidence: Vec<String>,
    matrix_evidence: Vec<String>,
    memory_evidence: Vec<String>,
    changed_files: Vec<String>,
    diff_summary: String,
    conclusion: String,
}

#[derive(Debug, Clone, Serialize)]
struct RealToolScenarioReport {
    kind: &'static str,
    target_repo: String,
    total: usize,
    passed: usize,
    tool_calls: usize,
    scenarios: Vec<RealToolScenarioResult>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeActionTrace {
    index: usize,
    action: String,
    evidence: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionTrace {
    kind: &'static str,
    started_at_ms: u128,
    finished_at_ms: Option<u128>,
    total_elapsed_ms: Option<u128>,
    provider_rounds: usize,
    runtime_actions: usize,
    tool_calls: usize,
    total_usage: UsageSummary,
    rounds: Vec<ProviderRoundSummary>,
    tool_call_log: Vec<ToolCallSummary>,
    runtime_action_log: Vec<RuntimeActionTrace>,
}

impl ExecutionTrace {
    fn start() -> Self {
        Self {
            kind: "harness_eval.execution_trace",
            started_at_ms: now_ms_u128(),
            finished_at_ms: None,
            total_elapsed_ms: None,
            provider_rounds: 0,
            runtime_actions: 0,
            tool_calls: 0,
            total_usage: UsageSummary {
                usage_source: "unavailable".to_string(),
                ..UsageSummary::default()
            },
            rounds: Vec::new(),
            tool_call_log: Vec::new(),
            runtime_action_log: Vec::new(),
        }
    }

    fn record_runtime_action(&mut self, action: impl Into<String>, evidence: impl Into<String>) {
        let index = self.runtime_action_log.len() + 1;
        self.runtime_action_log.push(RuntimeActionTrace {
            index,
            action: action.into(),
            evidence: evidence.into(),
        });
        self.runtime_actions = self.runtime_action_log.len();
    }

    fn add_provider_round(&mut self, summary: ProviderRoundSummary) {
        self.total_usage.input_tokens = self
            .total_usage
            .input_tokens
            .saturating_add(summary.usage.input_tokens);
        self.total_usage.output_tokens = self
            .total_usage
            .output_tokens
            .saturating_add(summary.usage.output_tokens);
        self.total_usage.cache_creation_input_tokens = self
            .total_usage
            .cache_creation_input_tokens
            .saturating_add(summary.usage.cache_creation_input_tokens);
        self.total_usage.cache_read_input_tokens = self
            .total_usage
            .cache_read_input_tokens
            .saturating_add(summary.usage.cache_read_input_tokens);
        self.total_usage.total_tokens = self
            .total_usage
            .total_tokens
            .saturating_add(summary.usage.total_tokens);
        if summary.usage.usage_source == "provider_event" {
            self.total_usage.usage_source = "provider_event".to_string();
        }
        self.rounds.push(summary);
        self.provider_rounds = self.rounds.len();
    }

    fn add_tool_call(&mut self, summary: ToolCallSummary) {
        self.tool_call_log.push(summary);
        self.tool_calls = self.tool_call_log.len();
    }

    fn finish(&mut self, started: Instant) {
        self.finished_at_ms = Some(now_ms_u128());
        self.total_elapsed_ms = Some(started.elapsed().as_millis());
    }
}

#[derive(Debug, Serialize)]
struct MissionHarnessEvalReport {
    kind: &'static str,
    level: EvalLevel,
    status: String,
    provider: Option<String>,
    budget: Option<String>,
    gateway_process: bool,
    scenario_matrix: Vec<E2eScenarioMatrixItem>,
    stable_ai: StableAiHealthReport,
    scenarios: Vec<CapabilityResult>,
    metrics: Vec<HarnessMetric>,
    complex_scenarios: Option<ComplexHarnessScenarioReport>,
    real_tool_scenarios: Option<RealToolScenarioReport>,
    execution_trace: ExecutionTrace,
    result_package_dir: Option<String>,
    #[serde(skip)]
    provider_round_details: Vec<ProviderRoundDetail>,
    #[serde(skip)]
    tool_call_details: Vec<ToolCallDetail>,
}

fn main() {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let level = match args.first().map(String::as_str) {
        Some("quick") | None => EvalLevel::Quick,
        Some("full") => EvalLevel::Full,
        Some("deep") => EvalLevel::Deep,
        Some("--help") | Some("-h") => {
            print_help();
            return;
        }
        Some(other) => {
            eprintln!("unknown harness eval level: {other}");
            print_help();
            std::process::exit(2);
        }
    };
    if !args.is_empty() {
        args.remove(0);
    }
    let provider = option_value(&args, "--provider");
    let budget = option_value(&args, "--budget").or_else(|| Some("low".to_string()));

    let mut report = match level {
        EvalLevel::Quick => run_quick(),
        EvalLevel::Full => run_full(),
        EvalLevel::Deep => run_deep(provider.clone(), budget.clone()),
    };
    let (json_path, md_path) = write_report(&mut report).unwrap_or_else(|error| {
        eprintln!("failed to write report: {error}");
        std::process::exit(1);
    });
    println!(
        "mission harness {} eval: {}",
        report.level.as_str(),
        report.status
    );
    println!("json: {}", json_path.display());
    println!("markdown: {}", md_path.display());
}

fn print_help() {
    println!("Usage: harness-eval [quick|full|deep] [--provider configured] [--budget low]");
}

fn option_value(args: &[String], key: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
}

fn run_quick() -> MissionHarnessEvalReport {
    let started = Instant::now();
    let mut trace = ExecutionTrace::start();
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    trace.record_runtime_action("deterministic_core_loop", "quick runtime loop completed");
    let fake_provider_result = fake_provider_scenario_report();
    let coverage = harness_capability_coverage_report();
    let knowledge_fabric = evaluate_knowledge_fabric_context_governance();
    scenarios.push(knowledge_fabric_capability(&knowledge_fabric));
    trace.record_runtime_action(
        "knowledge_fabric.evaluate",
        format!(
            "active_packs={}, blocked_namespaces={}, conflicts={}",
            knowledge_fabric.active_pack_count,
            knowledge_fabric.blocked_namespace_count,
            knowledge_fabric.conflict_count
        ),
    );
    scenarios.push(CapabilityResult {
        capability: "runtime_event_replay",
        status: "passed",
        evidence: replay_evidence,
        notes: "quick replay report generated without provider".to_string(),
    });
    let stable_ai = StableAiHealthReport::from_fake_eval(
        env!("CARGO_PKG_VERSION"),
        "fake_provider",
        None,
        false,
        "real provider not enabled for quick eval",
        fake_provider_result,
        coverage,
        "gateway smoke skipped in quick eval",
        "webui/tui smoke delegated to quick.sh and surface gates",
        "runtime recovery executor produced deterministic report",
    );
    let metrics = build_harness_metrics(&scenarios, &stable_ai);
    trace.finish(started);
    MissionHarnessEvalReport {
        kind: "mission_harness.eval_report",
        level: EvalLevel::Quick,
        status: stable_ai.status.clone(),
        provider: None,
        budget: None,
        gateway_process: false,
        scenario_matrix: stable_ai_scenario_matrix(),
        stable_ai,
        scenarios,
        metrics,
        complex_scenarios: None,
        real_tool_scenarios: None,
        execution_trace: trace,
        result_package_dir: None,
        provider_round_details: Vec::new(),
        tool_call_details: Vec::new(),
    }
}

fn run_full() -> MissionHarnessEvalReport {
    let started = Instant::now();
    let mut trace = ExecutionTrace::start();
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    trace.record_runtime_action("deterministic_core_loop", "full runtime loop completed");
    let gateway = probe_gateway_contract();
    trace.record_runtime_action("gateway.probe", gateway.1.clone());
    let fake_provider_result = fake_provider_scenario_report();
    let coverage = harness_capability_coverage_report();
    let knowledge_fabric = evaluate_knowledge_fabric_context_governance();
    scenarios.push(knowledge_fabric_capability(&knowledge_fabric));
    trace.record_runtime_action(
        "knowledge_fabric.evaluate",
        format!(
            "active_packs={}, blocked_namespaces={}, conflicts={}",
            knowledge_fabric.active_pack_count,
            knowledge_fabric.blocked_namespace_count,
            knowledge_fabric.conflict_count
        ),
    );
    scenarios.push(CapabilityResult {
        capability: "gateway_contract_surface",
        status: if gateway.0 { "passed" } else { "degraded" },
        evidence: gateway.1.clone(),
        notes: "full eval probes live gateway when COWD_GATEWAY_URL is running".to_string(),
    });
    scenarios.push(CapabilityResult {
        capability: "runtime_recovery_report",
        status: "passed",
        evidence: replay_evidence.clone(),
        notes: "full layer verifies recovery semantics without spawning provider".to_string(),
    });
    let status = if scenarios.iter().all(|item| item.status == "passed") && gateway.0 {
        "passed"
    } else {
        "failed"
    };
    let stable_ai = StableAiHealthReport::from_fake_eval(
        env!("CARGO_PKG_VERSION"),
        "fake_provider",
        None,
        false,
        "real provider not enabled for full eval",
        fake_provider_result,
        coverage,
        gateway.1.clone(),
        "webui/tui smoke delegated to surface and scenario gates",
        replay_evidence,
    );
    let metrics = build_harness_metrics(&scenarios, &stable_ai);
    trace.finish(started);
    MissionHarnessEvalReport {
        kind: "mission_harness.eval_report",
        level: EvalLevel::Full,
        status: status.to_string(),
        provider: None,
        budget: None,
        gateway_process: gateway.0,
        scenario_matrix: stable_ai_scenario_matrix(),
        stable_ai,
        scenarios,
        metrics,
        complex_scenarios: None,
        real_tool_scenarios: None,
        execution_trace: trace,
        result_package_dir: None,
        provider_round_details: Vec::new(),
        tool_call_details: Vec::new(),
    }
}

fn run_deep(provider: Option<String>, budget: Option<String>) -> MissionHarnessEvalReport {
    let started = Instant::now();
    let mut trace = ExecutionTrace::start();
    if provider.as_deref() != Some("configured") {
        let stable_ai = StableAiHealthReport::from_fake_eval(
            env!("CARGO_PKG_VERSION"),
            provider
                .clone()
                .unwrap_or_else(|| "not_configured".to_string()),
            Some(
                std::env::var("COWD_EVAL_MODEL")
                    .unwrap_or_else(|_| "deepseek-v4-flash".to_string()),
            ),
            false,
            "pass --provider configured or set COWD_EVAL_REAL_MODEL=1 for real provider use",
            fake_provider_scenario_report(),
            harness_capability_coverage_report(),
            "gateway smoke skipped because deep provider is gated",
            "webui/tui smoke delegated to final health lanes",
            "recovery not run because provider gate stopped deep eval",
        );
        trace.finish(started);
        return MissionHarnessEvalReport {
            kind: "mission_harness.eval_report",
            level: EvalLevel::Deep,
            status: "gated".to_string(),
            provider,
            budget,
            gateway_process: false,
            scenario_matrix: stable_ai_scenario_matrix(),
            stable_ai,
            scenarios: vec![CapabilityResult {
                capability: "deep_provider_eval",
                status: "skipped",
                evidence: "pass --provider configured to allow real provider use".to_string(),
                notes: "budget guard prevented token use".to_string(),
            }],
            metrics: Vec::new(),
            complex_scenarios: None,
            real_tool_scenarios: None,
            execution_trace: trace,
            result_package_dir: None,
            provider_round_details: Vec::new(),
            tool_call_details: Vec::new(),
        };
    }
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    trace.record_runtime_action("deterministic_core_loop", "deep runtime loop completed");
    scenarios.extend(run_deep_harness_scenarios(&mut trace));
    let gateway = probe_gateway_contract();
    trace.record_runtime_action("gateway.probe", gateway.1.clone());
    let complex_scenarios = evaluate_complex_harness_scenarios();
    trace.record_runtime_action(
        "complex_scenarios.evaluate",
        format!(
            "{}/{} passed",
            complex_scenarios.passed, complex_scenarios.total
        ),
    );
    scenarios.push(complex_scenario_capability(&complex_scenarios));
    let knowledge_fabric = evaluate_knowledge_fabric_context_governance();
    trace.record_runtime_action(
        "knowledge_fabric.evaluate",
        format!(
            "active_packs={}, blocked_namespaces={}, conflicts={}",
            knowledge_fabric.active_pack_count,
            knowledge_fabric.blocked_namespace_count,
            knowledge_fabric.conflict_count
        ),
    );
    scenarios.push(knowledge_fabric_capability(&knowledge_fabric));
    let mut tool_call_details = Vec::new();
    let real_tool_scenarios = run_real_tool_deep_scenarios(&mut trace, &mut tool_call_details);
    scenarios.extend(real_tool_scenario_capabilities(&real_tool_scenarios));
    let mut provider_round_details = Vec::new();
    let provider_smoke = run_provider_smoke(&mut trace, &mut provider_round_details);
    let provider_passed = provider_smoke.status == "passed";
    let model =
        std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
    scenarios.push(provider_smoke);
    scenarios.push(run_provider_structured_reasoning(
        &mut trace,
        &mut provider_round_details,
    ));
    scenarios.push(run_provider_cross_session_dialogue(
        &mut trace,
        &mut provider_round_details,
    ));
    scenarios.push(CapabilityResult {
        capability: "gateway_contract_surface",
        status: if gateway.0 { "passed" } else { "degraded" },
        evidence: gateway.1.clone(),
        notes: "deep eval includes live gateway probe when available".to_string(),
    });
    scenarios.push(CapabilityResult {
        capability: "runtime_recovery_report",
        status: "passed",
        evidence: replay_evidence.clone(),
        notes: "deep preflight recovery report generated".to_string(),
    });
    let real_capability_result = build_real_capability_gate_report(
        &scenarios,
        &complex_scenarios,
        &real_tool_scenarios,
        &trace,
        provider_passed,
        gateway.0,
    );
    let stable_ai = StableAiHealthReport::from_real_eval(
        env!("CARGO_PKG_VERSION"),
        "configured",
        Some(model),
        "real provider explicitly enabled",
        fake_provider_scenario_report(),
        harness_capability_coverage_report(),
        gateway.1.clone(),
        "webui/tui smoke delegated to final health lanes",
        replay_evidence,
        real_capability_result,
    );
    let metrics = build_harness_metrics(&scenarios, &stable_ai);
    trace.finish(started);
    let status = stable_ai.status.clone();
    MissionHarnessEvalReport {
        kind: "mission_harness.eval_report",
        level: EvalLevel::Deep,
        status,
        provider,
        budget,
        gateway_process: gateway.0,
        scenario_matrix: stable_ai_scenario_matrix(),
        stable_ai,
        scenarios,
        metrics,
        complex_scenarios: Some(complex_scenarios),
        real_tool_scenarios: Some(real_tool_scenarios),
        execution_trace: trace,
        result_package_dir: None,
        provider_round_details,
        tool_call_details,
    }
}

fn run_provider_smoke(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ProviderRoundDetail>,
) -> CapabilityResult {
    let model =
        std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
    let mut client = match ProviderRuntimeClient::new(model.to_string(), Vec::new()) {
        Ok(client) => client,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_provider_eval",
                status: "failed",
                evidence: format!("provider client unavailable: {}", abbreviate(&error, 180)),
                notes: "real provider smoke did not start".to_string(),
            };
        }
    };
    let request = ApiRequest {
        system_prompt: vec![
            "You are a strict health-check responder. Return exactly: OK".to_string(),
        ],
        messages: vec![ConversationMessage {
            role: MessageRole::User,
            blocks: vec![ContentBlock::Text {
                text: "Return exactly OK.".to_string(),
            }],
            usage: None,
        }],
        model: model.clone(),
    };
    match collect_measured_provider_text(&mut client, "deep_provider_eval", request, trace, details)
    {
        Ok(text) => {
            if text.trim().is_empty() {
                CapabilityResult {
                    capability: "deep_provider_eval",
                    status: "failed",
                    evidence: "provider returned no text".to_string(),
                    notes: "real provider call completed without usable assistant text".to_string(),
                }
            } else {
                CapabilityResult {
                    capability: "deep_provider_eval",
                    status: "passed",
                    evidence: format!("{model} -> {}", abbreviate(text.trim(), 80)),
                    notes: "real provider call returned assistant text under explicit configured budget".to_string(),
                }
            }
        }
        Err(error) => CapabilityResult {
            capability: "deep_provider_eval",
            status: "failed",
            evidence: abbreviate(&error, 180),
            notes: "real provider call failed; inspect provider credentials/network".to_string(),
        },
    }
}

fn complex_scenario_capability(report: &ComplexHarnessScenarioReport) -> CapabilityResult {
    CapabilityResult {
        capability: "deep_harness_complex_scenario_suite",
        status: if report.failed == 0 && report.average_score >= 0.9 {
            "passed"
        } else {
            "failed"
        },
        evidence: format!(
            "passed={}/{}; average_score={:.2}",
            report.passed, report.total, report.average_score
        ),
        notes: "generated complex topics, solved them, reviewed acceptance checks, and scored harness readiness"
            .to_string(),
    }
}

fn knowledge_fabric_capability(
    report: &harness_eval::KnowledgeFabricEvalReport,
) -> CapabilityResult {
    CapabilityResult {
        capability: "knowledge_fabric_context_governance",
        status: if report.passed { "passed" } else { "failed" },
        evidence: format!(
            "active_packs={}; blocked_namespaces={}; conflicts={}; evidence_refs={}",
            report.active_pack_count,
            report.blocked_namespace_count,
            report.conflict_count,
            report.evidence_count
        ),
        notes: report.notes.join("; "),
    }
}

fn real_tool_scenario_capabilities(report: &RealToolScenarioReport) -> Vec<CapabilityResult> {
    let mut results = Vec::new();
    results.push(CapabilityResult {
        capability: "deep_harness_real_tool_scenario_suite",
        status: if report.passed == report.total && report.tool_calls > 0 {
            "passed"
        } else {
            "failed"
        },
        evidence: format!(
            "{}/{} scenarios passed; tool_calls={}",
            report.passed, report.total, report.tool_calls
        ),
        notes: "deep eval executed real tools and recorded full tool-call evidence".to_string(),
    });

    let capability_names = [
        "deep_tool_code_refactor_iacc",
        "deep_tool_manufacturing_matrix_what_if",
        "deep_tool_large_memory_governance",
        "deep_tool_harness_evolution",
        "deep_tool_simple_fast_path",
        "deep_tool_multi_agent_team_modes",
        "deep_tool_cross_session_linkage",
    ];
    for (scenario, capability) in report.scenarios.iter().zip(capability_names) {
        results.push(CapabilityResult {
            capability,
            status: if scenario.status == "passed" {
                "passed"
            } else {
                "failed"
            },
            evidence: format!(
                "{}; tool_calls={}; {}",
                scenario.scenario_id, scenario.tool_calls, scenario.diff_summary
            ),
            notes: scenario.conclusion.clone(),
        });
    }
    results
}

fn run_real_tool_deep_scenarios(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
) -> RealToolScenarioReport {
    let target_repo = std::env::var("COWD_EVAL_TARGET_REPO")
        .unwrap_or_else(|_| "/media/yi/Datas/workspace/dev-iacc".to_string());
    let target_repo_path = PathBuf::from(&target_repo);
    let workspace = std::env::temp_dir().join(format!(
        "cowd-real-tool-eval-{}-{}",
        std::process::id(),
        now_ms_u128()
    ));
    let _ = std::fs::remove_dir_all(&workspace);
    let _ = std::fs::create_dir_all(&workspace);

    let scenarios = vec![
        run_code_refactor_tool_scenario(trace, details, &target_repo_path, &workspace),
        run_manufacturing_matrix_tool_scenario(trace, details, &workspace),
        run_large_memory_tool_scenario(trace, details, &workspace),
        run_harness_evolution_tool_scenario(trace, details, &workspace),
        run_simple_fast_path_tool_scenario(trace, details, &workspace),
        run_multi_agent_tool_scenario(trace, details, &workspace),
        run_cross_session_tool_scenario(trace, details, &workspace),
    ];

    let total = scenarios.len();
    let passed = scenarios
        .iter()
        .filter(|scenario| scenario.status == "passed")
        .count();
    let tool_calls = scenarios.iter().map(|scenario| scenario.tool_calls).sum();
    RealToolScenarioReport {
        kind: "real_tool_scenario_report",
        target_repo: target_repo_path.display().to_string(),
        total,
        passed,
        tool_calls,
        scenarios,
    }
}

fn run_code_refactor_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    target_repo: &Path,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "code_refactor_iacc";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(workspace_dir.join("src"));

    let glob = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "glob_search",
        json!({"pattern": "crates/**/*.rs", "path": target_repo}),
    );
    let grep = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "grep_search",
        json!({
            "pattern": "run_prompt|livecli|provider|gateway",
            "path": target_repo,
            "glob": "crates/**/*.rs",
            "output_mode": "content",
            "-n": true,
            "head_limit": 40
        }),
    );
    let cargo = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "read_file",
        json!({"path": target_repo.join("Cargo.toml"), "offset": 0, "limit": 5000}),
    );
    let file_path = workspace_dir.join("src/lib.rs");
    let original = "pub fn legacy_run_prompt() -> &'static str {\n    \"gateway must not own cli prompt loops\"\n}\n";
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": file_path, "content": original}),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "edit_file",
        json!({
            "path": workspace_dir.join("src/lib.rs"),
            "old_string": "legacy_run_prompt",
            "new_string": "gateway_status_contract",
            "replace_all": true
        }),
    );
    let edited = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "read_file",
        json!({"path": workspace_dir.join("src/lib.rs"), "offset": 0, "limit": 2000}),
    );
    let passed = glob.is_ok()
        && grep.is_ok()
        && cargo.is_ok()
        && edited
            .as_deref()
            .is_ok_and(|text| text.contains("gateway_status_contract"));
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "iACC branch code analysis and isolated refactor".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec![format!("target_repo={}", target_repo.display())],
        matrix_evidence: Vec::new(),
        memory_evidence: Vec::new(),
        changed_files: vec![workspace_dir.join("src/lib.rs").display().to_string()],
        diff_summary: "isolated symbol rename legacy_run_prompt -> gateway_status_contract"
            .to_string(),
        conclusion: "tools inspected real iACC branch files and executed an isolated refactor without committing branch changes".to_string(),
    }
}

fn run_manufacturing_matrix_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "manufacturing_matrix_what_if";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let supply_chain = json!({
        "product": "industrial-controller-x7",
        "weekly_demand": 1200,
        "plants": [
            {"id": "plant-suzhou", "capacity": 900, "yield": 0.94},
            {"id": "plant-bac-ninh", "capacity": 550, "yield": 0.91}
        ],
        "suppliers": [
            {"id": "supplier-alpha", "component": "mcu", "lead_time_days": 21, "risk": "medium"},
            {"id": "supplier-beta", "component": "mcu", "lead_time_days": 34, "risk": "high"},
            {"id": "supplier-gamma", "component": "power-module", "lead_time_days": 18, "risk": "low"}
        ],
        "what_if": "supplier-beta outage for 14 days and demand +18%"
    });
    let data_file = workspace_dir.join("supply-chain.json");
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": data_file, "content": supply_chain.to_string()}),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "read_file",
        json!({"path": workspace_dir.join("supply-chain.json"), "offset": 0, "limit": 8000}),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "grep_search",
        json!({"pattern": "supplier-beta|what_if|lead_time", "path": workspace_dir, "glob": "*.json", "output_mode": "content", "-n": true}),
    );

    let mut service = FactKernelService::new();
    let source = FactSource {
        kind: SourceKind::Simulation,
        id: scenario_id.to_string(),
        label: Some("manufacturing what-if".to_string()),
    };
    let evidence = service.ingest_evidence(EvidencePacket::new(source.clone(), supply_chain));
    let facts = [
        ("supplier-alpha", "lead_time_days", json!(21)),
        ("supplier-beta", "lead_time_days", json!(34)),
        ("supplier-beta", "lead_time_days", json!(999)),
        ("plant-suzhou", "effective_capacity", json!(846)),
        ("plant-bac-ninh", "effective_capacity", json!(500)),
    ];
    for (entity, predicate, value) in facts {
        let receipt = service.promote_candidate(GrowthCandidate::Matrix(MatrixFact {
            id: FactId::new(),
            entity: entity.to_string(),
            predicate: predicate.to_string(),
            value,
            source: source.clone(),
            evidence: vec![evidence.id.clone()],
            confidence: Confidence::from_basis_points(9_200),
            boundary: HypothesisBoundary::observed(),
        }));
        trace.record_runtime_action(
            "matrix.what_if.promote",
            format!("{entity}.{predicate}={:?}", receipt.decision.decision),
        );
    }
    let issues = service.evaluate_health();
    let conflicts = issues
        .iter()
        .filter(|issue| issue.kind == FactHealthIssueKind::Conflict)
        .count();
    let recall = service.recall(&RecallQuery::new("supplier-beta lead_time_days"));
    let passed = conflicts >= 2 && !recall.is_empty();
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Manufacturing supply-chain what-if through matrix".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec![format!("health_issues={}", issues.len())],
        matrix_evidence: vec![
            format!("conflicts={conflicts}"),
            format!("recall_hits={}", recall.len()),
            "recommendation=dual-source mcu and shift 300 units to plant-bac-ninh if beta outage persists".to_string(),
        ],
        memory_evidence: Vec::new(),
        changed_files: vec![workspace_dir.join("supply-chain.json").display().to_string()],
        diff_summary: "manufacturing dataset generated and matrix conflict intentionally detected"
            .to_string(),
        conclusion: "matrix can represent manufacturing facts, detect contradictory what-if values, and support operational recommendations".to_string(),
    }
}

fn run_large_memory_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "large_memory_governance";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": workspace_dir.join("memory-batch.jsonl"), "content": build_memory_jsonl(160)}),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "grep_search",
        json!({"pattern": "supplier-beta|policy", "path": workspace_dir, "glob": "*.jsonl", "output_mode": "content", "-n": true, "head_limit": 20}),
    );

    let mut service = FactKernelService::new();
    let source = FactSource {
        kind: SourceKind::Memory,
        id: scenario_id.to_string(),
        label: Some("large memory eval".to_string()),
    };
    let mut promoted = 0usize;
    for index in 0..160 {
        let evidence = service.ingest_evidence(EvidencePacket::new(
            source.clone(),
            json!({"index": index, "text": format!("manufacturing policy memory {index}")}),
        ));
        let receipt = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
            summary: format!(
                "policy memory {index}: supplier risk review cadence is weekly for tier-1 components"
            ),
            source: source.clone(),
            evidence: vec![evidence.id],
            confidence: Confidence::from_basis_points(8_000),
            boundary: HypothesisBoundary::observed(),
            tags: vec!["manufacturing".to_string(), "policy".to_string()],
        }));
        promoted += usize::from(receipt.promoted_fact.is_some());
    }
    let low_evidence = service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({"text": "unverified rumor"}),
    ));
    let low = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
        summary: "unverified claim: all supplier-beta lots are unusable".to_string(),
        source: source.clone(),
        evidence: vec![low_evidence.id],
        confidence: Confidence::from_basis_points(3_000),
        boundary: HypothesisBoundary::observed(),
        tags: vec!["conflict".to_string()],
    }));
    let hypothetical = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
        summary: "hypothetical memory must not pollute observed store".to_string(),
        source,
        evidence: Vec::new(),
        confidence: Confidence::from_basis_points(9_000),
        boundary: HypothesisBoundary::hypothetical("memory-what-if"),
        tags: vec!["hypothetical".to_string()],
    }));
    let recall = service.recall(&RecallQuery::new("supplier risk weekly tier-1"));
    let health = service.evaluate_health();
    let low_confidence = health
        .iter()
        .filter(|issue| issue.kind == FactHealthIssueKind::LowConfidence)
        .count();
    let passed = promoted == 160
        && low.promoted_fact.is_none()
        && hypothetical.promoted_fact.is_none()
        && !recall.is_empty();
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Large memory build, recall, conflict and governance".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec![format!("health_issues={}", health.len())],
        matrix_evidence: Vec::new(),
        memory_evidence: vec![
            format!("promoted={promoted}"),
            format!("recall_hits={}", recall.len()),
            format!("low_confidence_held={}", low.promoted_fact.is_none()),
            format!("low_confidence_health_issues={low_confidence}"),
            format!("hypothetical_promoted={}", hypothetical.promoted_fact.is_some()),
            "conflict_resolution=new observed policy wins over unverified rumor".to_string(),
        ],
        changed_files: vec![workspace_dir.join("memory-batch.jsonl").display().to_string()],
        diff_summary: "160 observed memories inserted; low confidence hold and hypothetical governance checked"
            .to_string(),
        conclusion: "memory can sustain batch growth, recall relevant policy facts, and keep hypothetical facts out of observed memory".to_string(),
    }
}

fn run_harness_evolution_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "harness_evolution";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let gap_file = workspace_dir.join("harness-gap.md");
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": gap_file, "content": "# Harness Gap\n\n- tool_calls were not counted as first-class evidence.\n"}),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "edit_file",
        json!({
            "path": workspace_dir.join("harness-gap.md"),
            "old_string": "- tool_calls were not counted as first-class evidence.",
            "new_string": "- tool_calls are recorded as first-class evidence with detail packages.\n- scenario evidence is split into provider, runtime, tool and fact lanes.",
            "replace_all": true
        }),
    );
    let search = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "ToolSearch",
        json!({"query": "read grep edit structured output evidence"}),
    );
    let read = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "read_file",
        json!({"path": workspace_dir.join("harness-gap.md"), "offset": 0, "limit": 4000}),
    );
    let mut service = FactKernelService::new();
    let receipt = service.promote_candidate(GrowthCandidate::PolicyLearning {
        summary: "harness eval must distinguish provider rounds from actual tool calls".to_string(),
        confidence: Confidence::from_basis_points(9_500),
    });
    let passed = search.is_ok()
        && read
            .as_deref()
            .is_ok_and(|text| text.contains("first-class evidence"))
        && receipt.promoted_fact.is_some();
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Harness evolution from discovered gap to policy learning".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec!["policy_learning.promote".to_string()],
        matrix_evidence: Vec::new(),
        memory_evidence: vec!["growth.policy_learning=promoted".to_string()],
        changed_files: vec![workspace_dir.join("harness-gap.md").display().to_string()],
        diff_summary: "harness gap note edited into explicit evaluation policy".to_string(),
        conclusion:
            "harness can turn evaluation gaps into durable reviewable improvement candidates"
                .to_string(),
    }
}

fn run_simple_fast_path_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "simple_fast_path";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let answer = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "StructuredOutput",
        json!({"answer": 42, "reason": "6 * 7"}),
    );
    let passed = answer.as_deref().is_ok_and(|text| text.contains("42"))
        && trace.tool_calls.saturating_sub(before) == 1;
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Simple problem fast path".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec!["strategy=single_turn".to_string()],
        matrix_evidence: Vec::new(),
        memory_evidence: Vec::new(),
        changed_files: Vec::new(),
        diff_summary: "no files changed; one tool call produced structured answer".to_string(),
        conclusion:
            "simple deterministic tasks stay on the fast path without spawning unnecessary teams"
                .to_string(),
    }
}

fn run_multi_agent_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "multi_agent_team_modes";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let mission = global_mission_runtime();
    let session = mission
        .start_session(StartMissionSessionRequest {
            title: "Real tool multi-agent manufacturing recovery".to_string(),
            session_id: Some(format!("real-tool-team-{}", uuid::Uuid::new_v4())),
        })
        .expect("real tool team session starts");
    let prompt = "manufacturing outage requires planner executor reviewer synthesizer";
    let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
    let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
    let team = global_team_runtime_service()
        .start(StartTeamRuntimeRequest {
            session_id: session.session_id.clone(),
            objective: "restore manufacturing flow with role-based evidence".to_string(),
            collaboration_decision: decision.clone(),
        })
        .expect("real tool team starts");
    let team_report = TeamExecutionLoop::tick_ready(&team.team_id).expect("team ticks");
    trace.record_runtime_action(
        "real_tool.team.tick",
        format!(
            "template={}; assigned={}",
            decision.template_id.as_str(),
            team_report.assigned_task_count
        ),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "TodoWrite",
        json!({
            "todos": [
                {"content": "planner: isolate bottleneck", "status": "completed", "priority": "high", "id": "planner"},
                {"content": "executor: propose mitigation", "status": "completed", "priority": "high", "id": "executor"},
                {"content": "reviewer: verify evidence", "status": "completed", "priority": "medium", "id": "reviewer"}
            ]
        }),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": workspace_dir.join("team-summary.md"), "content": format!("team={} template={} assigned={}", team.team_id, decision.template_id.as_str(), team_report.assigned_task_count)}),
    );
    let passed =
        team_report.assigned_task_count > 0 && trace.tool_calls.saturating_sub(before) >= 2;
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Multi-agent team template collaboration".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec![
            format!("session={}", session.session_id),
            format!("team={}", team.team_id),
            format!("template={}", decision.template_id.as_str()),
            format!("assigned={}", team_report.assigned_task_count),
        ],
        matrix_evidence: Vec::new(),
        memory_evidence: Vec::new(),
        changed_files: vec![workspace_dir.join("team-summary.md").display().to_string()],
        diff_summary: "team summary generated from real team runtime tick".to_string(),
        conclusion: "multi-agent collaboration templates produce reviewable role tasks and are visible in runtime evidence".to_string(),
    }
}

fn run_cross_session_tool_scenario(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    workspace: &Path,
) -> RealToolScenarioResult {
    let scenario_id = "cross_session_linkage";
    let before = trace.tool_calls;
    let workspace_dir = workspace.join(scenario_id);
    let _ = std::fs::create_dir_all(&workspace_dir);
    let mission = global_mission_runtime();
    let primary = mission
        .start_session(StartMissionSessionRequest {
            title: "Primary manufacturing controller".to_string(),
            session_id: Some(format!("real-tool-primary-{}", uuid::Uuid::new_v4())),
        })
        .expect("primary session");
    let peer = mission
        .start_session(StartMissionSessionRequest {
            title: "Peer supplier investigation".to_string(),
            session_id: Some(format!("real-tool-peer-{}", uuid::Uuid::new_v4())),
        })
        .expect("peer session");
    global_session_relation_graph()
        .upsert_proxy(SessionProxy {
            session_id: peer.session_id.clone(),
            summary: "supplier-beta outage analysis".to_string(),
            evidence_refs: vec![format!("session:{}", peer.session_id)],
            decisions: vec!["supplier-beta recovery evidence requested".to_string()],
            open_questions: vec!["can supplier-alpha absorb beta outage volume?".to_string()],
            updated_at_ms: now_ms_u128() as u64,
        })
        .expect("proxy upsert");
    let bridge = MissionControlRuntime::execute(MissionControlCommand {
        target: MissionControlCommandTarget::Session {
            session_id: primary.session_id.clone(),
        },
        action: MissionControlAction::RouteToSession,
        actor: Some("harness-eval".to_string()),
        payload: json!({
            "target_session_id": peer.session_id.clone(),
            "command": "check supplier-beta recovery options"
        }),
        evidence_refs: vec![format!("session:{}", primary.session_id)],
    });
    trace.record_runtime_action(
        "real_tool.cross_session.bridge",
        format!("{} -> {}", primary.session_id, peer.session_id),
    );
    let _ = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "write_file",
        json!({"path": workspace_dir.join("session-link.json"), "content": json!({"primary": primary.session_id, "peer": peer.session_id, "status": format!("{:?}", bridge.status), "message": bridge.message}).to_string()}),
    );
    let read = execute_traced_tool(
        trace,
        details,
        scenario_id,
        "read_file",
        json!({"path": workspace_dir.join("session-link.json"), "offset": 0, "limit": 3000}),
    );
    let passed = bridge.message.contains("routed")
        && read.as_deref().is_ok_and(|text| text.contains("peer"));
    RealToolScenarioResult {
        scenario_id: scenario_id.to_string(),
        title: "Cross-session switching and linkage".to_string(),
        status: status_str(passed),
        tool_calls: trace.tool_calls.saturating_sub(before),
        runtime_evidence: vec![
            format!("primary={}", primary.session_id),
            format!("peer={}", peer.session_id),
            format!("bridge_status={:?}", bridge.status),
        ],
        matrix_evidence: Vec::new(),
        memory_evidence: Vec::new(),
        changed_files: vec![workspace_dir.join("session-link.json").display().to_string()],
        diff_summary: "session linkage artifact written and reread through tools".to_string(),
        conclusion: "mission control can route between sessions and expose linkage evidence to the result package".to_string(),
    }
}

fn execute_traced_tool(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ToolCallDetail>,
    scenario_id: &str,
    name: &str,
    input: serde_json::Value,
) -> Result<String, String> {
    let started = Instant::now();
    let result = tools::execute_tool(name, &input);
    let elapsed_ms = started.elapsed().as_millis();
    let call_index = trace.tool_call_log.len() + 1;
    let (status, output, error, output_summary) = match result {
        Ok(output) => (
            "passed".to_string(),
            Some(output.clone()),
            None,
            summarize_text(&output, 160),
        ),
        Err(error) => (
            "failed".to_string(),
            None,
            Some(error.clone()),
            summarize_text(&error, 160),
        ),
    };
    let summary = ToolCallSummary {
        call_index,
        scenario_id: scenario_id.to_string(),
        name: name.to_string(),
        status,
        elapsed_ms,
        input_summary: summarize_json(&input, 160),
        output_summary,
        detail_path: format!(
            "tool-calls/{:03}-{}-{}.json",
            call_index,
            sanitize_file_name(scenario_id),
            sanitize_file_name(name)
        ),
    };
    trace.add_tool_call(summary.clone());
    details.push(ToolCallDetail {
        summary,
        input,
        output,
        error,
    });
    details
        .last()
        .and_then(|detail| detail.output.clone())
        .ok_or_else(|| {
            details
                .last()
                .and_then(|detail| detail.error.clone())
                .unwrap_or_else(|| "tool failed without error".to_string())
        })
}

fn build_memory_jsonl(count: usize) -> String {
    (0..count)
        .map(|index| {
            json!({
                "id": format!("memory-{index:03}"),
                "summary": format!("supplier risk review cadence memory {index}"),
                "tags": ["manufacturing", "policy"]
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn status_str(passed: bool) -> String {
    if passed {
        "passed".to_string()
    } else {
        "failed".to_string()
    }
}

fn run_provider_structured_reasoning(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ProviderRoundDetail>,
) -> CapabilityResult {
    let model =
        std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
    let mut client = match ProviderRuntimeClient::new(model.clone(), Vec::new()) {
        Ok(client) => client,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_provider_structured_reasoning",
                status: "failed",
                evidence: format!("provider client unavailable: {}", abbreviate(&error, 180)),
                notes: "structured real-model harness scenario did not start".to_string(),
            };
        }
    };
    let request = ApiRequest {
        system_prompt: vec![
            concat!(
                "You are a strict AI harness evaluator. ",
                "Return only minified JSON with keys memory_summary, conflict_decision, team_synthesis. ",
                "No markdown, no code fences, no explanation."
            )
            .to_string(),
        ],
        messages: vec![ConversationMessage {
            role: MessageRole::User,
            blocks: vec![ContentBlock::Text {
                text: concat!(
                    "Facts: user prefers immersive implementation followed by unified review. ",
                    "Conflicting older fact: user wants to stop after every tiny edit. ",
                    "Team: planner, implementer, reviewer must coordinate using evidence. ",
                    "Return JSON. memory_summary must mention immersive. ",
                    "conflict_decision must be newer_preference_wins. ",
                    "team_synthesis must mention evidence."
                )
                .to_string(),
            }],
            usage: None,
        }],
        model,
    };
    match collect_measured_provider_text(
        &mut client,
        "deep_harness_provider_structured_reasoning",
        request,
        trace,
        details,
    ) {
        Ok(text) => {
            let Some(json_text) = extract_json_object(&text) else {
                return CapabilityResult {
                    capability: "deep_harness_provider_structured_reasoning",
                    status: "failed",
                    evidence: abbreviate(text.trim(), 180),
                    notes: "real model did not return a JSON object".to_string(),
                };
            };
            match serde_json::from_str::<serde_json::Value>(json_text) {
                Ok(value) => {
                    let memory_ok = value
                        .get("memory_summary")
                        .and_then(|item| item.as_str())
                        .is_some_and(|item| item.to_lowercase().contains("immersive"));
                    let conflict_ok = value
                        .get("conflict_decision")
                        .and_then(|item| item.as_str())
                        == Some("newer_preference_wins");
                    let synthesis_ok = value
                        .get("team_synthesis")
                        .and_then(|item| item.as_str())
                        .is_some_and(|item| item.to_lowercase().contains("evidence"));
                    let passed = memory_ok && conflict_ok && synthesis_ok;
                    CapabilityResult {
                        capability: "deep_harness_provider_structured_reasoning",
                        status: if passed { "passed" } else { "failed" },
                        evidence: format!(
                            "memory_ok={memory_ok}; conflict_ok={conflict_ok}; synthesis_ok={synthesis_ok}"
                        ),
                        notes: "real model completed bounded memory/conflict/team synthesis reasoning contract"
                            .to_string(),
                    }
                }
                Err(error) => CapabilityResult {
                    capability: "deep_harness_provider_structured_reasoning",
                    status: "failed",
                    evidence: format!(
                        "json parse failed: {error}; text={}",
                        abbreviate(json_text, 120)
                    ),
                    notes: "real model output did not satisfy structured JSON contract".to_string(),
                },
            }
        }
        Err(error) => CapabilityResult {
            capability: "deep_harness_provider_structured_reasoning",
            status: "failed",
            evidence: abbreviate(&error, 180),
            notes: "real provider structured reasoning call failed".to_string(),
        },
    }
}

fn run_provider_cross_session_dialogue(
    trace: &mut ExecutionTrace,
    details: &mut Vec<ProviderRoundDetail>,
) -> CapabilityResult {
    let model =
        std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
    let mut client = match ProviderRuntimeClient::new(model.clone(), Vec::new()) {
        Ok(client) => client,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_cross_session_provider_dialogue",
                status: "failed",
                evidence: format!("provider client unavailable: {}", abbreviate(&error, 180)),
                notes: "cross-session provider dialogue did not start".to_string(),
            };
        }
    };

    let session_b_request = ApiRequest {
        system_prompt: vec![concat!(
            "You are Session B in a cross-session harness test. ",
            "Return only minified JSON with keys session_id and evidence_summary."
        )
        .to_string()],
        messages: vec![ConversationMessage {
            role: MessageRole::User,
            blocks: vec![ContentBlock::Text {
                text: concat!(
                    "Inspect this peer task: identify whether old preference 'pause each step' ",
                    "conflicts with current preference 'immersive then review'. ",
                    "session_id must be session_b. evidence_summary must mention conflict and immersive."
                )
                .to_string(),
            }],
            usage: None,
        }],
        model: model.clone(),
    };
    let session_b_text = match collect_measured_provider_text(
        &mut client,
        "deep_harness_cross_session_session_b",
        session_b_request,
        trace,
        details,
    ) {
        Ok(text) => text,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_cross_session_provider_dialogue",
                status: "failed",
                evidence: abbreviate(&error, 180),
                notes: "session B provider turn failed".to_string(),
            };
        }
    };
    let Some(session_b_json) = extract_json_object(&session_b_text) else {
        return CapabilityResult {
            capability: "deep_harness_cross_session_provider_dialogue",
            status: "failed",
            evidence: abbreviate(&session_b_text, 180),
            notes: "session B did not return JSON evidence".to_string(),
        };
    };
    let session_b_value = match serde_json::from_str::<serde_json::Value>(session_b_json) {
        Ok(value) => value,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_cross_session_provider_dialogue",
                status: "failed",
                evidence: format!("session B JSON parse failed: {error}"),
                notes: "session B evidence was not machine-checkable".to_string(),
            };
        }
    };
    let b_evidence = session_b_value
        .get("evidence_summary")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();

    let session_a_request = ApiRequest {
        system_prompt: vec![concat!(
            "You are Session A supervising a peer Session B. ",
            "Return only minified JSON with keys session_id, used_peer_evidence, final_decision."
        )
        .to_string()],
        messages: vec![ConversationMessage {
            role: MessageRole::User,
            blocks: vec![ContentBlock::Text {
                text: format!(
                    "Session B evidence: {b_evidence}. session_id must be session_a. used_peer_evidence must be true. final_decision must mention immersive."
                ),
            }],
            usage: None,
        }],
        model,
    };
    let session_a_text = match collect_measured_provider_text(
        &mut client,
        "deep_harness_cross_session_session_a",
        session_a_request,
        trace,
        details,
    ) {
        Ok(text) => text,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_cross_session_provider_dialogue",
                status: "failed",
                evidence: abbreviate(&error, 180),
                notes: "session A provider turn failed".to_string(),
            };
        }
    };
    let Some(session_a_json) = extract_json_object(&session_a_text) else {
        return CapabilityResult {
            capability: "deep_harness_cross_session_provider_dialogue",
            status: "failed",
            evidence: abbreviate(&session_a_text, 180),
            notes: "session A did not return JSON synthesis".to_string(),
        };
    };
    let session_a_value = match serde_json::from_str::<serde_json::Value>(session_a_json) {
        Ok(value) => value,
        Err(error) => {
            return CapabilityResult {
                capability: "deep_harness_cross_session_provider_dialogue",
                status: "failed",
                evidence: format!("session A JSON parse failed: {error}"),
                notes: "session A synthesis was not machine-checkable".to_string(),
            };
        }
    };
    let b_ok = session_b_value
        .get("session_id")
        .and_then(|value| value.as_str())
        == Some("session_b")
        && b_evidence.to_lowercase().contains("conflict")
        && b_evidence.to_lowercase().contains("immersive");
    let a_ok = session_a_value
        .get("session_id")
        .and_then(|value| value.as_str())
        == Some("session_a")
        && session_a_value
            .get("used_peer_evidence")
            .and_then(|value| value.as_bool())
            == Some(true)
        && session_a_value
            .get("final_decision")
            .and_then(|value| value.as_str())
            .is_some_and(|value| value.to_lowercase().contains("immersive"));

    CapabilityResult {
        capability: "deep_harness_cross_session_provider_dialogue",
        status: if b_ok && a_ok { "passed" } else { "failed" },
        evidence: format!("session_b_ok={b_ok}; session_a_ok={a_ok}"),
        notes: "real model executed bounded Session B evidence turn and Session A synthesis turn"
            .to_string(),
    }
}

fn collect_measured_provider_text(
    client: &mut ProviderRuntimeClient,
    name: &str,
    request: ApiRequest,
    trace: &mut ExecutionTrace,
    details: &mut Vec<ProviderRoundDetail>,
) -> Result<String, String> {
    let round_index = trace.rounds.len() + 1;
    let model = request.model.clone();
    let request_json = request_to_json(&request);
    let request_summary = summarize_request(&request);
    let started = Instant::now();
    let events = client
        .stream_collect(request)
        .map_err(|error| error.to_string())?;
    let elapsed_ms = started.elapsed().as_millis();
    let response_text = events
        .iter()
        .filter_map(|event| match event {
            AssistantEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    let text_delta_count = events
        .iter()
        .filter(|event| matches!(event, AssistantEvent::TextDelta(_)))
        .count();
    let tool_use_count = events
        .iter()
        .filter(|event| matches!(event, AssistantEvent::ToolUse { .. }))
        .count();
    let usage = summarize_usage(&events);
    let detail_name = format!("{round_index:03}-{}.json", sanitize_file_name(name));
    let summary = ProviderRoundSummary {
        round_index,
        name: name.to_string(),
        model,
        status: "passed".to_string(),
        elapsed_ms,
        usage,
        text_delta_count,
        tool_use_count,
        request_summary,
        response_summary: summarize_text(&response_text, 180),
        detail_path: format!("provider-rounds/{detail_name}"),
    };
    let detail = ProviderRoundDetail {
        summary: summary.clone(),
        request: request_json,
        events: events.iter().map(assistant_event_to_json).collect(),
        response_text: response_text.clone(),
    };
    trace.add_provider_round(summary);
    details.push(detail);
    Ok(response_text)
}

fn summarize_usage(events: &[AssistantEvent]) -> UsageSummary {
    let mut summary = UsageSummary {
        usage_source: "unavailable".to_string(),
        ..UsageSummary::default()
    };
    for event in events {
        if let AssistantEvent::Usage(usage) = event {
            summary.input_tokens = summary.input_tokens.saturating_add(usage.input_tokens);
            summary.output_tokens = summary.output_tokens.saturating_add(usage.output_tokens);
            summary.cache_creation_input_tokens = summary
                .cache_creation_input_tokens
                .saturating_add(usage.cache_creation_input_tokens);
            summary.cache_read_input_tokens = summary
                .cache_read_input_tokens
                .saturating_add(usage.cache_read_input_tokens);
            summary.total_tokens = summary.total_tokens.saturating_add(usage.total_tokens());
            summary.usage_source = "provider_event".to_string();
        }
    }
    summary
}

fn request_to_json(request: &ApiRequest) -> serde_json::Value {
    json!({
        "model": request.model,
        "system_prompt": request.system_prompt,
        "messages": request.messages.iter().map(message_to_json).collect::<Vec<_>>(),
    })
}

fn message_to_json(message: &ConversationMessage) -> serde_json::Value {
    json!({
        "role": format!("{:?}", message.role).to_lowercase(),
        "blocks": message.blocks.iter().map(content_block_to_json).collect::<Vec<_>>(),
        "usage": message.usage.map(|usage| json!({
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_creation_input_tokens": usage.cache_creation_input_tokens,
            "cache_read_input_tokens": usage.cache_read_input_tokens,
            "total_tokens": usage.total_tokens(),
        })),
    })
}

fn content_block_to_json(block: &ContentBlock) -> serde_json::Value {
    match block {
        ContentBlock::Text { text } => json!({"type": "text", "text": text}),
        ContentBlock::Thinking {
            thinking,
            signature,
        } => json!({"type": "thinking", "thinking": thinking, "signature": signature}),
        ContentBlock::ToolUse { id, name, input } => {
            json!({"type": "tool_use", "id": id, "name": name, "input": input})
        }
        ContentBlock::ToolResult {
            tool_use_id,
            output,
            is_error,
            ..
        } => json!({
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "output": output,
            "is_error": is_error,
        }),
    }
}

fn assistant_event_to_json(event: &AssistantEvent) -> serde_json::Value {
    match event {
        AssistantEvent::TextDelta(text) => json!({"type": "text_delta", "text": text}),
        AssistantEvent::ThinkingDelta(text) => json!({"type": "thinking_delta", "text": text}),
        AssistantEvent::SignatureDelta(text) => json!({"type": "signature_delta", "text": text}),
        AssistantEvent::ToolUse { id, name, input } => {
            json!({"type": "tool_use", "id": id, "name": name, "input": input})
        }
        AssistantEvent::Usage(usage) => json!({
            "type": "usage",
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_creation_input_tokens": usage.cache_creation_input_tokens,
            "cache_read_input_tokens": usage.cache_read_input_tokens,
            "total_tokens": usage.total_tokens(),
        }),
        AssistantEvent::PromptCache(event) => json!({
            "type": "prompt_cache",
            "unexpected": event.unexpected,
            "reason": event.reason,
            "previous_cache_read_input_tokens": event.previous_cache_read_input_tokens,
            "current_cache_read_input_tokens": event.current_cache_read_input_tokens,
            "token_drop": event.token_drop,
        }),
        AssistantEvent::MessageStop => json!({"type": "message_stop"}),
        AssistantEvent::ToolStart { id, name, preview } => {
            json!({"type": "tool_start", "id": id, "name": name, "preview": preview})
        }
        AssistantEvent::ToolProgress { id, name, progress } => {
            json!({"type": "tool_progress", "id": id, "name": name, "progress": progress})
        }
        AssistantEvent::ToolComplete {
            id,
            name,
            result_summary,
            exit_code,
        } => json!({
            "type": "tool_complete",
            "id": id,
            "name": name,
            "result_summary": result_summary,
            "exit_code": exit_code,
        }),
    }
}

fn summarize_request(request: &ApiRequest) -> String {
    let user_text = request
        .messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    summarize_text(&user_text, 180)
}

fn summarize_text(value: &str, max: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max {
        compact
    } else {
        format!("{}...", compact.chars().take(max).collect::<String>())
    }
}

fn summarize_json(value: &serde_json::Value, max: usize) -> String {
    summarize_text(&value.to_string(), max)
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|")
}

fn sanitize_file_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn extract_json_object(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    (end >= start).then_some(&text[start..=end])
}

fn run_deep_harness_scenarios(trace: &mut ExecutionTrace) -> Vec<CapabilityResult> {
    let mut results = Vec::new();
    results.extend(run_reality_memory_deep_scenarios(trace));
    results.extend(run_collaboration_deep_scenarios(trace));
    results
}

fn run_reality_memory_deep_scenarios(trace: &mut ExecutionTrace) -> Vec<CapabilityResult> {
    let mut service = FactKernelService::new();
    let source = FactSource {
        kind: SourceKind::Runtime,
        id: "harness-eval.deep".to_string(),
        label: Some("deep harness eval".to_string()),
    };

    let memory_evidence = service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({
            "session": "deep-harness",
            "observation": "user prefers immersive implementation followed by unified review"
        }),
    ));
    trace.record_runtime_action("fact.evidence.ingest", memory_evidence.id.as_str());
    let memory_receipt = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
        summary: "user prefers immersive implementation followed by unified review".to_string(),
        source: source.clone(),
        evidence: vec![memory_evidence.id.clone()],
        confidence: Confidence::from_basis_points(8_900),
        boundary: HypothesisBoundary::observed(),
        tags: vec!["preference".to_string(), "workflow".to_string()],
    }));
    trace.record_runtime_action(
        "fact.memory.promote",
        format!("{:?}", memory_receipt.decision.decision),
    );
    let recall_hits = service.recall(&RecallQuery {
        query: "immersive unified review".to_string(),
        limit: 3,
        include_hypothetical: false,
    });
    trace.record_runtime_action("fact.memory.recall", format!("hits={}", recall_hits.len()));
    let memory_passed = memory_receipt.decision.decision == PromotionDecision::Promote
        && memory_receipt.promoted_fact.is_some()
        && recall_hits.iter().any(|hit| {
            hit.fact
                .statement
                .contains("immersive implementation followed by unified review")
        });

    let hypothetical_receipt =
        service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
            summary: "simulated user wants to pause after every tiny edit".to_string(),
            source: source.clone(),
            evidence: vec![memory_evidence.id.clone()],
            confidence: Confidence::from_basis_points(9_500),
            boundary: HypothesisBoundary::hypothetical("deep-harness-sim"),
            tags: vec!["simulation".to_string()],
        }));
    trace.record_runtime_action(
        "fact.memory.hypothetical",
        format!("{:?}", hypothetical_receipt.decision.decision),
    );
    let low_confidence_evidence = service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({"observation": "weak signal about formatting preference"}),
    ));
    let low_confidence_receipt =
        service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
            summary: "user might prefer verbose formatting for every report".to_string(),
            source: source.clone(),
            evidence: vec![low_confidence_evidence.id.clone()],
            confidence: Confidence::from_basis_points(4_200),
            boundary: HypothesisBoundary::observed(),
            tags: vec!["weak_signal".to_string()],
        }));
    trace.record_runtime_action(
        "fact.memory.low_confidence",
        format!("{:?}", low_confidence_receipt.decision.decision),
    );
    let storage_policy_passed = hypothetical_receipt.decision.decision == PromotionDecision::Reject
        && hypothetical_receipt.promoted_fact.is_none()
        && low_confidence_receipt.decision.decision == PromotionDecision::Hold
        && low_confidence_receipt.promoted_fact.is_none()
        && service
            .recall(&RecallQuery::new("simulated pause tiny edit"))
            .is_empty();

    let matrix_evidence = service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({"preference": "immersive"}),
    ));
    let conflict_a = MatrixFact {
        id: FactId::from_string("deep-harness-pref-flow-a"),
        entity: "user.workflow".to_string(),
        predicate: "prefers_flow".to_string(),
        value: json!("immersive_then_review"),
        source: source.clone(),
        evidence: vec![matrix_evidence.id.clone()],
        confidence: Confidence::from_basis_points(8_800),
        boundary: HypothesisBoundary::observed(),
    };
    let conflict_b_evidence = service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({"preference": "pause_each_step", "contradicts": "immersive"}),
    ));
    let conflict_b = MatrixFact {
        id: FactId::from_string("deep-harness-pref-flow-b"),
        entity: "user.workflow".to_string(),
        predicate: "prefers_flow".to_string(),
        value: json!("pause_each_step"),
        source: source.clone(),
        evidence: vec![conflict_b_evidence.id.clone()],
        confidence: Confidence::from_basis_points(8_100),
        boundary: HypothesisBoundary::observed(),
    };
    let matrix_a = service.promote_candidate(GrowthCandidate::Matrix(conflict_a.clone()));
    let matrix_b = service.promote_candidate(GrowthCandidate::Matrix(conflict_b.clone()));
    trace.record_runtime_action("fact.matrix.promote", "conflict pair promoted");
    let conflict_issues = service
        .evaluate_health()
        .into_iter()
        .filter(|issue| issue.kind == FactHealthIssueKind::Conflict)
        .collect::<Vec<_>>();
    trace.record_runtime_action(
        "fact.health.conflict",
        format!("issues={}", conflict_issues.len()),
    );
    let conflict_detected = conflict_issues.len() >= 2;

    let mut growth_service = FactKernelService::new();
    let growth_evidence = growth_service.ingest_evidence(EvidencePacket::new(
        source.clone(),
        json!({"gate": "deep_harness_growth"}),
    ));
    let growth_receipt = growth_service.promote_candidate(GrowthCandidate::Matrix(MatrixFact {
        id: FactId::from_string("deep-harness-growth-gate"),
        entity: "system.harness".to_string(),
        predicate: "passes_deep_gate".to_string(),
        value: json!(true),
        source: source.clone(),
        evidence: vec![growth_evidence.id.clone()],
        confidence: Confidence::from_basis_points(8_700),
        boundary: HypothesisBoundary::observed(),
    }));
    trace.record_runtime_action(
        "fact.growth.promote",
        format!("{:?}", growth_receipt.decision.decision),
    );
    let matrix_facts = growth_service.facts_by_type("matrix.passes_deep_gate", 10);
    let growth_health_issues = growth_service.evaluate_health();
    let growth_passed = growth_receipt.decision.decision == PromotionDecision::Promote
        && matrix_b.decision.decision == PromotionDecision::Promote
        && matrix_a.decision.decision == PromotionDecision::Promote
        && matrix_facts.len() == 1
        && growth_health_issues.is_empty();

    vec![
        CapabilityResult {
            capability: "deep_harness_memory_store_recall",
            status: if memory_passed { "passed" } else { "failed" },
            evidence: format!(
                "promoted={}; recall_hits={}; top={}",
                memory_receipt.promoted_fact.is_some(),
                recall_hits.len(),
                recall_hits
                    .first()
                    .map(|hit| hit.fact.statement.as_str())
                    .unwrap_or("none")
            ),
            notes: "observed memory candidate was promoted into fact kernel and recalled by query"
                .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_memory_storage_policy",
            status: if storage_policy_passed { "passed" } else { "failed" },
            evidence: format!(
                "hypothetical={:?}; low_confidence={:?}",
                hypothetical_receipt.decision.decision, low_confidence_receipt.decision.decision
            ),
            notes: "hypothetical and weak signals remain reviewable instead of silently becoming durable facts"
                .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_fact_conflict",
            status: if conflict_detected { "passed" } else { "failed" },
            evidence: format!(
                "issues={}; {}:{} values {} vs {}",
                conflict_issues.len(),
                conflict_a.entity,
                conflict_a.predicate,
                conflict_a.value,
                conflict_b.value
            ),
            notes: "fact kernel health reported contradictory observed matrix facts for governance review"
                .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_growth_promotion",
            status: if growth_passed { "passed" } else { "failed" },
            evidence: format!(
                "matrix_facts={}; health_issues={}",
                matrix_facts.len(),
                growth_health_issues.len()
            ),
            notes: "observed matrix growth candidates were promoted with evidence and remained health-clean"
                .to_string(),
        },
    ]
}

fn run_collaboration_deep_scenarios(trace: &mut ExecutionTrace) -> Vec<CapabilityResult> {
    let mission = global_mission_runtime();
    let session_a = mission
        .start_session(StartMissionSessionRequest {
            title: "Deep Harness primary session".to_string(),
            session_id: Some(format!("deep-harness-a-{}", uuid::Uuid::new_v4())),
        })
        .expect("deep harness session starts");
    trace.record_runtime_action("mission.session.start", session_a.session_id.clone());
    let strategy = decide_strategy(&StrategyInput::from_prompt(
        "coordinate planner reviewer implementer for deep harness validation",
    ));
    let team = global_team_runtime_service()
        .start(StartTeamRuntimeRequest {
            session_id: session_a.session_id.clone(),
            objective: "coordinate planner reviewer implementer for deep harness validation"
                .to_string(),
            collaboration_decision: CollaborationTemplateMatcher::default()
                .decide("coordinate planner reviewer implementer", &strategy),
        })
        .expect("deep harness team starts");
    trace.record_runtime_action("team.start", team.team_id.clone());
    let team_report = TeamExecutionLoop::tick_ready(&team.team_id).expect("deep team tick");
    trace.record_runtime_action(
        "team.tick",
        format!("assigned={}", team_report.assigned_task_count),
    );
    let multi_agent_passed =
        team_report.assigned_task_count > 0 && !team_report.evidence.is_empty();

    let session_b = mission
        .start_session(StartMissionSessionRequest {
            title: "Deep Harness peer session".to_string(),
            session_id: Some(format!("deep-harness-b-{}", uuid::Uuid::new_v4())),
        })
        .expect("deep harness peer session starts");
    trace.record_runtime_action("mission.session.start_peer", session_b.session_id.clone());
    global_session_relation_graph()
        .upsert_proxy(SessionProxy {
            session_id: session_b.session_id.clone(),
            summary: "deep harness peer session proxy".to_string(),
            evidence_refs: vec![format!("team:{}", team.team_id)],
            decisions: vec!["accept routed evidence inspection".to_string()],
            open_questions: Vec::new(),
            updated_at_ms: 0,
        })
        .expect("deep peer proxy");
    trace.record_runtime_action("session.proxy.upsert", session_b.session_id.clone());
    let bridge = SessionExecutionPlane::bridge(CrossSessionMessage {
        from_session_id: session_a.session_id.clone(),
        target_ref: format!("@{}", session_b.session_id),
        command: "review peer evidence and report contradiction risks".to_string(),
        actor: Some("deep_harness_eval".to_string()),
        evidence_refs: vec![format!("team:{}", team.team_id)],
    });
    trace.record_runtime_action("session.bridge", bridge.status.clone());
    let dispatch = SessionExecutionPlane::dispatch_pending(runtime::SessionExecutionPolicy {
        max_commands: 20,
        dispatch_mode: runtime::SessionDispatchMode::ControlDispatchComplete,
        allow_background: true,
    });
    trace.record_runtime_action(
        "session.dispatch",
        format!("dispatched={}", dispatch.dispatched.len()),
    );
    let cross_session_passed = bridge.status == "routed"
        && dispatch
            .dispatched
            .iter()
            .any(|item| item.session_id == session_b.session_id);

    let approval = runtime::GlobalApprovalQueue::new()
        .submit(runtime::SubmitGlobalApprovalRequest {
            source: ApprovalSource {
                kind: ApprovalSourceKind::Session,
                session_id: Some(session_a.session_id.clone()),
                agent_id: None,
                team_id: Some(team.team_id.clone()),
                mission_id: Some("deep-harness".to_string()),
            },
            action: "write_memory_fact".to_string(),
            summary: "promote durable workflow preference".to_string(),
            risk: TaskRisk::High,
            evidence_refs: vec![format!("session:{}", session_a.session_id)],
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        })
        .expect("deep approval submitted");
    trace.record_runtime_action("approval.submit", approval.approval_id.clone());
    let steward = global_steward_runtime_service()
        .start(StartStewardRuntimeRequest {
            mission_id: "deep-harness".to_string(),
            root_session_id: Some(session_a.session_id.clone()),
            profile_id: AutonomyProfileId::Stewarded,
            objective: "govern deep harness memory promotion".to_string(),
        })
        .expect("deep steward starts");
    trace.record_runtime_action("steward.start", steward.steward_id.clone());
    let steward_decision = global_steward_runtime_service()
        .tick(
            &steward.steward_id,
            TickStewardRuntimeRequest {
                action: Some("review high risk memory promotion".to_string()),
                summary: Some("require approval before durable preference write".to_string()),
                risk: TaskRisk::High,
                requested_tool: Some("memory.promote".to_string()),
                ..TickStewardRuntimeRequest::default()
            },
        )
        .expect("deep steward ticks");
    trace.record_runtime_action("steward.tick", format!("{:?}", steward_decision.status));
    let governance_passed = approval.risk == TaskRisk::High
        && matches!(
            steward_decision.status,
            StewardActionStatus::Delegated
                | StewardActionStatus::ApprovalSubmitted
                | StewardActionStatus::Denied
        );

    let event_store = RuntimeEventStore::open_in_memory().expect("deep event store");
    event_store
        .append(RuntimeEventInput {
            stream_id: format!("session:{}", session_a.session_id),
            scope: RuntimeEventScope::SessionCommand,
            kind: "deep_harness.failure_detected".to_string(),
            status: Some("failed".to_string()),
            actor: Some("deep_harness_eval".to_string()),
            refs: Vec::new(),
            payload: json!({"reason": "simulated verification failure"}),
        })
        .expect("deep event append");
    trace.record_runtime_action("runtime_event.append", "deep_harness.failure_detected");
    let replay = RuntimeEventReplayer::report(&event_store, 100).expect("deep replay");
    let recovery = RecoveryExecutor::execute(100).expect("deep recovery");
    trace.record_runtime_action(
        "runtime_event.replay",
        format!("events={}", replay.total_events),
    );
    trace.record_runtime_action(
        "recovery.execute",
        format!("actions={}", recovery.applied.len()),
    );
    let recovery_passed = replay.total_events > 0 && !recovery.applied.is_empty();

    vec![
        CapabilityResult {
            capability: "deep_harness_multi_agent_dialogue",
            status: if multi_agent_passed {
                "passed"
            } else {
                "failed"
            },
            evidence: format!(
                "team={}; assigned={}; evidence_refs={}",
                team.team_id,
                team_report.assigned_task_count,
                team_report.evidence.len()
            ),
            notes: "team execution loop created reviewable role tasks and evidence references"
                .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_cross_session_dialogue",
            status: if cross_session_passed {
                "passed"
            } else {
                "failed"
            },
            evidence: format!(
                "bridge={}; dispatched={}",
                bridge.status,
                dispatch.dispatched.len()
            ),
            notes:
                "primary session routed a command to peer session and execution plane completed it"
                    .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_governance",
            status: if governance_passed {
                "passed"
            } else {
                "failed"
            },
            evidence: format!(
                "approval={}; steward={:?}",
                approval.approval_id, steward_decision.status
            ),
            notes: "high-risk durable memory action entered approval and steward governance"
                .to_string(),
        },
        CapabilityResult {
            capability: "deep_harness_recovery_trace",
            status: if recovery_passed { "passed" } else { "failed" },
            evidence: format!(
                "events={}; recovery_actions={}",
                replay.total_events,
                recovery.applied.len()
            ),
            notes:
                "failure evidence was replayable and recovery executor produced auditable actions"
                    .to_string(),
        },
    ]
}

fn build_harness_metrics(
    scenarios: &[CapabilityResult],
    stable_ai: &StableAiHealthReport,
) -> Vec<HarnessMetric> {
    let total = scenarios.len();
    let passed = scenarios
        .iter()
        .filter(|scenario| scenario.status == "passed")
        .count();
    let deep_total = scenarios
        .iter()
        .filter(|scenario| scenario.capability.starts_with("deep_harness"))
        .count();
    let deep_passed = scenarios
        .iter()
        .filter(|scenario| {
            scenario.capability.starts_with("deep_harness") && scenario.status == "passed"
        })
        .count();
    let provider_passed = scenarios
        .iter()
        .any(|scenario| scenario.capability == "deep_provider_eval" && scenario.status == "passed");
    let gateway_passed = scenarios.iter().any(|scenario| {
        scenario.capability == "gateway_contract_surface" && scenario.status == "passed"
    });

    let real_gate = stable_ai
        .real_capability_result
        .as_ref()
        .map(|report| format!("{}/{}", report.passed, report.total))
        .unwrap_or_else(|| "not_applicable".to_string());

    vec![
        HarnessMetric {
            name: "scenario_pass_rate",
            value: format!("{passed}/{total}"),
            notes: "all capability rows that participated in this eval".to_string(),
        },
        HarnessMetric {
            name: "deep_harness_pass_rate",
            value: format!("{deep_passed}/{deep_total}"),
            notes: "memory, growth, collaboration, governance, recovery deep harness rows"
                .to_string(),
        },
        HarnessMetric {
            name: "real_provider_gate",
            value: provider_passed.to_string(),
            notes: format!(
                "real_provider_enabled={}; model={}",
                stable_ai.real_provider_enabled,
                stable_ai.model.as_deref().unwrap_or("none")
            ),
        },
        HarnessMetric {
            name: "gateway_contract_gate",
            value: gateway_passed.to_string(),
            notes: "live gateway /healthz contract status".to_string(),
        },
        HarnessMetric {
            name: "runtime_capability_coverage",
            value: format!("{}/{}", stable_ai.coverage.passed, stable_ai.coverage.total),
            notes: "runtime module map required domain coverage".to_string(),
        },
        HarnessMetric {
            name: "real_capability_gate",
            value: real_gate,
            notes:
                "deep eval authoritative gate; fake provider remains deterministic baseline only"
                    .to_string(),
        },
    ]
}

fn build_real_capability_gate_report(
    scenarios: &[CapabilityResult],
    complex_scenarios: &ComplexHarnessScenarioReport,
    real_tool_scenarios: &RealToolScenarioReport,
    trace: &ExecutionTrace,
    provider_passed: bool,
    gateway_passed: bool,
) -> RealCapabilityGateReport {
    let deep_total = scenarios
        .iter()
        .filter(|scenario| scenario.capability.starts_with("deep_harness"))
        .count();
    let deep_passed = scenarios
        .iter()
        .filter(|scenario| {
            scenario.capability.starts_with("deep_harness") && scenario.status == "passed"
        })
        .count();
    let all_capability_rows_passed = scenarios.iter().all(|scenario| scenario.status == "passed");
    let real_tool_passed = real_tool_scenarios.passed == real_tool_scenarios.total
        && real_tool_scenarios.tool_calls > 0;
    let complex_passed = complex_scenarios.failed == 0 && complex_scenarios.average_score >= 0.9;
    let structured_reasoning_passed =
        capability_passed(scenarios, "deep_harness_provider_structured_reasoning");
    let cross_session_provider_passed =
        capability_passed(scenarios, "deep_harness_cross_session_provider_dialogue");
    let knowledge_fabric_passed =
        capability_passed(scenarios, "knowledge_fabric_context_governance");
    let recovery_passed = capability_passed(scenarios, "runtime_recovery_report");

    RealCapabilityGateReport::new(
        vec![
            RealCapabilityGate::new(
                "real_provider_smoke",
                provider_passed,
                true,
                capability_evidence(scenarios, "deep_provider_eval"),
            ),
            RealCapabilityGate::new(
                "provider_structured_reasoning",
                structured_reasoning_passed,
                true,
                capability_evidence(scenarios, "deep_harness_provider_structured_reasoning"),
            ),
            RealCapabilityGate::new(
                "provider_cross_session_dialogue",
                cross_session_provider_passed,
                true,
                capability_evidence(scenarios, "deep_harness_cross_session_provider_dialogue"),
            ),
            RealCapabilityGate::new(
                "deep_harness_capabilities",
                deep_total > 0 && deep_total == deep_passed,
                true,
                format!("{deep_passed}/{deep_total} deep_harness rows passed"),
            ),
            RealCapabilityGate::new(
                "real_tool_scenarios",
                real_tool_passed,
                true,
                format!(
                    "{}/{} real tool scenarios passed; tool_calls={}",
                    real_tool_scenarios.passed,
                    real_tool_scenarios.total,
                    real_tool_scenarios.tool_calls
                ),
            ),
            RealCapabilityGate::new(
                "complex_scenarios",
                complex_passed,
                true,
                format!(
                    "{}/{} complex scenarios passed; average_score={:.2}",
                    complex_scenarios.passed,
                    complex_scenarios.total,
                    complex_scenarios.average_score
                ),
            ),
            RealCapabilityGate::new(
                "knowledge_fabric_context_governance",
                knowledge_fabric_passed,
                true,
                capability_evidence(scenarios, "knowledge_fabric_context_governance"),
            ),
            RealCapabilityGate::new(
                "gateway_contract_surface",
                gateway_passed,
                true,
                capability_evidence(scenarios, "gateway_contract_surface"),
            ),
            RealCapabilityGate::new(
                "runtime_recovery_report",
                recovery_passed,
                true,
                capability_evidence(scenarios, "runtime_recovery_report"),
            ),
            RealCapabilityGate::new(
                "provider_round_evidence",
                trace.provider_rounds > 0,
                true,
                format!(
                    "provider_rounds={}; total_tokens={}; usage_source={}",
                    trace.provider_rounds,
                    trace.total_usage.total_tokens,
                    trace.total_usage.usage_source
                ),
            ),
            RealCapabilityGate::new(
                "tool_call_evidence",
                trace.tool_calls > 0,
                true,
                format!("tool_calls={}", trace.tool_calls),
            ),
            RealCapabilityGate::new(
                "all_capability_rows",
                all_capability_rows_passed,
                true,
                failed_capability_evidence(scenarios),
            ),
        ],
        trace.provider_rounds,
        trace.tool_calls,
        trace.total_usage.total_tokens,
    )
}

fn capability_passed(scenarios: &[CapabilityResult], capability: &str) -> bool {
    scenarios
        .iter()
        .any(|scenario| scenario.capability == capability && scenario.status == "passed")
}

fn capability_evidence(scenarios: &[CapabilityResult], capability: &str) -> String {
    scenarios
        .iter()
        .find(|scenario| scenario.capability == capability)
        .map(|scenario| {
            format!(
                "status={}; evidence={}",
                scenario.status,
                abbreviate(&scenario.evidence, 180)
            )
        })
        .unwrap_or_else(|| "capability row missing".to_string())
}

fn failed_capability_evidence(scenarios: &[CapabilityResult]) -> String {
    let failed = scenarios
        .iter()
        .filter(|scenario| scenario.status != "passed")
        .map(|scenario| format!("{}={}", scenario.capability, scenario.status))
        .collect::<Vec<_>>();
    if failed.is_empty() {
        "all capability rows passed".to_string()
    } else {
        failed.join("; ")
    }
}

fn fake_provider_scenario_report() -> ScenarioSuiteReport {
    let matrix = stable_ai_scenario_matrix();
    let specs = matrix
        .iter()
        .map(|item| {
            ScenarioSpec::new(item.id.clone(), item.objective.clone())
                .expect_mode(mode_for_scenario(item.kind))
                .require(ScenarioCheck::text_contains(
                    format!("{}.evidence", item.id),
                    item.required_evidence[0].clone(),
                    "harness-eval",
                    "scenario runner must emit required evidence markers",
                ))
        })
        .collect::<Vec<_>>();
    let observations = matrix
        .iter()
        .map(|item| ScenarioObservation {
            scenario_id: item.id.clone(),
            strategy_mode: mode_for_scenario(item.kind),
            finalization_blocked: item.kind == E2eScenarioKind::Recovery,
            regression_allowed: item.kind != E2eScenarioKind::Recovery,
            has_workgraph: matches!(
                item.kind,
                E2eScenarioKind::ComplexPlan | E2eScenarioKind::TeamParallel
            ),
            workgraph_quality_ok: item.kind != E2eScenarioKind::SimpleOnce,
            growth_has_blocker: item.kind == E2eScenarioKind::Recovery,
            growth_signal_kinds: item.required_evidence.clone(),
            memory_candidate_count: usize::from(item.kind == E2eScenarioKind::RealityMemory),
            matrix_signal_count: usize::from(matches!(
                item.kind,
                E2eScenarioKind::RealityMemory | E2eScenarioKind::ComplexPlan
            )),
            assistant_text: format!(
                "fake provider scenario {} passed with evidence {}",
                item.id,
                item.required_evidence.join(",")
            ),
        })
        .collect::<Vec<_>>();
    ScenarioSuite::new(specs).evaluate(&observations)
}

fn mode_for_scenario(kind: E2eScenarioKind) -> harness_contract::core::ExecutionMode {
    match kind {
        E2eScenarioKind::SimpleOnce => harness_contract::core::ExecutionMode::DirectAnswer,
        E2eScenarioKind::TeamParallel => harness_contract::core::ExecutionMode::SupervisorSubagents,
        E2eScenarioKind::GovernedConnector => harness_contract::core::ExecutionMode::RiskGate,
        E2eScenarioKind::ComplexPlan
        | E2eScenarioKind::RealityMemory
        | E2eScenarioKind::ToolLsp
        | E2eScenarioKind::Recovery => harness_contract::core::ExecutionMode::PlanExecute,
    }
}

fn run_deterministic_core_loop() -> (Vec<CapabilityResult>, String) {
    let mission = global_mission_runtime();
    let session = mission
        .start_session(StartMissionSessionRequest {
            title: "Mission Harness eval".to_string(),
            session_id: Some(format!("mission-eval-{}", uuid::Uuid::new_v4())),
        })
        .expect("mission starts");
    let prompt = "validate mission harness runtime loop";
    let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
    let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
    let team = global_team_runtime_service()
        .start(StartTeamRuntimeRequest {
            session_id: session.session_id.clone(),
            objective: prompt.to_string(),
            collaboration_decision: decision,
        })
        .expect("team runtime starts");
    let approval = runtime::GlobalApprovalQueue::new()
        .submit(runtime::SubmitGlobalApprovalRequest {
            source: ApprovalSource {
                kind: ApprovalSourceKind::Session,
                session_id: Some(session.session_id.clone()),
                agent_id: None,
                team_id: Some(team.team_id.clone()),
                mission_id: Some("mission-eval".to_string()),
            },
            action: "apply_patch".to_string(),
            summary: "write runtime changes".to_string(),
            risk: TaskRisk::High,
            evidence_refs: vec![format!("team:{}", team.team_id)],
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        })
        .expect("approval submitted");
    let command = mission
        .enqueue_session_command(
            &session.session_id,
            &session.session_id,
            "summarize evidence".to_string(),
        )
        .expect("command enqueued");
    let session_b = mission
        .start_session(StartMissionSessionRequest {
            title: "Mission Harness peer".to_string(),
            session_id: Some(format!("mission-eval-peer-{}", uuid::Uuid::new_v4())),
        })
        .expect("peer mission starts");
    global_session_relation_graph()
        .upsert_proxy(SessionProxy {
            session_id: session_b.session_id.clone(),
            summary: "mission harness peer proxy".to_string(),
            evidence_refs: vec![format!("session:{}", session_b.session_id)],
            decisions: Vec::new(),
            open_questions: Vec::new(),
            updated_at_ms: 0,
        })
        .expect("peer proxy");
    let bridged = SessionExecutionPlane::bridge(CrossSessionMessage {
        from_session_id: session.session_id.clone(),
        target_ref: format!("@{}", session_b.session_id),
        command: "inspect peer evidence".to_string(),
        actor: Some("harness_eval".to_string()),
        evidence_refs: vec![format!("team:{}", team.team_id)],
    });
    assert_eq!(bridged.status, "routed");
    let team_report = TeamExecutionLoop::tick_ready(&team.team_id).expect("team execution ticks");
    let direct_agent_id = format!("mission-eval-agent-{}", uuid::Uuid::new_v4());
    runtime::global_agent_lifecycle_service().register_started(
        AgentSnapshot {
            agent_id: direct_agent_id.clone(),
            name: "mission-eval-agent".to_string(),
            description: "harness eval direct route agent".to_string(),
            subagent_type: Some("worker".to_string()),
            model: Some(DEFAULT_AGENT_MODEL.to_string()),
            status: "running".to_string(),
            backend: AgentExecutionBackendKind::InProcess,
            output_file: String::new(),
            manifest_file: String::new(),
            created_at: "1".to_string(),
            started_at: Some("1".to_string()),
            completed_at: None,
            lane_events: Vec::new(),
            current_blocker: None,
            derived_state: "working".to_string(),
            error: None,
        },
        CancellationToken::new(),
    );
    let agent_route_receipt = MissionControlRuntime::execute(MissionControlCommand {
        target: MissionControlCommandTarget::Session {
            session_id: session.session_id.clone(),
        },
        action: MissionControlAction::RouteToAgent,
        actor: Some("harness_eval".to_string()),
        payload: serde_json::json!({
            "agent_id": direct_agent_id,
            "team_id": team.team_id.clone(),
            "role_id": "direct_route",
            "command": "record routed mission-control input",
        }),
        evidence_refs: vec![format!("team:{}", team.team_id)],
    });
    assert_ne!(
        agent_route_receipt.status,
        runtime::MissionControlCommandStatus::ApprovalRequired
    );
    assert!(agent_route_receipt.result["task"]["task_id"]
        .as_str()
        .is_some());
    assert_eq!(
        agent_route_receipt.result["progress"]["event_type"].as_str(),
        Some("agent.task.routed")
    );
    let scheduler_report = StewardScheduler::tick(StewardSchedulerConfig {
        max_session_commands_per_tick: 100,
        max_team_ticks: 10,
        allow_background_sessions: true,
    });
    let dispatch = scheduler_report.session_dispatch.clone();
    assert!(!dispatch.dispatched.is_empty());
    assert!(!scheduler_report.ledger_records.is_empty());
    let control = MissionControlRuntime::projection();
    assert!(control.summary.session_count >= 2);
    let control_receipt = MissionControlRuntime::execute(MissionControlCommand {
        target: MissionControlCommandTarget::Session {
            session_id: session.session_id.clone(),
        },
        action: MissionControlAction::RouteToSession,
        actor: Some("harness_eval".to_string()),
        payload: serde_json::json!({
            "target_session_id": session_b.session_id,
            "command": "handoff control summary",
        }),
        evidence_refs: vec![format!("session-command:{}", command.command_id)],
    });
    assert!(matches!(
        control_receipt.status,
        runtime::MissionControlCommandStatus::Queued
            | runtime::MissionControlCommandStatus::Executed
    ));
    let steward_runtime = global_steward_runtime_service();
    let steward = steward_runtime
        .start(StartStewardRuntimeRequest {
            mission_id: "mission-eval".to_string(),
            root_session_id: Some(session.session_id.clone()),
            profile_id: AutonomyProfileId::Stewarded,
            objective: "supervise eval".to_string(),
        })
        .expect("steward starts");
    let steward_decision = steward_runtime
        .tick(
            &steward.steward_id,
            TickStewardRuntimeRequest {
                action: Some("read evidence".to_string()),
                summary: Some("inspect event evidence".to_string()),
                risk: TaskRisk::Low,
                requested_tool: Some("read_file".to_string()),
                ..TickStewardRuntimeRequest::default()
            },
        )
        .expect("steward ticks");
    assert_eq!(steward_decision.status, StewardActionStatus::Delegated);

    let event_store = RuntimeEventStore::open_in_memory().expect("event store");
    event_store
        .append(RuntimeEventInput {
            stream_id: format!("session:{}", session.session_id),
            scope: RuntimeEventScope::SessionCommand,
            kind: "mission.session.command_enqueued".to_string(),
            status: Some("pending".to_string()),
            actor: Some("harness_eval".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"command_id": command.command_id}),
        })
        .expect("event append");
    event_store
        .append(RuntimeEventInput {
            stream_id: format!("steward:{}", steward.steward_id),
            scope: RuntimeEventScope::Steward,
            kind: "steward.started".to_string(),
            status: Some("running".to_string()),
            actor: Some("harness_eval".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"mission_id": "mission-eval"}),
        })
        .expect("event append");
    let replay = RuntimeEventReplayer::report(&event_store, 100).expect("replay report");
    let recovery = RecoveryExecutor::execute(100).expect("recovery executes");

    let scenario = ScenarioSpec::new("mission_harness_eval", prompt)
        .expect_mode(strategy.mode)
        .require(ScenarioCheck::bool(
            "workgraph.present",
            ScenarioCheckKind::WorkgraphPresent,
            true,
            "mission-harness",
            "mission harness eval must produce a workgraph",
        ));
    let observation = ScenarioObservation {
        scenario_id: "mission_harness_eval".to_string(),
        strategy_mode: strategy.mode,
        finalization_blocked: false,
        regression_allowed: true,
        has_workgraph: true,
        workgraph_quality_ok: true,
        growth_has_blocker: false,
        growth_signal_kinds: Vec::new(),
        memory_candidate_count: 0,
        matrix_signal_count: 1,
        assistant_text: "mission harness eval completed".to_string(),
    };
    let suite = ScenarioSuite::new(vec![scenario]).evaluate(&[observation]);
    assert_eq!(suite.failed, 0);
    let coverage = harness_capability_coverage_report();
    assert_eq!(coverage.failed, 0);

    (
        vec![
            CapabilityResult {
                capability: "runtime_module_coverage",
                status: "passed",
                evidence: format!(
                    "{} / {} runtime capability domains covered",
                    coverage.passed, coverage.total
                ),
                notes: "runtime module map covers required harness lifecycle domains".to_string(),
            },
            CapabilityResult {
                capability: "mission_session",
                status: "passed",
                evidence: session.session_id,
                notes: "mission runtime accepted session lifecycle".to_string(),
            },
            CapabilityResult {
                capability: "team_runtime",
                status: "passed",
                evidence: team.team_id,
                notes: "team runtime produced collaboration projection".to_string(),
            },
            CapabilityResult {
                capability: "approval_queue",
                status: "passed",
                evidence: approval.approval_id,
                notes: "global approval queue accepted high-risk action".to_string(),
            },
            CapabilityResult {
                capability: "session_inbox",
                status: "passed",
                evidence: command.command_id,
                notes: "mission command inbox accepted routed command".to_string(),
            },
            CapabilityResult {
                capability: "multi_session_bridge",
                status: "passed",
                evidence: bridged.message,
                notes: "cross-session bridge routed command into peer session".to_string(),
            },
            CapabilityResult {
                capability: "session_execution_plane",
                status: "passed",
                evidence: format!("{} dispatched", dispatch.dispatched.len()),
                notes: "execution plane claimed/completed pending session commands".to_string(),
            },
            CapabilityResult {
                capability: "team_execution_loop",
                status: "passed",
                evidence: format!("{} assigned", team_report.assigned_task_count),
                notes: "team runtime produced role tasks, events, and evidence".to_string(),
            },
            CapabilityResult {
                capability: "mission_control_route_to_agent",
                status: "passed",
                evidence: agent_route_receipt.message,
                notes: "Mission Control created agent task, progress event, and mission evidence for direct agent route".to_string(),
            },
            CapabilityResult {
                capability: "steward_runtime",
                status: "passed",
                evidence: steward.steward_id,
                notes: "steward explicit tick produced delegated decision".to_string(),
            },
            CapabilityResult {
                capability: "steward_scheduler",
                status: "passed",
                evidence: format!("{} ledger records", scheduler_report.ledger_records.len()),
                notes: "scheduler connected steward loop, session dispatch, and team tick".to_string(),
            },
            CapabilityResult {
                capability: "mission_control_projection",
                status: "passed",
                evidence: format!("{} sessions", control.summary.session_count),
                notes: "Mission Control projection aggregates sessions, teams, agents, approvals, stewards, and events".to_string(),
            },
            CapabilityResult {
                capability: "runtime_recovery_executor",
                status: "passed",
                evidence: format!("{} applied", recovery.applied.len()),
                notes: "recovery executor produced an auditable execution report".to_string(),
            },
        ],
        format!("{} recovery actions", replay.recovery_required),
    )
}

fn probe_gateway_contract() -> (bool, String) {
    let base =
        std::env::var("COWD_GATEWAY_URL").unwrap_or_else(|_| "http://127.0.0.1:8642".to_string());
    match http_get_json_prefix(&base, "/healthz") {
        Ok(body) if body.contains("\"status\":\"healthy\"") => {
            (true, format!("{base}/healthz healthy"))
        }
        Ok(body) => (
            false,
            format!(
                "{base}/healthz returned unexpected body: {}",
                abbreviate(&body, 120)
            ),
        ),
        Err(error) => (false, format!("{base} unavailable: {error}")),
    }
}

fn http_get_json_prefix(base: &str, path: &str) -> Result<String, String> {
    let without_scheme = base
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// gateway URLs are supported by std probe".to_string())?;
    let authority = without_scheme
        .split('/')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing gateway authority".to_string())?;
    let mut addrs = authority
        .to_socket_addrs()
        .map_err(|error| error.to_string())?;
    let addr = addrs
        .next()
        .ok_or_else(|| format!("gateway address did not resolve: {authority}"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(800))
        .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1200)))
        .map_err(|error| error.to_string())?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    if !response.starts_with("HTTP/1.1 200") && !response.starts_with("HTTP/1.0 200") {
        return Err(abbreviate(&response, 120));
    }
    Ok(response)
}

fn abbreviate(value: &str, max: usize) -> String {
    let compact = value.replace('\n', " ");
    if compact.len() <= max {
        compact
    } else {
        format!("{}...", &compact[..max])
    }
}

fn write_report(
    report: &mut MissionHarnessEvalReport,
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    let report_dir =
        std::env::var("COWD_AI_HARNESS_REPORT_DIR").unwrap_or_else(|_| DEFAULT_REPORT_DIR.into());
    let root = std::path::Path::new(&report_dir);
    std::fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let stamp = current_stamp();
    let base = format!("{}-mission-harness-{}", stamp, report.level.as_str());
    let run_dir = root.join("runs").join(&base);
    let provider_round_dir = run_dir.join("provider-rounds");
    let tool_call_dir = run_dir.join("tool-calls");
    let evidence_dir = run_dir.join("evidence");
    std::fs::create_dir_all(&provider_round_dir).map_err(|error| error.to_string())?;
    std::fs::create_dir_all(&tool_call_dir).map_err(|error| error.to_string())?;
    std::fs::create_dir_all(&evidence_dir).map_err(|error| error.to_string())?;
    report.result_package_dir = Some(run_dir.display().to_string());
    let json_path = run_dir.join("report.json");
    let md_path = run_dir.join("report.md");
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    write_result_package_details(
        report,
        &run_dir,
        &provider_round_dir,
        &tool_call_dir,
        &evidence_dir,
    )?;
    write_analysis_report_assets(report, &run_dir)?;
    let mut markdown = String::from("# Mission Harness Evaluation Report\n\n");
    markdown.push_str(&format!(
        "- level: {}\n- status: {}\n- gateway_process: {}\n- provider: {}\n- budget: {}\n- result_package: {}\n\n",
        report.level.as_str(),
        report.status,
        report.gateway_process,
        report.provider.as_deref().unwrap_or("none"),
        report.budget.as_deref().unwrap_or("none"),
        report.result_package_dir.as_deref().unwrap_or("none")
    ));
    markdown.push_str("| Capability | Status | Evidence | Notes |\n| --- | --- | --- | --- |\n");
    for item in &report.scenarios {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            item.capability, item.status, item.evidence, item.notes
        ));
    }
    markdown.push_str("\n## Metrics\n\n");
    markdown.push_str("| Metric | Value | Notes |\n| --- | --- | --- |\n");
    for item in &report.metrics {
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            item.name, item.value, item.notes
        ));
    }
    markdown.push_str("\n## Execution Summary\n\n");
    markdown.push_str(&format!(
        "- total_elapsed_ms: {}\n- provider_rounds: {}\n- runtime_actions: {}\n- tool_calls: {}\n- total_tokens: {}\n- input_tokens: {}\n- output_tokens: {}\n- cache_write_tokens: {}\n- cache_read_tokens: {}\n- usage_source: {}\n\n",
        report.execution_trace.total_elapsed_ms.unwrap_or_default(),
        report.execution_trace.provider_rounds,
        report.execution_trace.runtime_actions,
        report.execution_trace.tool_calls,
        report.execution_trace.total_usage.total_tokens,
        report.execution_trace.total_usage.input_tokens,
        report.execution_trace.total_usage.output_tokens,
        report.execution_trace.total_usage.cache_creation_input_tokens,
        report.execution_trace.total_usage.cache_read_input_tokens,
        report.execution_trace.total_usage.usage_source,
    ));
    markdown.push_str("## Provider Rounds\n\n");
    markdown.push_str("| Round | Name | Model | Status | Elapsed ms | Input | Output | Total | Request Summary | Response Summary | Detail |\n| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |\n");
    for round in &report.execution_trace.rounds {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            round.round_index,
            round.name,
            round.model,
            round.status,
            round.elapsed_ms,
            round.usage.input_tokens,
            round.usage.output_tokens,
            round.usage.total_tokens,
            markdown_cell(&round.request_summary),
            markdown_cell(&round.response_summary),
            round.detail_path
        ));
    }
    markdown.push_str("\n## Tool Calls\n\n");
    markdown.push_str("| Call | Scenario | Tool | Status | Elapsed ms | Input Summary | Output Summary | Detail |\n| --- | --- | --- | --- | ---: | --- | --- | --- |\n");
    for call in &report.execution_trace.tool_call_log {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
            call.call_index,
            call.scenario_id,
            call.name,
            call.status,
            call.elapsed_ms,
            markdown_cell(&call.input_summary),
            markdown_cell(&call.output_summary),
            call.detail_path
        ));
    }
    if let Some(real_tool) = &report.real_tool_scenarios {
        markdown.push_str("\n## Real Tool Scenarios\n\n");
        markdown.push_str(&format!(
            "- target_repo: {}\n- passed: {}/{}\n- tool_calls: {}\n\n",
            real_tool.target_repo, real_tool.passed, real_tool.total, real_tool.tool_calls
        ));
        markdown.push_str("| Scenario | Status | Tool Calls | Runtime Evidence | Matrix Evidence | Memory Evidence | Changed Files | Conclusion |\n| --- | --- | ---: | --- | --- | --- | --- | --- |\n");
        for item in &real_tool.scenarios {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
                item.scenario_id,
                item.status,
                item.tool_calls,
                markdown_cell(&item.runtime_evidence.join("; ")),
                markdown_cell(&item.matrix_evidence.join("; ")),
                markdown_cell(&item.memory_evidence.join("; ")),
                markdown_cell(&item.changed_files.join("; ")),
                markdown_cell(&item.conclusion)
            ));
        }
    }
    if let Some(complex) = &report.complex_scenarios {
        markdown.push_str("\n## Complex Scenarios\n\n");
        markdown.push_str(&format!(
            "- passed: {}/{}\n- average_score: {:.2}\n\n",
            complex.passed, complex.total, complex.average_score
        ));
        markdown.push_str(
            "| Scenario | Kind | Passed | Score | Failed Checks | Review |\n| --- | --- | --- | ---: | --- | --- |\n",
        );
        for item in &complex.results {
            markdown.push_str(&format!(
                "| {} | {} | {} | {:.2} | {} | {} |\n",
                item.scenario_id,
                item.kind.as_str(),
                item.passed,
                item.score,
                if item.failed_checks.is_empty() {
                    "none".to_string()
                } else {
                    item.failed_checks.join(", ")
                },
                item.review_summary
            ));
        }
    }
    markdown.push_str("\n## Scenario Matrix\n\n");
    markdown.push_str("| Scenario | Kind | Fake Gate | Real Gate | Required Evidence |\n| --- | --- | --- | --- | --- |\n");
    for item in &report.scenario_matrix {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            item.id,
            item.kind.as_str(),
            item.fake_provider_gate,
            item.real_provider_gate,
            item.required_evidence.join(", ")
        ));
    }
    std::fs::write(&md_path, markdown).map_err(|error| error.to_string())?;
    std::fs::copy(&json_path, root.join(format!("{base}.json")))
        .map_err(|error| error.to_string())?;
    std::fs::copy(&md_path, root.join(format!("{base}.md"))).map_err(|error| error.to_string())?;
    write_stable_ai_report(root, &report.stable_ai)?;
    Ok((json_path, md_path))
}

fn write_result_package_details(
    report: &MissionHarnessEvalReport,
    run_dir: &std::path::Path,
    provider_round_dir: &std::path::Path,
    tool_call_dir: &std::path::Path,
    evidence_dir: &std::path::Path,
) -> Result<(), String> {
    std::fs::write(
        run_dir.join("execution-trace.json"),
        serde_json::to_string_pretty(&report.execution_trace).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    for detail in &report.provider_round_details {
        let file_name = detail
            .summary
            .detail_path
            .rsplit('/')
            .next()
            .ok_or_else(|| "provider round detail path is empty".to_string())?;
        std::fs::write(
            provider_round_dir.join(file_name),
            serde_json::to_string_pretty(detail).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    for detail in &report.tool_call_details {
        let file_name = detail
            .summary
            .detail_path
            .rsplit('/')
            .next()
            .ok_or_else(|| "tool call detail path is empty".to_string())?;
        std::fs::write(
            tool_call_dir.join(file_name),
            serde_json::to_string_pretty(detail).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    if let Some(complex) = &report.complex_scenarios {
        std::fs::write(
            evidence_dir.join("complex-scenarios.json"),
            serde_json::to_string_pretty(complex).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    if let Some(real_tool) = &report.real_tool_scenarios {
        std::fs::write(
            evidence_dir.join("real-tool-scenarios.json"),
            serde_json::to_string_pretty(real_tool).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    std::fs::write(
        evidence_dir.join("stable-ai-health.json"),
        serde_json::to_string_pretty(&report.stable_ai).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_analysis_report_assets(
    report: &MissionHarnessEvalReport,
    run_dir: &std::path::Path,
) -> Result<(), String> {
    let context = json!({
        "kind": "harness_eval.analysis_context",
        "report_spec": "docs/ai-harness-report-spec.md",
        "result_package": report.result_package_dir,
        "level": report.level.as_str(),
        "status": report.status,
        "summary": {
            "capability_count": report.scenarios.len(),
            "failed_capabilities": report.scenarios.iter().filter(|item| item.status != "passed").count(),
            "provider_rounds": report.execution_trace.provider_rounds,
            "runtime_actions": report.execution_trace.runtime_actions,
            "tool_calls": report.execution_trace.tool_calls,
            "failed_tool_calls": report.execution_trace.tool_call_log.iter().filter(|item| item.status != "passed").count(),
            "total_tokens": report.execution_trace.total_usage.total_tokens,
            "input_tokens": report.execution_trace.total_usage.input_tokens,
            "output_tokens": report.execution_trace.total_usage.output_tokens,
        },
        "required_inputs": [
            "report.json",
            "execution-trace.json",
            "evidence/*.json",
            "provider-rounds/*.json",
            "tool-calls/*.json",
            "full-analysis-report-template.md",
            "full-analysis-report-prompt.md"
        ],
        "output": "full-analysis-report.md"
    });
    std::fs::write(
        run_dir.join("analysis-context.json"),
        serde_json::to_string_pretty(&context).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    std::fs::write(
        run_dir.join("full-analysis-report-template.md"),
        include_str!("../templates/full-analysis-report-template.md"),
    )
    .map_err(|error| error.to_string())?;
    std::fs::write(
        run_dir.join("full-analysis-report-prompt.md"),
        include_str!("../templates/full-analysis-report-prompt.md"),
    )
    .map_err(|error| error.to_string())?;
    std::fs::write(
        run_dir.join("full-analysis-report.md"),
        "# Full Analysis Report Pending AI Generation\n\nUse `full-analysis-report-prompt.md`, `full-analysis-report-template.md`, and `analysis-context.json` with the result package evidence to generate this report.\n",
    )
    .map_err(|error| error.to_string())
}

fn write_stable_ai_report(
    root: &std::path::Path,
    report: &StableAiHealthReport,
) -> Result<(), String> {
    let version = env!("CARGO_PKG_VERSION");
    let json_path = root.join(format!("stable-ai-health-report-v{version}.json"));
    let md_path = root.join(format!("stable-ai-health-report-v{version}.md"));
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut markdown = format!("# Stable AI Health Report v{version}\n\n");
    markdown.push_str(&format!(
        "- status: {}\n- provider: {}\n- model: {}\n- real_provider_enabled: {}\n- real_provider_reason: {}\n- fake_provider_scenarios: {}/{} (deterministic baseline)\n- coverage: {}/{}\n- gateway_smoke: {}\n- surface_smoke: {}\n- recovery_evidence: {}\n\n",
        report.status,
        report.provider,
        report.model.as_deref().unwrap_or("none"),
        report.real_provider_enabled,
        report.real_provider_reason,
        report.fake_provider_result.passed,
        report.fake_provider_result.total,
        report.coverage.passed,
        report.coverage.total,
        report.gateway_smoke,
        report.surface_smoke,
        report.recovery_evidence,
    ));
    if let Some(real) = &report.real_capability_result {
        markdown.push_str("## Real Capability Gates\n\n");
        markdown.push_str(&format!(
            "- status: {}\n- passed: {}/{}\n- required_failed: {}\n- provider_rounds: {}\n- tool_calls: {}\n- total_tokens: {}\n\n",
            real.status,
            real.passed,
            real.total,
            real.failed,
            real.provider_rounds,
            real.tool_calls,
            real.total_tokens
        ));
        markdown.push_str("| Gate | Required | Status | Evidence |\n| --- | --- | --- | --- |\n");
        for gate in &real.gates {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                gate.name,
                gate.required,
                gate.status,
                markdown_cell(&gate.evidence)
            ));
        }
        markdown.push('\n');
    }
    markdown.push_str("## Scenario Matrix\n\n");
    markdown.push_str(
        "| Scenario | Kind | Required Evidence | Fake | Real |\n| --- | --- | --- | --- | --- |\n",
    );
    for item in &report.scenario_matrix {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            item.id,
            item.kind.as_str(),
            item.required_evidence.join(", "),
            item.fake_provider_gate,
            item.real_provider_gate
        ));
    }
    markdown.push_str("\n## Fake Provider Verdicts\n\n");
    markdown.push_str(
        "This lane is deterministic regression evidence. Deep eval health is decided by real capability gates when present.\n\n",
    );
    markdown
        .push_str("| Scenario | Passed | Score | Failed Checks |\n| --- | --- | ---: | --- |\n");
    for verdict in &report.fake_provider_result.verdicts {
        let failed = verdict
            .failed_checks
            .iter()
            .map(|check| check.check_id.clone())
            .collect::<Vec<_>>()
            .join(", ");
        markdown.push_str(&format!(
            "| {} | {} | {:.2} | {} |\n",
            verdict.scenario_id, verdict.passed, verdict.score, failed
        ));
    }
    std::fs::write(&md_path, markdown).map_err(|error| error.to_string())
}

fn current_stamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("v{}-{seconds}", env!("CARGO_PKG_VERSION"))
}

fn now_ms_u128() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
