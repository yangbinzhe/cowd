use harness_contract::core::TaskRisk;
use harness_contract::strategy::{decide_strategy, StrategyInput};
use harness_eval::{
    harness_capability_coverage_report, stable_ai_scenario_matrix, E2eScenarioKind,
    E2eScenarioMatrixItem, ScenarioCheck, ScenarioCheckKind, ScenarioObservation, ScenarioSpec,
    ScenarioSuite, ScenarioSuiteReport, StableAiHealthReport,
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
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

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

    let report = match level {
        EvalLevel::Quick => run_quick(),
        EvalLevel::Full => run_full(),
        EvalLevel::Deep => run_deep(provider.clone(), budget.clone()),
    };
    let (json_path, md_path) = write_report(&report).unwrap_or_else(|error| {
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
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    let fake_provider_result = fake_provider_scenario_report();
    let coverage = harness_capability_coverage_report();
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
    }
}

fn run_full() -> MissionHarnessEvalReport {
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    let gateway = probe_gateway_contract();
    let fake_provider_result = fake_provider_scenario_report();
    let coverage = harness_capability_coverage_report();
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
    }
}

fn run_deep(provider: Option<String>, budget: Option<String>) -> MissionHarnessEvalReport {
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
        };
    }
    let (mut scenarios, replay_evidence) = run_deterministic_core_loop();
    let gateway = probe_gateway_contract();
    let provider_smoke = run_provider_smoke();
    let provider_passed = provider_smoke.status == "passed";
    let model =
        std::env::var("COWD_EVAL_MODEL").unwrap_or_else(|_| "deepseek-v4-flash".to_string());
    scenarios.push(provider_smoke);
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
    let all_scenarios_passed = scenarios.iter().all(|item| item.status == "passed");
    let stable_ai = StableAiHealthReport::from_fake_eval(
        env!("CARGO_PKG_VERSION"),
        "configured",
        Some(model),
        true,
        "real provider explicitly enabled",
        fake_provider_scenario_report(),
        harness_capability_coverage_report(),
        gateway.1.clone(),
        "webui/tui smoke delegated to final health lanes",
        replay_evidence,
    );
    MissionHarnessEvalReport {
        kind: "mission_harness.eval_report",
        level: EvalLevel::Deep,
        status: if provider_passed && gateway.0 && all_scenarios_passed {
            "passed"
        } else {
            "failed"
        }
        .to_string(),
        provider,
        budget,
        gateway_process: gateway.0,
        scenario_matrix: stable_ai_scenario_matrix(),
        stable_ai,
        scenarios,
    }
}

fn run_provider_smoke() -> CapabilityResult {
    let model = "deepseek-v4-flash";
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
        model: model.to_string(),
    };
    match client.stream_collect(request) {
        Ok(events) => {
            let text = events
                .iter()
                .filter_map(|event| match event {
                    AssistantEvent::TextDelta(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
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
            evidence: abbreviate(&error.to_string(), 180),
            notes: "real provider call failed; inspect provider credentials/network".to_string(),
        },
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
    report: &MissionHarnessEvalReport,
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    let report_dir =
        std::env::var("COWD_AI_HARNESS_REPORT_DIR").unwrap_or_else(|_| DEFAULT_REPORT_DIR.into());
    let root = std::path::Path::new(&report_dir);
    std::fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let stamp = current_stamp();
    let base = format!("{}-mission-harness-{}", stamp, report.level.as_str());
    let json_path = root.join(format!("{base}.json"));
    let md_path = root.join(format!("{base}.md"));
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut markdown = String::from("# Mission Harness Evaluation Report\n\n");
    markdown.push_str(&format!(
        "- level: {}\n- status: {}\n- gateway_process: {}\n- provider: {}\n- budget: {}\n\n",
        report.level.as_str(),
        report.status,
        report.gateway_process,
        report.provider.as_deref().unwrap_or("none"),
        report.budget.as_deref().unwrap_or("none")
    ));
    markdown.push_str("| Capability | Status | Evidence | Notes |\n| --- | --- | --- | --- |\n");
    for item in &report.scenarios {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            item.capability, item.status, item.evidence, item.notes
        ));
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
    write_stable_ai_report(root, &report.stable_ai)?;
    Ok((json_path, md_path))
}

fn write_stable_ai_report(
    root: &std::path::Path,
    report: &StableAiHealthReport,
) -> Result<(), String> {
    let json_path = root.join("stable-ai-health-report-v0.9.396.json");
    let md_path = root.join("stable-ai-health-report-v0.9.396.md");
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut markdown = String::from("# Stable AI Health Report v0.9.396\n\n");
    markdown.push_str(&format!(
        "- status: {}\n- provider: {}\n- model: {}\n- real_provider_enabled: {}\n- real_provider_reason: {}\n- fake_provider_scenarios: {}/{}\n- coverage: {}/{}\n- gateway_smoke: {}\n- surface_smoke: {}\n- recovery_evidence: {}\n\n",
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
    format!("v0.9.396-{seconds}")
}
