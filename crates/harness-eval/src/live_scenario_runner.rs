use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    live_scenario_observer::{LiveScenarioObserver, MAX_DRAIN_PAGES_PER_PROBE},
    session_actor::SessionActor,
    HarnessEvalRunnerOptions,
};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_DEFAULT_SCENARIO_TIMEOUT: Duration = Duration::from_secs(600);
const GROUP_THEORY_SCENARIO_ID: &str = "live_group_theory_ai_research_simulation";
const LARGE_SCALE_SCENARIO_ID: &str = "live_qwen38_large_scale_collaboration";
const AUTONOMOUS_DEEPSEEK_SCENARIO_ID: &str = "live_autonomous_collaboration_deepseek";
const AUTONOMOUS_DEEPSEEK_OUTPUT_PATH: &str = "group-theory-ai-autonomous-evaluation.html";
/// Live scenario that pins an OPERATOR-named Team count. The scenario (the
/// operator) explicitly requires three Teams, the only remaining source of a
/// binding Team obligation now that prose inference was removed.
const EXPLICIT_TEAM_SCENARIO_ID: &str = "live_explicit_team_requirement";
/// A deliberately small, short collaboration used to exercise the whole
/// team/task/review/completion loop at a scale a weak local model can finish.
const SMALL_TEAM_SCENARIO_ID: &str = "live_small_two_team_collaboration";
/// Exactly ONE Team, named explicitly by the operator. This is the control for
/// the no-team and two-team scenarios: it proves a single-Team topology is
/// reachable and honoured (floor == ceiling == 1).
const ONE_TEAM_SCENARIO_ID: &str = "live_one_team_execution";
const RELEASE_CERTIFICATION_SCENARIOS: [&str; 6] = [
    "live_direct_terminal",
    "live_tool_evidence",
    "live_single_architecture_baseline",
    EXPLICIT_TEAM_SCENARIO_ID,
    "live_team_projection",
    "live_agent_escalation",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveClaimScope {
    Focused,
    ReleaseCertification,
}

impl LiveClaimScope {
    fn from_environment() -> Result<Self, String> {
        match std::env::var("COWD_EVAL_CLAIM_SCOPE")
            .unwrap_or_else(|_| "focused".to_string())
            .trim()
        {
            "focused" => Ok(Self::Focused),
            "release-certification" => Ok(Self::ReleaseCertification),
            value => Err(format!(
                "COWD_EVAL_CLAIM_SCOPE must be `focused` or `release-certification`, got `{value}`"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Focused => "focused",
            Self::ReleaseCertification => "release-certification",
        }
    }
}

fn release_certification_scenarios_present(scenarios: &[Value]) -> bool {
    let observed = scenarios
        .iter()
        .filter_map(|scenario| scenario.get("scenario_id").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    RELEASE_CERTIFICATION_SCENARIOS
        .iter()
        .all(|scenario_id| observed.contains(scenario_id))
}

fn env_flag_enabled(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn scenario_enabled(
    selected: Option<&BTreeSet<String>>,
    scenario_id: &str,
    legacy_opt_in: bool,
) -> bool {
    selected.map_or(legacy_opt_in, |ids| ids.contains(scenario_id))
}

/// Keep the expensive, real-provider research exercise opt-in.  It is a
/// production-path acceptance scenario, but should not silently add provider
/// usage to the standard regression suite.
fn group_theory_research_scenario_enabled() -> bool {
    let selected = selected_live_scenario_ids();
    scenario_enabled(
        selected.as_ref(),
        GROUP_THEORY_SCENARIO_ID,
        env_flag_enabled("COWD_EVAL_GROUP_THEORY_RESEARCH"),
    )
}

fn large_scale_collaboration_scenario_enabled() -> bool {
    let selected = selected_live_scenario_ids();
    scenario_enabled(
        selected.as_ref(),
        LARGE_SCALE_SCENARIO_ID,
        env_flag_enabled("COWD_EVAL_LARGE_SCALE_COLLABORATION"),
    )
}

fn autonomous_deepseek_scenario_enabled() -> bool {
    let selected = selected_live_scenario_ids();
    scenario_enabled(
        selected.as_ref(),
        AUTONOMOUS_DEEPSEEK_SCENARIO_ID,
        env_flag_enabled("COWD_EVAL_AUTONOMOUS_COLLABORATION"),
    )
}

fn small_team_scenario_enabled() -> bool {
    let selected = selected_live_scenario_ids();
    scenario_enabled(
        selected.as_ref(),
        SMALL_TEAM_SCENARIO_ID,
        env_flag_enabled("COWD_EVAL_SMALL_TEAM"),
    )
}

/// Two Teams over two independent responsibility domains, each with a bounded
/// Task that reads one real source file, is independently reviewed, and feeds a
/// short cross-Team synthesis. Small enough for a weak local model to close the
/// loop, representative of the full contract (teams, tasks, review, integration).
fn small_team_spec() -> Result<LiveScenarioSpec, String> {
    let prompt = "This is a small, bounded Agent-first collaboration in an isolated workspace. Cover exactly TWO independent responsibility domains: (1) the runtime execution boundary (crates/runtime), and (2) the gateway tool/authority boundary (crates/gateway). Autonomously create TWO Teams with real Tasks; do not reuse fixed names or fixed member counts. Each Team must read at least ONE real source file under its crate with a read-only tool and cite the exact `crates/.../*.rs` path it actually opened. Every Task must be claimed by an Agent, submitted with a durable artifact/evidence reference, and independently reviewed by a different Agent. Publish ONE cross-Team synthesis Task that depends_on both upstream Tasks and consumes their accepted artifacts, then submit it for independent review. Do not invent files, links, or tool output; keep any unresolved item explicit. Use only read-only tools and Agent actions; do not use bash or write files.";
    Ok(LiveScenarioSpec {
        id: SMALL_TEAM_SCENARIO_ID,
        prompt,
        acceptance: LiveAcceptance::ArchitectureQuality {
            minimum_teams: 2,
            minimum_claimed_cross_team_edges: 1,
            evidence_profile: ArchitectureEvidenceProfile::Basic,
        },
        timeout: LiveScenarioTimeout::team(),
    })
}

#[derive(Deserialize)]
struct AutonomousDeepseekTemplate {
    schema_version: u32,
    scenario_id: String,
    output_path: String,
    prompt_template: String,
}

fn autonomous_deepseek_spec() -> Result<LiveScenarioSpec, String> {
    let template: AutonomousDeepseekTemplate = serde_json::from_str(include_str!(
        "../templates/autonomous-collaboration-deepseek-v1.json"
    ))
    .map_err(|error| format!("invalid autonomous DeepSeek template: {error}"))?;
    if template.schema_version != 2
        || template.scenario_id != AUTONOMOUS_DEEPSEEK_SCENARIO_ID
        || template.output_path != AUTONOMOUS_DEEPSEEK_OUTPUT_PATH
    {
        return Err("autonomous DeepSeek template identity is invalid".to_string());
    }
    let minimum_agents = std::env::var("COWD_EVAL_AUTONOMOUS_AGENT_SCALE")
        .unwrap_or_else(|_| "8".to_string())
        .parse::<usize>()
        .map_err(|_| "COWD_EVAL_AUTONOMOUS_AGENT_SCALE must be an integer".to_string())?;
    if !(2..=64).contains(&minimum_agents) {
        return Err("COWD_EVAL_AUTONOMOUS_AGENT_SCALE must be between 2 and 64".to_string());
    }
    let minimum_teams = std::env::var("COWD_EVAL_AUTONOMOUS_TEAM_MINIMUM")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<usize>()
        .map_err(|_| "COWD_EVAL_AUTONOMOUS_TEAM_MINIMUM must be an integer".to_string())?;
    if !(2..=16).contains(&minimum_teams) || minimum_teams > minimum_agents {
        return Err(
            "COWD_EVAL_AUTONOMOUS_TEAM_MINIMUM must be between 2 and 16 and no greater than the Agent minimum"
                .to_string(),
        );
    }
    let minimum_tasks = minimum_agents;
    let minimum_reviews = minimum_teams;
    let minimum_topics = minimum_teams;
    let minimum_cross_team_edges = minimum_teams.saturating_sub(1);
    let prompt = template
        .prompt_template
        .replace("{minimum_teams}", &minimum_teams.to_string())
        .replace("{minimum_agents}", &minimum_agents.to_string())
        .replace("{minimum_tasks}", &minimum_tasks.to_string())
        .replace("{output_path}", AUTONOMOUS_DEEPSEEK_OUTPUT_PATH);
    Ok(LiveScenarioSpec {
        id: AUTONOMOUS_DEEPSEEK_SCENARIO_ID,
        // The template is compiled into the evaluator binary and one spec is
        // built per process, so promoting the selected owned string to a
        // process-lifetime prompt keeps the long established static spec
        // contract without introducing production state or an extra clone.
        prompt: Box::leak(prompt.into_boxed_str()),
        acceptance: LiveAcceptance::AutonomousCollaboration {
            minimum_teams,
            minimum_agents,
            minimum_tasks,
            minimum_reviews,
            minimum_cross_team_edges,
            minimum_topics,
            output_path: AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
        },
        timeout: LiveScenarioTimeout::large_scale(minimum_agents),
    })
}

fn bounded_scenario_minimum(key: &str, default: usize, maximum: usize) -> Result<usize, String> {
    let value = std::env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .map_err(|_| format!("{key} must be an integer"))?;
    if !(2..=maximum).contains(&value) {
        return Err(format!("{key} must be between 2 and {maximum}"));
    }
    Ok(value)
}

fn group_theory_spec() -> Result<LiveScenarioSpec, String> {
    let minimum_teams = bounded_scenario_minimum("COWD_EVAL_GROUP_THEORY_TEAM_MINIMUM", 3, 12)?;
    let minimum_edges = minimum_teams.saturating_sub(1);
    let prompt = format!(
        "这是隔离环境中的 Agent-first 深度任务：调研群论在当前 AI 中的应用并形成可复核测评方案。根据目标自主设计 Team、Agent、Task 和依赖关系，不要套用固定名称或每队固定人数。本场景按 {minimum_teams} 个有真实 Task 的 Team 验收（这是验收标准，不是你的拓扑模板：你仍然自主决定建几个、怎么分）；并让可独立的研究工作物理并发。工作必须覆盖数学定义与可证伪边界、当前应用证据、C4 对照实验设计、综合风险。每项有效 Task 都要由 Agent 领取，提交 artifact/evidence，再由不同 Agent 独立 review；综合 Task 必须通过 depends_on 消费上游 accepted Task。使用 Program/Team topic 交换有引用的摘要，最终 artifact 只能在依赖满足后提交。不得编造论文、链接、实验或工具输出；外部事实无法取得时保留 unresolved。最终结论包含 C4、至少三个实际读取的完整源码路径，并区分研究、调研、分析、处理、模拟的输入输出。只使用只读工具与 Agent Action，不用 bash 或写文件工具。"
    );
    Ok(LiveScenarioSpec {
        id: GROUP_THEORY_SCENARIO_ID,
        prompt: Box::leak(prompt.into_boxed_str()),
        acceptance: LiveAcceptance::ArchitectureQuality {
            minimum_teams,
            minimum_claimed_cross_team_edges: minimum_edges,
            evidence_profile: ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis,
        },
        timeout: LiveScenarioTimeout::large_scale(minimum_teams),
    })
}

fn large_scale_spec() -> Result<LiveScenarioSpec, String> {
    let minimum_teams = bounded_scenario_minimum("COWD_EVAL_LARGE_SCALE_TEAM_MINIMUM", 6, 16)?;
    let minimum_edges = std::env::var("COWD_EVAL_LARGE_SCALE_CROSS_TEAM_MINIMUM")
        .unwrap_or_else(|_| minimum_teams.saturating_sub(1).to_string())
        .parse::<usize>()
        .map_err(|_| "COWD_EVAL_LARGE_SCALE_CROSS_TEAM_MINIMUM must be an integer".to_string())?;
    let paths = LARGE_SCALE_SOURCE_PATHS.join("`、`");
    let prompt = format!(
        "这是单 Program 的 Agent-first 大规模协同压力验收。根据源码责任和实际发现自主设计 Team/Agent/Task 拓扑，禁止套用固定 Team 名、固定每队人数或预先写死全部流程。本场景按 {minimum_teams} 个有真实 Task 的 Team 验收（这是验收标准，不是你的拓扑模板：你仍然自主决定建几个、怎么分）；并最大化无依赖分支的物理并发。必须完整读取并覆盖 `{paths}`。每个目标源码必须由两个不同 Agent 身份完整读取到 EOF；每项 Task 通过 task_claim、artifact_commit、task_submit 和不同 Agent 的 task_review 收敛，跨 Team 综合必须使用 depends_on，topic 消息必须携带 artifact/evidence refs。最终 artifact 必须比较正常、过载、取消、恢复和维护追赶路径，给出并发波次、瓶颈、失效模式、容量边界及扩容判断。不能由根模型文本伪造 Team、Agent、证据或完成状态；无法证实时保留 unresolved，不得请求 Objective 完成。只使用只读源码工具和 Agent Action，不用 bash 或写文件工具。"
    );
    Ok(LiveScenarioSpec {
        id: LARGE_SCALE_SCENARIO_ID,
        prompt: Box::leak(prompt.into_boxed_str()),
        acceptance: LiveAcceptance::ArchitectureQuality {
            minimum_teams,
            minimum_claimed_cross_team_edges: minimum_edges,
            evidence_profile: ArchitectureEvidenceProfile::LargeScaleIndependentReview,
        },
        timeout: LiveScenarioTimeout::large_scale(minimum_teams),
    })
}

const LARGE_SCALE_SOURCE_PATHS: [&str; 12] = [
    "crates/runtime/src/agentic/program.rs",
    "crates/runtime/src/agentic/action_service.rs",
    "crates/runtime/src/agentic/execution.rs",
    "crates/runtime/src/agentic/work_market.rs",
    "crates/runtime/src/agentic/topic.rs",
    "crates/runtime/src/agentic/supervision.rs",
    "crates/runtime/src/agent/in_process_worker.rs",
    "crates/gateway/src/runtime/gateway_tool_executor.rs",
    "crates/gateway/src/api_routes/runtime_routes.rs",
    "crates/runtime/src/conversation/host.rs",
    "crates/runtime/src/execution_core/services.rs",
    "crates/runtime/src/recovery/runtime_event_reactor.rs",
];

const GROUP_THEORY_SOURCE_PATHS: [&str; 3] = [
    "crates/runtime/src/agentic/program.rs",
    "crates/runtime/src/agentic/action_service.rs",
    "crates/runtime/src/agentic/execution.rs",
];

const LARGE_SCALE_TERMINAL_COVERAGE_CLAUSE: &str =
    "最终结论必须原样包含结构化覆盖声明“12/12 目标源码已完整读取到 EOF”和独立复核声明“12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF”；只有 Runtime 的完整读取收据证明全部目标满足时才允许输出，否则必须保留 unresolved 并拒绝请求 Objective 完成。";

/// An operator may isolate named production-path scenarios without changing
/// the default suite. This is useful for a costly, focused provider exercise
/// whose result must not be obscured by an unrelated scenario's verdict.
fn selected_live_scenario_ids() -> Option<BTreeSet<String>> {
    let selected = std::env::var("COWD_EVAL_LIVE_SCENARIOS")
        .ok()?
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    (!selected.is_empty()).then_some(selected)
}

fn live_scenario_selection_errors(
    selected: Option<&BTreeSet<String>>,
    registered: &BTreeSet<String>,
) -> Vec<Value> {
    selected
        .map(|selected| {
            selected
                .difference(registered)
                .map(|scenario_id| {
                    json!({
                        "kind": "unregistered_live_scenario",
                        "scenario_id": scenario_id,
                        "message": "selected live scenario is not registered for this invocation",
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn live_scenario_selection_passed(
    selected: Option<&BTreeSet<String>>,
    selection_errors: &[Value],
    selected_spec_count: usize,
) -> bool {
    selection_errors.is_empty()
        && selected.is_none_or(|selected| selected_spec_count == selected.len())
}

fn live_provider_token_telemetry_limit() -> Result<Option<u64>, String> {
    let Some(raw) = std::env::var("COWD_EVAL_MAX_PROVIDER_TOKENS")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        // Live execution is already an explicit harness opt-in.  An omitted
        // cost ceiling means "measure every provider receipt", not "block
        // the task".  A supplied value remains useful telemetry and is never
        // turned into a model-output or task-completion cap.
        return Ok(None);
    };
    let limit = raw.parse::<u64>().map_err(|_| {
        "COWD_EVAL_MAX_PROVIDER_TOKENS must be a positive integer when supplied".to_string()
    })?;
    if limit == 0 {
        return Err("COWD_EVAL_MAX_PROVIDER_TOKENS must be positive when supplied".to_string());
    }
    Ok(Some(limit))
}

fn controlled_live_prompt(spec_id: &str, prompt: String, telemetry_limit: Option<u64>) -> String {
    let mut resource_scopes = vec!["provider", "provider_account", "provider_token_pool"];
    // Scenario admission is a capability lease, not a hint. The tool-evidence
    // fixture asks the root execution to read this exact file, so grant only
    // that read rather than weakening Runtime's fail-closed scope ceiling.
    if spec_id == "live_tool_evidence" {
        resource_scopes.push("read:Cargo.toml");
    }
    if spec_id == AUTONOMOUS_DEEPSEEK_SCENARIO_ID {
        // The scenario asks the model to design its own files, experiments,
        // evidence log and final artifact inside a disposable workspace. A
        // final-file-only lease contradicts that autonomy contract and makes
        // valid Agent missions physically impossible. The boundary remains
        // exact: the isolated workspace plus network reads, never the host.
        resource_scopes.push("workspace:.");
        resource_scopes.push("network:*");
    } else {
        // Read-only source-analysis scenarios must be able to read the isolated
        // candidate workspace (`crates/...`) to ground their evidence. Without
        // this lease every source read is denied by the resource ceiling.
        resource_scopes.push("read:.");
    }
    let control = json!({
        "corpus_id": "live-scenarios-v1",
        "workspace_fixture": "none",
        // The disposable Gateway config is the single-route authority. This
        // field belongs to Runtime's execution-resource contract, where the
        // canonical no-override value is `normal`.
        "provider_constraint": "normal",
        "temperature_milli": 0,
        "resource_scopes": resource_scopes,
        "prompt": prompt,
    });
    let mut control = control;
    if let Some(limit) = telemetry_limit {
        control["budget_lease_id"] =
            json!(format!("live-scenario:{spec_id}:{}", uuid::Uuid::new_v4()));
        control["max_total_tokens"] = json!(limit);
    }
    format!("COWD_EVAL_CONTROL {control}\n{prompt}")
}

/// Run production-path scenarios against an explicitly supplied, isolated
/// Gateway. This runner never constructs Runtime objects or fakes receipts:
/// every result is derived from public Gateway responses and durable messages.
pub fn run_live_gateway_scenarios(options: &HarnessEvalRunnerOptions) -> Value {
    let claim_scope = match LiveClaimScope::from_environment() {
        Ok(scope) => scope,
        Err(reason) => {
            return json!({
                "kind": "harness_eval.live_gateway_scenarios",
                "status": "gated",
                "claim_scope": "invalid",
                "release_certified": false,
                "reason": reason,
                "scenarios": [],
            });
        }
    };
    let provider_token_telemetry_limit = match live_provider_token_telemetry_limit() {
        Ok(limit) => limit,
        Err(reason) => {
            return json!({
                "kind": "harness_eval.live_gateway_scenarios",
                "status": "gated",
                "reason": reason,
                "scenarios": [],
            });
        }
    };
    let Some(base_url) = std::env::var("COWD_EVAL_GATEWAY_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
    else {
        return json!({
            "kind": "harness_eval.live_gateway_scenarios",
            "status": "gated",
            "reason": "COWD_EVAL_GATEWAY_URL must name an isolated Gateway; live scenarios never default to the calling Gateway",
            "scenarios": [],
        });
    };

    // This is an explicit operator cap for the *evaluation process*, not a
    // Runtime completion policy. Without it, every scenario keeps the timeout
    // derived from its own complexity profile.
    let timeout_cap = env_duration_secs("COWD_EVAL_SCENARIO_TIMEOUT_SECS");
    let poll_interval = env_duration_millis("COWD_EVAL_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL);
    let client_timeout = timeout_cap
        .unwrap_or(MAX_DEFAULT_SCENARIO_TIMEOUT)
        .saturating_add(Duration::from_secs(15));
    let mut builder = Client::builder().timeout(client_timeout);
    if let Some(token) = std::env::var("COWD_API_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        let mut headers = HeaderMap::new();
        let value = match HeaderValue::from_str(&format!("Bearer {token}")) {
            Ok(value) => value,
            Err(error) => {
                return json!({
                    "kind": "harness_eval.live_gateway_scenarios",
                    "status": "failed",
                    "gateway_url": base_url,
                    "reason": format!("COWD_API_TOKEN cannot form an HTTP bearer header: {error}"),
                    "scenarios": [],
                });
            }
        };
        headers.insert(AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    let client = match builder.build() {
        Ok(client) => client,
        Err(error) => {
            return json!({
                "kind": "harness_eval.live_gateway_scenarios",
                "status": "failed",
                "gateway_url": base_url,
                "reason": format!("cannot build live scenario HTTP client: {error}"),
                "scenarios": [],
            })
        }
    };
    let runner = LiveScenarioRunner {
        client,
        base_url,
        timeout_cap,
        poll_interval,
        model: options.provider.clone(),
        claim_scope,
    };
    runner.run(provider_token_telemetry_limit)
}

struct LiveScenarioRunner {
    client: Client,
    base_url: String,
    timeout_cap: Option<Duration>,
    poll_interval: Duration,
    model: Option<String>,
    claim_scope: LiveClaimScope,
}

#[derive(Clone, Copy)]
enum LiveHealthContract {
    Gateway,
    Runtime,
    RuntimeOutbox,
    RuntimeControlPlane,
    EvolutionProjectors,
    SurfaceHost,
}

fn health_check(name: &str, passed: bool, expected: Value, actual: Value) -> Value {
    json!({
        "name": name,
        "passed": passed,
        "expected": expected,
        "actual": actual,
    })
}

fn semantic_health_observation(path: &str, contract: LiveHealthContract, response: Value) -> Value {
    let checks = match contract {
        LiveHealthContract::Gateway => vec![health_check(
            "gateway.status",
            response.get("status").and_then(Value::as_str) == Some("healthy"),
            json!("healthy"),
            response.get("status").cloned().unwrap_or(Value::Null),
        )],
        LiveHealthContract::Runtime => vec![
            health_check(
                "runtime.ok",
                response.get("ok").and_then(Value::as_bool) == Some(true),
                json!(true),
                response.get("ok").cloned().unwrap_or(Value::Null),
            ),
            health_check(
                "runtime.execution.lifecycle",
                response
                    .pointer("/execution/lifecycle")
                    .and_then(Value::as_str)
                    == Some("open"),
                json!("open"),
                response
                    .pointer("/execution/lifecycle")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "runtime.execution.last_error",
                response
                    .pointer("/execution/last_error")
                    .is_some_and(Value::is_null),
                Value::Null,
                response
                    .pointer("/execution/last_error")
                    .cloned()
                    .unwrap_or_else(|| json!("missing")),
            ),
        ],
        LiveHealthContract::RuntimeOutbox => vec![health_check(
            "runtime_outbox.healthy",
            response.get("healthy").and_then(Value::as_bool) == Some(true),
            json!(true),
            response.get("healthy").cloned().unwrap_or(Value::Null),
        )],
        LiveHealthContract::RuntimeControlPlane => vec![
            health_check(
                "runtime_control_plane.production_ready",
                response
                    .pointer("/readiness/production_ready")
                    .and_then(Value::as_bool)
                    == Some(true),
                json!(true),
                response
                    .pointer("/readiness/production_ready")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "runtime_control_plane.required_blocked",
                response
                    .pointer("/readiness/required_blocked")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/readiness/required_blocked")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
        ],
        LiveHealthContract::EvolutionProjectors => vec![
            health_check(
                "evolution_projector.worker_running",
                response
                    .pointer("/projector/worker_running")
                    .and_then(Value::as_bool)
                    == Some(true),
                json!(true),
                response
                    .pointer("/projector/worker_running")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "evolution_projector.consecutive_failures",
                response
                    .pointer("/projector/consecutive_failures")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/projector/consecutive_failures")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "evolution_projector.dead_letter_count",
                response
                    .pointer("/projector/dead_letter_count")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/projector/dead_letter_count")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "outcome_projector.worker_running",
                response
                    .pointer("/outcome_projector/worker_running")
                    .and_then(Value::as_bool)
                    == Some(true),
                json!(true),
                response
                    .pointer("/outcome_projector/worker_running")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "outcome_projector.consecutive_failures",
                response
                    .pointer("/outcome_projector/consecutive_failures")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/outcome_projector/consecutive_failures")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "outcome_projector.dlq_count",
                response
                    .pointer("/outcome_projector/dlq_count")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/outcome_projector/dlq_count")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
        ],
        LiveHealthContract::SurfaceHost => vec![
            health_check(
                "surface_host.status",
                response.get("status").and_then(Value::as_str) == Some("ready"),
                json!("ready"),
                response.get("status").cloned().unwrap_or(Value::Null),
            ),
            health_check(
                "surface_host.failed_count",
                response
                    .pointer("/host/failed_count")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/host/failed_count")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "surface_host.circuit_open_count",
                response
                    .pointer("/host/circuit_open_count")
                    .and_then(Value::as_u64)
                    == Some(0),
                json!(0),
                response
                    .pointer("/host/circuit_open_count")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            health_check(
                "surface_host.task_ownership.overloaded",
                response
                    .pointer("/host/task_ownership/overloaded")
                    .and_then(Value::as_bool)
                    == Some(false),
                json!(false),
                response
                    .pointer("/host/task_ownership/overloaded")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
        ],
    };
    let failed_checks = checks
        .iter()
        .filter(|check| check.get("passed").and_then(Value::as_bool) != Some(true))
        .cloned()
        .collect::<Vec<_>>();
    json!({
        "status": if failed_checks.is_empty() { "passed" } else { "failed" },
        "path": path,
        "reason": if failed_checks.is_empty() {
            Value::Null
        } else {
            json!("HTTP transport succeeded but the endpoint semantic health contract failed")
        },
        "semantic_checks": checks,
        "failed_checks": failed_checks,
        "response": response,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RootExecutionTerminal {
    Pending,
    Completed,
    Failed(String),
}

impl RootExecutionTerminal {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Completed => "completed",
            Self::Failed(_) => "failed",
        }
    }
}

#[derive(Clone, Debug)]
struct RootExecutionObservation {
    terminal: RootExecutionTerminal,
    fingerprint: String,
    summary: Value,
    response_body_bytes: u64,
}

#[derive(Clone, Debug)]
struct ExecutionLiveObservation {
    fingerprint: String,
    summary: Value,
    response_body_bytes: u64,
}

fn live_terminal_belongs_to_root(observation: &ExecutionLiveObservation, root_id: &str) -> bool {
    observation
        .summary
        .get("execution_id")
        .and_then(Value::as_str)
        == Some(root_id)
        && observation
            .summary
            .get("live_status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "complete" | "error" | "cancelled"))
}

fn root_node_statuses(projection: &Value) -> Vec<Value> {
    projection
        .pointer("/graph/nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .map(|node| {
                    json!({
                        "node_id": node.get("node_id"),
                        "kind": node.get("kind"),
                        "status": node.get("status"),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn root_progress_fingerprint(projection: &Value, statuses: &[Value]) -> String {
    let live = projection.get("live").unwrap_or(&Value::Null);
    serde_json::to_string(&json!({
        "projection_revision": projection.get("revision"),
        "node_statuses": statuses,
        "live_revision": live.get("revision"),
        "live_status": live.get("status"),
        "live_output_bytes": live.get("output_bytes"),
        "live_last_progress_at_ms": live.get("last_progress_at_ms"),
    }))
    .unwrap_or_default()
}

fn root_execution_terminal_state(projection: &Value) -> RootExecutionTerminal {
    let Some(nodes) = projection.pointer("/graph/nodes").and_then(Value::as_array) else {
        return RootExecutionTerminal::Pending;
    };
    if nodes.is_empty() {
        return RootExecutionTerminal::Pending;
    }
    let terminal_status =
        |status: &str| matches!(status, "completed" | "failed" | "cancelled" | "blocked");
    if nodes.iter().any(|node| {
        node.get("status")
            .and_then(Value::as_str)
            .is_none_or(|status| !terminal_status(status))
    }) {
        return RootExecutionTerminal::Pending;
    }
    if nodes.iter().any(|node| {
        node.get("kind").and_then(Value::as_str) == Some("synthesize")
            && node.get("status").and_then(Value::as_str) == Some("completed")
    }) {
        return RootExecutionTerminal::Completed;
    }
    let statuses = nodes
        .iter()
        .map(|node| {
            format!(
                "{}:{}",
                node.get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                node.get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    RootExecutionTerminal::Failed(format!(
        "root execution reached a terminal graph state without completed synthesis: {statuses}"
    ))
}

impl LiveScenarioRunner {
    fn run(&self, provider_token_telemetry_limit: Option<u64>) -> Value {
        let claim_scope = self.claim_scope;
        let health_observations = [
            ("gateway", "/healthz", LiveHealthContract::Gateway),
            (
                "runtime",
                "/api/runtime/status",
                LiveHealthContract::Runtime,
            ),
            (
                "runtime_outbox",
                "/api/runtime/outbox",
                LiveHealthContract::RuntimeOutbox,
            ),
            (
                "runtime_control_plane",
                "/api/runtime/control-plane",
                LiveHealthContract::RuntimeControlPlane,
            ),
            (
                "evolution_projector",
                "/api/evolution/signals",
                LiveHealthContract::EvolutionProjectors,
            ),
            (
                "surface_host",
                "/api/surfaces/health",
                LiveHealthContract::SurfaceHost,
            ),
        ]
        .into_iter()
        .map(|(id, path, contract)| {
            let observed = self.get_json(path);
            (
                id.to_string(),
                match observed {
                    Ok(response) => semantic_health_observation(path, contract, response),
                    Err(error) => json!({
                        "status": "failed",
                        "path": path,
                        "error": error,
                    }),
                },
            )
        })
        .collect::<serde_json::Map<String, Value>>();
        let health_passed = health_observations
            .values()
            .all(|observation| observation["status"] == "passed");
        let mut scenario_specs = vec![
            LiveScenarioSpec {
                id: "live_direct_terminal",
                prompt: "只回答 7 乘以 8 的结果。不要调用工具，不要组队。",
                acceptance: LiveAcceptance::Contains("56"),
                timeout: LiveScenarioTimeout::direct(),
            },
            // PLAN Experiment Level 1: the minimal capability probe.
            //
            // Deliberately neutral - it names the objective and the paths and
            // says nothing about batching, orchestration or efficiency. That is
            // what makes it a controlled measurement: any difference between
            // builds is attributable to the tool surface the model can see, not
            // to a hint we planted in the prompt. Earlier probes used a
            // multi-team collaboration scenario, which is far too heavy to
            // isolate whether one primitive works at all.
            //
            // Required facts are pinned per file so completion is unambiguous.
            LiveScenarioSpec {
                id: "live_minimal_batch_read",
                prompt: "请读取以下 6 个源码文件，并对每个文件给出一个可核验的事实（文件路径 + 事实）。crates/tools/src/lib.rs —— workspace 的成员 crate 数量。crates/runtime/src/lib.rs —— 顶层 `pub mod` / `pub use` 的数量。crates/memory/src/lib.rs —— 记忆 crate 的模块数量。crates/gateway/src/main.rs —— main.rs 的行数。crates/provider/src/lib.rs —— provider crate 的公开导出条数。crates/harness-eval/src/lib.rs —— harness-eval 的行数。只使用只读工具，不要调用 bash 或任何写工具；读不到就保留 unresolved，不要凭先验知识作答。",
                acceptance: LiveAcceptance::Facet {
                    facet_id: "read_six_named_files",
                    capability: "read every named file and report one verifiable fact from it",
                    required_read_paths: &[
                        "crates/tools/src/lib.rs",
                        "crates/runtime/src/lib.rs",
                        "crates/memory/src/lib.rs",
                        "crates/gateway/src/main.rs",
                        "crates/provider/src/lib.rs",
                        "crates/harness-eval/src/lib.rs",
                    ],
                    answer_contains: &[],
                    keep_unresolved_ok: true,
                },
                timeout: LiveScenarioTimeout::tool(),
            },
            // PLAN Experiment Level 1, facet 2: deterministic reduction OUTSIDE
            // the model. Same neutral-prompt discipline as facet 1 - the
            // objective names the work and says nothing about sandboxes,
            // scripting or efficiency. This facet isolates whether the model can
            // move counting/aggregation into `compute` instead of pulling every
            // file body into context, which is the only path available now that
            // we know there is no AST extractor.
            LiveScenarioSpec {
                id: "live_minimal_compute_reduce",
                prompt: "请统计本 workspace 中 Rust 源码的总行数、以及 `.rs` 文件的数量，并给出 `crates/` 下哪一个 crate 的 `.rs` 文件最多（给出 crate 名与文件数）。只使用只读工具，不要调用 bash 或任何写工具；不要逐个人工累加文件内容。",
                acceptance: LiveAcceptance::Facet {
                    facet_id: "deterministic_reduction_outside_the_model",
                    capability: "aggregate across files without moving their bodies into context",
                    required_read_paths: &[],
                    answer_contains: &[],
                    keep_unresolved_ok: true,
                },
                timeout: LiveScenarioTimeout::tool(),
            },
            LiveScenarioSpec {
                id: "live_tool_evidence",
                prompt: "请读取当前工作区的 Cargo.toml，给出 workspace package version 和文件路径。必须通过只读工具取得证据，不要猜测。",
                acceptance: LiveAcceptance::RequiresToolEvidence {
                    tool_name: "read_file",
                    target_path: "Cargo.toml",
                },
                timeout: LiveScenarioTimeout::tool(),
            },
            LiveScenarioSpec {
                id: "live_single_architecture_baseline",
                prompt: "请单独完成一次复杂架构审查，不要启动团队。【取证要求】结论必须建立在本次真实读取到的源码内容上：至少 3 个来自不同 crate 的源码文件要被真正读到内容（列举路径不算）。如何读最有效由你自己决定——可以一次 tool_orchestrate 批量读，也可以逐个读；读不到就保留 unresolved，不要凭先验知识下结论。基于你实际读到的源码内容，分别分析 runtime、memory、gateway 的职责边界、各自的 canonical state 或事件真相、一个潜在风险，并在结论里引用你实际读过的文件路径。只陈述本次实际读取到源码所能验证的结论；不要加入“无法确认/无法判断/未确认/需要进一步检查”之类的保留项。选择最短路径。能用一次 `tool_orchestrate` 编排完成的，就不要拆成多个回合逐个调用：一个编排最多 400 次子调用、50 个步骤，其中 `map` 可对列表逐项并行处理，`filter`/`collect` 做筛选与聚合(count/first/last/join/sum)，`when`/`repeat` 表达条件与循环，`compute` 在只读沙箱内对编排已取到的数据做确定性地计算与统计。因此：跨文件的统计、过滤、分支与交叉校验，请放进编排的 `compute` 步骤在模型外完成，只把摘要带回上下文，而不要把大量原文逐段搬进上下文；需要多轮才能收敛的长确定性任务，也可以用 `repeat` 在一个编排内描述完整流程。如果你判断某个目标用单个工具更直接，那也尽管直接用——编排是手段，最短路径才是目的。只读结果被截断时，用 `read_ref` 按工具返回的 `tool://` 证据引用取回正文。可用工具：read_file、glob_search、grep_search、workspace_snapshot、tool_orchestrate、execute_code、read_ref。不要调用 bash 或任何写工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 0,
                    minimum_claimed_cross_team_edges: 0,
                    evidence_profile: ArchitectureEvidenceProfile::Basic,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: EXPLICIT_TEAM_SCENARIO_ID,
                prompt: "【操作者要求】本场景要求实际启动 3 个 Team（操作者给定的硬性验收约束）；Team 的用途、角色、Task 划分、依赖与综合仍由你自主决定。请对三个独立责任域分别取得只读工具证据并交叉核验，最后统一综合结论。责任域一核查策略选择与执行义务，责任域二核查状态持久化与恢复，责任域三核查最终验收与投影。必须列出至少三个本次实际读取的完整 `crates/.../*.rs` 源码路径，只陈述工具证据能够验证的事实；选择最短路径。能用一次 `tool_orchestrate` 编排完成的，就不要拆成多个回合逐个调用：一个编排最多 400 次子调用、50 个步骤，其中 `map` 可对列表逐项并行处理，`filter`/`collect` 做筛选与聚合(count/first/last/join/sum)，`when`/`repeat` 表达条件与循环，`compute` 在只读沙箱内对编排已取到的数据做确定性地计算与统计。因此：跨文件的统计、过滤、分支与交叉校验，请放进编排的 `compute` 步骤在模型外完成，只把摘要带回上下文，而不要把大量原文逐段搬进上下文；需要多轮才能收敛的长确定性任务，也可以用 `repeat` 在一个编排内描述完整流程。如果你判断某个目标用单个工具更直接，那也尽管直接用——编排是手段，最短路径才是目的。只读结果被截断时，用 `read_ref` 按工具返回的 `tool://` 证据引用取回正文。可用工具：read_file、glob_search、grep_search、workspace_snapshot、tool_orchestrate、execute_code、read_ref。不要调用 bash 或任何写工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 3,
                    minimum_claimed_cross_team_edges: 0,
                    evidence_profile: ArchitectureEvidenceProfile::Basic,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: "live_team_projection",
                prompt: "【操作者要求】本场景要求实际启动 3 个 Team（操作者给定的硬性验收约束）；Team 的名称、角色、Task 与依赖拓扑由你自主决定，不要套用预设名称或每队人数。目标包含若干相互独立的责任域、需要不同视角交叉核验，应拆分并让无依赖的分支物理并行推进，而不是用简单路径一点一点顺序探索。拆分粒度由你自己判断——该合并的合并，该拆细的拆细；跨分支的综合必须通过 depends_on 消费上游已 accepted 的结果。遇到运行中暴露的新风险或缺口，按需再建 Team 或 Task，流程不是预先写死的。使用 Program/Team topic 交换带 artifact/evidence 引用的摘要，最终 artifact 只能在依赖满足后提交。【读取纪律】只读工具的输出**可能被截断**（返回 head+tail 预览并给 `persistedOutputPath` / `nextOffset`），这是**正常现象**，不是错误：请用 `read_file` 的 `offset`/`limit` 只读你需要的小范围，或按 `nextOffset` 续读；**禁止反复重试整文件读取**；遇到截断必须**继续推进**（建任务、派工、提交），不要卡在读取上。【建队后】这是 Agent-first 复杂架构审查。根据 runtime、memory、gateway 的责任边界自主创本场景按【操作者要求】实际启动 3 个有真实 Task 的 Team（操作者给定的硬性验收约束）；Team 的名称、角色与 Task 划分由你自主决定，但总数必须满足 3 个。不要套用固定 Team 名或固定角色。无依赖审查必须物理并发，跨组件综合 Task 必须 depends_on 至少两个已 accepted 的上游 Task，并通过 topic 的 artifact/evidence refs 消费其结果。每项 Task 都要由 Agent 领取、提交 artifact/evidence，并由不同 Agent 独立 review。最终 artifact 列出至少三个本次实际读取的完整 `crates/.../*.rs` 路径。只使用只读源码工具与 Agent Action，不用 bash 或写文件工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 3,
                    minimum_claimed_cross_team_edges: 2,
                    evidence_profile: ArchitectureEvidenceProfile::Basic,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: "live_agent_escalation",
                prompt: "【操作者要求】本场景要求累计实际启动 3 个 Team（操作者给定的硬性验收约束）；Team 的用途、角色与 Task 划分由你自主决定。这是 Agent-first 动态扩队验收。先让至少两个自主选择责任域的 Team 并发取得 Runtime 与 Gateway 的真实源码证据；不要固定 Team 名、角色名或人数。观察首批 artifact/evidence 后，根 Agent 当实际证据暴露出前序结论未覆盖的交叉风险时，按需再建 Team 或 Task 复核并依赖前序 accepted 结果，建不建、建几个由证据决定。所有 Task 必须经过 claim、artifact/evidence submit 与不同 Agent review；新 Team 通过 topic refs 消费上游事实后提出质疑并综合。最终 artifact 列出至少三个实际读取的完整源码路径。只使用只读源码工具和 Agent Action，不用 bash 或写文件工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 3,
                    minimum_claimed_cross_team_edges: 1,
                    evidence_profile: ArchitectureEvidenceProfile::Basic,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            // Exactly ONE Team, named explicitly by the operator. Team/role/Task
            // shape stays model-owned; the operator's constraint is the count.
            LiveScenarioSpec {
                id: ONE_TEAM_SCENARIO_ID,
                prompt: "【操作者要求】本场景要求实际启动 1 个 Team(操作者给定的硬性验收约束);Team 的名称、角色与 Task 划分由你自主决定，但总数必须是 1 个。在这个 Team 内完成：读取 crates/runtime/src/lib.rs、crates/tools/src/lib.rs、crates/memory/src/lib.rs 三个源码文件，每个文件给出一个可核验事实（文件路径 + 事实），并在结论里列出至少三个本次实际读取的完整 `crates/.../*.rs` 路径。每项 Task 要由 Agent 领取、提交 durable artifact/evidence 引用，并由不同的 Agent 独立 review 后才算完成；最终 artifact 综合三项事实。只使用只读源码工具与 Agent Action，不要调用 bash 或任何写文件工具。Do not invent files, links or tool output; keep any unresolved item explicit.",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 1,
                    minimum_claimed_cross_team_edges: 0,
                    evidence_profile: ArchitectureEvidenceProfile::Basic,
                },
                timeout: LiveScenarioTimeout::team(),
            },
        ];
        for (enabled, build_spec) in [
            (
                small_team_scenario_enabled(),
                small_team_spec as fn() -> Result<LiveScenarioSpec, String>,
            ),
            (
                group_theory_research_scenario_enabled(),
                group_theory_spec as fn() -> Result<LiveScenarioSpec, String>,
            ),
            (
                large_scale_collaboration_scenario_enabled(),
                large_scale_spec,
            ),
            (
                autonomous_deepseek_scenario_enabled(),
                autonomous_deepseek_spec,
            ),
        ] {
            if !enabled {
                continue;
            }
            match build_spec() {
                Ok(spec) => scenario_specs.push(spec),
                Err(error) => {
                    return json!({
                        "kind": "harness_eval.live_gateway_scenarios",
                        "status": "failed",
                        "claim_scope": claim_scope.as_str(),
                        "claim_status": "component_failed",
                        "release_certified": false,
                        "health_status": if health_passed { "passed" } else { "failed" },
                        "health_observations": health_observations,
                        "scenario_count": 0,
                        "passed": 0,
                        "failed": 1,
                        "configuration_error": error,
                    });
                }
            }
        }
        let selected_scenario_ids = selected_live_scenario_ids();
        let registered_scenario_ids = scenario_specs
            .iter()
            .map(|spec| spec.id.to_string())
            .collect::<BTreeSet<_>>();
        let selection_errors = live_scenario_selection_errors(
            selected_scenario_ids.as_ref(),
            &registered_scenario_ids,
        );
        let scenario_specs = scenario_specs
            .into_iter()
            .filter(|spec| {
                selected_scenario_ids
                    .as_ref()
                    .is_none_or(|selected| selected.contains(spec.id))
            })
            .collect::<Vec<_>>();
        let selection_passed = live_scenario_selection_passed(
            selected_scenario_ids.as_ref(),
            &selection_errors,
            scenario_specs.len(),
        );
        let scenarios = if selection_passed {
            scenario_specs
                .into_iter()
                .map(|spec| self.run_scenario(spec, provider_token_telemetry_limit))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let passed = scenarios
            .iter()
            .filter(|scenario| scenario.get("status").and_then(Value::as_str) == Some("passed"))
            .count();
        let comparison_requested = scenarios.iter().any(|scenario| {
            scenario.get("scenario_id").and_then(Value::as_str)
                == Some("live_single_architecture_baseline")
        }) && scenarios.iter().any(|scenario| {
            scenario.get("scenario_id").and_then(Value::as_str) == Some("live_team_projection")
        });
        let collaboration_comparison = if comparison_requested {
            collaboration_comparison(&scenarios)
        } else {
            json!({
                "status": "skipped",
                "reason": "baseline/team projection pair was not selected",
            })
        };
        let comparison_status = collaboration_comparison
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("failed");
        let comparison_passed = comparison_status == "passed"
            || (claim_scope == LiveClaimScope::Focused && comparison_status == "skipped");
        let certification_scenarios_present = release_certification_scenarios_present(&scenarios);
        let release_certified = claim_scope == LiveClaimScope::ReleaseCertification
            && selected_scenario_ids.is_none()
            && certification_scenarios_present
            && comparison_status == "passed"
            && health_passed
            && selection_passed
            && passed == scenarios.len();
        let claim_status = match claim_scope {
            LiveClaimScope::Focused
                if health_passed
                    && selection_passed
                    && passed == scenarios.len()
                    && comparison_passed =>
            {
                "component_passed"
            }
            LiveClaimScope::Focused => "component_failed",
            LiveClaimScope::ReleaseCertification if release_certified => "certified",
            LiveClaimScope::ReleaseCertification => "certification_failed",
        };
        let metrics = aggregate_scenario_metrics(&scenarios);
        json!({
            "kind": "harness_eval.live_gateway_scenarios",
            "status": if health_passed && selection_passed && passed == scenarios.len() && comparison_passed && (claim_scope == LiveClaimScope::Focused || release_certified) { "passed" } else { "failed" },
            "claim_scope": claim_scope.as_str(),
            "claim_status": claim_status,
            "release_certified": release_certified,
            "certification_scenarios_present": certification_scenarios_present,
            "gateway_url": self.base_url,
            "model": self.model,
            "provider_token_telemetry_limit_per_scenario": provider_token_telemetry_limit,
            "timeout_cap_ms": self.timeout_cap.map(|value| value.as_millis()),
            "poll_interval_ms": self.poll_interval.as_millis(),
            "health_status": if health_passed { "passed" } else { "failed" },
            "health_observations": health_observations,
            "scenario_count": scenarios.len(),
            "selected_scenario_ids": selected_scenario_ids,
            "selection_errors": selection_errors,
            "selection_status": if selection_passed { "passed" } else { "failed" },
            "passed": passed,
            "failed": scenarios.len().saturating_sub(passed),
            "metrics": metrics,
            "scenarios": scenarios,
            "collaboration_comparison": collaboration_comparison,
        })
    }

    fn run_scenario(
        &self,
        spec: LiveScenarioSpec,
        provider_token_telemetry_limit: Option<u64>,
    ) -> Value {
        let started = Instant::now();
        let mut trace = Vec::new();
        let timeout = spec.timeout.with_cap(self.timeout_cap);
        // Resume mode reattaches to a retained session's in-flight turn instead
        // of creating a new session and posting a new prompt: the Runtime
        // recovers the durable execution graph from the same database and
        // session, so the spent model work is continued rather than redone.
        let resume_session = std::env::var("COWD_EVAL_RESUME_SESSION")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let actor = match &resume_session {
            Some(session_id) => {
                SessionActor::resume(&self.client, &self.base_url, session_id, "harness-eval-live")
            }
            None => SessionActor::create(
                &self.client,
                &self.base_url,
                self.model.as_deref(),
                "harness-eval-live",
            ),
        };
        let Ok(mut actor) = actor else {
            return failed_scenario(spec, started, trace, actor.err().unwrap_or_default());
        };
        trace.extend(actor.drain_trace());
        let session_id = actor.session_id().to_string();
        let (execution_id, turn_id) = if resume_session.is_some() {
            match self.execution_live_observation(&session_id) {
                Ok(observation) => {
                    let execution_id = observation
                        .summary
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(ToString::to_string);
                    let Some(execution_id) = execution_id else {
                        return failed_scenario_with_session(
                            spec,
                            started,
                            trace,
                            session_id,
                            "resume found no in-flight session execution to observe".to_string(),
                            Value::Null,
                        );
                    };
                    let turn_id = self.root_execution_turn_id(&execution_id);
                    (Some(execution_id), turn_id)
                }
                Err(error) => {
                    return failed_scenario_with_session(
                        spec, started, trace, session_id, error, Value::Null,
                    );
                }
            }
        } else {
            let prompt = if spec.id == "live_qwen38_large_scale_collaboration" {
                format!("{}{}", spec.prompt, LARGE_SCALE_TERMINAL_COVERAGE_CLAUSE)
            } else {
                spec.prompt.to_string()
            };
            let prompt = controlled_live_prompt(spec.id, prompt, provider_token_telemetry_limit);
            let admission = actor.post_mutation(
                &format!("/api/sessions/{session_id}/messages"),
                json!({
                    "content": prompt,
                    "idempotency_key": format!("live-eval-{}", uuid::Uuid::new_v4()),
                }),
            );
            trace.extend(actor.drain_trace());
            let admission = match admission {
                Ok(value) => value,
                Err(error) => {
                    return failed_scenario_with_session(
                        spec,
                        started,
                        trace,
                        session_id,
                        error,
                        Value::Null,
                    );
                }
            };
            let execution_id = admission
                .get("execution")
                .and_then(|execution| execution.get("graph_id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToString::to_string);
            let turn_id = admission
                .pointer("/execution/turn_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToString::to_string);
            (execution_id, turn_id)
        };
        let Some(execution_id_ref) = execution_id.as_deref() else {
            return failed_scenario_with_session(
                spec,
                started,
                trace,
                session_id,
                "scenario admission lacks canonical execution.graph_id".to_string(),
                Value::Null,
            );
        };
        let Some(turn_id_ref) = turn_id.as_deref() else {
            return failed_scenario_with_session_and_execution(
                spec,
                started,
                trace,
                session_id,
                execution_id,
                "scenario admission lacks canonical execution.turn_id".to_string(),
                Value::Null,
            );
        };

        let mut observer = LiveScenarioObserver::default();
        let terminal =
            self.wait_for_terminal_message(&session_id, execution_id_ref, &timeout, &mut observer);
        let Ok(terminal) = terminal else {
            let terminal_error = terminal.err().unwrap_or_default();
            let mut diagnostics = self.capture_diagnostics(
                &session_id,
                Some(execution_id_ref),
                &mut observer,
                started,
                &mut trace,
            );
            let (observation_trace, observation_integrity) = observer.finalize();
            if let Some(object) = diagnostics.as_object_mut() {
                object.insert("observation_integrity".to_string(), observation_integrity);
            }
            trace.extend(observation_trace);
            let cleanup = self.cancel_execution_lineage(execution_id_ref, &mut actor, &mut trace);
            if let Some(object) = diagnostics.as_object_mut() {
                object.insert("cancellation".to_string(), cleanup);
            }
            return failed_scenario_with_session_and_execution(
                spec,
                started,
                trace,
                session_id,
                execution_id,
                terminal_error,
                diagnostics,
            );
        };
        let terminal_wait = terminal;
        let response_text = message_text(&terminal_wait.message);
        let descendant_wait = self.wait_for_descendant_team_acceptance(
            spec.acceptance,
            &response_text,
            &session_id,
            turn_id_ref,
            execution_id_ref,
            started,
            &timeout,
            &mut observer,
            &mut trace,
        );
        let timeline = descendant_wait.timeline;
        let projections = descendant_wait.projections;
        let agentic_program = descendant_wait.agentic_program;
        let mut acceptance = descendant_wait.acceptance;
        let terminal_id = terminal_wait
            .message
            .get("id")
            .or_else(|| terminal_wait.message.get("message_id"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let commit_cursor = find_u64_by_key(&timeline, &["commit_cursor", "runtime_commit_cursor"]);
        let metrics = scenario_metrics(
            &timeline,
            &projections,
            agentic_program.as_ref(),
            started.elapsed(),
        );
        let requested_model = self
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty());
        let effective_models = metrics
            .get("effective_models")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let model_verified = requested_model.is_none_or(|expected| {
            !effective_models.is_empty()
                && effective_models
                    .iter()
                    .all(|model| model.as_str() == Some(expected))
        });
        acceptance.checks.push(json!({
            "name": "requested_model_executed_without_fallback",
            "expected": requested_model,
            "effective_models": effective_models,
            "passed": model_verified,
        }));
        acceptance.passed &= model_verified;
        let (observation_trace, observation_integrity) = observer.finalize();
        let observation_passed = observation_integrity.get("status").and_then(Value::as_str)
            == Some("passed")
            && observation_integrity
                .get("timeline_drained")
                .and_then(Value::as_bool)
                == Some(true)
            && observation_integrity
                .get("message_pages_drained")
                .and_then(Value::as_bool)
                == Some(true)
            && observation_integrity
                .get("stall_detected")
                .and_then(Value::as_bool)
                == Some(false);
        acceptance.checks.push(json!({
            "name": "autonomous_observation_integrity",
            "passed": observation_passed,
            "evidence": observation_integrity.clone(),
        }));
        acceptance.passed &= observation_passed;
        let cleanup = actor.finish().map_or_else(
            |error| json!({"status":"failed","error":error}),
            |_| json!({"status":"passed"}),
        );
        let cleanup_passed = cleanup.get("status").and_then(Value::as_str) == Some("passed");
        acceptance.checks.push(json!({
            "name": "session_actor_cleanup",
            "passed": cleanup_passed,
        }));
        acceptance.passed &= cleanup_passed;
        trace.extend(observation_trace);
        trace.extend(actor.drain_trace());
        json!({
            "scenario_id": spec.id,
            "status": if acceptance.passed { "passed" } else { "failed" },
            "acceptance": acceptance.to_value(),
            "session_id": session_id,
            "execution_id": execution_id,
            "turn_id": turn_id,
            "terminal_id": terminal_id,
            "terminal_response_summary": summarize(&response_text, 320),
            "runtime_commit_cursor": commit_cursor,
            "elapsed_ms": started.elapsed().as_millis(),
            "metrics": metrics,
            "observation_integrity": observation_integrity,
            "timeout": {
                "root_terminal_wait": terminal_wait.report,
                "descendant_team_wait": descendant_wait.report,
            },
            "session_actor_cleanup": cleanup,
            "trace": trace,
            "production_trace": {
                "session_id": session_id,
                "execution_id": execution_id,
                "turn_id": turn_id,
                "agentic_program": agentic_program,
                "terminal_id": terminal_id,
                "runtime_commit_cursor": commit_cursor,
                "message_materialized": true,
            }
        })
    }

    fn wait_for_terminal_message(
        &self,
        session_id: &str,
        root_execution_id: &str,
        timeout: &LiveScenarioTimeout,
        observer: &mut LiveScenarioObserver,
    ) -> Result<TerminalWait, String> {
        let started = Instant::now();
        let mut next_message_probe = started;
        let mut next_root_probe = started;
        let mut next_session_progress_probe = started;
        let mut root_terminal = RootExecutionTerminal::Pending;
        loop {
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let now = Instant::now();
            let mut message_polled = false;
            if now >= next_message_probe {
                next_message_probe = now + Duration::from_secs(1);
                self.poll_message_deltas(session_id, elapsed_ms, observer)?;
                message_polled = true;
            }
            let live = self.execution_live_observation(session_id);
            let live_path = format!("/api/sessions/{session_id}/execution/live");
            let force_root_probe = match live.as_ref() {
                Ok(observation) => {
                    observer.observe_live(
                        &live_path,
                        &Ok(observation.summary.clone()),
                        Some(&observation.fingerprint),
                        Some(observation.response_body_bytes),
                        elapsed_ms,
                    );
                    live_terminal_belongs_to_root(observation, root_execution_id)
                }
                Err(error) => {
                    observer.observe_live(&live_path, &Err(error.clone()), None, None, elapsed_ms);
                    false
                }
            };
            if now >= next_root_probe
                || (force_root_probe && root_terminal == RootExecutionTerminal::Pending)
            {
                // The session live endpoint and cursor-complete timeline are
                // the cheap progress plane. A full execution projection can
                // be megabytes once many Agent receipts exist, so use it only
                // as a periodic safety check or immediately at a live terminal
                // boundary instead of downloading it every two seconds.
                next_root_probe = now + Duration::from_secs(30);
                let root = self.root_execution_observation(root_execution_id);
                let root_path = format!("/api/runtime/executions/{root_execution_id}");
                match root {
                    Ok(observation) => {
                        observer.observe_root(
                            &root_path,
                            &Ok(observation.summary),
                            Some(&observation.fingerprint),
                            Some(observation.response_body_bytes),
                            elapsed_ms,
                        );
                        root_terminal = observation.terminal;
                        if let RootExecutionTerminal::Failed(reason) = &root_terminal {
                            return Err(reason.clone());
                        }
                    }
                    Err(error) => {
                        observer.observe_root(&root_path, &Err(error), None, None, elapsed_ms);
                    }
                }
            }
            if now >= next_session_progress_probe {
                next_session_progress_probe = now + Duration::from_secs(1);
                self.poll_timeline_deltas(session_id, elapsed_ms, observer)?;
            }
            if root_terminal == RootExecutionTerminal::Completed && !message_polled {
                self.poll_message_deltas(session_id, elapsed_ms, observer)?;
            }
            if let Some(message) = observer
                .assistant_message()
                .filter(|message| !message_text(message).trim().is_empty())
            {
                match &root_terminal {
                    RootExecutionTerminal::Completed => {
                        // Close the observation window only after both durable
                        // delta streams have been drained at the terminal
                        // boundary. A root can complete between periodic probes.
                        self.poll_message_deltas(session_id, elapsed_ms, observer)?;
                        self.poll_timeline_deltas(session_id, elapsed_ms, observer)?;
                        return Ok(TerminalWait {
                            message,
                            report: timeout.report(
                                started.elapsed(),
                                Duration::from_millis(observer.since_last_progress_ms(elapsed_ms)),
                                observer.progress_observations() as usize,
                                "root_execution_terminal_and_message",
                                observer.phase(),
                                observer.last_active_phase(),
                            ),
                        });
                    }
                    RootExecutionTerminal::Failed(reason) => return Err(reason.clone()),
                    RootExecutionTerminal::Pending => {}
                }
            }
            let elapsed = started.elapsed();
            let elapsed_ms = elapsed.as_millis() as u64;
            let since_progress = Duration::from_millis(observer.since_last_progress_ms(elapsed_ms));
            if since_progress >= Duration::from_secs(30) {
                observer.mark_quiet();
            }
            if timeout.should_abort_for_absolute_wait(elapsed) {
                observer.mark_stalled();
                // PLAN D3: the evaluator's absolute window is an observation
                // limit, not a verdict on the work. The old message read as
                // "the turn produced nothing", which was wrong for both observed
                // cases (arm C was still at calling_model over revisions 1..27;
                // Facet A had 367 progress observations) and it made every such
                // failure undiagnosable. Emit the classified report instead, and
                // keep the raw diagnostics for the reader.
                let report = crate::observation_limit::observation_limit_report(
                    elapsed.as_millis(),
                    timeout.absolute_wait.map(|wait| wait.as_millis()),
                    u128::from(observer.since_last_progress_ms(elapsed.as_millis() as u64)),
                    observer.progress_observations() as usize,
                );
                return Err(format!(
                    "observation limit reached (not a verdict on the work): {}; diagnostics: waiting_for=durable_assistant_message phase={} last_active_phase={} progress_observations={} message_cursor={} timeline_cursor={}",
                    report,
                    observer.phase(),
                    observer.last_active_phase(),
                    observer.progress_observations(),
                    observer.next_message_sequence(),
                    observer.timeline_cursor().unwrap_or("-"),
                ));
            }
            if timeout
                .should_abort_for_no_progress(elapsed, observer.progress_observations() as usize)
            {
                observer.mark_stalled();
                return Err(format!(
                    "no durable execution progress before the nominal scenario wait elapsed after {}ms; nominal wait={}ms, absolute safety wait={}ms, phase={}, last_active_phase={}, message_cursor={}, timeline_cursor={}",
                    elapsed.as_millis(),
                    timeout.nominal_wait.as_millis(),
                    timeout.absolute_wait.map(|wait| wait.as_millis()).map_or_else(|| "none".to_string(), |wait| wait.to_string()),
                    observer.phase(),
                    observer.last_active_phase(),
                    observer.next_message_sequence(),
                    observer.timeline_cursor().unwrap_or("-"),
                ));
            }
            if timeout.should_abort_for_inactivity(
                elapsed,
                since_progress,
                observer.progress_observations() as usize,
            ) {
                observer.mark_stalled();
                return Err(format!(
                    "no durable execution progress for {}ms after {}ms; inactivity window={}ms, nominal wait={}ms, absolute safety wait={}ms, phase={}, last_active_phase={}, progress_observations={}, message_cursor={}, timeline_cursor={}",
                    since_progress.as_millis(),
                    elapsed.as_millis(),
                    timeout.inactivity_wait.as_millis(),
                    timeout.nominal_wait.as_millis(),
                    timeout.absolute_wait.map(|wait| wait.as_millis()).map_or_else(|| "none".to_string(), |wait| wait.to_string()),
                    observer.phase(),
                    observer.last_active_phase(),
                    observer.progress_observations(),
                    observer.next_message_sequence(),
                    observer.timeline_cursor().unwrap_or("-"),
                ));
            }
            thread::sleep(self.poll_interval);
        }
    }

    fn execution_live_observation(
        &self,
        session_id: &str,
    ) -> Result<ExecutionLiveObservation, String> {
        let path = format!("/api/sessions/{session_id}/execution/live");
        let response = self.get_json(&path)?;
        let response_body_bytes = serde_json::to_vec(&response)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        let live = response.get("live").unwrap_or(&Value::Null);
        let summary = json!({
            "execution_id": response.get("execution_id"),
            "live_revision": live.get("revision"),
            "live_status": live.get("status"),
            "live_output_bytes": live.get("output_bytes"),
            "live_last_progress_at_ms": live.get("last_progress_at_ms"),
        });
        let fingerprint = serde_json::to_string(&summary).unwrap_or_default();
        Ok(ExecutionLiveObservation {
            fingerprint,
            summary,
            response_body_bytes,
        })
    }

    /// Read the turn id bound to a root execution, used to continue a resumed
    /// scenario's acceptance checks without posting a new prompt.
    fn root_execution_turn_id(&self, execution_id: &str) -> Option<String> {
        let path = format!("/api/runtime/executions/{execution_id}?detail_scope=summary");
        let projection = self.get_json(&path).ok()?;
        projection
            .get("turn_id")
            .or_else(|| {
                projection
                    .get("identity")
                    .and_then(|identity| identity.get("turn_id"))
            })
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
    }

    /// A delegated AgentTask shares the parent session's durable message
    /// store. Its intermediate assistant response is useful progress, but it
    /// is not the parent turn's answer. Only the root ingress graph's own
    /// completed synthesis closes a live scenario.
    fn root_execution_observation(
        &self,
        execution_id: &str,
    ) -> Result<RootExecutionObservation, String> {
        let path = format!("/api/runtime/executions/{execution_id}?detail_scope=summary");
        match self.get_json(&path) {
            Ok(projection) => {
                let response_body_bytes = serde_json::to_vec(&projection)
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or_default();
                let terminal = root_execution_terminal_state(&projection);
                let statuses = root_node_statuses(&projection);
                let fingerprint = root_progress_fingerprint(&projection, &statuses);
                let live = projection.get("live").unwrap_or(&Value::Null);
                let summary = json!({
                    "execution_id": projection.get("execution_id"),
                    "revision": projection.get("revision"),
                    "terminal_state": terminal.as_str(),
                    "node_statuses": statuses,
                    "live_revision": live.get("revision"),
                    "live_status": live.get("status"),
                    "live_output_bytes": live.get("output_bytes"),
                    "live_last_progress_at_ms": live.get("last_progress_at_ms"),
                });
                Ok(RootExecutionObservation {
                    terminal,
                    fingerprint,
                    summary,
                    response_body_bytes,
                })
            }
            Err(error) => Err(error),
        }
    }

    fn poll_message_deltas(
        &self,
        session_id: &str,
        elapsed_ms: u64,
        observer: &mut LiveScenarioObserver,
    ) -> Result<(), String> {
        for _ in 0..MAX_DRAIN_PAGES_PER_PROBE {
            let path = observer.message_path(session_id);
            let response = self.get_json(&path);
            let page = observer.observe_message_page(&path, &response, elapsed_ms)?;
            if !page.has_more {
                return Ok(());
            }
        }
        observer.fail_integrity(format!(
            "message pagination exceeded {} pages without draining",
            MAX_DRAIN_PAGES_PER_PROBE
        ))
    }

    /// Drain every durable Session/Runtime timeline delta from the last v2
    /// cursor. Root revisions intentionally do not mirror every delegated
    /// Team transition, so cursor-complete Session activity is an independent
    /// progress and evidence source.
    fn poll_timeline_deltas(
        &self,
        session_id: &str,
        elapsed_ms: u64,
        observer: &mut LiveScenarioObserver,
    ) -> Result<(), String> {
        for _ in 0..MAX_DRAIN_PAGES_PER_PROBE {
            let path = observer.timeline_path(session_id);
            let response = self.get_json(&path);
            let page = observer.observe_timeline_page(&path, &response, elapsed_ms)?;
            if !page.has_more {
                return Ok(());
            }
        }
        observer.fail_integrity(format!(
            "timeline pagination exceeded {} pages without draining",
            MAX_DRAIN_PAGES_PER_PROBE
        ))
    }

    fn capture_diagnostics(
        &self,
        session_id: &str,
        execution_id: Option<&str>,
        observer: &mut LiveScenarioObserver,
        scenario_started: Instant,
        trace: &mut Vec<Value>,
    ) -> Value {
        let timeline_error = self
            .poll_timeline_deltas(
                session_id,
                scenario_started.elapsed().as_millis() as u64,
                observer,
            )
            .err();
        let projection = execution_id.map(|id| {
            let path = format!("/api/runtime/executions/{id}?detail_scope=full");
            let response = self.get_json(&path);
            trace.push(trace_json_entry("GET", path, Value::Null, &response));
            response.unwrap_or_else(|error| json!({"error": error}))
        });
        json!({
            "timeline": observer.timeline(),
            "timeline_collection_error": timeline_error,
            "projection": projection.unwrap_or(Value::Null),
            // Where an Agent-first turn actually stopped, derived from the
            // collected timeline so a stall is diagnosable instead of being a
            // single uninterpretable error string. `remaining` is the protocol
            // checklist the framework owns; the model's own judgement is what
            // advances it, so knowing which rung was never reached is the whole
            // answer to "where did it stop".
            "agentic_progress": Self::agentic_progress_from_timeline(&observer.timeline()),
        })
    }

    /// Derive the Agent-first protocol rungs reached from a collected timeline.
    fn agentic_progress_from_timeline(timeline: &Value) -> Value {
        let serialized = serde_json::to_string(timeline).unwrap_or_default();
        let count = |needle: &str| serialized.matches(needle).count();
        let teams = count("\"team_create\"");
        let published = count("\"task_publish\"");
        let submitted = count("\"task_submit\"");
        let reviewed = count("\"task_review\"");
        let completion_requested = serialized.contains("objective_complete_request");
        let mut remaining = Vec::new();
        if teams == 0 {
            remaining.push("team_create".to_string());
        }
        if published == 0 {
            remaining.push("task_publish".to_string());
        }
        if published > submitted {
            remaining.push("task_submit".to_string());
        }
        if submitted > reviewed {
            remaining.push("independent_task_review".to_string());
        }
        if !completion_requested {
            remaining.push("objective_complete_request".to_string());
        }
        json!({
            "teams_created": teams,
            "tasks_published": published,
            "tasks_submitted": submitted,
            "tasks_reviewed": reviewed,
            "objective_complete_requested": completion_requested,
            "remaining": remaining,
        })
    }

    fn execution_lineage_projections(
        &self,
        root_execution_id: &str,
        trace: &mut Vec<Value>,
        cache: &mut BTreeMap<String, (u64, Value)>,
    ) -> Vec<Value> {
        let mut pending = vec![root_execution_id.to_string()];
        let mut visited = BTreeSet::new();
        let mut projections = Vec::new();
        while let Some(execution_id) = pending.pop() {
            if !visited.insert(execution_id.clone()) {
                continue;
            }
            let summary_path =
                format!("/api/runtime/executions/{execution_id}?detail_scope=summary");
            let Ok(summary) = self.get_json(&summary_path) else {
                continue;
            };
            if let Some(children) = summary.get("child_executions").and_then(Value::as_array) {
                pending.extend(children.iter().filter_map(|child| {
                    child
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(ToString::to_string)
                }));
            }
            let revision = summary
                .get("revision")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            if let Some((cached_revision, projection)) = cache.get(&execution_id) {
                if *cached_revision == revision {
                    projections.push(projection.clone());
                    continue;
                }
            }
            // Acceptance consumes public graph topology, work state, bounded
            // activities and terminal evidence refs. Those are all present in
            // Summary. Full entity/model-event detail can exceed megabytes
            // and changes on every graph revision, so polling it multiplied a
            // single 16-Agent run into roughly a gigabyte of observation I/O.
            let path = format!("/api/runtime/executions/{execution_id}?detail_scope=summary");
            let response = self.get_json(&path);
            trace.push(bounded_projection_trace_entry(&path, &response));
            let Ok(projection) = response else {
                continue;
            };
            cache.insert(execution_id, (revision, projection.clone()));
            projections.push(projection);
        }
        projections
    }

    /// Root ingress completion is not a global join: a collaboration Program
    /// may still have Team descendants running after its durable synthesis is
    /// materialized. Team-oriented acceptance therefore observes the complete
    /// execution lineage until its evidence passes or all known Team work is
    /// terminal. This keeps evaluator cleanup from canceling valid work.
    fn wait_for_descendant_team_acceptance(
        &self,
        acceptance: LiveAcceptance,
        response_text: &str,
        session_id: &str,
        turn_id: &str,
        root_execution_id: &str,
        scenario_started: Instant,
        timeout: &LiveScenarioTimeout,
        observer: &mut LiveScenarioObserver,
        trace: &mut Vec<Value>,
    ) -> DescendantTeamWait {
        let wait_started = Instant::now();
        let mut observations = 0_usize;
        let mut projection_cache = BTreeMap::new();
        loop {
            let timeline_error = self
                .poll_timeline_deltas(
                    session_id,
                    scenario_started.elapsed().as_millis() as u64,
                    observer,
                )
                .err();
            let timeline = observer.timeline();
            // The public projection makes child execution lineage explicit. A
            // session ingress graph often delegates provider/tool/team work to
            // descendants, so reporting only the root would incorrectly claim
            // zero model rounds and zero token/tool usage for a real execution.
            let projections =
                self.execution_lineage_projections(root_execution_id, trace, &mut projection_cache);
            let agentic_program = if acceptance.requires_descendant_team_closure() {
                self.root_agentic_program(session_id, turn_id, trace).ok()
            } else {
                None
            };
            let mut result = acceptance.evaluate(
                response_text,
                &timeline,
                &projections,
                agentic_program.as_ref(),
                root_execution_id,
            );
            observations = observations.saturating_add(1);

            if let Some(error) = timeline_error {
                result.checks.push(json!({
                    "name": "autonomous_observation_integrity",
                    "passed": false,
                    "error": error.clone(),
                }));
                result.passed = false;
                return DescendantTeamWait {
                    timeline,
                    projections,
                    agentic_program,
                    acceptance: result,
                    report: json!({
                        "required": acceptance.requires_descendant_team_closure(),
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": "timeline_observation_integrity_failed",
                        "error": error,
                    }),
                };
            }

            if !acceptance.requires_descendant_team_closure() || result.passed {
                return DescendantTeamWait {
                    timeline,
                    projections,
                    agentic_program,
                    acceptance: result,
                    report: json!({
                        "required": acceptance.requires_descendant_team_closure(),
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": if acceptance.requires_descendant_team_closure() {
                            "team_acceptance_satisfied"
                        } else {
                            "not_required"
                        },
                    }),
                };
            }

            let Some(program) = agentic_program.as_ref() else {
                return DescendantTeamWait {
                    timeline,
                    projections,
                    agentic_program,
                    acceptance: result,
                    report: json!({
                        "required": true,
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": "root_terminal_without_agentic_program",
                    }),
                };
            };
            let health = projected_team_health(program);
            if !health.has_pending_work() {
                return DescendantTeamWait {
                    timeline,
                    projections,
                    agentic_program,
                    acceptance: result,
                    report: json!({
                        "required": true,
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": "team_lineage_terminal_without_acceptance",
                        "team_health": health.to_value(),
                    }),
                };
            }

            let scenario_elapsed = scenario_started.elapsed();
            let since_progress = Duration::from_millis(
                observer.since_last_progress_ms(scenario_elapsed.as_millis() as u64),
            );
            if timeout.should_abort_for_absolute_wait(scenario_elapsed)
                || timeout.should_abort_for_inactivity(
                    scenario_elapsed,
                    since_progress,
                    observer.progress_observations() as usize,
                )
            {
                return DescendantTeamWait {
                    timeline,
                    projections,
                    agentic_program,
                    acceptance: result,
                    report: json!({
                        "required": true,
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": if timeout.should_abort_for_absolute_wait(scenario_elapsed) {
                            "scenario_absolute_wait_elapsed_while_team_descendants_running"
                        } else {
                            "scenario_inactivity_wait_elapsed_while_team_descendants_running"
                        },
                        "since_last_progress_ms": since_progress.as_millis(),
                        "team_health": health.to_value(),
                    }),
                };
            }
            thread::sleep(self.poll_interval);
        }
    }

    /// Evaluation timeouts must not leave a real lineage running after its
    /// report has declared failure. A Session root may only be terminalized
    /// through the durable Session cancellation protocol; that owner records
    /// the Requested/Cancelled receipt and propagates to descendants.
    fn cancel_execution_lineage(
        &self,
        root_execution_id: &str,
        actor: &mut SessionActor<'_>,
        trace: &mut Vec<Value>,
    ) -> Value {
        let path = format!("/api/sessions/{}/cancel", actor.session_id());
        let response = actor.post_mutation(
            &path,
            json!({
                "reason": "isolated live evaluation timed out; cancelling owned Session lineage",
                "expected_execution_id": root_execution_id,
            }),
        );
        trace.extend(actor.drain_trace());
        let complete = response.is_ok();
        json!({
            "status": if complete { "passed" } else { "failed" },
            "attempted": 1,
            "succeeded": usize::from(complete),
            "failed": usize::from(!complete),
            "reason": (!complete).then_some("durable Session cancellation was rejected"),
            "receipt": response,
        })
    }

    fn get_json(&self, path: &str) -> Result<Value, String> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .map_err(|error| error.to_string())?;
        response_json(response)
    }

    fn root_agentic_program(
        &self,
        session_id: &str,
        turn_id: &str,
        trace: &mut Vec<Value>,
    ) -> Result<runtime::AgenticProgramProjection, String> {
        let path =
            format!("/api/runtime/agentic/programs/root?session_id={session_id}&turn_id={turn_id}");
        let response = self.get_json(&path);
        trace.push(trace_json_entry("GET", path, Value::Null, &response));
        serde_json::from_value(response?).map_err(|error| {
            format!("invalid AgenticProgramProjection response from Gateway: {error}")
        })
    }
}

fn aggregate_scenario_metrics(scenarios: &[Value]) -> Value {
    let total = |key: &str| {
        scenarios
            .iter()
            .filter_map(|scenario| {
                scenario
                    .pointer(&format!("/metrics/{key}"))
                    .and_then(Value::as_u64)
            })
            .sum::<u64>()
    };
    let maximum = |key: &str| {
        scenarios
            .iter()
            .filter_map(|scenario| {
                scenario
                    .pointer(&format!("/metrics/{key}"))
                    .and_then(Value::as_u64)
            })
            .max()
            .unwrap_or_default()
    };
    let wall_ms = scenarios
        .iter()
        .filter_map(|scenario| scenario.pointer("/metrics/wall_ms").and_then(Value::as_u64))
        .collect::<Vec<_>>();
    let first_token_ms = scenarios
        .iter()
        .filter_map(|scenario| {
            scenario
                .pointer("/metrics/first_token_latency_ms")
                .and_then(Value::as_u64)
        })
        .collect::<Vec<_>>();
    json!({
        "input_tokens": total("input_tokens"),
        "output_tokens": total("output_tokens"),
        "cache_creation_input_tokens": total("cache_creation_input_tokens"),
        "cache_read_input_tokens": total("cache_read_input_tokens"),
        "model_visible_prompt_bytes": total("model_visible_prompt_bytes"),
        "reusable_prefix_bytes": total("reusable_prefix_bytes"),
        "tool_array_bytes": total("tool_array_bytes"),
        "warm_model_visible_prompt_bytes": total("warm_model_visible_prompt_bytes"),
        "warm_reusable_prefix_bytes": total("warm_reusable_prefix_bytes"),
        "warm_input_miss_tokens": total("warm_input_miss_tokens"),
        "warm_cache_creation_input_tokens": total("warm_cache_creation_input_tokens"),
        "warm_cache_read_input_tokens": total("warm_cache_read_input_tokens"),
        "cache_cold_leader_count": total("cache_cold_leader_count"),
        "cache_waiter_count": total("cache_waiter_count"),
        "provider_attempt_count": total("provider_attempt_count"),
        "provider_attempt_usage_unknown_count": total("provider_attempt_usage_unknown_count"),
        "provider_attempt_cache_dimensions_unknown_count": total("provider_attempt_cache_dimensions_unknown_count"),
        "cache_tokens": total("cache_tokens"),
        "total_tokens": total("total_tokens"),
        "model_rounds": total("model_rounds"),
        "tool_calls": total("tool_calls"),
        "max_agent_count": maximum("agent_count"),
        "max_team_count": maximum("team_count"),
        "wall_ms": distribution(&wall_ms),
        "first_token_latency_ms": distribution(&first_token_ms),
    })
}

fn distribution(values: &[u64]) -> Value {
    if values.is_empty() {
        return json!({"samples": 0});
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    json!({
        "samples": sorted.len(),
        "min": sorted[0],
        "p50": percentile(&sorted, 50),
        "p95": percentile(&sorted, 95),
        "p99": percentile(&sorted, 99),
        "max": sorted[sorted.len() - 1],
    })
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .saturating_add(99)
        .saturating_div(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted[index]
}

/// How many tool calls a scenario actually made.
///
/// Four sources record one and each is silent for a different reason: a provider
/// usage may omit the count (the local backend does), a timeline projection may
/// not be materialized for the scenario, the run's own captured details are the
/// only record that survives all of that, and a Gateway run records calls as event
/// payloads that none of the three reads.
///
/// The fourth source counts terminal statuses only. A `running` event and its
/// `completed` event are the same call twice: one live run held 497 payloads with a
/// tool name while exactly 95 were terminal, and the durable store agreed with 95.
fn scenario_tool_calls(usage: &ScenarioTokenUsage, timeline: &Value) -> u64 {
    let timeline_tool_calls = timeline
        .pointer("/tool_summary/count")
        .and_then(Value::as_u64)
        .or_else(|| {
            timeline
                .get("tool_timeline")
                .and_then(Value::as_array)
                .map(|items| items.len() as u64)
        })
        .unwrap_or_default();
    let captured_tool_calls = timeline
        .get("tool_call_details")
        .and_then(Value::as_array)
        .map_or(0, |details| details.len() as u64);
    usage
        .tool_calls
        .max(timeline_tool_calls)
        .max(captured_tool_calls)
        .max(crate::report::terminal_tool_events_in_events(
            timeline
                .get("events")
                .and_then(Value::as_array)
                .map_or(&[][..], |events| events.as_slice()),
        ))
}

fn scenario_metrics(
    timeline: &Value,
    projections: &[Value],
    agentic_program: Option<&runtime::AgenticProgramProjection>,
    elapsed: Duration,
) -> Value {
    let provider_attempts = provider_attempt_metrics(timeline);
    let graph_usage = execution_graph_usage_metrics(projections);
    let timeline_usage = token_usage_metrics(timeline);
    let usage = if provider_attempts.usage.provider_usage_records > 0 {
        provider_attempts.usage.clone()
    } else if graph_usage.record_count > 0 {
        graph_usage
    } else {
        timeline_usage
    };
    let input_tokens = usage.input_tokens;
    let output_tokens = usage.output_tokens;
    let cache_creation_input_tokens = usage.cache_creation_input_tokens;
    let cache_read_input_tokens = usage.cache_read_input_tokens;
    let cache_tokens = cache_creation_input_tokens.saturating_add(cache_read_input_tokens);
    let tool_calls = scenario_tool_calls(&usage, timeline);
    let mut agents = BTreeSet::new();
    let mut teams = BTreeSet::new();
    for projection in projections {
        agents.extend(
            projection
                .get("agents")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| {
                    item.get("id")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                }),
        );
        teams.extend(
            projection
                .get("teams")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| {
                    item.get("id")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                }),
        );
    }
    let projected_health = agentic_program
        .map(projected_team_health)
        .unwrap_or_default();
    let timeline_model_rounds = timeline
        .pointer("/team_session/runtime_run_count")
        .and_then(Value::as_u64)
        .or_else(|| {
            timeline
                .get("runs")
                .and_then(Value::as_array)
                .map(|runs| runs.len() as u64)
        })
        .unwrap_or_default();
    let model_rounds = usage.model_rounds.max(timeline_model_rounds);
    let usage_unknown_attempts = if provider_attempts.attempt_count > 0 {
        provider_attempts.usage_unknown_count
    } else {
        model_rounds.saturating_sub(usage.provider_usage_records)
    };
    let telemetry = timeline
        .pointer("/token_speed/model_telemetry")
        .cloned()
        .unwrap_or(Value::Null);
    let first_token_latency_ms = telemetry
        .get("first_token_latency_ms")
        .and_then(Value::as_u64);
    let wall_tokens_per_second = telemetry
        .get("wall_tokens_per_second")
        .or_else(|| telemetry.get("tokens_per_second"))
        .and_then(Value::as_f64);
    let active_tokens_per_second = telemetry
        .get("active_tokens_per_second")
        .and_then(Value::as_f64);
    let elapsed_ms = elapsed.as_millis() as u64;
    let output_tokens_per_second =
        (elapsed_ms > 0).then(|| output_tokens.saturating_mul(1_000) as f64 / elapsed_ms as f64);
    let agentic_tasks = agentic_program
        .map(|program| active_agentic_tasks(program).count())
        .unwrap_or_default();
    let accepted_tasks = agentic_program
        .map(|program| {
            active_agentic_tasks(program)
                .filter(|task| task.status == runtime::AgenticTaskStatus::Accepted)
                .count()
        })
        .unwrap_or_default();
    let agentic_reviews = agentic_program
        .map(|program| {
            active_agentic_tasks(program)
                .filter(|task| task.reviewed_by.is_some())
                .count()
        })
        .unwrap_or_default();
    let topic_entries = agentic_program
        .map(|program| program.topics.values().map(Vec::len).sum::<usize>())
        .unwrap_or_default();
    let artifact_count = agentic_program.map_or(0, |program| program.artifacts.len());
    // S1: a reviewer that requested rework records a durable reason on the Task,
    // so this is the honest rework signal (not a replayable counter).
    let reworked_tasks = agentic_program
        .map(|program| {
            active_agentic_tasks(program)
                .filter(|task| task.review_reason.is_some())
                .count()
        })
        .unwrap_or_default();
    let rework_rate_bp = if accepted_tasks.saturating_add(reworked_tasks) == 0 {
        0
    } else {
        u32::try_from(
            reworked_tasks
                .saturating_mul(10_000)
                .saturating_div(accepted_tasks.saturating_add(reworked_tasks)),
        )
        .unwrap_or(0)
    };
    let mut metrics = json!({
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
        "cache_tokens": cache_tokens,
        "total_tokens": input_tokens.saturating_add(output_tokens).saturating_add(cache_tokens),
        "token_usage_records": usage.record_count,
        "provider_usage_records": usage.provider_usage_records,
        "usage_unknown_attempts": usage_unknown_attempts,
        "model_rounds": model_rounds,
        "effective_models": usage.models.into_iter().collect::<Vec<_>>(),
        "tool_calls": tool_calls,
        "agent_count": agents.len().max(projected_health.agent_count),
        "team_count": teams.len().max(projected_health.team_count),
        "agentic_task_count": agentic_tasks,
        "accepted_agentic_task_count": accepted_tasks,
        "rounds_per_task": if agentic_tasks == 0 {
            0
        } else {
            model_rounds / agentic_tasks as u64
        },
        "reworked_agentic_task_count": reworked_tasks,
        "rework_rate_bp": rework_rate_bp,
        "agentic_review_count": agentic_reviews,
        "agentic_topic_entry_count": topic_entries,
        "agentic_artifact_count": artifact_count,
        "accepted_cross_team_dependency_count": agentic_program.map(accepted_cross_team_dependency_count).unwrap_or_default(),
        "physical_parallel_overlap_count": physical_parallel_overlap_count(projections),
        "physical_failed_agent_activity_count": physical_failed_agent_activity_count(projections),
        "wall_ms": elapsed_ms,
        "first_token_latency_ms": first_token_latency_ms,
        "wall_tokens_per_second": wall_tokens_per_second.or(output_tokens_per_second),
        "active_tokens_per_second": active_tokens_per_second,
    });
    if let Some(object) = metrics.as_object_mut() {
        object.extend([
            (
                "provider_attempt_count".to_string(),
                json!(provider_attempts.attempt_count),
            ),
            (
                "provider_attempt_usage_unknown_count".to_string(),
                json!(provider_attempts.usage_unknown_count),
            ),
            (
                "provider_attempt_cache_dimensions_unknown_count".to_string(),
                json!(provider_attempts.cache_dimensions_unknown_count),
            ),
            (
                "provider_cache_identity_count".to_string(),
                json!(provider_attempts.cache_identities.len()),
            ),
            (
                "cache_cold_leader_count".to_string(),
                json!(provider_attempts.cold_leaders),
            ),
            (
                "cache_waiter_count".to_string(),
                json!(provider_attempts.waiters),
            ),
            (
                "structural_reuse_ratio_bp".to_string(),
                json!(provider_attempts.structural_ratio_bp()),
            ),
            (
                "warm_structural_reuse_ratio_bp".to_string(),
                json!(provider_attempts.warm_structural_ratio_bp()),
            ),
            (
                "model_visible_prompt_bytes".to_string(),
                json!(provider_attempts.prompt_bytes),
            ),
            (
                "reusable_prefix_bytes".to_string(),
                json!(provider_attempts.reusable_prefix_bytes),
            ),
            (
                "tool_array_bytes".to_string(),
                json!(provider_attempts.tool_array_bytes),
            ),
            (
                "warm_model_visible_prompt_bytes".to_string(),
                json!(provider_attempts.warm_prompt_bytes),
            ),
            (
                "warm_reusable_prefix_bytes".to_string(),
                json!(provider_attempts.warm_reusable_prefix_bytes),
            ),
            (
                "warm_input_miss_tokens".to_string(),
                json!(provider_attempts.warm_input_miss_tokens),
            ),
            (
                "warm_cache_creation_input_tokens".to_string(),
                json!(provider_attempts.warm_cache_creation_input_tokens),
            ),
            (
                "warm_cache_read_input_tokens".to_string(),
                json!(provider_attempts.warm_cache_read_input_tokens),
            ),
            (
                "warm_cache_hit_ratio_bp".to_string(),
                json!(provider_attempts.warm_cache_ratio_bp()),
            ),
            (
                "exact_prefix_extension_count".to_string(),
                json!(provider_attempts.exact_extensions),
            ),
        ]);
    }
    metrics
}

#[derive(Clone, Default)]
struct ScenarioTokenUsage {
    models: BTreeSet<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    tool_calls: u64,
    model_rounds: u64,
    record_count: u64,
    provider_usage_records: u64,
}

#[derive(Default)]
struct ProviderAttemptMetrics {
    usage: ScenarioTokenUsage,
    attempt_count: u64,
    usage_unknown_count: u64,
    cache_dimensions_unknown_count: u64,
    cache_identities: BTreeSet<String>,
    cold_leaders: u64,
    waiters: u64,
    exact_extensions: u64,
    prompt_bytes: u64,
    reusable_prefix_bytes: u64,
    warm_prompt_bytes: u64,
    warm_reusable_prefix_bytes: u64,
    warm_input_miss_tokens: u64,
    warm_cache_creation_input_tokens: u64,
    warm_cache_read_input_tokens: u64,
    tool_array_bytes: u64,
}

impl ProviderAttemptMetrics {
    fn structural_ratio_bp(&self) -> u64 {
        ratio_bp(self.reusable_prefix_bytes, self.prompt_bytes)
    }

    fn warm_structural_ratio_bp(&self) -> u64 {
        ratio_bp(self.warm_reusable_prefix_bytes, self.warm_prompt_bytes)
    }

    fn warm_cache_ratio_bp(&self) -> u64 {
        ratio_bp(
            self.warm_cache_read_input_tokens,
            self.warm_input_miss_tokens
                .saturating_add(self.warm_cache_creation_input_tokens)
                .saturating_add(self.warm_cache_read_input_tokens),
        )
    }
}

fn ratio_bp(numerator: u64, denominator: u64) -> u64 {
    (denominator > 0)
        .then(|| numerator.saturating_mul(10_000) / denominator)
        .unwrap_or_default()
        .min(10_000)
}

/// Runtime persists exact request and terminal usage as separate events with
/// the same request id. Collect them recursively because the public timeline
/// groups root and child Session events into multiple causal sections. A map
/// deduplicates pagination/replay copies, making physical Provider attempts
/// the only additive token source.
fn provider_attempt_metrics(timeline: &Value) -> ProviderAttemptMetrics {
    let mut packed = BTreeMap::<String, Value>::new();
    let mut outcomes = BTreeMap::<String, Value>::new();
    collect_provider_attempt_events(timeline, &mut packed, &mut outcomes);
    let mut metrics = ProviderAttemptMetrics::default();
    metrics.attempt_count = packed
        .keys()
        .chain(outcomes.keys())
        .collect::<BTreeSet<_>>()
        .len() as u64;
    // A request packed onto the wire is a physical Provider attempt even if
    // the process or transport disappeared before a terminal outcome could
    // be committed. Count that attempt as usage-unknown; silently dropping it
    // understates both cost risk and cache evidence completeness.
    metrics.usage_unknown_count = packed
        .keys()
        .filter(|request_id| !outcomes.contains_key(*request_id))
        .count() as u64;
    metrics.cache_dimensions_unknown_count = metrics.usage_unknown_count;
    for (request_id, outcome) in outcomes {
        let usage = outcome.get("usage").unwrap_or(&Value::Null);
        let usage_known = outcome.get("usage_status").and_then(Value::as_str) == Some("known")
            && usage.is_object();
        if !usage_known {
            metrics.usage_unknown_count = metrics.usage_unknown_count.saturating_add(1);
            metrics.cache_dimensions_unknown_count =
                metrics.cache_dimensions_unknown_count.saturating_add(1);
            continue;
        }
        if outcome
            .get("cache_dimensions_status")
            .and_then(Value::as_str)
            != Some("known")
        {
            metrics.cache_dimensions_unknown_count =
                metrics.cache_dimensions_unknown_count.saturating_add(1);
        }
        metrics.usage.input_tokens = metrics
            .usage
            .input_tokens
            .saturating_add(value_u64(usage, &["input_tokens"]));
        metrics.usage.output_tokens = metrics
            .usage
            .output_tokens
            .saturating_add(value_u64(usage, &["output_tokens"]));
        metrics.usage.cache_creation_input_tokens = metrics
            .usage
            .cache_creation_input_tokens
            .saturating_add(value_u64(usage, &["cache_creation_input_tokens"]));
        metrics.usage.cache_read_input_tokens = metrics
            .usage
            .cache_read_input_tokens
            .saturating_add(value_u64(usage, &["cache_read_input_tokens"]));
        metrics.usage.record_count = metrics.usage.record_count.saturating_add(1);
        metrics.usage.provider_usage_records =
            metrics.usage.provider_usage_records.saturating_add(1);
        if outcome.get("terminal_status").and_then(Value::as_str) == Some("completed") {
            metrics.usage.model_rounds = metrics.usage.model_rounds.saturating_add(1);
        }
        if let Some(request) = packed.get(&request_id) {
            if let Some(model) = request.get("model").and_then(Value::as_str) {
                metrics.usage.models.insert(model.to_string());
            }
            if request.get("cache_cold_leader").and_then(Value::as_bool) != Some(true) {
                metrics.warm_input_miss_tokens = metrics
                    .warm_input_miss_tokens
                    .saturating_add(value_u64(usage, &["input_tokens"]));
                metrics.warm_cache_creation_input_tokens = metrics
                    .warm_cache_creation_input_tokens
                    .saturating_add(value_u64(usage, &["cache_creation_input_tokens"]));
                metrics.warm_cache_read_input_tokens = metrics
                    .warm_cache_read_input_tokens
                    .saturating_add(value_u64(usage, &["cache_read_input_tokens"]));
            }
        }
    }
    for request in packed.into_values() {
        if let Some(identity) = request.get("cache_identity_sha256").and_then(Value::as_str) {
            metrics.cache_identities.insert(identity.to_string());
        }
        let prompt = value_u64(&request, &["model_visible_prompt_bytes"]);
        let reusable = value_u64(&request, &["reusable_prefix_bytes"]);
        metrics.prompt_bytes = metrics.prompt_bytes.saturating_add(prompt);
        metrics.reusable_prefix_bytes = metrics.reusable_prefix_bytes.saturating_add(reusable);
        metrics.tool_array_bytes = metrics.tool_array_bytes.saturating_add(
            request
                .pointer("/prompt_composition/tool_array_bytes")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        );
        let cold = request.get("cache_cold_leader").and_then(Value::as_bool) == Some(true);
        if cold {
            metrics.cold_leaders = metrics.cold_leaders.saturating_add(1);
        } else {
            metrics.warm_prompt_bytes = metrics.warm_prompt_bytes.saturating_add(prompt);
            metrics.warm_reusable_prefix_bytes =
                metrics.warm_reusable_prefix_bytes.saturating_add(reusable);
        }
        if request
            .get("waited_for_cache_warmup")
            .and_then(Value::as_bool)
            == Some(true)
        {
            metrics.waiters = metrics.waiters.saturating_add(1);
        }
        if request
            .get("exact_prefix_extension")
            .and_then(Value::as_bool)
            == Some(true)
        {
            metrics.exact_extensions = metrics.exact_extensions.saturating_add(1);
        }
    }
    metrics
}

fn collect_provider_attempt_events(
    value: &Value,
    packed: &mut BTreeMap<String, Value>,
    outcomes: &mut BTreeMap<String, Value>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_provider_attempt_events(item, packed, outcomes);
            }
        }
        Value::Object(object) => {
            let kind = object.get("kind").and_then(Value::as_str);
            let payload = object.get("payload").unwrap_or(value);
            let payload_type = payload.get("type").and_then(Value::as_str);
            if matches!(
                (kind, payload_type),
                (Some("context.provider_request_packed"), _) | (_, Some("ProviderRequestPacked"))
            ) {
                if let Some(request_id) = payload.get("request_id").and_then(Value::as_str) {
                    packed.insert(request_id.to_string(), payload.clone());
                }
            } else if matches!(
                (kind, payload_type),
                (Some("context.provider_attempt_outcome"), _) | (_, Some("ProviderAttemptOutcome"))
            ) {
                if let Some(request_id) = payload.get("request_id").and_then(Value::as_str) {
                    outcomes.insert(request_id.to_string(), payload.clone());
                }
            }
            for child in object.values() {
                collect_provider_attempt_events(child, packed, outcomes);
            }
        }
        _ => {}
    }
}

/// Summarize physical leaf usage across the canonical root and all durable
/// child projections. Container nodes intentionally carry cumulative child
/// usage for local recovery/projection and are therefore never additive.
/// Provider tokens belong to `inline_model`; tool effects belong to
/// `tool_batch`. No report-time token estimation is allowed.
fn execution_graph_usage_metrics(projections: &[Value]) -> ScenarioTokenUsage {
    let mut seen_nodes = BTreeSet::new();
    let mut usage = ScenarioTokenUsage::default();
    for projection in projections {
        let graph_id = projection
            .pointer("/graph/graph_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        for node in projection
            .pointer("/graph/nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(node_id) = node.get("node_id").and_then(Value::as_str) else {
                continue;
            };
            if !seen_nodes.insert(format!("{graph_id}:{node_id}")) {
                continue;
            }
            let kind = node.get("kind").and_then(Value::as_str).unwrap_or_default();
            if !matches!(kind, "inline_model" | "tool_batch") {
                continue;
            }
            let node_usage = node.get("usage").unwrap_or(&Value::Null);
            if kind == "inline_model" {
                if let Some(model) = node_usage.get("model").and_then(Value::as_str) {
                    if !model.trim().is_empty() {
                        usage.models.insert(model.to_string());
                    }
                }
            }
            let input_tokens = (kind == "inline_model")
                .then(|| value_u64(node_usage, &["input_tokens"]))
                .unwrap_or_default();
            let output_tokens = (kind == "inline_model")
                .then(|| value_u64(node_usage, &["output_tokens"]))
                .unwrap_or_default();
            let cache_read_input_tokens = (kind == "inline_model")
                .then(|| value_u64(node_usage, &["cache_read_input_tokens"]))
                .unwrap_or_default();
            let cache_creation_input_tokens = (kind == "inline_model")
                .then(|| value_u64(node_usage, &["cache_creation_input_tokens"]))
                .unwrap_or_default();
            let tool_calls = (kind == "tool_batch")
                .then(|| value_u64(node_usage, &["tool_calls"]))
                .unwrap_or_default();
            if input_tokens > 0
                || output_tokens > 0
                || cache_creation_input_tokens > 0
                || cache_read_input_tokens > 0
                || tool_calls > 0
            {
                usage.record_count = usage.record_count.saturating_add(1);
            }
            if kind == "inline_model"
                && (input_tokens > 0
                    || output_tokens > 0
                    || cache_creation_input_tokens > 0
                    || cache_read_input_tokens > 0)
            {
                usage.provider_usage_records = usage.provider_usage_records.saturating_add(1);
            }
            usage.input_tokens = usage.input_tokens.saturating_add(input_tokens);
            usage.output_tokens = usage.output_tokens.saturating_add(output_tokens);
            usage.cache_creation_input_tokens = usage
                .cache_creation_input_tokens
                .saturating_add(cache_creation_input_tokens);
            usage.cache_read_input_tokens = usage
                .cache_read_input_tokens
                .saturating_add(cache_read_input_tokens);
            usage.tool_calls = usage.tool_calls.saturating_add(tool_calls);
            if kind == "inline_model"
                && node.get("status").and_then(Value::as_str) == Some("completed")
            {
                usage.model_rounds = usage.model_rounds.saturating_add(1);
            }
        }
    }
    usage
}

fn token_usage_metrics(timeline: &Value) -> ScenarioTokenUsage {
    let Some(records) = timeline
        .pointer("/token_speed/token_usage")
        .and_then(Value::as_array)
    else {
        return ScenarioTokenUsage::default();
    };
    records
        .iter()
        .fold(ScenarioTokenUsage::default(), |mut usage, record| {
            usage.record_count = usage.record_count.saturating_add(1);
            usage.input_tokens = usage
                .input_tokens
                .saturating_add(value_u64(record, &["input", "input_tokens"]));
            usage.output_tokens = usage
                .output_tokens
                .saturating_add(value_u64(record, &["output", "output_tokens"]));
            usage.cache_creation_input_tokens = usage
                .cache_creation_input_tokens
                .saturating_add(value_u64(record, &["cache_create", "cache_create_tokens"]));
            usage.cache_read_input_tokens = usage
                .cache_read_input_tokens
                .saturating_add(value_u64(record, &["cache_read", "cache_read_tokens"]));
            usage.tool_calls = usage
                .tool_calls
                .saturating_add(value_u64(record, &["tool_calls"]));
            if value_u64(record, &["input", "input_tokens"]) > 0
                || value_u64(record, &["output", "output_tokens"]) > 0
                || value_u64(record, &["cache_read", "cache_read_tokens"]) > 0
            {
                usage.provider_usage_records = usage.provider_usage_records.saturating_add(1);
            }
            usage
        })
}

fn value_u64(value: &Value, keys: &[&str]) -> u64 {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(Value::as_u64))
        .sum()
}

#[derive(Clone, Copy)]
struct LiveScenarioSpec {
    id: &'static str,
    prompt: &'static str,
    acceptance: LiveAcceptance,
    timeout: LiveScenarioTimeout,
}

/// Evaluation-side waiting policy. The Runtime never receives this value and
/// therefore cannot use it as a business-finalization deadline. A scenario
/// with no progress stops at its nominal wait. Once durable
/// progress exists, each observation renews the inactivity window up to an
/// absolute safety ceiling. The Runtime never sees any of these values.
#[derive(Clone, Copy)]
struct LiveScenarioTimeout {
    initial_wait: Duration,
    inactivity_wait: Duration,
    nominal_wait: Duration,
    absolute_wait: Option<Duration>,
}

impl LiveScenarioTimeout {
    const fn direct() -> Self {
        Self {
            initial_wait: Duration::from_secs(45),
            inactivity_wait: Duration::from_secs(45),
            nominal_wait: Duration::from_secs(120),
            absolute_wait: Some(Duration::from_secs(240)),
        }
    }

    const fn tool() -> Self {
        Self {
            initial_wait: Duration::from_secs(90),
            inactivity_wait: Duration::from_secs(75),
            nominal_wait: Duration::from_secs(300),
            absolute_wait: Some(Duration::from_secs(600)),
        }
    }

    const fn team() -> Self {
        Self {
            // A team can have several active provider/agent subgraphs whose
            // work is not visible as a root revision until a reduction or
            // handoff commits. These values govern only the isolated evaluator
            // process; the Runtime retains its own provider-progress policy.
            initial_wait: Duration::from_secs(360),
            inactivity_wait: Duration::from_secs(600),
            nominal_wait: Duration::from_secs(1_800),
            // Team work is completion- and progress-governed. A fixed wall
            // clock limit can only be supplied explicitly by the evaluator
            // operator; it must not silently cancel productive descendants.
            absolute_wait: None,
        }
    }

    const fn large_scale(agent_count: usize) -> Self {
        // `agent_count` is a minimum acceptance threshold, not the topology
        // the model will actually choose. It is therefore valid for sizing
        // the no-progress window, but never for deriving a destructive wall
        // clock deadline. Productive autonomous work ends through semantic
        // closure; stalled work ends through the inactivity window. An
        // operator can still opt into a visible absolute cap with
        // COWD_EVAL_SCENARIO_TIMEOUT_SECS.
        let topology_wait_secs = 900_u64.saturating_add((agent_count as u64).saturating_mul(180));
        let nominal_wait_secs = if topology_wait_secs < 1_800 {
            1_800
        } else {
            topology_wait_secs
        };
        Self {
            initial_wait: Duration::from_secs(360),
            inactivity_wait: Duration::from_secs(480),
            nominal_wait: Duration::from_secs(nominal_wait_secs),
            absolute_wait: None,
        }
    }

    fn with_cap(self, cap: Option<Duration>) -> Self {
        // An operator may tighten the isolated test window, but cannot make a
        // scenario less patient than its complexity needs by accident: a cap
        // lower than the normal initial wait is ignored.
        let Some(cap) = cap else {
            return self;
        };
        if cap < self.initial_wait {
            return self;
        }
        Self {
            initial_wait: self.initial_wait,
            inactivity_wait: self.inactivity_wait.min(cap),
            nominal_wait: self.nominal_wait.min(cap),
            absolute_wait: Some(self.absolute_wait.map_or(cap, |wait| wait.min(cap))),
        }
    }

    fn report(
        self,
        elapsed: Duration,
        since_progress: Duration,
        progress_observations: usize,
        terminal_reason: &str,
        phase: &str,
        last_active_phase: &str,
    ) -> Value {
        json!({
            "initial_wait_ms": self.initial_wait.as_millis(),
            "inactivity_wait_ms": self.inactivity_wait.as_millis(),
            "nominal_wait_ms": self.nominal_wait.as_millis(),
            "absolute_wait_ms": self.absolute_wait.map(|wait| wait.as_millis()),
            "elapsed_ms": elapsed.as_millis(),
            "since_last_progress_ms": since_progress.as_millis(),
            "progress_observations": progress_observations,
            "terminal_reason": terminal_reason,
            "phase": phase,
            "last_active_phase": last_active_phase,
        })
    }

    fn should_abort_for_no_progress(self, elapsed: Duration, progress_observations: usize) -> bool {
        progress_observations == 0 && elapsed >= self.nominal_wait
    }

    fn should_abort_for_absolute_wait(self, elapsed: Duration) -> bool {
        self.absolute_wait.is_some_and(|wait| elapsed >= wait)
    }

    fn should_abort_for_inactivity(
        self,
        elapsed: Duration,
        since_progress: Duration,
        progress_observations: usize,
    ) -> bool {
        // Before the first post-admission durable update the provider may be
        // reasoning, negotiating a large tool schema, or constructing a team.
        // Only the complexity-specific maximum bounds that phase. Once the
        // execution has emitted durable progress, a quiet period is a useful
        // outage signal and the shorter recovery threshold may apply.
        progress_observations > 0
            && elapsed >= self.initial_wait
            && since_progress >= self.inactivity_wait
    }
}

struct TerminalWait {
    message: Value,
    report: Value,
}

struct DescendantTeamWait {
    timeline: Value,
    projections: Vec<Value>,
    agentic_program: Option<runtime::AgenticProgramProjection>,
    acceptance: LiveAcceptanceResult,
    report: Value,
}

#[derive(Clone, Copy)]
enum LiveAcceptance {
    Contains(&'static str),
    RequiresToolEvidence {
        tool_name: &'static str,
        target_path: &'static str,
    },
    ArchitectureQuality {
        minimum_teams: usize,
        minimum_claimed_cross_team_edges: usize,
        evidence_profile: ArchitectureEvidenceProfile,
    },
    AutonomousCollaboration {
        minimum_teams: usize,
        minimum_agents: usize,
        minimum_tasks: usize,
        minimum_reviews: usize,
        minimum_cross_team_edges: usize,
        minimum_topics: usize,
        output_path: &'static str,
    },
    /// PLAN Phase 4 / L1: a single-facet probe gated on what that facet tests.
    ///
    /// Both facets were previously declared failed by `ArchitectureQuality` - a
    /// gate for a multi-team architecture review - even when the model had
    /// answered correctly. arm E returned three verbatim source quotes in 3
    /// rounds and Facet B answered 1125 files / 771040 lines with cited receipts,
    /// and both still failed on `architecture_quality`.
    ///
    /// A gate that cannot pass a correct answer measures nothing. A facet
    /// therefore names only what its own capability must have produced, and
    /// never requires teams, tasks, reviews or topics.
    Facet {
        /// The facet identifier, for the report.
        facet_id: &'static str,
        /// The single capability under test.
        capability: &'static str,
        /// Paths that must have a durable read receipt. Empty means "not gated
        /// on reads".
        required_read_paths: &'static [&'static str],
        /// Substrings the final answer must contain.
        answer_contains: &'static [&'static str],
        /// Whether leaving something unresolved is acceptable. For a facet whose
        /// point is honest reporting this MUST be true, or the gate punishes the
        /// behaviour it is trying to teach.
        keep_unresolved_ok: bool,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArchitectureEvidenceProfile {
    Basic,
    /// The unique dependency sink must itself reacquire the exact sources;
    /// global lineage coverage is insufficient because predecessor evidence
    /// proves handoff, not synthesis review.
    GroupTheoryFinalSynthesis,
    /// Every exact source must be independently acquired by two distinct
    /// Agent identities. Team/Agent topology remains model-selected.
    LargeScaleIndependentReview,
}

impl LiveAcceptance {
    fn requires_descendant_team_closure(self) -> bool {
        matches!(
            self,
            Self::ArchitectureQuality {
                minimum_teams: 1..,
                ..
            } | Self::AutonomousCollaboration {
                minimum_teams: 1..,
                ..
            }
        )
    }

    fn evaluate(
        self,
        response: &str,
        timeline: &Value,
        projections: &[Value],
        agentic_program: Option<&runtime::AgenticProgramProjection>,
        root_execution_id: &str,
    ) -> LiveAcceptanceResult {
        let result = match self {
            Self::Contains(expected) => LiveAcceptanceResult {
                passed: response.contains(expected),
                quality: None,
                checks: vec![
                    json!({"name": "response_contains", "expected": expected, "passed": response.contains(expected)}),
                ],
            },
            Self::RequiresToolEvidence {
                tool_name,
                target_path,
            } => {
                let tool_evidence =
                    has_successful_runtime_tool_completion(timeline, tool_name, target_path);
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty() && tool_evidence,
                    quality: None,
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "tool_evidence", "passed": tool_evidence}),
                    ],
                }
            }
            Self::Facet {
                facet_id,
                capability,
                required_read_paths,
                answer_contains,
                keep_unresolved_ok,
            } => {
                // Collect the paths that actually have a durable read receipt
                // from the timeline, so "the model claims it read X" and "the
                // model read X" stay distinguishable.
                let read_paths = durable_read_receipt_paths(timeline, projections);
                let outcome = crate::facet_gate::FacetOutcome {
                    read_paths,
                    answer: response.to_string(),
                    marked_unresolved: response_marks_unresolved(response),
                };
                let verdict = crate::facet_gate::evaluate_facet(
                    &crate::facet_gate::FacetAcceptance {
                        facet_id: facet_id.to_string(),
                        capability: capability.to_string(),
                        required_read_paths: required_read_paths
                            .iter()
                            .map(|path| (*path).to_string())
                            .collect(),
                        answer_contains: answer_contains
                            .iter()
                            .map(|needle| (*needle).to_string())
                            .collect(),
                        keep_unresolved_ok,
                    },
                    &outcome,
                );
                LiveAcceptanceResult {
                    passed: verdict.passed,
                    quality: None,
                    checks: verdict
                        .requirements
                        .iter()
                        .map(|requirement| {
                            json!({
                                "name": requirement.requirement,
                                "passed": requirement.passed,
                                "observed": requirement.observed,
                            })
                        })
                        .collect(),
                }
            }
            Self::ArchitectureQuality {
                minimum_teams,
                minimum_claimed_cross_team_edges,
                evidence_profile,
            } => {
                let team_health = agentic_program
                    .map(projected_team_health)
                    .unwrap_or_default();
                let accepted_cross_team_dependencies = agentic_program
                    .map(accepted_cross_team_dependency_count)
                    .unwrap_or_default();
                let evidence_integrity = agentic_program
                    .map(agentic_evidence_integrity)
                    .unwrap_or_default();
                let evidence_satisfied = minimum_teams == 0 || evidence_integrity.passed();
                let program_verified = minimum_teams == 0
                    || agentic_program.is_some_and(|program| {
                        program.status == runtime::AgenticProgramStatus::Verified
                            && program.root_execution_id.as_deref() == Some(root_execution_id)
                            && program.objective_verdict.is_some()
                            && program.unresolved.is_empty()
                    });
                let quality = architecture_quality(timeline, projections, agentic_program);
                let team_projection = team_health.satisfies(minimum_teams);
                let edges_satisfied =
                    accepted_cross_team_dependencies >= minimum_claimed_cross_team_edges;
                let parallel_overlaps = physical_parallel_overlap_count(projections);
                let parallel_satisfied = minimum_teams <= 1 || parallel_overlaps > 0;
                let physical_terminal = no_active_agent_waits(projections);
                let physical_failed_agents = physical_failed_agent_activity_count(projections);
                let physical_success = physical_failed_agents == 0;
                let presentation_checks = match evidence_profile {
                    ArchitectureEvidenceProfile::LargeScaleIndependentReview => {
                        large_scale_presentation_checks(response)
                    }
                    ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis => {
                        group_theory_presentation_checks(response)
                    }
                    ArchitectureEvidenceProfile::Basic => Vec::new(),
                };
                let presentation_satisfied = presentation_checks
                    .iter()
                    .all(|check| check["passed"].as_bool() == Some(true));
                let complete_source_paths =
                    complete_exact_source_receipt_paths(timeline, projections);
                let required_complete_source_paths: &[&str] = match evidence_profile {
                    ArchitectureEvidenceProfile::LargeScaleIndependentReview => {
                        &LARGE_SCALE_SOURCE_PATHS
                    }
                    ArchitectureEvidenceProfile::Basic
                    | ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis => &[],
                };
                let missing_complete_source_paths = required_complete_source_paths
                    .iter()
                    .filter(|path| !complete_source_paths.contains(**path))
                    .copied()
                    .collect::<Vec<_>>();
                let complete_source_coverage = missing_complete_source_paths.is_empty();
                let independently_reviewed_source_paths =
                    independently_reviewed_complete_source_receipt_paths(timeline, projections);
                let required_independent_source_paths: &[&str] = match evidence_profile {
                    ArchitectureEvidenceProfile::LargeScaleIndependentReview => {
                        &LARGE_SCALE_SOURCE_PATHS
                    }
                    ArchitectureEvidenceProfile::Basic
                    | ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis => &[],
                };
                let missing_independently_reviewed_source_paths = required_independent_source_paths
                    .iter()
                    .filter(|path| !independently_reviewed_source_paths.contains(**path))
                    .copied()
                    .collect::<Vec<_>>();
                let independent_source_review =
                    missing_independently_reviewed_source_paths.is_empty();
                let terminal_team_ids = agentic_program
                    .map(terminal_semantic_team_ids)
                    .unwrap_or_default();
                let terminal_team_source_paths =
                    complete_exact_source_receipt_paths_for_semantic_teams(
                        timeline,
                        projections,
                        &terminal_team_ids,
                    );
                let missing_terminal_team_source_paths =
                    if evidence_profile == ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis {
                        GROUP_THEORY_SOURCE_PATHS
                            .iter()
                            .filter(|path| !terminal_team_source_paths.contains(**path))
                            .copied()
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                let terminal_team_source_review = evidence_profile
                    != ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis
                    || (terminal_team_ids.len() == 1
                        && missing_terminal_team_source_paths.is_empty());
                let mut checks = vec![
                    json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                    json!({"name": "architecture_quality", "passed": quality.score >= quality.required, "score": quality.score, "required": quality.required, "criteria": quality.criteria}),
                    json!({"name": "agentic_program_verified", "passed": program_verified}),
                    json!({"name": "accepted_agent_first_teams", "required": minimum_teams, "passed": team_projection, "engaged_agents": team_health.agent_count, "accepted_agents": team_health.completed_agents, "failed_agents": team_health.failed_agents, "teams": team_health.team_count, "accepted_teams": team_health.completed_teams, "failed_teams": team_health.failed_teams}),
                    json!({"name": "durable_task_artifact_evidence", "required": minimum_teams > 0, "accepted_tasks": evidence_integrity.accepted_tasks, "missing_claimants": evidence_integrity.missing_claimants, "missing_independent_reviews": evidence_integrity.missing_independent_reviews, "missing_artifacts": evidence_integrity.missing_artifacts, "missing_evidence": evidence_integrity.missing_evidence, "dangling_artifact_refs": evidence_integrity.dangling_artifact_refs, "passed": evidence_satisfied}),
                    json!({"name": "accepted_cross_team_dependencies", "required": minimum_claimed_cross_team_edges, "observed": accepted_cross_team_dependencies, "passed": edges_satisfied}),
                    json!({"name": "physical_agent_concurrency", "required_overlaps": usize::from(minimum_teams > 1), "observed_overlaps": parallel_overlaps, "passed": parallel_satisfied}),
                    json!({"name": "physical_agent_waits_resolved", "passed": physical_terminal}),
                    json!({"name": "physical_agent_graphs_succeeded", "failed_activities": physical_failed_agents, "passed": physical_success}),
                    json!({"name": "runtime_attested_complete_source_coverage", "required": required_complete_source_paths.len(), "observed": complete_source_paths.len(), "missing": missing_complete_source_paths, "passed": complete_source_coverage}),
                    json!({"name": "runtime_attested_independent_source_review", "required": required_independent_source_paths.len(), "observed": independently_reviewed_source_paths.len(), "missing": missing_independently_reviewed_source_paths, "receipt_rule": "distinct exact-content receipts from two different Agent identities", "passed": independent_source_review}),
                    json!({"name": "runtime_attested_terminal_team_source_review", "required": if evidence_profile == ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis { GROUP_THEORY_SOURCE_PATHS.len() } else { 0 }, "observed": terminal_team_source_paths.len(), "terminal_semantic_team_ids": terminal_team_ids, "missing": missing_terminal_team_source_paths, "receipt_rule": "exact-content read receipt must belong to the unique sink Team Agent identity", "passed": terminal_team_source_review}),
                ];
                checks.extend(presentation_checks);
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty()
                        && quality.score >= quality.required
                        && team_projection
                        && program_verified
                        && evidence_satisfied
                        && edges_satisfied
                        && parallel_satisfied
                        && physical_terminal
                        && physical_success
                        && complete_source_coverage
                        && independent_source_review
                        && terminal_team_source_review
                        && presentation_satisfied,
                    quality: Some(quality.clone()),
                    checks,
                }
            }
            Self::AutonomousCollaboration {
                minimum_teams,
                minimum_agents,
                minimum_tasks,
                minimum_reviews,
                minimum_cross_team_edges,
                minimum_topics,
                output_path,
            } => {
                let Some(program) = agentic_program else {
                    return LiveAcceptanceResult {
                        passed: false,
                        quality: None,
                        checks: vec![json!({"name": "agentic_program_present", "passed": false})],
                    };
                };
                let health = projected_team_health(program);
                let evidence = agentic_evidence_integrity(program);
                let accepted_tasks = active_agentic_tasks(program)
                    .filter(|task| task.status == runtime::AgenticTaskStatus::Accepted)
                    .count();
                let review_count = active_agentic_tasks(program)
                    .filter(|task| task.reviewed_by.is_some())
                    .count();
                let topic_entries = program.topics.values().map(Vec::len).sum::<usize>();
                let cross_team_edges = accepted_cross_team_dependency_count(program);
                let overlaps = physical_parallel_overlap_count(projections);
                let physical_failed_agents = physical_failed_agent_activity_count(projections);
                let physical_success = physical_failed_agents == 0;
                let program_verified = program.status == runtime::AgenticProgramStatus::Verified
                    && program.root_execution_id.as_deref() == Some(root_execution_id)
                    && program.objective_verdict.is_some()
                    && program.unresolved.is_empty();
                let teams_satisfied = health.team_count >= minimum_teams
                    && health.completed_teams == health.team_count
                    && health.failed_teams == 0;
                let agents_satisfied = health.agent_count >= minimum_agents
                    && health.completed_agents == health.agent_count
                    && health.failed_agents == 0;
                let tasks_satisfied = accepted_tasks >= minimum_tasks;
                let reviews_satisfied = review_count >= minimum_reviews;
                let topics_satisfied = topic_entries >= minimum_topics;
                let edges_satisfied = cross_team_edges >= minimum_cross_team_edges;
                let final_artifact = program
                    .final_artifact_ref
                    .as_ref()
                    .and_then(|reference| program.artifacts.get(reference));
                let final_artifact_satisfied = final_artifact.is_some_and(|artifact| {
                    !artifact.content_ref.trim().is_empty()
                        && active_agentic_tasks(program).any(|task| {
                            task.status == runtime::AgenticTaskStatus::Accepted
                                && task.artifact_refs.contains(&artifact.artifact_ref)
                        })
                        && (output_path.is_empty()
                            || artifact.title.contains(output_path)
                            || artifact.content_ref.contains(output_path))
                });
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty()
                        && program_verified
                        && teams_satisfied
                        && agents_satisfied
                        && tasks_satisfied
                        && reviews_satisfied
                        && topics_satisfied
                        && evidence.passed()
                        && edges_satisfied
                        && overlaps > 0
                        && no_active_agent_waits(projections)
                        && physical_success
                        && final_artifact_satisfied,
                    quality: None,
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "agentic_program_verified", "passed": program_verified}),
                        json!({"name": "accepted_autonomous_teams", "required": minimum_teams, "observed": health.team_count, "accepted": health.completed_teams, "failed": health.failed_teams, "passed": teams_satisfied}),
                        json!({"name": "engaged_autonomous_agents", "required": minimum_agents, "observed": health.agent_count, "accepted": health.completed_agents, "failed": health.failed_agents, "passed": agents_satisfied}),
                        json!({"name": "accepted_agent_tasks", "required": minimum_tasks, "observed": accepted_tasks, "passed": tasks_satisfied}),
                        json!({"name": "independent_task_reviews", "required": minimum_reviews, "observed": review_count, "passed": reviews_satisfied}),
                        json!({"name": "durable_topic_observations", "required": minimum_topics, "observed": topic_entries, "passed": topics_satisfied}),
                        json!({"name": "durable_task_artifact_evidence", "accepted_tasks": evidence.accepted_tasks, "missing_claimants": evidence.missing_claimants, "missing_independent_reviews": evidence.missing_independent_reviews, "missing_artifacts": evidence.missing_artifacts, "missing_evidence": evidence.missing_evidence, "dangling_artifact_refs": evidence.dangling_artifact_refs, "passed": evidence.passed()}),
                        json!({"name": "accepted_cross_team_dependencies", "required": minimum_cross_team_edges, "observed": cross_team_edges, "passed": edges_satisfied}),
                        json!({"name": "physical_agent_concurrency", "required_overlaps": 1, "observed_overlaps": overlaps, "passed": overlaps > 0}),
                        json!({"name": "physical_agent_waits_resolved", "passed": no_active_agent_waits(projections)}),
                        json!({"name": "physical_agent_graphs_succeeded", "failed_activities": physical_failed_agents, "passed": physical_success}),
                        json!({"name": "durable_final_artifact", "expected_path": output_path, "final_artifact_ref": program.final_artifact_ref, "passed": final_artifact_satisfied}),
                    ],
                }
            }
        };
        finalize_live_acceptance(result, timeline, root_execution_id)
    }
}

fn finalize_live_acceptance(
    mut result: LiveAcceptanceResult,
    timeline: &Value,
    root_execution_id: &str,
) -> LiveAcceptanceResult {
    let outcome = root_business_outcome(timeline, root_execution_id);
    result.checks.push(json!({
        "name": "root_business_outcome_succeeded",
        "execution_graph_ref": root_execution_id,
        "observed_event_status": outcome.event_status,
        "observed_terminal_class": outcome.terminal_class,
        "passed": outcome.passed,
    }));
    result.passed &= outcome.passed;
    result
}

fn group_theory_presentation_checks(response: &str) -> Vec<Value> {
    let required_stages = ["研究", "调研", "分析", "处理", "模拟"];
    let paths_present = GROUP_THEORY_SOURCE_PATHS
        .iter()
        .all(|path| response.contains(path));
    let stages_present = required_stages.iter().all(|stage| response.contains(stage));
    // A read-only research scenario performs no execution. It must say so rather
    // than presenting a "simulation" as if it had actually run.
    let unexecuted_simulation_declared = ["未执行的模拟", "未执行模拟", "unexecuted simulation"]
        .iter()
        .any(|marker| response.contains(marker));
    vec![
        json!({"name": "presentation_contains_c4", "passed": response.contains("C4")}),
        json!({"name": "presentation_lists_all_required_source_paths", "required": GROUP_THEORY_SOURCE_PATHS.len(), "passed": paths_present}),
        json!({"name": "presentation_covers_research_pipeline", "required": required_stages, "passed": stages_present}),
        json!({"name": "presentation_declares_unexecuted_simulation", "passed": unexecuted_simulation_declared}),
    ]
}

fn large_scale_presentation_checks(response: &str) -> Vec<Value> {
    let trimmed = response.trim();
    let normalized = trimmed.to_ascii_lowercase();
    let source_paths = trimmed
        .split_whitespace()
        .map(|token| {
            token.trim_matches(|character: char| {
                !character.is_alphanumeric() && !matches!(character, '/' | '_' | '-' | '.')
            })
        })
        .filter(|token| token.starts_with("crates/") && token.contains(".rs"))
        .collect::<std::collections::BTreeSet<_>>();
    let transport_clean = [
        "[truncated]",
        "# Verified Team evidence bundle",
        "Runtime delivery facts:",
        "cowd.runtime.collaboration_evidence.v1",
        "team-graph:runtime-team:",
    ]
    .iter()
    .all(|marker| !trimmed.contains(marker));
    let complete_ending = trimmed
        .chars()
        .last()
        .is_some_and(|character| !character.is_alphanumeric())
        && trimmed.matches("```").count() % 2 == 0;
    let source_coverage_contradicted = [
        "源码完整覆盖维度：未通过",
        "源码完整覆盖维度:未通过",
        "不能将本次任务判定为完全通过",
        "整体任务不能判定为完全通过",
        "本次任务不能判定为完全通过",
        "仅有 4 个文件完成",
        "只有 4 个文件完成",
        "only 4 files were complete",
        "reviewer 未看到本地文件",
        "reviewer 没有看到本地文件",
        "reviewer 未独立重读",
        "reviewer 没有独立重读",
        "结构/收据级而非逐行语义级",
        "结构化收据级而非逐行语义级",
        "仅在收据层级确认",
        "只在收据层级确认",
        "收据支持、内容未",
        "内容级复核未完成",
        "内容级复核尚未完成",
        "内容级复核列入未解决",
        "正文未保留",
        "host.rs 内容未",
        "reviewer did not independently read",
        "reviewer did not see the local file",
        "content-level review was not completed",
        "content omitted",
        "body omitted",
    ]
    .iter()
    .any(|marker| normalized.contains(&marker.to_ascii_lowercase()));
    let source_coverage_declared =
        !source_coverage_contradicted && normalized.contains("12/12 目标源码已完整读取到 eof");
    let independent_source_review_declared = !source_coverage_contradicted
        && normalized.contains("12/12 目标源码已由两个不同 agent 身份独立完整读取到 eof");
    let required_concepts = [
        ("verified_facts", &["已验证事实", "verified facts"][..]),
        (
            "source_inference",
            &["源码推断", "source-grounded inference"][..],
        ),
        (
            "unexecuted_simulation",
            &["未执行的模拟", "未执行模拟", "unexecuted simulation"][..],
        ),
        ("concurrency_waves", &["并发波次", "concurrency wave"][..]),
        ("bottlenecks", &["关键瓶颈", "bottleneck"][..]),
        ("failure_modes", &["失效模式", "failure mode"][..]),
        ("capacity_boundaries", &["容量边界", "capacity bound"][..]),
    ];
    let mut checks = vec![
        json!({"name": "presentation_transport_clean", "passed": transport_clean}),
        json!({"name": "presentation_complete_ending", "passed": complete_ending}),
        json!({"name": "presentation_source_paths", "required": 6, "observed": source_paths.len(), "passed": source_paths.len() >= 6}),
        json!({"name": "presentation_complete_source_coverage", "passed": source_coverage_declared}),
        json!({"name": "presentation_independent_source_review", "passed": independent_source_review_declared}),
    ];
    checks.extend(required_concepts.into_iter().map(|(name, markers)| {
        let passed = markers
            .iter()
            .any(|marker| normalized.contains(&marker.to_ascii_lowercase()));
        json!({"name": format!("presentation_{name}"), "passed": passed})
    }));
    checks.push(json!({
        "name": "presentation_scale_recommendation",
        "passed": has_scale_recommendation(trimmed),
    }));
    checks
}

fn has_scale_recommendation(response: &str) -> bool {
    const SCALE_SUBJECTS: &[&str] = &[
        "扩大规模",
        "扩大协作规模",
        "更大规模",
        "扩容",
        "横向扩展",
        "增加 team",
        "增加 agent",
        "scale recommendation",
        "scale up",
        "scale-up",
        "scale out",
        "scale-out",
        "scalability",
        "expand collaboration",
    ];
    const SCALE_DECISIONS: &[&str] = &[
        "建议扩容",
        "建议扩大",
        "建议增加",
        "建议继续",
        "不建议",
        "适合",
        "不适合",
        "可继续",
        "可以继续",
        "暂不",
        "应先",
        "需先",
        "需要先",
        "必须先",
        "前提",
        "recommend scaling",
        "recommend scale",
        "recommend expanding",
        "do not recommend",
        "should",
        "must",
        "suitable",
        "unsuitable",
        "can continue",
        "prerequisite",
        "not ready",
    ];

    let normalized_newlines = response.replace("\r\n", "\n").replace('\r', "\n");
    let mut section_has_scale_subject = false;
    for block in normalized_newlines
        .split("\n\n")
        .map(str::trim)
        .filter(|block| !block.is_empty())
    {
        let mut body = Vec::new();
        for line in block.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') {
                let heading = trimmed.to_ascii_lowercase();
                section_has_scale_subject =
                    SCALE_SUBJECTS.iter().any(|marker| heading.contains(marker));
            } else {
                body.push(line);
            }
        }
        let body = body.join(" ").to_ascii_lowercase();
        if body.is_empty() {
            continue;
        }
        let has_subject =
            section_has_scale_subject || SCALE_SUBJECTS.iter().any(|marker| body.contains(marker));
        let has_decision = SCALE_DECISIONS.iter().any(|marker| body.contains(marker));
        if has_subject && has_decision {
            return true;
        }
    }
    false
}

fn complete_exact_source_receipt_paths(
    timeline: &Value,
    projections: &[Value],
) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    collect_complete_exact_source_receipt_paths(timeline, &mut paths);
    for projection in projections {
        collect_complete_exact_source_receipt_paths(projection, &mut paths);
    }
    paths
}

fn independently_reviewed_complete_source_receipt_paths(
    timeline: &Value,
    projections: &[Value],
) -> BTreeSet<String> {
    let mut receipt_agents = BTreeMap::<String, BTreeSet<String>>::new();
    collect_complete_exact_source_receipt_agents(timeline, &mut receipt_agents);
    for projection in projections {
        collect_complete_exact_source_receipt_agents(projection, &mut receipt_agents);
    }
    receipt_agents
        .into_iter()
        .filter_map(|(path, agents)| (agents.len() >= 2).then_some(path))
        .collect()
}

/// Resolve terminal semantic Teams from the Runtime-owned Program topology,
/// never from model-chosen labels such as "final", "review", or "Team D".
/// A terminal Team is a required instance with incoming claimed edges and no
/// outgoing edge. The group-theory gate additionally requires this set to
/// contain exactly one sink.
fn terminal_semantic_team_ids(program: &runtime::AgenticProgramProjection) -> BTreeSet<String> {
    let mut predecessor_teams = BTreeSet::new();
    let mut consumer_teams = BTreeSet::new();
    for task in active_agentic_tasks(program) {
        for dependency_id in &task.depends_on {
            let Some(dependency) = program.tasks.get(dependency_id) else {
                continue;
            };
            if dependency.team_id != task.team_id {
                predecessor_teams.insert(dependency.team_id.clone());
                consumer_teams.insert(task.team_id.clone());
            }
        }
    }
    consumer_teams
        .difference(&predecessor_teams)
        .cloned()
        .collect()
}

fn complete_exact_source_receipt_paths_for_semantic_teams(
    timeline: &Value,
    projections: &[Value],
    semantic_team_ids: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut receipt_agents = BTreeMap::<String, BTreeSet<String>>::new();
    collect_complete_exact_source_receipt_agents(timeline, &mut receipt_agents);
    for projection in projections {
        collect_complete_exact_source_receipt_agents(projection, &mut receipt_agents);
    }
    receipt_agents
        .into_iter()
        .filter_map(|(path, agents)| {
            agents
                .iter()
                .any(|identity| {
                    semantic_team_ids
                        .iter()
                        .any(|team_id| receipt_identity_belongs_to_semantic_team(identity, team_id))
                })
                .then_some(path)
        })
        .collect()
}

fn receipt_identity_belongs_to_semantic_team(identity: &str, semantic_team_id: &str) -> bool {
    identity
        .split(':')
        .any(|component| component == semantic_team_id)
}

fn collect_complete_exact_source_receipt_agents(
    value: &Value,
    receipt_agents: &mut BTreeMap<String, BTreeSet<String>>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_complete_exact_source_receipt_agents(value, receipt_agents);
            }
        }
        Value::Object(values) => {
            // Only the Agent terminal's Runtime-owned observed acceptance is
            // semantic model-observation evidence. Raw ToolHost receipts also
            // appear elsewhere in the timeline, but prove acquisition only.
            if let Some(observed) = values
                .get("observed_acceptance")
                .and_then(|acceptance| acceptance.get("observed_evidence"))
            {
                collect_exact_source_evidence_agents(observed, receipt_agents);
            }
            for (key, value) in values {
                if key != "observed_acceptance" {
                    collect_complete_exact_source_receipt_agents(value, receipt_agents);
                }
            }
        }
        _ => {}
    }
}

fn collect_exact_source_evidence_agents(
    value: &Value,
    receipt_agents: &mut BTreeMap<String, BTreeSet<String>>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_exact_source_evidence_agents(value, receipt_agents);
            }
        }
        Value::Object(values) => {
            let sequence = values
                .get("observed_at_sequence")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let scope = values.get("target").and_then(|target| target.get("scope"));
            let exact_read = sequence > 0
                && values.get("tool_name").and_then(Value::as_str) == Some("read_file")
                && scope
                    .and_then(|scope| scope.get("access_mode"))
                    .and_then(Value::as_str)
                    == Some("read")
                && scope
                    .and_then(|scope| scope.get("coverage"))
                    .and_then(Value::as_str)
                    == Some("exact_content");
            if exact_read {
                let path = scope
                    .and_then(|scope| scope.get("path"))
                    .and_then(|path| path.get("workspace_relative_path"))
                    .and_then(Value::as_str);
                let digest = scope
                    .and_then(|scope| scope.get("path"))
                    .and_then(|path| path.get("observed_revision_or_digest"))
                    .and_then(Value::as_str);
                let receipt_id = values
                    .get("evidence_ref")
                    .and_then(|reference| reference.get("evidence_ref"))
                    .and_then(|reference| reference.get("id"))
                    .and_then(Value::as_str);
                if let (Some(path), Some(digest), Some(agent_identity)) =
                    (path, digest, receipt_id.and_then(receipt_agent_identity))
                {
                    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        receipt_agents
                            .entry(path.to_string())
                            .or_default()
                            .insert(agent_identity.to_string());
                    }
                }
            }
            for value in values.values() {
                collect_exact_source_evidence_agents(value, receipt_agents);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn receipt_agent_identity(receipt_id: &str) -> Option<&str> {
    let (execution_prefix, _) = receipt_id.rsplit_once(":read_file:")?;
    let mut components = execution_prefix.rsplitn(4, ':');
    let sequence = components.next()?;
    let attempt = components.next()?;
    let slot = components.next()?;
    let identity = components.next()?;
    sequence.parse::<u64>().ok()?;
    attempt.parse::<u64>().ok()?;
    slot.parse::<u64>().ok()?;
    (!identity.trim().is_empty()).then_some(identity)
}

fn collect_complete_exact_source_receipt_paths(value: &Value, paths: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_complete_exact_source_receipt_paths(value, paths);
            }
        }
        Value::Object(values) => {
            if let Some(observed) = values
                .get("observed_acceptance")
                .and_then(|acceptance| acceptance.get("observed_evidence"))
            {
                collect_exact_source_evidence_paths(observed, paths);
            }
            for (key, value) in values {
                if key != "observed_acceptance" {
                    collect_complete_exact_source_receipt_paths(value, paths);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn collect_exact_source_evidence_paths(value: &Value, paths: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_exact_source_evidence_paths(value, paths);
            }
        }
        Value::Object(values) => {
            let sequence = values
                .get("observed_at_sequence")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let tool_name = values.get("tool_name").and_then(Value::as_str);
            let scope = values.get("target").and_then(|target| target.get("scope"));
            let exact_read = sequence > 0
                && tool_name == Some("read_file")
                && scope
                    .and_then(|scope| scope.get("access_mode"))
                    .and_then(Value::as_str)
                    == Some("read")
                && scope
                    .and_then(|scope| scope.get("coverage"))
                    .and_then(Value::as_str)
                    == Some("exact_content");
            if exact_read {
                let path = scope
                    .and_then(|scope| scope.get("path"))
                    .and_then(|path| path.get("workspace_relative_path"))
                    .and_then(Value::as_str);
                let digest = scope
                    .and_then(|scope| scope.get("path"))
                    .and_then(|path| path.get("observed_revision_or_digest"))
                    .and_then(Value::as_str);
                if let (Some(path), Some(digest)) = (path, digest) {
                    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        paths.insert(path.to_string());
                    }
                }
            }
            for value in values.values() {
                collect_exact_source_evidence_paths(value, paths);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

struct LiveAcceptanceResult {
    passed: bool,
    checks: Vec<Value>,
    quality: Option<ArchitectureQuality>,
}

impl LiveAcceptanceResult {
    fn to_value(&self) -> Value {
        json!({"passed": self.passed, "checks": self.checks, "quality": self.quality})
    }
}

#[derive(Clone, serde::Serialize)]
struct ArchitectureQuality {
    score: u64,
    required: u64,
    criteria: Vec<Value>,
}

/// Judge the architecture scenario from Runtime-owned evidence, never from
/// incidental words in a model's prose.  A model may accurately complete the
/// work in Chinese, another language, or a compact summary; requiring it to
/// spell words such as "canonical" made the evaluator reject real successful
/// runs for a presentation choice rather than a system defect.
fn architecture_quality(
    timeline: &Value,
    projections: &[Value],
    agentic_program: Option<&runtime::AgenticProgramProjection>,
) -> ArchitectureQuality {
    let checked_source_receipts = checked_source_receipt_count(timeline, projections);
    let canonical_program_projection = agentic_program
        .is_some_and(|program| !program.program_id.is_empty() && program.revision > 0)
        || projections.iter().any(|projection| {
            projection
                .get("execution_id")
                .and_then(Value::as_str)
                .is_some_and(|execution_id| !execution_id.trim().is_empty())
        });
    let durable_projection_lineage = projections.iter().any(|projection| {
        projection
            .get("revision")
            .and_then(Value::as_u64)
            .is_some_and(|revision| revision > 0)
            || projection
                .get("runtime_commit_cursor")
                .and_then(Value::as_u64)
                .is_some_and(|cursor| cursor > 0)
    });
    let criteria = [
        ("canonical_program_projection", canonical_program_projection),
        ("durable_projection_lineage", durable_projection_lineage),
        ("checked_source_receipts", checked_source_receipts >= 2),
    ]
    .into_iter()
    .map(|(name, passed)| json!({"name": name, "passed": passed}))
    .collect::<Vec<_>>();
    let score = criteria
        .iter()
        .filter(|criterion| criterion["passed"].as_bool() == Some(true))
        .count() as u64;
    ArchitectureQuality {
        score,
        required: 3,
        criteria,
    }
}

fn active_agentic_tasks(
    program: &runtime::AgenticProgramProjection,
) -> impl Iterator<Item = &runtime::AgenticTaskProjection> {
    program
        .tasks
        .values()
        .filter(|task| !task.status.is_retired())
}

#[derive(Default)]
struct AgenticEvidenceIntegrity {
    accepted_tasks: usize,
    missing_claimants: usize,
    missing_independent_reviews: usize,
    missing_artifacts: usize,
    missing_evidence: usize,
    dangling_artifact_refs: usize,
}

impl AgenticEvidenceIntegrity {
    fn passed(&self) -> bool {
        self.accepted_tasks > 0
            && self.missing_claimants == 0
            && self.missing_independent_reviews == 0
            && self.missing_artifacts == 0
            && self.missing_evidence == 0
            && self.dangling_artifact_refs == 0
    }
}

fn agentic_evidence_integrity(
    program: &runtime::AgenticProgramProjection,
) -> AgenticEvidenceIntegrity {
    let mut integrity = AgenticEvidenceIntegrity::default();
    for task in active_agentic_tasks(program)
        .filter(|task| task.status == runtime::AgenticTaskStatus::Accepted)
    {
        integrity.accepted_tasks = integrity.accepted_tasks.saturating_add(1);
        let claimant = task
            .claimant
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        let reviewer = task
            .reviewed_by
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        integrity.missing_claimants = integrity
            .missing_claimants
            .saturating_add(usize::from(claimant.is_none()));
        integrity.missing_independent_reviews = integrity
            .missing_independent_reviews
            .saturating_add(usize::from(reviewer.is_none() || reviewer == claimant));
        integrity.missing_artifacts = integrity
            .missing_artifacts
            .saturating_add(usize::from(task.artifact_refs.is_empty()));
        integrity.missing_evidence = integrity
            .missing_evidence
            .saturating_add(usize::from(task.evidence_refs.is_empty()));
        integrity.dangling_artifact_refs = integrity.dangling_artifact_refs.saturating_add(
            task.artifact_refs
                .iter()
                .filter(|reference| !program.artifacts.contains_key(*reference))
                .count(),
        );
    }
    integrity
}

fn physical_parallel_overlap_count(projections: &[Value]) -> usize {
    let activities = projections
        .iter()
        .flat_map(|projection| {
            projection
                .get("activities")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|activity| {
            let agent_id = activity.get("agent_instance_id")?.as_str()?;
            let start = activity.get("started_at_ms")?.as_u64()?;
            let end = activity.get("completed_at_ms")?.as_u64()?;
            (end > start).then(|| {
                (
                    agent_id,
                    activity.get("team_run_id").and_then(Value::as_str),
                    start,
                    end,
                )
            })
        })
        .collect::<Vec<_>>();
    let mut overlaps = 0_usize;
    for (index, (agent, team, start, end)) in activities.iter().enumerate() {
        for (other_agent, other_team, other_start, other_end) in &activities[index + 1..] {
            if agent != other_agent
                && team
                    .zip(*other_team)
                    .is_none_or(|(left, right)| left != right)
                && *start < *other_end
                && *other_start < *end
            {
                overlaps = overlaps.saturating_add(1);
            }
        }
    }
    overlaps
}

fn no_active_agent_waits(projections: &[Value]) -> bool {
    projections.iter().all(|projection| {
        projection
            .get("activities")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|activity| {
                activity
                    .get("agent_instance_id")
                    .and_then(Value::as_str)
                    .is_some()
            })
            .all(|activity| {
                matches!(
                    activity.get("status").and_then(Value::as_str),
                    Some("completed" | "failed" | "cancelled" | "canceled" | "skipped")
                )
            })
    })
}

fn physical_failed_agent_activity_count(projections: &[Value]) -> usize {
    projections
        .iter()
        .flat_map(|projection| {
            projection
                .get("activities")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter(|activity| {
            activity
                .get("agent_instance_id")
                .and_then(Value::as_str)
                .is_some()
                && matches!(
                    activity.get("status").and_then(Value::as_str),
                    Some("failed" | "cancelled" | "canceled")
                )
        })
        .count()
}

#[derive(Default)]
struct ProjectedTeamHealth {
    agent_count: usize,
    completed_agents: usize,
    failed_agents: usize,
    team_count: usize,
    completed_teams: usize,
    failed_teams: usize,
    pending_tasks: usize,
    completion_verdict_pending: bool,
}

impl ProjectedTeamHealth {
    fn satisfies(&self, minimum_teams: usize) -> bool {
        if minimum_teams == 0 {
            return true;
        }
        // Team templates may legitimately have one role. Requiring two
        // Agents per Team was an evaluator-only assumption that rejected a
        // fully completed, Runtime-attested single-role escalation Team.
        self.agent_count >= minimum_teams
            && self.completed_agents == self.agent_count
            && self.failed_agents == 0
            && self.team_count >= minimum_teams
            && self.completed_teams == self.team_count
            && self.failed_teams == 0
    }

    fn has_pending_work(&self) -> bool {
        // Pending is a Task/Program fact, not an inferred difference between
        // invited and terminal Agent counts. In particular, a Published Task
        // has no claimant yet but the Runtime can still dispatch it, while an
        // accepted Program awaiting its supervisor verdict can still close.
        self.pending_tasks > 0 || self.completion_verdict_pending
    }

    fn to_value(&self) -> Value {
        json!({
            "agents": self.agent_count,
            "completed_agents": self.completed_agents,
            "failed_agents": self.failed_agents,
            "teams": self.team_count,
            "completed_teams": self.completed_teams,
            "failed_teams": self.failed_teams,
            "pending_tasks": self.pending_tasks,
            "completion_verdict_pending": self.completion_verdict_pending,
            "has_pending_work": self.has_pending_work(),
        })
    }
}

fn projected_team_health(program: &runtime::AgenticProgramProjection) -> ProjectedTeamHealth {
    let mut accepted_agents = BTreeSet::new();
    let mut failed_agents = BTreeSet::new();
    let mut engaged_agent_ids = BTreeSet::new();
    for task in active_agentic_tasks(program) {
        let participants = task
            .claimant
            .iter()
            .chain(task.reviewed_by.iter())
            .cloned()
            .collect::<Vec<_>>();
        engaged_agent_ids.extend(participants.iter().cloned());
        match task.status {
            runtime::AgenticTaskStatus::Accepted => {
                accepted_agents.extend(participants);
            }
            runtime::AgenticTaskStatus::Blocked => {
                failed_agents.extend(participants);
            }
            runtime::AgenticTaskStatus::Published
            | runtime::AgenticTaskStatus::Claimed
            | runtime::AgenticTaskStatus::Submitted
            | runtime::AgenticTaskStatus::Rework
            | runtime::AgenticTaskStatus::CancelRequested
            | runtime::AgenticTaskStatus::Withdrawn
            | runtime::AgenticTaskStatus::Superseded => {}
        }
    }
    let engaged_agents = engaged_agent_ids
        .iter()
        .filter(|agent_id| program.agents.contains_key(*agent_id))
        .count();
    let completed_teams = program
        .teams
        .values()
        .filter(|team| {
            let tasks = team
                .task_ids
                .iter()
                .filter_map(|task_id| program.tasks.get(task_id))
                .filter(|task| !task.status.is_retired())
                .collect::<Vec<_>>();
            !tasks.is_empty()
                && tasks
                    .iter()
                    .all(|task| task.status == runtime::AgenticTaskStatus::Accepted)
        })
        .count();
    let failed_teams = program
        .teams
        .values()
        .filter(|team| {
            team.task_ids.iter().any(|task_id| {
                program
                    .tasks
                    .get(task_id)
                    .is_some_and(|task| task.status == runtime::AgenticTaskStatus::Blocked)
            })
        })
        .count();
    let pending_tasks = active_agentic_tasks(program)
        .filter(|task| {
            matches!(
                task.status,
                runtime::AgenticTaskStatus::Published
                    | runtime::AgenticTaskStatus::Claimed
                    | runtime::AgenticTaskStatus::Submitted
                    | runtime::AgenticTaskStatus::Rework
            )
        })
        .count();
    ProjectedTeamHealth {
        agent_count: engaged_agents,
        completed_agents: accepted_agents
            .iter()
            .filter(|agent_id| program.agents.contains_key(*agent_id))
            .count(),
        failed_agents: failed_agents
            .iter()
            .filter(|agent_id| program.agents.contains_key(*agent_id))
            .count(),
        team_count: program.teams.len(),
        completed_teams,
        failed_teams,
        pending_tasks,
        completion_verdict_pending: program.status
            == runtime::AgenticProgramStatus::CompletionRequested,
    }
}

fn accepted_cross_team_dependency_count(program: &runtime::AgenticProgramProjection) -> usize {
    let mut dependencies = BTreeSet::new();
    for task in active_agentic_tasks(program)
        .filter(|task| task.status == runtime::AgenticTaskStatus::Accepted)
    {
        for dependency_id in &task.depends_on {
            let Some(dependency) = program.tasks.get(dependency_id) else {
                continue;
            };
            if dependency.status == runtime::AgenticTaskStatus::Accepted
                && dependency.team_id != task.team_id
            {
                dependencies.insert((dependency.task_id.clone(), task.task_id.clone()));
            }
        }
    }
    dependencies.len()
}

fn checked_source_receipt_count(timeline: &Value, projections: &[Value]) -> usize {
    let mut receipts = BTreeSet::new();
    collect_checked_source_receipts(timeline, &mut receipts);
    for projection in projections {
        collect_checked_source_receipts(projection, &mut receipts);
    }
    receipts.len()
}

fn collect_checked_source_receipts(value: &Value, receipts: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_checked_source_receipts(value, receipts);
            }
        }
        Value::Object(values) => {
            let source_tool = values
                .get("tool_name")
                .and_then(Value::as_str)
                .is_some_and(|tool| {
                    matches!(
                        tool,
                        "read_file" | "tool_orchestrate" | "grep_search" | "grep_search"
                    )
                });
            let succeeded = values.get("is_error").and_then(Value::as_bool) == Some(false);
            if source_tool && succeeded {
                if let Some(id) = values
                    .get("evidence_id")
                    .or_else(|| values.get("tool_call_id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                {
                    receipts.insert(id.to_string());
                }
            }
            for value in values.values() {
                collect_checked_source_receipts(value, receipts);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[cfg(test)]
fn source_paths(response: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    let mut remainder = response;
    while let Some(index) = remainder.find("crates/") {
        let candidate = &remainder[index..];
        let length = candidate
            .chars()
            .take_while(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-' | '.')
            })
            .map(char::len_utf8)
            .sum();
        if length > "crates/".len() {
            let path = candidate[..length].trim_end_matches('.').to_string();
            if looks_like_workspace_file_reference(&path) {
                paths.insert(path);
            }
        }
        remainder = &candidate["crates/".len()..];
    }
    paths
}

#[cfg(test)]
fn looks_like_workspace_file_reference(path: &str) -> bool {
    matches!(
        path.rsplit_once('.').map(|(_, extension)| extension),
        Some(
            "rs" | "toml"
                | "md"
                | "json"
                | "yaml"
                | "yml"
                | "ts"
                | "tsx"
                | "vue"
                | "js"
                | "mjs"
                | "cjs"
                | "py"
                | "go"
                | "java"
                | "kt"
                | "c"
                | "h"
                | "cc"
                | "cpp"
                | "hpp"
        )
    )
}

fn collaboration_comparison(scenarios: &[Value]) -> Value {
    let find = |id| {
        scenarios
            .iter()
            .find(|scenario| scenario["scenario_id"].as_str() == Some(id))
    };
    let single = find("live_single_architecture_baseline");
    let team = find("live_team_projection");
    let single_score = single
        .and_then(|scenario| scenario.pointer("/acceptance/quality/score"))
        .and_then(Value::as_u64);
    let team_score = team
        .and_then(|scenario| scenario.pointer("/acceptance/quality/score"))
        .and_then(Value::as_u64);
    let single_wall = single
        .and_then(|scenario| scenario.pointer("/metrics/wall_ms"))
        .and_then(Value::as_u64);
    let team_wall = team
        .and_then(|scenario| scenario.pointer("/metrics/wall_ms"))
        .and_then(Value::as_u64);
    let quality_delta_pp = single_score
        .zip(team_score)
        .map(|(single, team)| (team as i64 - single as i64) * 100 / 6);
    let quality_route = quality_delta_pp.is_some_and(|delta| delta >= 10)
        && single_wall
            .zip(team_wall)
            .is_some_and(|(single, team)| team <= single.saturating_mul(110) / 100);
    let speed_route = single_wall
        .zip(team_wall)
        .is_some_and(|(single, team)| team <= single.saturating_mul(80) / 100)
        && quality_delta_pp.is_some_and(|delta| delta >= -2);
    // Durable Agent-first facts, rather than transient root metrics, are the
    // source of truth for Team participation and cross-Team dependencies.
    let team_capability_passed = team.is_some_and(|scenario| {
        scenario.get("status").and_then(Value::as_str) == Some("passed")
            && scenario
                .pointer("/acceptance/checks")
                .and_then(Value::as_array)
                .is_some_and(|checks| {
                    let teams_completed = checks.iter().any(|check| {
                        check.get("name").and_then(Value::as_str)
                            == Some("accepted_agent_first_teams")
                            && check.get("passed").and_then(Value::as_bool) == Some(true)
                            && check
                                .get("engaged_agents")
                                .and_then(Value::as_u64)
                                .is_some_and(|agents| agents >= 3)
                            && check
                                .get("teams")
                                .and_then(Value::as_u64)
                                .is_some_and(|teams| teams >= 3)
                    });
                    let merge_claimed = checks.iter().any(|check| {
                        check.get("name").and_then(Value::as_str)
                            == Some("accepted_cross_team_dependencies")
                            && check.get("passed").and_then(Value::as_bool) == Some(true)
                            && check
                                .get("observed")
                                .and_then(Value::as_u64)
                                .is_some_and(|edges| edges >= 2)
                    });
                    teams_completed && merge_claimed
                })
    });
    // The live team scenario explicitly instructs the model to start a real
    // team. It is a capability/correctness proof, not an automatic-strategy
    // benchmark: treating unavoidable user-mandated collaboration overhead as
    // a product regression would reject a correct runtime decision. Keep the
    // paired quality/speed routes as evidence, but only call efficiency proven
    // when one of their pre-registered criteria actually wins.
    let efficiency_proven = quality_route || speed_route;
    json!({
        "status": if team_capability_passed { "passed" } else { "failed" },
        "single_scenario": "live_single_architecture_baseline",
        "team_scenario": "live_team_projection",
        "single_quality_score": single_score,
        "team_quality_score": team_score,
        "quality_delta_percentage_points": quality_delta_pp,
        "single_wall_ms": single_wall,
        "team_wall_ms": team_wall,
        "quality_route": {
            "passed": quality_route,
            "requirement": "team quality improves by >=10 percentage points and critical path is no worse than 10%"
        },
        "speed_route": {
            "passed": speed_route,
            "requirement": "team critical path is >=20% shorter and quality declines by <=2 percentage points"
        },
        "team_capability": {
            "passed": team_capability_passed,
            "requirement": "the Agent-first scenario has at least three accepted Teams, three engaged Agents, and two accepted cross-Team Task dependencies"
        },
        "efficiency_proven": efficiency_proven,
        "efficiency_note": if efficiency_proven {
            "paired comparison demonstrated a pre-registered quality or critical-path advantage"
        } else {
            "paired comparison did not demonstrate an automatic-efficiency advantage; this forced-team scenario remains a capability result, not a strategy-selection endorsement"
        },
    })
}

fn response_json(response: reqwest::blocking::Response) -> Result<Value, String> {
    let status = response.status();
    let body = response.text().map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", summarize(&body, 400)));
    }
    serde_json::from_str(&body)
        .map_err(|error| format!("invalid JSON response: {error}: {}", summarize(&body, 400)))
}

fn trace_json_entry(
    method: &str,
    path: String,
    request: Value,
    response: &Result<Value, String>,
) -> Value {
    json!({
        "method": method,
        "path": path,
        "request": request,
        "response": match response {
            Ok(value) => json!({"status": "ok", "body": value}),
            Err(error) => json!({"status": "error", "error": error}),
        }
    })
}

fn bounded_projection_trace_entry(path: &str, response: &Result<Value, String>) -> Value {
    let response = match response {
        Ok(value) => {
            let canonical = serde_json::to_vec(value).unwrap_or_default();
            json!({
                "status": "ok",
                "body": {
                    "execution_id": value.get("execution_id"),
                    "revision": value.get("revision"),
                    "cursor": value.get("cursor"),
                    "detail_scope": value.get("detail_scope"),
                    "child_execution_count": value.get("child_executions").and_then(Value::as_array).map_or(0, Vec::len),
                    "graph_node_count": value.pointer("/graph/nodes").and_then(Value::as_array).map_or(0, Vec::len),
                    "autonomous_work_count": value.pointer("/graph/autonomous_work").and_then(Value::as_array).map_or(0, Vec::len),
                    "team_count": value.get("teams").and_then(Value::as_array).map_or(0, Vec::len),
                    "agent_count": value.get("agents").and_then(Value::as_array).map_or(0, Vec::len),
                    "canonical_bytes": canonical.len(),
                    "canonical_sha256": format!("{:x}", Sha256::digest(&canonical)),
                }
            })
        }
        Err(error) => json!({"status": "error", "error": error}),
    };
    json!({
        "method": "GET",
        "path": path,
        "request": Value::Null,
        "response": response,
    })
}

fn failed_scenario(
    spec: LiveScenarioSpec,
    started: Instant,
    trace: Vec<Value>,
    error: String,
) -> Value {
    failed_scenario_with_session(spec, started, trace, String::new(), error, Value::Null)
}

fn failed_scenario_with_session(
    spec: LiveScenarioSpec,
    started: Instant,
    trace: Vec<Value>,
    session_id: String,
    error: String,
    diagnostics: Value,
) -> Value {
    failed_scenario_with_session_and_execution(
        spec,
        started,
        trace,
        session_id,
        None,
        error,
        diagnostics,
    )
}

fn failed_scenario_with_session_and_execution(
    spec: LiveScenarioSpec,
    started: Instant,
    trace: Vec<Value>,
    session_id: String,
    execution_id: Option<String>,
    error: String,
    diagnostics: Value,
) -> Value {
    json!({
        "scenario_id": spec.id,
        "status": "failed",
        "session_id": if session_id.is_empty() { Value::Null } else { Value::String(session_id) },
        "execution_id": execution_id,
        "elapsed_ms": started.elapsed().as_millis(),
        "error": error,
        "failure_diagnostics": diagnostics,
        "trace": trace,
        "production_trace": Value::Null,
    })
}

fn message_text(message: &Value) -> String {
    for key in ["blocks", "content", "text", "response", "content_json"] {
        if let Some(value) = message.get(key) {
            if let Some(text) = value.as_str() {
                if key == "content_json" {
                    if let Ok(parts) = serde_json::from_str::<Value>(text) {
                        if let Some(text) = find_string_by_key(&parts, &["text"]) {
                            return text;
                        }
                    }
                }
                return text.to_string();
            }
            if let Some(text) = find_string_by_key(value, &["text"]) {
                return text;
            }
        }
    }
    String::new()
}

fn find_string_by_key(value: &Value, keys: &[&str]) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(value) = map
                    .get(*key)
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                {
                    return Some(value.to_string());
                }
            }
            map.values()
                .find_map(|value| find_string_by_key(value, keys))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|value| find_string_by_key(value, keys)),
        _ => None,
    }
}

fn find_u64_by_key(value: &Value, keys: &[&str]) -> Option<u64> {
    match value {
        Value::Object(map) => {
            let own = keys
                .iter()
                .filter_map(|key| map.get(*key).and_then(Value::as_u64))
                .max();
            map.values()
                .filter_map(|value| find_u64_by_key(value, keys))
                .fold(own, |current, value| {
                    Some(current.map_or(value, |known| known.max(value)))
                })
        }
        Value::Array(values) => values
            .iter()
            .filter_map(|value| find_u64_by_key(value, keys))
            .max(),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct RootBusinessOutcome {
    passed: bool,
    event_status: Option<String>,
    terminal_class: Option<String>,
}

/// Bind acceptance to the canonical Runtime outcome for the root execution
/// graph. A completed execution graph only proves lifecycle closure: the
/// business outcome may still be partial or failed.
fn root_business_outcome(timeline: &Value, root_execution_id: &str) -> RootBusinessOutcome {
    let outcome = timeline
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .rev()
        .find(|event| {
            event.get("kind").and_then(Value::as_str) == Some("runtime.outcome.recorded.v1")
                && event
                    .pointer("/payload/identity/execution_graph_ref")
                    .and_then(Value::as_str)
                    == Some(root_execution_id)
        });
    let Some(outcome) = outcome else {
        return RootBusinessOutcome::default();
    };
    let event_status = outcome
        .get("status")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let terminal_class = outcome
        .pointer("/payload/terminal/class")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    RootBusinessOutcome {
        passed: event_status.as_deref() == Some("succeeded")
            && terminal_class.as_deref() == Some("succeeded"),
        event_status,
        terminal_class,
    }
}

/// Successful tool evidence is a canonical Runtime completion receipt. It is
/// deliberately not a recursive key search: provider capability metadata and
/// zero-valued usage summaries are declarations, not executed effects.
/// PLAN L1: the paths that actually got a durable successful read receipt.
///
/// The extraction lives in `facet_gate::collect_read_paths` so the gate and its
/// tests use the same code; this exists to keep the call site readable.
fn durable_read_receipt_paths(timeline: &Value, projections: &[Value]) -> Vec<String> {
    crate::facet_gate::collect_read_paths(timeline, projections)
}

/// PLAN L1: whether the final answer explicitly keeps something unresolved.
///
/// Honest reporting is a behaviour the gate must reward, so this has to be
/// detected rather than assumed absent.
pub(crate) fn response_marks_unresolved(response: &str) -> bool {
    let lower = response.to_lowercase();
    const MARKERS: [&str; 8] = [
        "unresolved",
        "无法从现有证据",
        "无法确定",
        "未能确定",
        "无法确认",
        "未能确认",
        "无法判断",
        "保留",
    ];
    MARKERS.iter().any(|marker| lower.contains(marker))
}

fn has_successful_runtime_tool_completion(
    timeline: &Value,
    expected_tool_name: &str,
    expected_target_path: &str,
) -> bool {
    // Reading via a batch is still reading the same file, and the model may
    // pass an absolute path. Accept equivalent tools and path forms so the
    // acceptance checks the *action*, not a presentation choice.
    let accepts_tool = |name: &str| match expected_tool_name {
        "read_file" => matches!(name, "read_file" | "tool_orchestrate"),
        other => name == other,
    };
    let target = expected_target_path.trim_start_matches("./");
    let accepts_path = |path: &str| {
        let path = path.trim_start_matches("./");
        path == target || path.ends_with(&format!("/{target}"))
    };
    timeline
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("tool.invocation.completed")
                && event.get("status").and_then(Value::as_str) == Some("completed")
                && event.pointer("/payload/status").and_then(Value::as_str) == Some("completed")
                && event
                    .pointer("/payload/tool_name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| accepts_tool(name))
                && event.pointer("/payload/is_error").and_then(Value::as_bool) == Some(false)
                && event
                    .pointer("/payload/input_preview")
                    .and_then(Value::as_str)
                    .and_then(|input| serde_json::from_str::<Value>(input).ok())
                    .is_some_and(|input| {
                        crate::facet_gate::read_paths_from_tool_input(&input)
                            .iter()
                            .any(|path| accepts_path(path))
                    })
                && ["invocation_id", "tool_call_id"].iter().all(|key| {
                    event
                        .get("payload")
                        .and_then(|payload| payload.get(*key))
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.trim().is_empty())
                })
        })
}

fn summarize(value: &str, max_chars: usize) -> String {
    let mut summary = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        summary.push_str("...");
    }
    summary
}

fn env_duration_secs(key: &str) -> Option<Duration> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_secs)
}

fn env_duration_millis(key: &str, default: Duration) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

#[cfg(test)]
#[path = "live_scenario_runner/tests.rs"]
mod tests;

#[cfg(test)]
mod tool_call_count_tests {
    use super::*;

    /// Evidence must not contradict itself: a run that captured tool calls must
    /// not report zero of them. The local provider reports no tool-call count at
    /// all, so before this the metric read zero while the same run wrote
    /// `tool-call-1.json` through `tool-call-3.json`.
    #[test]
    fn captured_tool_calls_are_counted_even_when_the_provider_reports_none() {
        let timeline = serde_json::json!({
            "tool_call_details": [
                {"summary": {"call_index": 1}, "input": {}, "output": {}},
                {"summary": {"call_index": 2}, "input": {}, "output": {}}
            ]
        });
        assert_eq!(
            scenario_tool_calls(&ScenarioTokenUsage::default(), &timeline),
            2,
            "the captured calls are the run's own record of them"
        );
    }

    /// Golden fixture for the counting definition, taken from the recorded live run
    /// `v0.9.786-1791291862-mission-harness-deep`.
    ///
    /// That run held 497 event payloads carrying a tool name, exactly 95 of which were
    /// terminal, and the durable store agreed with 95 while the report read 3. Three
    /// numbers, three record paths. The definition that reconciles them is "terminal
    /// statuses only": a `running` payload is the same call as its terminal payload, so
    /// counting both inflates the number by the number of in-flight records.
    ///
    /// This fixture exists so the definition cannot be changed by changing a test.
    #[test]
    fn golden_fixture_the_recorded_run_counts_95_terminal_not_497_payloads() {
        let mut events = Vec::new();
        for index in 0..402 {
            events.push(serde_json::json!({
                "payload": {"tool_name": "task_review", "status": "running", "input_hash": format!("r{index}")}
            }));
        }
        for index in 0..95 {
            events.push(serde_json::json!({
                "payload": {"tool_name": "task_review", "status": "failed", "input_hash": format!("f{index}")}
            }));
        }
        assert_eq!(
            crate::report::terminal_tool_events_in_events(&events),
            95,
            "the durable store agreed with 95; the definition must agree with it"
        );
        let timeline = serde_json::json!({ "events": events });
        assert_eq!(
            scenario_tool_calls(&ScenarioTokenUsage::default(), &timeline),
            95,
            "the metric and the gate must read the same function"
        );
    }

    /// Inverted from the characterization in 1006i: the fourth source is the Gateway
    /// event payload, and it counts terminal statuses only.
    ///
    /// The characterization recorded that none of the three sources saw a Gateway event
    /// payload. That was true and is now closed - the same evidence that made the
    /// characterization true is what the fourth source reads.
    #[test]
    fn gateway_terminal_events_are_counted_and_running_events_are_not() {
        let timeline = serde_json::json!({
            "events": [
                {"payload": {"tool_name": "task_review", "status": "failed", "input_hash": "a"}},
                {"payload": {"tool_name": "task_review", "status": "failed", "input_hash": "b"}},
                {"payload": {"tool_name": "artifact_publish", "status": "completed"}},
                {"payload": {"tool_name": "artifact_publish", "status": "running"}},
                {"payload": {"tool_name": "task_review", "status": "refused"}}
            ]
        });
        assert_eq!(
            scenario_tool_calls(&ScenarioTokenUsage::default(), &timeline),
            4,
            "terminal statuses count; `running` is the same call as its terminal event"
        );
    }
}

#[cfg(test)]
mod scenario_tool_advertisement_tests {
    use std::collections::BTreeSet;

    /// The scenario prompts used to hand-write their tool allow-list, and that
    /// list had drifted to advertise names the runtime had already removed
    /// (`read_many`, `glob_many`, ...) - the same class of hand-written list that
    /// caused the V1 regression, and one that could mask a real "tool not
    /// reachable" failure. This pins every advertised name to the live catalog.
    #[test]
    fn scenario_prompts_only_advertise_tools_the_runtime_exposes() {
        let catalog = tools::mvp_tool_specs()
            .into_iter()
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();
        // Runtime-owned read-only tools that live outside the built-in catalog.
        let runtime_readonly = ["read_ref", "context_retrieve", "evidence_retrieve"];
        let source = include_str!("live_scenario_runner.rs");
        // Escaped so this test's own source does not contain the literal it scans for.
        let anchor = "\u{53ef}\u{7528}\u{5de5}\u{5177}\u{ff1a}";
        let mut checked = 0_usize;
        for line in source.lines() {
            let Some(rest) = line.split(anchor).nth(1) else {
                continue;
            };
            let list = rest.split('。').next().unwrap_or("");
            for name in list.split('、').map(str::trim).filter(|name| !name.is_empty()) {
                assert!(
                    catalog.contains(name) || runtime_readonly.contains(&name),
                    "a scenario prompt advertises `{name}`, which the runtime does not expose"
                );
                checked += 1;
            }
        }
        assert!(checked >= 7, "expected to validate the scenario tool lists, checked {checked}");
    }
}
