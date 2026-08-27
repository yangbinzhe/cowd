use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
};
use serde_json::{json, Value};

use crate::{session_actor::SessionActor, HarnessEvalRunnerOptions};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_DEFAULT_SCENARIO_TIMEOUT: Duration = Duration::from_secs(600);

/// Run production-path scenarios against an explicitly supplied, isolated
/// Gateway. This runner never constructs Runtime objects or fakes receipts:
/// every result is derived from public Gateway responses and durable messages.
pub fn run_live_gateway_scenarios(options: &HarnessEvalRunnerOptions) -> Value {
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
    };
    runner.run()
}

struct LiveScenarioRunner {
    client: Client,
    base_url: String,
    timeout_cap: Option<Duration>,
    poll_interval: Duration,
    model: Option<String>,
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
    fn run(&self) -> Value {
        let health_observations = [
            ("gateway", "/healthz"),
            ("runtime", "/api/runtime/status"),
            ("runtime_outbox", "/api/runtime/outbox"),
            ("runtime_control_plane", "/api/runtime/control-plane"),
            ("evolution_projector", "/api/evolution/signals"),
            ("surface_host", "/api/surfaces/health"),
        ]
        .into_iter()
        .map(|(id, path)| {
            let observed = self.get_json(path);
            (
                id.to_string(),
                match observed {
                    Ok(response) => json!({
                        "status": "passed",
                        "path": path,
                        "response": response,
                    }),
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
        let scenarios = [
            LiveScenarioSpec {
                id: "live_direct_terminal",
                prompt: "只回答 7 乘以 8 的结果。不要调用工具，不要组队。",
                acceptance: LiveAcceptance::Contains("56"),
                timeout: LiveScenarioTimeout::direct(),
            },
            LiveScenarioSpec {
                id: "live_tool_evidence",
                prompt: "请读取当前工作区的 Cargo.toml，给出 workspace package version 和文件路径。必须通过只读工具取得证据，不要猜测。",
                acceptance: LiveAcceptance::RequiresToolEvidence,
                timeout: LiveScenarioTimeout::tool(),
            },
            LiveScenarioSpec {
                id: "live_single_architecture_baseline",
                prompt: "请单独完成一次复杂架构审查，不要启动团队：分别分析 runtime、memory、gateway 的职责边界、各自的 canonical state 或事件真相、一个潜在风险，并给出至少三个完整的 `crates/.../*.rs` 源码路径作为证据。只陈述本次实际读取到源码所能验证的结论；不要加入“无法确认/无法判断/未确认/需要进一步检查”之类的保留项。只能使用 read_file、read_many、glob_search、glob_many、grep_search、grep_many、workspace_snapshot 这些只读工具，不要调用 bash 或任何写工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 0,
                    minimum_claimed_cross_team_edges: 0,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: "live_team_projection",
                prompt: "这是复杂架构审查：必须实际启动三个协作 Team，不可用一个 Team 或模型文本替代。Team A 独立审查 runtime，Team B 独立审查 memory 与 gateway；两者可并行。Team C 必须在收到 A 和 B 的经过授权的证据/摘要后，汇合并审查跨组件边界，再综合最终结论。不得在 A/B 的事实交接完成前启动 Team C 的实质审查。最终结论必须字面列出至少三个完整的 `crates/.../*.rs` 源码路径（不能只写文件名），只陈述各 Team 实际读取到源码所能验证的结论；不要加入“无法确认/无法判断/未确认/需要进一步检查”之类的保留项。只能使用 read_file、read_many、glob_search、glob_many、grep_search、grep_many、workspace_snapshot 这些只读工具；不要调用 bash 或任何写工具。",
                acceptance: LiveAcceptance::ArchitectureQuality {
                    minimum_teams: 3,
                    minimum_claimed_cross_team_edges: 2,
                },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: "live_agent_escalation",
                prompt: "这是一次受控协作升级验收。初始 Program 合同**恰好只有两个** required Team obligation：Team A 审查 runtime 的 durable Program/edge 事实，Team B 审查 gateway 的受管 Agent 工具边界；两者可并行。初始 `runtime_orchestrate` proposal 绝不可包含额外 Team、reviewer、aggregator 或预先规划的 follow-up。每个 semantic node 都必须显式给出 `managed_agent_escalation` 枚举：仅 Team A 填 `required`，Team B 填 `none`；这是 Runtime 持久化的受管升级义务，不是目标文本提示。Team A 被 Runtime 选定的受管 Agent 在读取到第一批源码证据后的安全检查点，必须实际调用 `request_collaboration_escalation` 申请一个独立复核工作流；只有该 Runtime-attested 工具调用可以使 Program 增加后续 Team。不可用模型文本替代该调用；不要猜测或提供 Program revision/digest，Runtime 会从已绑定父 Program 派生它们。最终结论必须字面列出至少三个完整的 `crates/.../*.rs` 源码路径，只陈述实际读取到的证据。只能使用 read_file、read_many、glob_search、glob_many、grep_search、grep_many、workspace_snapshot 和 request_collaboration_escalation；不要调用 bash 或任何写工具。",
                acceptance: LiveAcceptance::EscalatedTeam {
                    minimum_teams: 3,
                    minimum_escalations: 1,
                },
                timeout: LiveScenarioTimeout::team(),
            },
        ]
        .into_iter()
        .map(|spec| self.run_scenario(spec))
        .collect::<Vec<_>>();
        let passed = scenarios
            .iter()
            .filter(|scenario| scenario.get("status").and_then(Value::as_str) == Some("passed"))
            .count();
        let collaboration_comparison = collaboration_comparison(&scenarios);
        let comparison_passed = collaboration_comparison
            .get("status")
            .and_then(Value::as_str)
            == Some("passed");
        let metrics = aggregate_scenario_metrics(&scenarios);
        json!({
            "kind": "harness_eval.live_gateway_scenarios",
            "status": if health_passed && passed == scenarios.len() && comparison_passed { "passed" } else { "failed" },
            "gateway_url": self.base_url,
            "model": self.model,
            "timeout_cap_ms": self.timeout_cap.map(|value| value.as_millis()),
            "poll_interval_ms": self.poll_interval.as_millis(),
            "health_status": if health_passed { "passed" } else { "failed" },
            "health_observations": health_observations,
            "scenario_count": scenarios.len(),
            "passed": passed,
            "failed": scenarios.len().saturating_sub(passed),
            "metrics": metrics,
            "scenarios": scenarios,
            "collaboration_comparison": collaboration_comparison,
        })
    }

    fn run_scenario(&self, spec: LiveScenarioSpec) -> Value {
        let started = Instant::now();
        let mut trace = Vec::new();
        let timeout = spec.timeout.with_cap(self.timeout_cap);
        let actor = SessionActor::create(
            &self.client,
            &self.base_url,
            self.model.as_deref(),
            "harness-eval-live",
        );
        let Ok(mut actor) = actor else {
            return failed_scenario(spec, started, trace, actor.err().unwrap_or_default());
        };
        trace.extend(actor.drain_trace());
        let session_id = actor.session_id().to_string();
        let admission = actor.post_mutation(
            &format!("/api/sessions/{session_id}/messages"),
            json!({
                "content": spec.prompt,
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
        let Some(execution_id_ref) = execution_id.as_deref() else {
            return failed_scenario_with_session(
                spec,
                started,
                trace,
                session_id,
                format!(
                    "message admission lacks canonical execution.graph_id: {}",
                    summarize_json(&admission)
                ),
                Value::Null,
            );
        };

        let terminal =
            self.wait_for_terminal_message(&session_id, execution_id_ref, &timeout, &mut trace);
        let Ok(terminal) = terminal else {
            let mut diagnostics =
                self.capture_diagnostics(&session_id, Some(execution_id_ref), &mut trace);
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
                terminal.err().unwrap_or_default(),
                diagnostics,
            );
        };
        let terminal_wait = terminal;
        let response_text = message_text(&terminal_wait.message);
        let descendant_wait = self.wait_for_descendant_team_acceptance(
            spec.acceptance,
            &response_text,
            &session_id,
            execution_id_ref,
            started,
            &timeout,
            &mut trace,
        );
        let timeline = descendant_wait.timeline;
        let projections = descendant_wait.projections;
        let mut acceptance = descendant_wait.acceptance;
        let terminal_id = terminal_wait
            .message
            .get("id")
            .or_else(|| terminal_wait.message.get("message_id"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let commit_cursor = find_u64_by_key(&timeline, &["commit_cursor", "runtime_commit_cursor"]);
        let metrics = scenario_metrics(&timeline, &projections, started.elapsed());
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
        let cleanup = actor.finish().map_or_else(
            |error| json!({"status":"failed","error":error}),
            |_| json!({"status":"passed"}),
        );
        trace.extend(actor.drain_trace());
        json!({
            "scenario_id": spec.id,
            "status": if acceptance.passed { "passed" } else { "failed" },
            "acceptance": acceptance.to_value(),
            "session_id": session_id,
            "execution_id": execution_id,
            "terminal_id": terminal_id,
            "terminal_response_summary": summarize(&response_text, 320),
            "runtime_commit_cursor": commit_cursor,
            "elapsed_ms": started.elapsed().as_millis(),
            "metrics": metrics,
            "timeout": {
                "root_terminal_wait": terminal_wait.report,
                "descendant_team_wait": descendant_wait.report,
            },
            "session_actor_cleanup": cleanup,
            "trace": trace,
            "production_trace": {
                "session_id": session_id,
                "execution_id": execution_id,
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
        trace: &mut Vec<Value>,
    ) -> Result<TerminalWait, String> {
        let started = Instant::now();
        let mut progress_observations = 0_usize;
        let mut last_progress_at = started;
        let mut last_message_fingerprint = None;
        let mut last_root_fingerprint = None;
        loop {
            let path = format!("/api/sessions/{session_id}/messages?limit=200");
            let response = self.get_json(&path);
            trace.push(trace_json_entry("GET", path, Value::Null, &response));
            if let Ok(value) = response {
                let messages = value
                    .as_array()
                    .cloned()
                    .or_else(|| value.get("messages").and_then(Value::as_array).cloned())
                    .unwrap_or_default();
                let fingerprint = messages
                    .iter()
                    .map(|message| {
                        message
                            .get("id")
                            .or_else(|| message.get("message_id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join(":");
                match last_message_fingerprint.as_deref() {
                    // The first observation is only the submitted user input.
                    // It is not execution progress and must not make a slow
                    // first provider response eligible for the short
                    // inactivity window.
                    None => last_message_fingerprint = Some(fingerprint),
                    Some(previous) if previous != fingerprint => {
                        last_message_fingerprint = Some(fingerprint);
                        progress_observations = progress_observations.saturating_add(1);
                        last_progress_at = Instant::now();
                    }
                    Some(_) => {}
                }
                let root = self.root_execution_observation(root_execution_id, trace);
                if let Ok(observation) = root.as_ref() {
                    match last_root_fingerprint.as_deref() {
                        // As with the submitted user message, the initial root
                        // snapshot establishes a baseline; it is not proof of
                        // useful provider progress.
                        None => last_root_fingerprint = Some(observation.fingerprint.clone()),
                        Some(previous) if previous != observation.fingerprint => {
                            last_root_fingerprint = Some(observation.fingerprint.clone());
                            progress_observations = progress_observations.saturating_add(1);
                            last_progress_at = Instant::now();
                        }
                        Some(_) => {}
                    }
                    if let RootExecutionTerminal::Failed(reason) = &observation.terminal {
                        return Err(reason.clone());
                    }
                }
                if let Some(message) = messages.into_iter().rev().find(|message| {
                    message.get("role").and_then(Value::as_str) == Some("assistant")
                        && !message_text(message).trim().is_empty()
                }) {
                    match root {
                        Ok(RootExecutionObservation {
                            terminal: RootExecutionTerminal::Completed,
                            ..
                        }) => {
                            return Ok(TerminalWait {
                                message,
                                report: timeout.report(
                                    started.elapsed(),
                                    last_progress_at.elapsed(),
                                    progress_observations,
                                    "root_execution_terminal_and_message",
                                ),
                            });
                        }
                        Ok(RootExecutionObservation {
                            terminal: RootExecutionTerminal::Failed(reason),
                            ..
                        }) => return Err(reason),
                        Ok(RootExecutionObservation {
                            terminal: RootExecutionTerminal::Pending,
                            ..
                        })
                        | Err(_) => {}
                    }
                }
            }
            let elapsed = started.elapsed();
            let since_progress = last_progress_at.elapsed();
            if elapsed >= timeout.max_wait {
                return Err(format!(
                    "timed out after {}ms waiting for a durable assistant message; maximum scenario wait={}ms, progress_observations={progress_observations}",
                    elapsed.as_millis(),
                    timeout.max_wait.as_millis(),
                ));
            }
            if timeout.should_abort_for_inactivity(elapsed, since_progress, progress_observations) {
                return Err(format!(
                    "no durable execution progress for {}ms after {}ms; inactivity window={}ms, maximum scenario wait={}ms, progress_observations={progress_observations}",
                    since_progress.as_millis(),
                    elapsed.as_millis(),
                    timeout.inactivity_wait.as_millis(),
                    timeout.max_wait.as_millis(),
                ));
            }
            thread::sleep(self.poll_interval);
        }
    }

    /// A delegated AgentTask shares the parent session's durable message
    /// store. Its intermediate assistant response is useful progress, but it
    /// is not the parent turn's answer. Only the root ingress graph's own
    /// completed synthesis closes a live scenario.
    fn root_execution_observation(
        &self,
        execution_id: &str,
        trace: &mut Vec<Value>,
    ) -> Result<RootExecutionObservation, String> {
        let path = format!("/api/runtime/executions/{execution_id}");
        let response = self.get_json(&path);
        match response {
            Ok(projection) => {
                let terminal = root_execution_terminal_state(&projection);
                let statuses = root_node_statuses(&projection);
                let fingerprint = root_progress_fingerprint(&projection, &statuses);
                let live = projection.get("live").unwrap_or(&Value::Null);
                trace.push(json!({
                    "method": "GET",
                    "path": path,
                    "request": Value::Null,
                    "response": {
                        "status": "ok",
                        "body": {
                            "execution_id": projection.get("execution_id"),
                            "revision": projection.get("revision"),
                            "terminal_state": terminal.as_str(),
                            "node_statuses": statuses,
                            "live_revision": live.get("revision"),
                            "live_status": live.get("status"),
                            "live_output_bytes": live.get("output_bytes"),
                            "live_last_progress_at_ms": live.get("last_progress_at_ms"),
                        }
                    }
                }));
                Ok(RootExecutionObservation {
                    terminal,
                    fingerprint,
                })
            }
            Err(error) => {
                trace.push(json!({
                    "method": "GET",
                    "path": path,
                    "request": Value::Null,
                    "response": {"status": "error", "error": error},
                }));
                Err(error)
            }
        }
    }

    fn capture_diagnostics(
        &self,
        session_id: &str,
        execution_id: Option<&str>,
        trace: &mut Vec<Value>,
    ) -> Value {
        let timeline_path = format!("/api/runtime/timeline?session_id={session_id}&limit=500");
        let timeline = self.get_json(&timeline_path);
        trace.push(trace_json_entry(
            "GET",
            timeline_path,
            Value::Null,
            &timeline,
        ));
        let projection = execution_id.map(|id| {
            let path = format!("/api/runtime/executions/{id}?detail_scope=full");
            let response = self.get_json(&path);
            trace.push(trace_json_entry("GET", path, Value::Null, &response));
            response.unwrap_or_else(|error| json!({"error": error}))
        });
        json!({
            "timeline": timeline.unwrap_or_else(|error| json!({"error": error})),
            "projection": projection.unwrap_or(Value::Null),
        })
    }

    fn execution_lineage_projections(
        &self,
        root_execution_id: &str,
        trace: &mut Vec<Value>,
    ) -> Vec<Value> {
        let mut pending = vec![root_execution_id.to_string()];
        let mut visited = BTreeSet::new();
        let mut projections = Vec::new();
        while let Some(execution_id) = pending.pop() {
            if !visited.insert(execution_id.clone()) {
                continue;
            }
            let path = format!("/api/runtime/executions/{execution_id}?detail_scope=full");
            let response = self.get_json(&path);
            trace.push(trace_json_entry("GET", path, Value::Null, &response));
            let Ok(projection) = response else {
                continue;
            };
            if let Some(children) = projection.get("child_executions").and_then(Value::as_array) {
                pending.extend(children.iter().filter_map(|child| {
                    child
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(ToString::to_string)
                }));
            }
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
        root_execution_id: &str,
        scenario_started: Instant,
        timeout: &LiveScenarioTimeout,
        trace: &mut Vec<Value>,
    ) -> DescendantTeamWait {
        let wait_started = Instant::now();
        let mut observations = 0_usize;
        loop {
            let timeline_path = format!("/api/runtime/timeline?session_id={session_id}&limit=500");
            let timeline_response = self.get_json(&timeline_path);
            trace.push(trace_json_entry(
                "GET",
                timeline_path,
                Value::Null,
                &timeline_response,
            ));
            let timeline = timeline_response.unwrap_or_else(|error| json!({"error": error}));
            // The public projection makes child execution lineage explicit. A
            // session ingress graph often delegates provider/tool/team work to
            // descendants, so reporting only the root would incorrectly claim
            // zero model rounds and zero token/tool usage for a real execution.
            let projections = self.execution_lineage_projections(root_execution_id, trace);
            let result = acceptance.evaluate(response_text, &timeline, &projections);
            observations = observations.saturating_add(1);

            if !acceptance.requires_descendant_team_closure() || result.passed {
                return DescendantTeamWait {
                    timeline,
                    projections,
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

            let health = projected_team_health(&projections);
            if !health.has_pending_work() {
                return DescendantTeamWait {
                    timeline,
                    projections,
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

            if scenario_started.elapsed() >= timeout.max_wait {
                return DescendantTeamWait {
                    timeline,
                    projections,
                    acceptance: result,
                    report: json!({
                        "required": true,
                        "elapsed_ms": wait_started.elapsed().as_millis(),
                        "observations": observations,
                        "terminal_reason": "scenario_max_wait_elapsed_while_team_descendants_running",
                        "team_health": health.to_value(),
                    }),
                };
            }
            thread::sleep(self.poll_interval);
        }
    }

    /// Evaluation timeouts must not leave a real graph running after its
    /// report has already declared failure. Cancel descendants first, then
    /// the root through the same revision-checked public command surface used
    /// by TUI/WebUI. Cleanup receipts stay in the raw trace for audit.
    fn cancel_execution_lineage(
        &self,
        root_execution_id: &str,
        actor: &mut SessionActor<'_>,
        trace: &mut Vec<Value>,
    ) -> Value {
        let projections = self.execution_lineage_projections(root_execution_id, trace);
        let mut receipts = Vec::new();
        for projection in projections.into_iter().rev() {
            let Some(execution_id) = projection.get("execution_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(revision) = projection.get("revision").and_then(Value::as_u64) else {
                continue;
            };
            let path = format!("/api/runtime/executions/{execution_id}/commands");
            let request = json!({
                "command_id": format!("live-eval-cleanup-{}", uuid::Uuid::new_v4()),
                "expected_revision": revision,
                "command": "cancel",
                "payload": {"reason": "isolated live evaluation timed out; canceling owned execution"},
            });
            let response = actor.post_control_mutation(&path, request);
            trace.extend(actor.drain_trace());
            receipts.push(json!({
                "execution_id": execution_id,
                "expected_revision": revision,
                "response": response,
            }));
        }
        json!({"attempted": receipts.len(), "receipts": receipts})
    }

    fn get_json(&self, path: &str) -> Result<Value, String> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .map_err(|error| error.to_string())?;
        response_json(response)
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

fn scenario_metrics(timeline: &Value, projections: &[Value], elapsed: Duration) -> Value {
    let graph_usage = execution_graph_usage_metrics(projections);
    let timeline_usage = token_usage_metrics(timeline);
    let usage = if graph_usage.record_count > 0 {
        graph_usage
    } else {
        timeline_usage
    };
    let input_tokens = usage.input_tokens;
    let output_tokens = usage.output_tokens;
    let cache_tokens = usage.cache_tokens;
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
    let tool_calls = usage.tool_calls.max(timeline_tool_calls);
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
    json!({
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_tokens": cache_tokens,
        "total_tokens": input_tokens.saturating_add(output_tokens).saturating_add(cache_tokens),
        "token_usage_records": usage.record_count,
        "model_rounds": model_rounds,
        "effective_models": usage.models.into_iter().collect::<Vec<_>>(),
        "tool_calls": tool_calls,
        "agent_count": agents.len(),
        "team_count": teams.len(),
        "wall_ms": elapsed_ms,
        "first_token_latency_ms": first_token_latency_ms,
        "wall_tokens_per_second": wall_tokens_per_second.or(output_tokens_per_second),
        "active_tokens_per_second": active_tokens_per_second,
    })
}

#[derive(Default)]
struct ScenarioTokenUsage {
    models: BTreeSet<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_tokens: u64,
    tool_calls: u64,
    model_rounds: u64,
    record_count: u64,
}

/// Summarize node-level usage across the canonical root and all of its
/// durable child projections. `ExecutionNodeProjection::usage` is the only
/// metric source here; no report-time token estimation is allowed.
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
            let node_usage = node.get("usage").unwrap_or(&Value::Null);
            if let Some(model) = node_usage.get("model").and_then(Value::as_str) {
                if !model.trim().is_empty() {
                    usage.models.insert(model.to_string());
                }
            }
            let input_tokens = value_u64(node_usage, &["input_tokens"]);
            let output_tokens = value_u64(node_usage, &["output_tokens"]);
            let cache_tokens = value_u64(node_usage, &["cached_tokens"]);
            let tool_calls = value_u64(node_usage, &["tool_calls"]);
            if input_tokens > 0 || output_tokens > 0 || cache_tokens > 0 || tool_calls > 0 {
                usage.record_count = usage.record_count.saturating_add(1);
            }
            usage.input_tokens = usage.input_tokens.saturating_add(input_tokens);
            usage.output_tokens = usage.output_tokens.saturating_add(output_tokens);
            usage.cache_tokens = usage.cache_tokens.saturating_add(cache_tokens);
            usage.tool_calls = usage.tool_calls.saturating_add(tool_calls);
            if node.get("kind").and_then(Value::as_str) == Some("inline_model")
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
            usage.cache_tokens = usage.cache_tokens.saturating_add(value_u64(
                record,
                &[
                    "cache_create",
                    "cache_read",
                    "cache_create_tokens",
                    "cache_read_tokens",
                ],
            ));
            usage.tool_calls = usage
                .tool_calls
                .saturating_add(value_u64(record, &["tool_calls"]));
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
/// therefore cannot use it as a business-finalization deadline. A durable
/// progress observation resets only the inactivity window; the bounded maximum
/// protects the isolated test process from a provider outage or hung Gateway.
#[derive(Clone, Copy)]
struct LiveScenarioTimeout {
    initial_wait: Duration,
    inactivity_wait: Duration,
    max_wait: Duration,
}

impl LiveScenarioTimeout {
    const fn direct() -> Self {
        Self {
            initial_wait: Duration::from_secs(45),
            inactivity_wait: Duration::from_secs(45),
            max_wait: Duration::from_secs(120),
        }
    }

    const fn tool() -> Self {
        Self {
            initial_wait: Duration::from_secs(90),
            inactivity_wait: Duration::from_secs(75),
            max_wait: Duration::from_secs(300),
        }
    }

    const fn team() -> Self {
        Self {
            // A team can have several active provider/agent subgraphs whose
            // work is not visible as a root revision until a reduction or
            // handoff commits. These values govern only the isolated evaluator
            // process; the Runtime retains its own provider-progress policy.
            initial_wait: Duration::from_secs(240),
            inactivity_wait: Duration::from_secs(300),
            max_wait: Duration::from_secs(900),
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
            max_wait: self.max_wait.min(cap),
        }
    }

    fn report(
        self,
        elapsed: Duration,
        since_progress: Duration,
        progress_observations: usize,
        terminal_reason: &str,
    ) -> Value {
        json!({
            "initial_wait_ms": self.initial_wait.as_millis(),
            "inactivity_wait_ms": self.inactivity_wait.as_millis(),
            "max_wait_ms": self.max_wait.as_millis(),
            "elapsed_ms": elapsed.as_millis(),
            "since_last_progress_ms": since_progress.as_millis(),
            "progress_observations": progress_observations,
            "terminal_reason": terminal_reason,
        })
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
    acceptance: LiveAcceptanceResult,
    report: Value,
}

#[derive(Clone, Copy)]
enum LiveAcceptance {
    Contains(&'static str),
    RequiresToolEvidence,
    ArchitectureQuality {
        minimum_teams: usize,
        minimum_claimed_cross_team_edges: usize,
    },
    EscalatedTeam {
        minimum_teams: usize,
        minimum_escalations: usize,
    },
}

impl LiveAcceptance {
    fn requires_descendant_team_closure(self) -> bool {
        matches!(
            self,
            Self::ArchitectureQuality {
                minimum_teams: 1..,
                ..
            } | Self::EscalatedTeam {
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
    ) -> LiveAcceptanceResult {
        let projection = projections.first().unwrap_or(&Value::Null);
        match self {
            Self::Contains(expected) => LiveAcceptanceResult {
                passed: response.contains(expected),
                quality: None,
                checks: vec![
                    json!({"name": "response_contains", "expected": expected, "passed": response.contains(expected)}),
                ],
            },
            Self::RequiresToolEvidence => {
                let tool_evidence = contains_key_with_nonempty_value(
                    timeline,
                    &["tool_name", "tool_call_id", "tool_calls"],
                ) || contains_key_with_nonempty_value(
                    projection,
                    &["tool_name", "tool_call_id", "tool_calls"],
                );
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty() && tool_evidence,
                    quality: None,
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "tool_evidence", "passed": tool_evidence}),
                    ],
                }
            }
            Self::ArchitectureQuality {
                minimum_teams,
                minimum_claimed_cross_team_edges,
            } => {
                let team_health = projected_team_health(projections);
                let claimed_cross_team_edges = claimed_cross_team_edge_count(projections);
                let quality = architecture_quality(timeline, projections);
                let team_projection = team_health.satisfies(minimum_teams);
                let edges_satisfied = claimed_cross_team_edges >= minimum_claimed_cross_team_edges;
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty()
                        && quality.score >= quality.required
                        && team_projection
                        && edges_satisfied,
                    quality: Some(quality.clone()),
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "architecture_quality", "passed": quality.score >= quality.required, "score": quality.score, "required": quality.required, "criteria": quality.criteria}),
                        json!({"name": "completed_evidence_team", "required": minimum_teams, "passed": team_projection, "agents": team_health.agent_count, "completed_agents": team_health.completed_agents, "failed_agents": team_health.failed_agents, "teams": team_health.team_count, "completed_teams": team_health.completed_teams, "failed_teams": team_health.failed_teams}),
                        json!({"name": "claimed_cross_team_edges", "required": minimum_claimed_cross_team_edges, "observed": claimed_cross_team_edges, "passed": edges_satisfied}),
                    ],
                }
            }
            Self::EscalatedTeam {
                minimum_teams,
                minimum_escalations,
            } => {
                let team_health = projected_team_health(projections);
                let escalation_count = applied_escalation_count(projections);
                let teams_satisfied = team_health.satisfies(minimum_teams);
                let escalations_satisfied = escalation_count >= minimum_escalations;
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty() && teams_satisfied && escalations_satisfied,
                    quality: None,
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "completed_escalated_teams", "required": minimum_teams, "passed": teams_satisfied, "agents": team_health.agent_count, "completed_agents": team_health.completed_agents, "failed_agents": team_health.failed_agents, "teams": team_health.team_count, "completed_teams": team_health.completed_teams, "failed_teams": team_health.failed_teams}),
                        json!({"name": "runtime_attested_agent_escalation", "required": minimum_escalations, "observed": escalation_count, "passed": escalations_satisfied}),
                    ],
                }
            }
        }
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
fn architecture_quality(timeline: &Value, projections: &[Value]) -> ArchitectureQuality {
    let checked_source_receipts = checked_source_receipt_count(timeline, projections);
    let canonical_program_projection = projections.iter().any(|projection| {
        projection
            .pointer("/graph/graph_id")
            .and_then(Value::as_str)
            .is_some_and(|graph_id| !graph_id.trim().is_empty())
            && (projection
                .pointer("/graph/orchestration/collaboration_program")
                .is_some_and(Value::is_object)
                || projection
                    .pointer("/graph/nodes")
                    .and_then(Value::as_array)
                    .is_some_and(|nodes| !nodes.is_empty()))
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

#[derive(Default)]
struct ProjectedTeamHealth {
    agent_count: usize,
    completed_agents: usize,
    failed_agents: usize,
    team_count: usize,
    completed_teams: usize,
    failed_teams: usize,
}

impl ProjectedTeamHealth {
    fn satisfies(&self, minimum_teams: usize) -> bool {
        if minimum_teams == 0 {
            return true;
        }
        self.agent_count >= minimum_teams.saturating_mul(2)
            && self.completed_agents == self.agent_count
            && self.failed_agents == 0
            && self.team_count >= minimum_teams
            && self.completed_teams == self.team_count
            && self.failed_teams == 0
    }

    fn has_pending_work(&self) -> bool {
        self.completed_agents.saturating_add(self.failed_agents) < self.agent_count
            || self.completed_teams.saturating_add(self.failed_teams) < self.team_count
    }

    fn to_value(&self) -> Value {
        json!({
            "agents": self.agent_count,
            "completed_agents": self.completed_agents,
            "failed_agents": self.failed_agents,
            "teams": self.team_count,
            "completed_teams": self.completed_teams,
            "failed_teams": self.failed_teams,
            "has_pending_work": self.has_pending_work(),
        })
    }
}

fn projected_team_health(projections: &[Value]) -> ProjectedTeamHealth {
    // A public root projection exposes the Team boundary while the child Team
    // graph owns its Agent task displays. Assess the complete public lineage,
    // rather than treating the root's intentionally agent-free projection as
    // evidence that no managed Agents ran.
    let mut teams = BTreeMap::<String, Value>::new();
    let mut agents = BTreeMap::<String, String>::new();
    for projection in projections {
        for agent in projection
            .get("agents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = agent
                .get("id")
                .or_else(|| agent.get("agent_id"))
                .or_else(|| agent.get("run_id"))
                .and_then(Value::as_str)
                .unwrap_or("unidentified-agent");
            let status = projected_status(agent).unwrap_or("unknown");
            agents.insert(id.to_string(), status.to_string());
        }
        for team in projection
            .get("teams")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = team
                .get("id")
                .or_else(|| team.pointer("/detail/team_id"))
                .and_then(Value::as_str)
                .unwrap_or("unidentified-team")
                .to_string();
            let candidate_task_count = team
                .pointer("/detail/tasks")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let existing_task_count = teams
                .get(&id)
                .and_then(|existing| existing.pointer("/detail/tasks"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if candidate_task_count >= existing_task_count {
                teams.insert(id, team.clone());
            }
        }
    }
    for team in teams.values() {
        for task in team
            .pointer("/detail/tasks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = task
                .get("run_id")
                .or_else(|| task.get("node_id"))
                .or_else(|| task.get("agent_id"))
                .and_then(Value::as_str)
                .unwrap_or("unidentified-team-task");
            let status = projected_status(task).unwrap_or("unknown");
            agents.insert(id.to_string(), status.to_string());
        }
    }
    let completed_agents = agents
        .values()
        .filter(|status| status.as_str() == "completed")
        .count();
    let failed_agents = agents
        .values()
        .filter(|status| projected_status_name_is_failure(status))
        .count();
    let completed_teams = teams
        .values()
        .filter(|team| {
            matches!(
                projected_status(team),
                Some("completed" | "terminal" | "passed")
            )
        })
        .count();
    let failed_teams = teams
        .values()
        .filter(|team| projected_status_is_failure(team))
        .count();
    ProjectedTeamHealth {
        agent_count: agents.len(),
        completed_agents,
        failed_agents,
        team_count: teams.len(),
        completed_teams,
        failed_teams,
    }
}

/// Count only fully claimed typed Program edges. A delivered edge still leaves
/// its consumer unauthorised to run, so it is not evidence of a real merge.
/// The set key keeps repeated lineage projections from inflating the result.
fn claimed_cross_team_edge_count(projections: &[Value]) -> usize {
    let mut claimed = BTreeSet::new();
    for projection in projections {
        let graph_id = projection
            .pointer("/graph/graph_id")
            .and_then(Value::as_str)
            .unwrap_or("unidentified-graph");
        for edge in projection
            .pointer("/graph/orchestration/collaboration_program/edges")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if edge.get("state").and_then(Value::as_str) == Some("claimed")
                && edge.get("delivery_receipt").is_some_and(Value::is_object)
                && edge.get("claim_receipt").is_some_and(Value::is_object)
            {
                let edge_id = edge
                    .get("edge_id")
                    .or_else(|| edge.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("unidentified-edge");
                claimed.insert(format!("{graph_id}:{edge_id}"));
            }
        }
    }
    claimed.len()
}

/// Count only applied, Runtime-attested escalation receipts. A model's tool
/// request or a generic patch is not sufficient: the receipt must be recorded
/// on the durable root Program projection after the fenced graph revision wins.
fn applied_escalation_count(projections: &[Value]) -> usize {
    let mut escalations = BTreeSet::new();
    for projection in projections {
        let graph_id = projection
            .pointer("/graph/graph_id")
            .and_then(Value::as_str)
            .unwrap_or("unidentified-graph");
        for escalation in projection
            .pointer("/graph/orchestration/collaboration_escalations")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(escalation_id) = escalation.get("escalation_id").and_then(Value::as_str)
            else {
                continue;
            };
            if escalation
                .get("applied_graph_revision")
                .and_then(Value::as_u64)
                .is_some_and(|revision| revision > 0)
            {
                escalations.insert(format!("{graph_id}:{escalation_id}"));
            }
        }
    }
    escalations.len()
}

fn projected_status(value: &Value) -> Option<&str> {
    value
        .get("status")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/detail/status").and_then(Value::as_str))
}

fn projected_status_is_failure(value: &Value) -> bool {
    projected_status(value).is_some_and(projected_status_name_is_failure)
}

/// `partial` is a durable terminal outcome, but it does not satisfy a Team's
/// required completion contract. Treat it like every other unsuccessful
/// terminal status so the evaluator reports the real contract gap promptly
/// instead of polling a graph that can no longer make progress.
fn projected_status_name_is_failure(status: &str) -> bool {
    matches!(
        status,
        "partial"
            | "blocked"
            | "failed"
            | "cancelled"
            | "canceled"
            | "skipped"
            | "timed_out"
            | "timeout"
            | "unavailable"
            | "error"
    )
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
                        "read_file" | "read_many" | "grep_search" | "grep_many"
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
    // Root scenario metrics intentionally describe the root graph only. Team
    // work runs in child graphs, so the durable acceptance checks, rather than
    // root metrics, are the source of truth for Team and merge evidence.
    let team_capability_passed = team.is_some_and(|scenario| {
        scenario.get("status").and_then(Value::as_str) == Some("passed")
            && scenario
                .pointer("/acceptance/checks")
                .and_then(Value::as_array)
                .is_some_and(|checks| {
                    let teams_completed = checks.iter().any(|check| {
                        check.get("name").and_then(Value::as_str) == Some("completed_evidence_team")
                            && check.get("passed").and_then(Value::as_bool) == Some(true)
                            && check
                                .get("agents")
                                .and_then(Value::as_u64)
                                .is_some_and(|agents| agents >= 6)
                            && check
                                .get("teams")
                                .and_then(Value::as_u64)
                                .is_some_and(|teams| teams >= 3)
                    });
                    let merge_claimed = checks.iter().any(|check| {
                        check.get("name").and_then(Value::as_str)
                            == Some("claimed_cross_team_edges")
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
            "requirement": "the explicit-team scenario has three completed Teams, at least six completed Agents, and two claimed typed cross-Team edges into its merge"
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

fn contains_key_with_nonempty_value(value: &Value, keys: &[&str]) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, value)| {
            (keys.contains(&key.as_str()) && is_material_evidence_value(value))
                || contains_key_with_nonempty_value(value, keys)
        }),
        Value::Array(values) => values
            .iter()
            .any(|value| contains_key_with_nonempty_value(value, keys)),
        _ => false,
    }
}

/// A schema field being present is not evidence of a tool call. In
/// particular, Gateway projections commonly contain `tool_calls: 0`; treating
/// that as non-empty makes a model's unsupported prose claim pass a live
/// evaluation. Only concrete identifiers, non-empty collections, positive
/// counts, or explicit `true` values satisfy an evidence check.
fn is_material_evidence_value(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => {
            value.as_u64().is_some_and(|count| count > 0)
                || value.as_i64().is_some_and(|count| count > 0)
                || value.as_f64().is_some_and(|count| count > 0.0)
        }
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
    }
}

fn summarize_json(value: &Value) -> String {
    summarize(&value.to_string(), 500)
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
mod tests {
    use super::*;

    #[test]
    fn root_terminal_requires_completed_synthesis_not_child_progress() {
        let pending = json!({
            "graph": {"nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed"},
                {"node_id": "tools", "kind": "tool_batch", "status": "running"}
            ]}
        });
        assert_eq!(
            root_execution_terminal_state(&pending),
            RootExecutionTerminal::Pending
        );

        let completed = json!({
            "graph": {"nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed"},
                {"node_id": "synthesis", "kind": "synthesize", "status": "completed"}
            ]}
        });
        assert_eq!(
            root_execution_terminal_state(&completed),
            RootExecutionTerminal::Completed
        );
    }

    #[test]
    fn root_terminal_reports_terminal_failure_without_synthesis() {
        let failed = json!({
            "graph": {"nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "failed"}
            ]}
        });
        assert!(matches!(
            root_execution_terminal_state(&failed),
            RootExecutionTerminal::Failed(_)
        ));
    }

    #[test]
    fn root_progress_fingerprint_tracks_streaming_output_without_graph_changes() {
        let first = json!({
            "revision": 7,
            "graph": {"nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "running"}
            ]},
            "live": {
                "revision": 11,
                "status": "calling_model",
                "output_bytes": 1024,
                "last_progress_at_ms": 100
            }
        });
        let second = json!({
            "revision": 7,
            "graph": {"nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "running"}
            ]},
            "live": {
                "revision": 12,
                "status": "calling_model",
                "output_bytes": 2048,
                "last_progress_at_ms": 200
            }
        });
        let first_statuses = root_node_statuses(&first);
        let second_statuses = root_node_statuses(&second);

        assert_eq!(first_statuses, second_statuses);
        assert_ne!(
            root_progress_fingerprint(&first, &first_statuses),
            root_progress_fingerprint(&second, &second_statuses)
        );
    }

    #[test]
    fn team_acceptance_does_not_pass_without_a_real_projection_team_or_agents() {
        let answer =
            "runtime memory gateway event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
        let receipts = json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]});
        let result = LiveAcceptance::ArchitectureQuality {
            minimum_teams: 1,
            minimum_claimed_cross_team_edges: 0,
        }
        .evaluate(answer, &receipts, &[json!({"agents": [], "teams": []})]);
        assert!(!result.passed);
        let result = LiveAcceptance::ArchitectureQuality {
            minimum_teams: 1,
            minimum_claimed_cross_team_edges: 0,
        }
        .evaluate(
            answer,
            &receipts,
            &[json!({
                "revision": 1,
                "agents": [
                    {"id": "agent-1", "status": "completed"},
                    {"id": "agent-2", "status": "completed"},
                    {"id": "agent-3", "status": "completed"}
                ],
                "teams": [{"id": "team-1", "status": "completed"}],
                "graph": {
                    "graph_id": "root",
                    "orchestration": {"collaboration_program": {"edges": []}}
                }
            })],
        );
        assert!(result.passed);
    }

    #[test]
    fn architecture_acceptance_rejects_failed_team_even_when_prose_claims_evidence() {
        let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs；但无法确认，因为没有任何文件内容的读取证据";
        let result = LiveAcceptance::ArchitectureQuality {
            minimum_teams: 1,
            minimum_claimed_cross_team_edges: 0,
        }
        .evaluate(
            answer,
            &json!({"evidence": [
                {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
                {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
            ]}),
            &[json!({
                "revision": 1,
                "agents": [
                    {"status": "completed"},
                    {"status": "failed"},
                    {"status": "blocked"}
                ],
                "teams": [{"status": "failed"}],
                "graph": {
                    "graph_id": "root",
                    "orchestration": {"collaboration_program": {"edges": []}}
                }
            })],
        );
        assert!(!result.passed);
    }

    #[test]
    fn partial_team_is_terminal_unsuccessful_not_pending_work() {
        let health = projected_team_health(&[json!({
            "agents": [{"id": "agent-1", "status": "completed"}],
            "teams": [{"id": "team-1", "status": "partial"}],
        })]);

        assert_eq!(health.failed_teams, 1);
        assert!(!health.has_pending_work());
        assert!(!health.satisfies(1));
    }

    #[test]
    fn architecture_quality_uses_durable_runtime_evidence_not_response_language() {
        let quality = architecture_quality(
            &json!({"evidence": [
                {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
                {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
            ]}),
            &[json!({
                "revision": 3,
                "graph": {
                    "graph_id": "root",
                    "orchestration": {"collaboration_program": {"edges": []}}
                }
            })],
        );
        assert_eq!(quality.score, quality.required);
    }

    #[test]
    fn projected_team_health_uses_child_team_task_displays() {
        let root = json!({
            "execution_id": "root",
            "agents": [],
            "teams": [{"id": "team-1", "status": "completed"}],
        });
        let child = json!({
            "execution_id": "team-graph:team-1",
            "teams": [{
                "id": "team-1",
                "status": "completed",
                "detail": {"tasks": [
                    {"run_id": "researcher-1", "status": "completed"},
                    {"run_id": "researcher-2", "status": "completed"},
                    {"run_id": "researcher-3", "status": "completed"},
                    {"run_id": "synthesizer-1", "status": "completed"}
                ]}
            }],
        });

        let health = projected_team_health(&[root, child]);

        assert!(health.satisfies(1));
        assert_eq!(health.team_count, 1);
        assert_eq!(health.completed_teams, 1);
        assert_eq!(health.agent_count, 4);
        assert_eq!(health.completed_agents, 4);
    }

    #[test]
    fn team_acceptance_waits_for_running_descendant_work() {
        let health = projected_team_health(&[json!({
            "agents": [
                {"id": "agent-complete", "status": "completed"},
                {"id": "agent-running", "status": "running"}
            ],
            "teams": [
                {"id": "team-complete", "status": "completed"},
                {"id": "team-running", "status": "running"}
            ]
        })]);

        assert!(health.has_pending_work());
        assert!(LiveAcceptance::ArchitectureQuality {
            minimum_teams: 1,
            minimum_claimed_cross_team_edges: 0,
        }
        .requires_descendant_team_closure());
        assert!(LiveAcceptance::EscalatedTeam {
            minimum_teams: 3,
            minimum_escalations: 1,
        }
        .requires_descendant_team_closure());
        assert!(!LiveAcceptance::ArchitectureQuality {
            minimum_teams: 0,
            minimum_claimed_cross_team_edges: 0,
        }
        .requires_descendant_team_closure());
    }

    #[test]
    fn architecture_acceptance_requires_claimed_fan_in_for_multi_team_merge() {
        let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
        let receipts = json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]});
        let projection = json!({
            "revision": 1,
            "agents": [
                {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
                {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
                {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
            ],
            "teams": [
                {"id": "team-a", "status": "completed"},
                {"id": "team-b", "status": "completed"},
                {"id": "team-c", "status": "completed"}
            ],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {"edges": [
                    {"edge_id": "a-to-c", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}},
                    {"edge_id": "b-to-c", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}}
                ]}}
            }
        });

        assert_eq!(claimed_cross_team_edge_count(&[projection.clone()]), 2);
        let result = LiveAcceptance::ArchitectureQuality {
            minimum_teams: 3,
            minimum_claimed_cross_team_edges: 2,
        }
        .evaluate(answer, &receipts, &[projection]);

        assert!(result.passed);
    }

    #[test]
    fn escalation_acceptance_requires_a_durable_applied_agent_receipt() {
        let projection = json!({
            "agents": [
                {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
                {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
                {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
            ],
            "teams": [
                {"id": "team-a", "status": "completed"},
                {"id": "team-b", "status": "completed"},
                {"id": "team-c", "status": "completed"}
            ],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_escalations": [
                    {"escalation_id": "attested-add-team", "applied_graph_revision": 4}
                ]}
            }
        });

        assert_eq!(applied_escalation_count(&[projection.clone()]), 1);
        let result = LiveAcceptance::EscalatedTeam {
            minimum_teams: 3,
            minimum_escalations: 1,
        }
        .evaluate(
            "completed with durable evidence",
            &Value::Null,
            &[projection],
        );

        assert!(result.passed);
    }

    #[test]
    fn escalation_acceptance_rejects_unapplied_or_missing_receipts() {
        let projection = json!({
            "agents": [
                {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
                {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
                {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
            ],
            "teams": [
                {"id": "team-a", "status": "completed"},
                {"id": "team-b", "status": "completed"},
                {"id": "team-c", "status": "completed"}
            ],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_escalations": [
                    {"escalation_id": "uncommitted-add-team", "applied_graph_revision": 0}
                ]}
            }
        });

        let result = LiveAcceptance::EscalatedTeam {
            minimum_teams: 3,
            minimum_escalations: 1,
        }
        .evaluate(
            "completed with durable evidence",
            &Value::Null,
            &[projection],
        );

        assert!(!result.passed);
    }

    #[test]
    fn architecture_acceptance_does_not_reject_durable_execution_for_hallucinated_paths_in_prose() {
        let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/not-a-real-module/src/memory.rs";
        let result = LiveAcceptance::ArchitectureQuality {
            minimum_teams: 0,
            minimum_claimed_cross_team_edges: 0,
        }
        .evaluate(
            answer,
            &json!({"evidence": [
                {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
                {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
            ]}),
            &[json!({
                "revision": 1,
                "agents": [],
                "teams": [],
                "graph": {
                    "graph_id": "root",
                    "orchestration": {"collaboration_program": {"edges": []}}
                }
            })],
        );
        assert!(result.passed);
    }

    #[test]
    fn source_path_extraction_stops_at_cjk_punctuation_before_explanation() {
        let paths = source_paths(
            "证据：`crates/runtime/src/lib.rs`：模块注释说明职责；另见 crates/memory/src/lib.rs。",
        );
        assert_eq!(
            paths,
            BTreeSet::from([
                "crates/memory/src/lib.rs".to_string(),
                "crates/runtime/src/lib.rs".to_string(),
            ])
        );
    }

    #[test]
    fn tool_acceptance_rejects_answer_without_runtime_evidence() {
        let result = LiveAcceptance::RequiresToolEvidence.evaluate(
            "Cargo.toml",
            &json!({"events": []}),
            &[],
        );
        assert!(!result.passed);
        let result = LiveAcceptance::RequiresToolEvidence.evaluate(
            "Cargo.toml",
            &json!({"events": [{"tool_name": "workspace.read"}]}),
            &[],
        );
        assert!(result.passed);
    }

    #[test]
    fn zero_tool_count_is_not_live_tool_evidence() {
        let result = LiveAcceptance::RequiresToolEvidence.evaluate(
            "I read Cargo.toml",
            &json!({"events": [{"tool_calls": 0}]}),
            &[json!({"usage": [{"detail": {"tool_calls": 0}}]})],
        );
        assert!(
            !result.passed,
            "a declared but zero tool count must never validate a claimed tool run"
        );
    }

    #[test]
    fn scenario_metrics_sum_only_canonical_token_usage_records() {
        let timeline = json!({
            "token_speed": {
                "token_usage": [
                    {"input": 10, "output": 5, "cache_create": 2, "cache_read": 3},
                    {"input": 7, "output": 11, "cache_create": 0, "cache_read": 4}
                ],
                "model_telemetry": {
                    "first_token_latency_ms": 125,
                    "wall_tokens_per_second": 42.5,
                    "active_tokens_per_second": 56.0
                }
            },
            "tool_summary": {"count": 2},
            "team_session": {"runtime_run_count": 2}
        });
        let metrics = scenario_metrics(
            &timeline,
            &[json!({"agents": [{"id":"agent"}], "teams": [{"id":"team"}]})],
            Duration::from_secs(2),
        );

        assert_eq!(metrics["input_tokens"], 17);
        assert_eq!(metrics["output_tokens"], 16);
        assert_eq!(metrics["cache_tokens"], 9);
        assert_eq!(metrics["total_tokens"], 42);
        assert_eq!(metrics["token_usage_records"], 2);
        assert_eq!(metrics["tool_calls"], 2);
        assert_eq!(metrics["model_rounds"], 2);
        assert_eq!(metrics["first_token_latency_ms"], 125);
        assert_eq!(metrics["wall_tokens_per_second"], 42.5);
    }

    #[test]
    fn scenario_metrics_aggregate_deduplicated_root_and_child_graph_usage() {
        let root = json!({
            "graph": {
                "graph_id": "root",
                "nodes": [
                    {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 0}},
                    {"node_id": "tool", "kind": "tool_batch", "status": "completed", "usage": {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "tool_calls": 1}}
                ]
            }
        });
        let child = json!({
            "graph": {
                "graph_id": "child",
                "nodes": [
                    {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 13, "output_tokens": 5, "cached_tokens": 1, "tool_calls": 0}}
                ]
            }
        });
        let metrics = scenario_metrics(
            &json!({"token_speed": {"token_usage": []}}),
            &[root.clone(), child, root],
            Duration::from_secs(2),
        );

        assert_eq!(metrics["input_tokens"], 34);
        assert_eq!(metrics["output_tokens"], 13);
        assert_eq!(metrics["cache_tokens"], 4);
        assert_eq!(metrics["tool_calls"], 1);
        assert_eq!(metrics["model_rounds"], 2);
        assert_eq!(metrics["token_usage_records"], 3);
        assert_eq!(metrics["effective_models"], json!(["deepseek-v4-flash"]));
    }

    #[test]
    fn live_metric_summary_uses_observed_scenario_values() {
        let metrics = aggregate_scenario_metrics(&[
            json!({"metrics": {
                "input_tokens": 10,
                "output_tokens": 2,
                "cache_tokens": 3,
                "total_tokens": 15,
                "model_rounds": 1,
                "tool_calls": 0,
                "agent_count": 0,
                "team_count": 0,
                "wall_ms": 100,
                "first_token_latency_ms": 40
            }}),
            json!({"metrics": {
                "input_tokens": 20,
                "output_tokens": 5,
                "cache_tokens": 0,
                "total_tokens": 25,
                "model_rounds": 2,
                "tool_calls": 3,
                "agent_count": 4,
                "team_count": 1,
                "wall_ms": 300,
                "first_token_latency_ms": 80
            }}),
        ]);

        assert_eq!(metrics["total_tokens"], 40);
        assert_eq!(metrics["model_rounds"], 3);
        assert_eq!(metrics["tool_calls"], 3);
        assert_eq!(metrics["max_agent_count"], 4);
        assert_eq!(metrics["max_team_count"], 1);
        assert_eq!(metrics["wall_ms"]["p95"], 300);
        assert_eq!(metrics["first_token_latency_ms"]["min"], 40);
    }

    #[test]
    fn collaboration_comparison_uses_public_child_team_evidence_not_root_metrics() {
        let comparison = collaboration_comparison(&[
            json!({
                "scenario_id": "live_single_architecture_baseline",
                "metrics": {"wall_ms": 100},
                "acceptance": {"quality": {"score": 9}}
            }),
            json!({
                "scenario_id": "live_team_projection",
                "status": "passed",
                // Root graph only: the actual Team Agents run in a child
                // graph and are represented by the public acceptance check.
                "metrics": {"agent_count": 0, "wall_ms": 200},
                "acceptance": {
                    "quality": {"score": 9},
                    "checks": [{
                        "name": "completed_evidence_team",
                        "passed": true,
                        "agents": 6,
                        "completed_agents": 6,
                        "teams": 3,
                        "completed_teams": 3
                    }, {
                        "name": "claimed_cross_team_edges",
                        "passed": true,
                        "observed": 2
                    }]
                }
            }),
        ]);

        assert_eq!(comparison["status"], "passed");
        assert_eq!(comparison["team_capability"]["passed"], true);
    }

    #[test]
    fn live_timeout_is_complexity_aware_and_not_default_capped() {
        let direct = LiveScenarioTimeout::direct().with_cap(None);
        let team = LiveScenarioTimeout::team().with_cap(None);
        assert!(team.max_wait > direct.max_wait);

        let capped = team.with_cap(Some(Duration::from_secs(300)));
        assert_eq!(capped.max_wait, Duration::from_secs(300));
        assert_eq!(capped.inactivity_wait, Duration::from_secs(300));

        // An accidentally tiny operator cap cannot make the team scenario
        // fail before it has had one normal progress window.
        assert_eq!(
            team.with_cap(Some(Duration::from_secs(30))).max_wait,
            team.max_wait
        );
    }

    #[test]
    fn first_provider_response_uses_the_full_complexity_deadline() {
        let team = LiveScenarioTimeout::team();
        assert!(
            !team.should_abort_for_inactivity(
                Duration::from_secs(181),
                Duration::from_secs(181),
                0,
            ),
            "a submitted user message is not provider progress"
        );
        assert!(team.should_abort_for_inactivity(
            Duration::from_secs(241),
            Duration::from_secs(301),
            1,
        ));
    }
}
