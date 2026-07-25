use std::{
    collections::BTreeSet,
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
        let health = self.get_json("/healthz");
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
                prompt: "请单独完成一次复杂架构审查，不要启动团队：分别分析 runtime、memory、gateway 的职责边界、各自的 canonical state 或事件真相、一个潜在风险，并给出至少两个实际源码路径作为证据。",
                acceptance: LiveAcceptance::ArchitectureQuality { require_team: false },
                timeout: LiveScenarioTimeout::team(),
            },
            LiveScenarioSpec {
                id: "live_team_projection",
                prompt: "这是复杂架构审查：必须自主选择并实际启动合适的协作团队，分别分析 runtime、memory、gateway 的职责边界、各自的 canonical state 或事件真相、一个潜在风险，再综合为一份带至少两个实际源码路径证据的结论。",
                acceptance: LiveAcceptance::ArchitectureQuality { require_team: true },
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
        json!({
            "kind": "harness_eval.live_gateway_scenarios",
            "status": if passed == scenarios.len() && comparison_passed { "passed" } else { "failed" },
            "gateway_url": self.base_url,
            "model": self.model,
            "timeout_cap_ms": self.timeout_cap.map(|value| value.as_millis()),
            "poll_interval_ms": self.poll_interval.as_millis(),
            "gateway_health": health,
            "scenario_count": scenarios.len(),
            "passed": passed,
            "failed": scenarios.len().saturating_sub(passed),
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
        let timeline = self.get_json(&format!(
            "/api/runtime/timeline?session_id={session_id}&limit=500"
        ));
        trace.push(trace_json_entry(
            "GET",
            format!("/api/runtime/timeline?session_id={session_id}&limit=500"),
            Value::Null,
            &timeline,
        ));
        let timeline = timeline.unwrap_or_else(|error| json!({"error": error}));
        // The public projection makes child execution lineage explicit. A
        // session ingress graph often delegates provider/tool/team work to
        // descendants, so reporting only the root would incorrectly claim
        // zero model rounds and zero token/tool usage for a real execution.
        let projections = execution_id
            .as_deref()
            .map(|id| self.execution_lineage_projections(id, &mut trace))
            .unwrap_or_default();
        let projection = projections.first().cloned().unwrap_or(Value::Null);
        let response_text = message_text(&terminal_wait.message);
        let mut acceptance = spec
            .acceptance
            .evaluate(&response_text, &timeline, &projection);
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
            "timeout": terminal_wait.report,
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
                let fingerprint = format!(
                    "{}:{}",
                    projection
                        .get("revision")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    serde_json::to_string(&statuses).unwrap_or_default()
                );
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
            let response = actor.post_mutation(&path, request);
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

#[derive(Clone, Copy)]
enum LiveAcceptance {
    Contains(&'static str),
    RequiresToolEvidence,
    ArchitectureQuality { require_team: bool },
}

impl LiveAcceptance {
    fn evaluate(
        self,
        response: &str,
        timeline: &Value,
        projection: &Value,
    ) -> LiveAcceptanceResult {
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
            Self::ArchitectureQuality { require_team } => {
                let agent_count = projection
                    .get("agents")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                let team_count = projection
                    .get("teams")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                let quality = architecture_quality(response);
                let team_projection = agent_count >= 2 || team_count >= 1;
                LiveAcceptanceResult {
                    passed: !response.trim().is_empty()
                        && quality.score >= quality.required
                        && (!require_team || team_projection),
                    quality: Some(quality.clone()),
                    checks: vec![
                        json!({"name": "durable_response", "passed": !response.trim().is_empty()}),
                        json!({"name": "architecture_quality", "passed": quality.score >= quality.required, "score": quality.score, "required": quality.required, "criteria": quality.criteria}),
                        json!({"name": "team_or_multi_agent_projection", "required": require_team, "passed": !require_team || team_projection, "agents": agent_count, "teams": team_count}),
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

fn architecture_quality(response: &str) -> ArchitectureQuality {
    let lowered = response.to_ascii_lowercase();
    let criteria = [
        (
            "runtime_boundary",
            contains_any(&lowered, &["runtime", "运行时"]),
        ),
        (
            "memory_boundary",
            contains_any(&lowered, &["memory", "记忆"]),
        ),
        (
            "gateway_boundary",
            contains_any(&lowered, &["gateway", "网关"]),
        ),
        (
            "canonical_state_or_event_truth",
            contains_any(
                &lowered,
                &["canonical", "event", "事件", "真相", "唯一状态"],
            ),
        ),
        (
            "risk_or_open_issue",
            contains_any(&lowered, &["risk", "风险", "缺口", "待处理", "open issue"]),
        ),
        ("source_path_evidence", source_path_count(response) >= 2),
        (
            "cited_source_paths_exist",
            cited_source_paths_exist(response),
        ),
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
        required: 7,
        criteria,
    }
}

fn contains_any(text: &str, values: &[&str]) -> bool {
    values
        .iter()
        .any(|value| text.contains(&value.to_ascii_lowercase()))
}

fn source_path_count(response: &str) -> usize {
    source_paths(response).len()
}

fn cited_source_paths_exist(response: &str) -> bool {
    let paths = source_paths(response);
    !paths.is_empty() && paths.iter().all(|path| workspace_source_path_exists(path))
}

fn workspace_source_path_exists(path: &str) -> bool {
    let Ok(mut current) = std::env::current_dir() else {
        return false;
    };
    loop {
        if current.join(path).is_file() {
            return true;
        }
        if !current.pop() {
            return false;
        }
    }
}

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
    let team_capability_passed = team.is_some_and(|scenario| {
        scenario.get("status").and_then(Value::as_str) == Some("passed")
            && scenario
                .pointer("/metrics/agent_count")
                .and_then(Value::as_u64)
                .is_some_and(|agents| agents >= 2)
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
            "requirement": "the explicit-team scenario has a terminal, evidence-backed result with at least two projected agents"
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
    fn team_acceptance_does_not_pass_without_a_real_projection_team_or_agents() {
        let answer =
            "runtime memory gateway event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
        let result = LiveAcceptance::ArchitectureQuality { require_team: true }.evaluate(
            answer,
            &Value::Null,
            &json!({"agents": [], "teams": []}),
        );
        assert!(!result.passed);
        let result = LiveAcceptance::ArchitectureQuality { require_team: true }.evaluate(
            answer,
            &Value::Null,
            &json!({"agents": [{}, {}], "teams": []}),
        );
        assert!(result.passed);
    }

    #[test]
    fn architecture_acceptance_rejects_hallucinated_workspace_paths() {
        let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/not-a-real-module/src/memory.rs";
        let result = LiveAcceptance::ArchitectureQuality {
            require_team: false,
        }
        .evaluate(answer, &Value::Null, &json!({"agents": [], "teams": []}));
        assert!(result.quality.as_ref().is_some_and(|quality| {
            quality.criteria.iter().any(|check| {
                check["name"] == "cited_source_paths_exist" && check["passed"] == false
            })
        }));
        assert!(!result.passed);
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
            &Value::Null,
        );
        assert!(!result.passed);
        let result = LiveAcceptance::RequiresToolEvidence.evaluate(
            "Cargo.toml",
            &json!({"events": [{"tool_name": "workspace.read"}]}),
            &Value::Null,
        );
        assert!(result.passed);
    }

    #[test]
    fn zero_tool_count_is_not_live_tool_evidence() {
        let result = LiveAcceptance::RequiresToolEvidence.evaluate(
            "I read Cargo.toml",
            &json!({"events": [{"tool_calls": 0}]}),
            &json!({"usage": [{"detail": {"tool_calls": 0}}]}),
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
