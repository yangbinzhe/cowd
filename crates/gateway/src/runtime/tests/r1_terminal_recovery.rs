use serde_json::{json, Value};
use std::time::Duration;
use std::{path::Path, sync::Arc};

use crate::selected_storage::SelectedStorageTopology;

#[path = "r1_startup_partition.rs"]
mod startup_partition_483;

#[path = "r1_receipt_owner.rs"]
mod receipt_owner;

use crate::session_runtime_bridge::terminal_delivery_faults::{
    checkpoint, observe, register, Point, Release, RequestIdentity,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalCall {
    tool_use_id: String,
    tool_name: String,
    input: String,
    idempotency_key: String,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    attempt: Option<u32>,
}

struct RecordingExecutionHost {
    inner: Arc<crate::gateway_tool_executor::GatewayToolExecutor>,
    calls: Arc<std::sync::Mutex<Vec<PhysicalCall>>>,
    holds: Arc<std::sync::Mutex<std::collections::BTreeMap<String, PhysicalHold>>>,
    bounded_missing: Arc<std::sync::atomic::AtomicU8>,
}

struct PhysicalHold {
    entered: tokio::sync::oneshot::Sender<runtime::RuntimeToolExecutionRequest>,
    released: tokio::sync::oneshot::Receiver<()>,
}

#[async_trait::async_trait]
impl runtime::RuntimeExecutionHost for RecordingExecutionHost {
    fn bounded_process_authority(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> Result<runtime::tool_host::BoundedProcessAuthority, runtime::ToolError> {
        if self.bounded_missing.load(std::sync::atomic::Ordering::SeqCst) == 2 {
            return Err(runtime::ToolError::new("fixture: bounded authority unavailable"));
        }
        runtime::RuntimeExecutionHost::bounded_process_authority(self.inner.as_ref(), request)
    }

    fn bounded_process_lease(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, runtime::ToolError> {
        if self.bounded_missing.load(std::sync::atomic::Ordering::SeqCst) == 1 {
            return Err(runtime::ToolError::new("fixture: bounded lease unavailable"));
        }
        runtime::RuntimeExecutionHost::bounded_process_lease(self.inner.as_ref(), request)
    }

    fn bounded_process_plane(&self) -> Option<Arc<runtime::ToolExecutionPlane>> {
        if self.bounded_missing.load(std::sync::atomic::Ordering::SeqCst) == 3 { return None; }
        runtime::RuntimeExecutionHost::bounded_process_plane(self.inner.as_ref())
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        runtime::RuntimeExecutionHost::pin_step_catalog(self.inner.as_ref())
    }

    fn delegated_tool_definitions(&self, names: &[String]) -> Vec<runtime::ProviderToolDefinition> {
        runtime::RuntimeExecutionHost::delegated_tool_definitions(self.inner.as_ref(), names)
    }

    fn delegated_tool_effect_descriptor(
        &self,
        name: &str,
        input: &Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        runtime::RuntimeExecutionHost::delegated_tool_effect_descriptor(
            self.inner.as_ref(),
            name,
            input,
        )
    }

    async fn execute_runtime_tool(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> runtime::RuntimeToolExecutionOutcome {
        self.calls.lock().unwrap().push(PhysicalCall {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            idempotency_key: request.idempotency_key.clone(),
            parent_execution: request.parent_execution.clone(),
            attempt: request.parent_execution_attempt,
        });
        let outcome =
            runtime::RuntimeExecutionHost::execute_runtime_tool(self.inner.as_ref(), request).await;
        let hold = self.holds.lock().unwrap().remove(&request.tool_use_id);
        if let Some(hold) = hold {
            assert_eq!(
                outcome.status,
                runtime::RuntimeToolExecutionStatus::Executed
            );
            if hold.entered.send(request.clone()).is_ok() {
                let _ = hold.released.await;
            }
        }
        outcome
    }
    async fn execute_runtime_tool_with_action_invocation(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
        capability: &runtime::agentic::action_invocation::VerifiedActionInvocation,
    ) -> runtime::RuntimeToolExecutionOutcome {
        self.calls.lock().unwrap().push(PhysicalCall {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            idempotency_key: request.idempotency_key.clone(),
            parent_execution: request.parent_execution.clone(),
            attempt: request.parent_execution_attempt,
        });
        let outcome = runtime::RuntimeExecutionHost::execute_runtime_tool_with_action_invocation(
            self.inner.as_ref(), request, capability,
        ).await;
        let hold = self.holds.lock().unwrap().remove(&request.tool_use_id);
        if let Some(hold) = hold {
            assert_eq!(outcome.status, runtime::RuntimeToolExecutionStatus::Executed);
            if hold.entered.send(request.clone()).is_ok() {
                let _ = hold.released.await;
            }
        }
        outcome
    }

}

struct ProviderRequest {
    wire_id: usize,
    body_sha256: String,
    wire_facts: Arc<std::sync::Mutex<Vec<Value>>>,
    body: Value,
    reply: tokio::sync::oneshot::Sender<Value>,
}

fn recovery_evidence_result_metadata(message: Option<&Value>) -> Value {
    let text = message.and_then(|message| message["content"].as_str());
    let parsed = text.and_then(|text| {
        serde_json::from_str::<Value>(text).ok().or_else(|| {
            let offset = text.find('{')?;
            serde_json::Deserializer::from_str(&text[offset..])
                .into_iter::<Value>().next().and_then(Result::ok)
        })
    });
    let value = parsed.as_ref().unwrap_or(&Value::Null);
    json!({
        "tool_result_present":message.is_some(), "text_bytes":text.map(str::len),
        "parsed":parsed.is_some(), "message_is_error":message.map(|message| &message["is_error"]),
        "kind":value["kind"], "available":value["available"],
        "error_present":parsed.as_ref().map(|value| !value["error"].is_null()),
        "plain_text_tool_failure":text.map(|text| text.starts_with("Tool `") && text.contains(" failed.")),
        "status":value["status"],
        "evidence_ref":value["evidence_ref"], "sha256":value["sha256"],
        "bytes":value["bytes"], "media_type":value["media_type"], "encoding":value["encoding"],
        "total_chunks":value["total_chunks"], "truncated":value["truncated"],
        "chunk_indices":value["chunks"].as_array().map(|chunks| chunks.iter()
            .take(16).map(|chunk| chunk["index"].clone()).collect::<Vec<_>>()),
        "returned_chunk_count":value["chunks"].as_array().map(Vec::len),
        "query_present":!value["query"].is_null(),
        "next_cursor_present":!value["next_cursor"].is_null(),
        "next_request_present":!value["next_request"].is_null(),
        "coverage":value["coverage"],
    })
}

fn recovery_read_event_metadata(event: &runtime::RuntimeEventRecord) -> Value {
    json!({
        "event_id":event.event_id, "stream_id":event.stream_id, "sequence":event.sequence,
        "schema_version":event.schema_version, "kind":event.kind, "actor":event.actor,
        "scope":event.scope, "status":event.status, "transaction_id":event.transaction_id,
        "transaction_index":event.transaction_index, "commit_cursor":event.commit_cursor,
        "tool_use_id":event.payload["tool_use_id"], "tool_name":event.payload["tool_name"],
        "outcome_status":event.payload["outcome"]["status"],
        "outcome_error_present":!event.payload["outcome"]["error"].is_null(),
        "outcome_truncated":event.payload["outcome_truncated"],
        "sequence_owner":event.payload["sequence_owner"],
        "sequence_base":event.payload["sequence_base"],
        "index_revision_before":event.payload["index_revision_before"],
        "index_sequence":event.payload["sequence"],
        "source_observation_wave_sequence":event.payload["source_observation_wave_sequence"],
        "acquisition":event.payload["read_acquisition_v1"],
    })
}

const LOOPBACK_CONTEXT_WINDOW_TOKENS: u32 = 128_000;
const LOOPBACK_MAX_OUTPUT_TOKENS: u64 = 64_000;

const TASK_ROUTE_CONTROL_SYSTEM: &str = "Route one user input to durable Tasks. Return one strict JSON object only. Continue only a clearly matching open Task. Split only genuinely independent objectives. Never invent candidate Task IDs.";

fn task_route_control_reply(body: &Value) -> Result<Option<Value>, String> {
    let streaming = body
        .get("stream")
        .and_then(Value::as_bool)
        .ok_or("loopback request requires an explicit boolean stream field")?;
    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .ok_or("loopback request requires messages")?;
    let is_router_system = |message: &Value| {
        message["role"] == "system"
            && message["content"].as_str().is_some_and(|system| {
                system.trim().split_inclusive('.').next()
                    == Some("Route one user input to durable Tasks.")
            })
    };
    let router_system = messages.iter().any(is_router_system);
    if streaming {
        if router_system {
            return Err("TaskRouter control must use the non-streaming RPC".into());
        }
        return Ok(None);
    }
    if messages.len() != 2
        || !is_router_system(&messages[0])
        || messages[1]["role"] != "user"
    {
        return Err(
            "unsupported control RPC: expected the TaskRouter opening sentence and one user message"
                .into(),
        );
    }
    if body
        .get("tools")
        .is_some_and(|tools| !tools.as_array().is_some_and(Vec::is_empty))
        || body.get("tool_choice").is_some()
    {
        return Err("TaskRouter control must expose no tools or tool choice".into());
    }
    let input: Value = serde_json::from_str(
        messages[1]["content"]
            .as_str()
            .ok_or("TaskRouter control user content must be JSON text")?,
    )
    .map_err(|error| format!("TaskRouter control user JSON: {error}"))?;
    let fields = input
        .as_object()
        .ok_or("TaskRouter control user JSON must be an object")?;
    if fields.len() != 4
        || !["input", "mission_hint", "candidates", "schema"]
            .iter()
            .all(|field| fields.contains_key(*field))
    {
        return Err("TaskRouter control user fields changed".into());
    }
    let schema = input["schema"]
        .as_object()
        .ok_or("TaskRouter control schema must be an object")?;
    if schema.len() != 4
        || !["action", "task_id", "additional_objectives", "reason"]
            .iter()
            .all(|field| schema.contains_key(*field))
    {
        return Err("TaskRouter control schema fields changed".into());
    }
    let mut actions = input["schema"]["action"]
        .as_str()
        .ok_or("TaskRouter control schema action must list route actions")?
        .split('|')
        .map(str::trim)
        .collect::<Vec<_>>();
    actions.sort_unstable();
    if actions != ["continue", "create_compound", "create_root"] {
        return Err("TaskRouter control schema must allow exactly the three route actions".into());
    }
    if !input["input"]
        .as_str()
        .is_some_and(|objective| !objective.trim().is_empty())
        || !(input["mission_hint"].is_null()
            || input["mission_hint"]
                .as_str()
                .is_some_and(|mission| !mission.trim().is_empty()))
    {
        return Err(
            "TaskRouter control requires a nonempty objective and a valid mission hint".into(),
        );
    }
    if !input["candidates"].as_array().is_some_and(Vec::is_empty) {
        return Err(
            "recovery fixture only answers fresh independent Task routes with no candidates".into(),
        );
    }
    Ok(Some(json!({"role":"assistant", "content":json!({
        "action":"create_root",
        "reason":"This fresh recovery fixture has one bounded result with dependent production and verification, not independent objectives.",
    }).to_string()})))
}

struct LoopbackProvider {
    endpoint: String,
    model: String,
    requests: tokio::sync::mpsc::Receiver<ProviderRequest>,
    sender: tokio::sync::mpsc::Sender<ProviderRequest>,
    routes: Arc<std::sync::Mutex<Vec<(String, tokio::sync::mpsc::Sender<ProviderRequest>)>>>,
    marker: String,
    task_route_controls: usize,
    wire_facts: Arc<std::sync::Mutex<Vec<Value>>>,
    pending_request: Option<ProviderRequest>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for LoopbackProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LoopbackProvider {
    async fn start(model: &str) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let (sender, requests) = tokio::sync::mpsc::channel(16);
        let retained_sender = sender.clone();
        let routes = Arc::new(std::sync::Mutex::new(Vec::<(
            String,
            tokio::sync::mpsc::Sender<ProviderRequest>,
        )>::new()));
        let routing = routes.clone();
        let wire_facts = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let task_wire_facts = wire_facts.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut connection_number = 0_u64;
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (mut socket, peer) = connection.unwrap();
                        assert!(peer.ip().is_loopback());
                        connection_number += 1;
                        let connection_number = connection_number;
                        eprintln!("b_scope_loopback connection={connection_number} stage=accepted");
                        let sender = sender.clone();
                        let routing = routing.clone();
                        let wire_facts = task_wire_facts.clone();
                        connections.spawn(async move {
                            let mut bytes = Vec::new();
                            let body = loop {
                                let mut buffer = [0_u8; 8192];
                                let count = match socket.read(&mut buffer).await {
                                    Ok(count) => count,
                                    Err(error) => {
                                        eprintln!(
                                            "recovery188 provider_connection_read_failed bytes={} error={error}",
                                            bytes.len()
                                        );
                                        return;
                                    }
                                };
                                if count == 0 {
                                    eprintln!(
                                        "recovery188 provider_connection_closed bytes={} abandoned_request={}",
                                        bytes.len(),
                                        !bytes.is_empty()
                                    );
                                    return;
                                }
                                assert!(bytes.len() < 16 * 1024 * 1024);
                                bytes.extend_from_slice(&buffer[..count]);
                                eprintln!("b_scope_loopback connection={connection_number} stage=read bytes={}", bytes.len());
                                if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..offset]);
                                    let length = headers.lines().find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse::<usize>().unwrap())
                                    }).unwrap();
                                    if bytes.len() >= offset + 4 + length {
                                        use sha2::Digest;
                                        let raw = &bytes[offset + 4..offset + 4 + length];
                                        let parsed = serde_json::from_slice::<Value>(raw).unwrap();
                                        eprintln!("b_scope_loopback connection={connection_number} stage=decoded");
                                        break (parsed, format!("{:x}", sha2::Sha256::digest(raw)));
                                    }
                                }
                            };
                            let (body, body_sha256) = body;
                            let wire_id = { let mut facts = wire_facts.lock().unwrap(); let id = facts.len(); facts.push(json!({"wire_id":id,"body_sha256":body_sha256,"queue":"receiving","reply":"pending","socket":"pending"})); id };
                            let model = body["model"].as_str().unwrap().to_string();
                            let streaming = body.get("stream").and_then(Value::as_bool)
                                .expect("explicit loopback wire stream mode");
                            let max_output = body.get("max_completion_tokens")
                                .or_else(|| body.get("max_tokens"))
                                .and_then(Value::as_u64).expect("bounded loopback output request");
                            assert!((1..=LOOPBACK_MAX_OUTPUT_TOKENS).contains(&max_output));
                            let wire = body.to_string();
                            let sender = routing.lock().unwrap().iter()
                                .find(|(marker, _)| wire.contains(marker))
                                .map(|(_, sender)| sender.clone()).unwrap_or(sender);
                            eprintln!("recovery188 wire_received model={model} max_output={max_output} bytes={}", wire.len());
                            let matched_routes = routing.lock().unwrap().iter()
                                .filter(|(marker, _)| wire.contains(marker))
                                .map(|(marker, _)| marker.clone()).collect::<Vec<_>>();
                            eprintln!("recovery188 matched_routes={matched_routes:?}");
                            let system_authority = body["messages"].as_array().into_iter().flatten()
                                .filter(|message| message["role"] == "system")
                                .filter_map(|message| message["content"].as_str())
                                .flat_map(str::lines)
                                .filter(|line| line.starts_with("Agent Definition: ")
                                    || (line.starts_with("RECOVERY_") && line.ends_with("_LANE")))
                                .collect::<Vec<_>>();
                            eprintln!("recovery215 wire_authority model={model} system_lines={system_authority:?}");
                            let (reply, response) = tokio::sync::oneshot::channel();
                            if sender.send(ProviderRequest { wire_id, body_sha256, wire_facts: wire_facts.clone(), body, reply }).await.is_err() {
                                eprintln!("b_scope_loopback connection={connection_number} stage=queue_closed");
                                return;
                            }
                            { let mut facts = wire_facts.lock().unwrap(); if facts[wire_id]["queue"] == "receiving" { facts[wire_id]["queue"] = json!("enqueued"); } }
                            eprintln!("b_scope_loopback connection={connection_number} stage=queued");
                            let Ok(delta) = response.await else {
                                eprintln!("b_scope_loopback connection={connection_number} stage=reply_closed");
                                return;
                            };
                            wire_facts.lock().unwrap()[wire_id]["reply"] = json!("received");
                            eprintln!("b_scope_loopback connection={connection_number} stage=reply_received");
                            eprintln!("recovery188 wire_reply model={model} tool_calls={}", delta.get("tool_calls").is_some());
                            let finish = if delta.get("tool_calls").is_some() { "tool_calls" } else { "stop" };
                            let (content_type, body) = if streaming {
                                let chunk = |delta, finish_reason| json!({
                                    "id":"recovery-loopback", "object":"chat.completion.chunk", "model":model,
                                    "choices":[{"index":0,"delta":delta,"finish_reason":finish_reason}],
                                    "usage":{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20},
                                });
                                ("text/event-stream", format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                                    chunk(delta, Value::Null), chunk(json!({}), json!(finish))))
                            } else {
                                assert!(delta.get("tool_calls").is_none(), "control RPC cannot consume a scripted tool reply");
                                assert!(delta["content"].as_str().is_some(), "control RPC requires text");
                                ("application/json", json!({
                                    "id":"recovery-loopback-control", "object":"chat.completion", "model":model,
                                    "choices":[{"index":0,"message":delta,"finish_reason":"stop"}],
                                    "usage":{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20},
                                }).to_string())
                            };
                            eprintln!("recovery206 wire_response model={model} stream={streaming} content_type={content_type}");
                            let written = socket.write_all(format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                content_type, body.len(), body,
                            ).as_bytes()).await;
                            wire_facts.lock().unwrap()[wire_id]["socket"] = json!(if written.is_ok() { "written" } else { "write_error" });
                            eprintln!("b_scope_loopback connection={connection_number} stage=response_written ok={}", written.is_ok());
                            eprintln!("recovery188 wire_write model={model} result={written:?}");
                        });
                    }
                    finished = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = finished {
                            eprintln!("recovery188 provider_connection_failed error={error}");
                        }
                    }
                }
            }
        });
        Self {
            endpoint,
            model: "test-model".into(),
            requests,
            sender: retained_sender,
            routes,
            marker: format!("RECOVERY_{}_LANE", model.to_ascii_uppercase()),
            task_route_controls: 0,
            wire_facts,
            pending_request: None,
            task,
        }
    }

    async fn next(&mut self) -> ProviderRequest {
        if let Some(request) = self.pending_request.take() {
            return request;
        }
        tokio::time::timeout(Duration::from_secs(90), async {
            for _ in 0..2 {
                let request = self.requests.recv().await.expect("loopback request channel closed");
                request.wire_facts.lock().unwrap()[request.wire_id]["queue"] = json!("dequeued");
                assert_eq!(request.body["model"], self.model);
                let reply = task_route_control_reply(&request.body)
                    .unwrap_or_else(|reason| panic!("recovery206 lane={} {reason}", self.marker));
                let Some(reply) = reply else { return request; };
                assert_eq!(self.marker, "RECOVERY_ROOT_LANE", "TaskRouter control belongs only to the root lane");
                assert_eq!(self.task_route_controls, 0, "unexpected repeated TaskRouter control RPC");
                self.task_route_controls += 1;
                eprintln!("recovery206 task_route_control lane={} count={} action=create_root", self.marker, self.task_route_controls);
                request.reply.send(reply).expect("TaskRouter control consumer dropped");
            }
            panic!("recovery206 request budget exhausted before the production model continuation");
        }).await.expect("production provider continuation did not arrive within the original shared 90s deadline")
    }

    // Sending a scripted call does not establish that its command committed.
    // Keep the actual continuation for the next scripted response.
    async fn wait_for_tool_result(&mut self, call_id: &str) {
        let request = self.next().await;
        assert!(
            request.body["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|message| message["role"] == "tool" && message["tool_call_id"] == call_id),
            "production continuation must carry the result for {call_id}"
        );
        self.pending_request = Some(request);
    }

    async fn tool(&mut self, id: &str, name: &str, input: Value, text: Option<&str>) {
        eprintln!(
            "recovery188 scripted_tool lane={} id={id} name={name}",
            self.marker
        );
        let request = self.next().await;
        if id == "recovery-review-file" && self.marker == "RECOVERY_REVIEWER_LANE" {
            let result = request.body["messages"].as_array().into_iter().flatten().rev()
                .find(|message| message["role"] == "tool"
                    && message["tool_call_id"] == "recovery-review-result");
            eprintln!("recovery_result_read_wire_metadata {}", json!({
                "capture_phase":"first_provider_request_after_result", "lane":self.marker,
                "call_id":"recovery-review-result", "result":recovery_evidence_result_metadata(result),
            }));
        }
        request
            .reply
            .send(json!({"role":"assistant", "content":text,
                "tool_calls":[{"index":0,"id":id,"type":"function", "function":{
                    "name":name,"arguments":input.to_string(),
                }}],
            }))
            .unwrap();
    }

    async fn finish(&mut self, text: &str) {
        eprintln!("recovery188 scripted_finish lane={}", self.marker);
        self.next()
            .await
            .reply
            .send(json!({"role":"assistant","content":text}))
            .unwrap();
    }
}

#[tokio::test]
async fn loopback_drop_closes_an_entered_rpc_even_if_its_reply_is_retained() {
    let mut provider = LoopbackProvider::start("root").await;
    let endpoint = format!("{}/chat/completions", provider.endpoint);
    let client = tokio::spawn(async move {
        reqwest::Client::new().post(endpoint).json(&json!({
            "model":"test-model", "stream":true, "max_tokens":512,
            "messages":[{"role":"system","content":"RECOVERY_ROOT_LANE"},{"role":"user","content":"bounded lifecycle handshake"}]
        })).send().await
    });
    let retained_reply = tokio::time::timeout(Duration::from_secs(2), provider.next())
        .await
        .unwrap();
    assert_eq!(
        retained_reply.body["messages"][1]["content"],
        "bounded lifecycle handshake"
    );
    drop(provider);
    assert!(tokio::time::timeout(Duration::from_secs(2), client)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    drop(retained_reply);
}

async fn await_satisfaction_or_offer_completion<T>(
    satisfied: impl std::future::Future<Output = T>,
    completion: impl std::future::Future<Output = ()>,
) -> T {
    tokio::pin!(satisfied);
    tokio::select! {
        biased;
        goal = &mut satisfied => goal,
        () = completion => satisfied.await,
    }
}

#[tokio::test]
async fn satisfied_goal_does_not_require_another_provider_continuation() {
    let offered = std::sync::atomic::AtomicBool::new(false);
    let original =
        await_satisfaction_or_offer_completion(async { "original satisfied Goal" }, async {
            offered.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending::<()>().await;
        })
        .await;
    assert_eq!(original, "original satisfied Goal");
    assert!(!offered.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn open_goal_can_consume_its_original_completion_request_before_satisfaction() {
    let (sent, accepted) = tokio::sync::oneshot::channel();
    let original = await_satisfaction_or_offer_completion(
        async {
            accepted.await.unwrap();
            "original satisfied Goal"
        },
        async {
            sent.send(()).unwrap();
        },
    )
    .await;
    assert_eq!(original, "original satisfied Goal");
}

#[test]
fn recovery_task_route_control_wire_contract() {
    let wire = |tools: Option<Value>| {
        let mut body = json!({"model":"test-model", "stream":false, "max_tokens":640,
            "messages":[{"role":"system","content":TASK_ROUTE_CONTROL_SYSTEM},
                {"role":"user","content":json!({
                    "input":"Produce and independently verify one bounded recovery result.",
                    "mission_hint":null, "candidates":[], "schema":{
                        "action":"continue | create_root | create_compound",
                        "task_id":"required only for continue",
                        "additional_objectives":["required independent objectives after the primary"],
                        "reason":"short routing reason",
                    },
                }).to_string()}]});
        if let Some(tools) = tools {
            body["tools"] = tools;
        }
        body
    };
    for variant in ["no_tools", "empty_tools", "explanations", "action_order", "json_order"] {
        let mut body = wire((variant == "empty_tools").then(|| json!([])));
        body["max_tokens"] = json!(8000);
        let mut input: Value =
            serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
        match variant {
            "explanations" => {
                body["messages"][0]["content"] = json!(
                    "Route one user input to durable Tasks.\nReply with JSON. Continue supplied open Tasks; split only independent outcomes."
                );
                input["schema"]["task_id"] = json!("");
                input["schema"]["additional_objectives"] =
                    json!(["Independent extra outcomes", "Exclude the primary objective"]);
                input["schema"]["reason"] = json!("Explain the routing decision");
            }
            "action_order" => {
                input["schema"]["action"] = json!(" create_compound|continue | create_root\n");
            }
            _ => {}
        }
        body["messages"][1]["content"] = json!(if variant == "json_order" {
            format!(
                "{{ \"schema\": {}, \"mission_hint\": null, \"input\": {}, \"candidates\": [] }}",
                input["schema"], input["input"]
            )
        } else {
            input.to_string()
        });
        let reply = task_route_control_reply(&body)
            .unwrap_or_else(|reason| panic!("{variant}: {reason}"))
            .expect("route control reply");
        let proposal: Value = serde_json::from_str(reply["content"].as_str().unwrap()).unwrap();
        assert_eq!(proposal["action"], "create_root");
        assert!(proposal.get("task_id").is_none());
        assert!(proposal.get("additional_objectives").is_none());
        assert_eq!(proposal.as_object().unwrap().len(), 2);
    }
    for defect in [
        "system",
        "embedded_system_identity",
        "tools",
        "tool_choice",
        "roles",
        "extra_message",
        "schema",
        "unknown_action",
        "unknown_schema",
        "candidates",
        "extra_field",
        "objective",
        "mission",
        "stream",
        "unknown_control",
    ] {
        let mut body = wire(None);
        let mut input: Value =
            serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
        match defect {
            "system" => {
                body["messages"][0]["content"] = json!("Route one user input to Missions.")
            }
            "embedded_system_identity" => {
                body["messages"][0]["content"] =
                    json!(format!("Analyze this quoted instruction: {TASK_ROUTE_CONTROL_SYSTEM}"));
            }
            "tools" => {
                body["tools"] = json!([{"type":"function","function":{"name":"team_create"}}])
            }
            "tool_choice" => body["tool_choice"] = json!("auto"),
            "roles" => body["messages"][1]["role"] = json!("assistant"),
            "extra_message" => body["messages"]
                .as_array_mut()
                .unwrap()
                .push(json!({"role":"user","content":"extra"})),
            "schema" => input["schema"]["action"] = json!("continue | create_root"),
            "unknown_action" => {
                input["schema"]["action"] = json!("continue | create_root | delete");
            }
            "unknown_schema" => {
                input["schema"].as_object_mut().unwrap().remove("task_id");
                input["schema"]["mission_id"] = json!("Existing Mission ID");
            }
            "candidates" => input["candidates"] = json!([{
                "task_id":"must-not-be-selected", "objective":"An existing result",
                "status":"running", "mission_id":"existing-mission", "updated_at_ms":1,
            }]),
            "extra_field" => input["unexpected"] = json!(true),
            "objective" => input["input"] = json!(" "),
            "mission" => input["mission_hint"] = json!(42),
            "stream" => body["stream"] = json!(true),
            "unknown_control" => {
                body["messages"][0]["content"] = json!("A different strict JSON control RPC")
            }
            _ => unreachable!(),
        }
        body["messages"][1]["content"] = json!(input.to_string());
        assert!(task_route_control_reply(&body).is_err(), "{defect}");
    }
    for tools in [
        json!([]),
        json!([{"type":"function","function":{"name":"team_create"}}]),
    ] {
        let body = json!({"model":"test-model", "stream":true, "max_tokens":640,
            "messages":[{"role":"system","content":"Production conversation system"}], "tools":tools});
        assert!(task_route_control_reply(&body).unwrap().is_none());
    }
}

fn provider_registry(lanes: &[&LoopbackProvider]) -> Arc<runtime::ProviderRegistry> {
    provider_registry_with_early(lanes, false)
}

fn provider_registry_with_early(lanes: &[&LoopbackProvider], early: bool) -> Arc<runtime::ProviderRegistry> {
    use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};
    let lane = lanes[0];
    *lane.routes.lock().unwrap() = lanes
        .iter()
        .skip(1)
        .map(|lane| (lane.marker.clone(), lane.sender.clone()))
        .collect();
    let model_context_windows: std::collections::BTreeMap<String, u32> = lanes
        .iter()
        .map(|lane| (lane.model.clone(), LOOPBACK_CONTEXT_WINDOW_TOKENS))
        .collect();
    let models = model_context_windows.keys().cloned().collect();
    let registry = runtime::ProviderRegistry::new_with_context_windows(
        ProvidersConfig {
            providers: std::collections::HashMap::from([(
                lane.model.clone(),
                ProviderConfig {
                    reasoning_effort: None,
                    name: lane.model.clone(),
                    base_url: lane.endpoint.clone(),
                    api_key: "loopback-only".into(),
                    models,
                    protocol: Some("completions".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: if early {
                        model_protocol::provider_config::EarlyToolStartMode::Enabled
                    } else { Default::default() },
                },
            )]),
        },
        model_context_windows,
    )
    .unwrap();
    let snapshot = registry.pin();
    for lane in lanes {
        assert!(snapshot.resolve(&lane.model).is_some());
        assert_eq!(
            snapshot.configured_context_window(&lane.model),
            Some(LOOPBACK_CONTEXT_WINDOW_TOKENS)
        );
    }
    Arc::new(registry)
}

fn set_recovery_capability_ceiling(
    contract: &mut harness_contract::agent::AgentCapabilityContract,
    ceiling: &[harness_contract::agent::AgentCapability],
) {
    use harness_contract::agent::AgentCapability;
    assert!(
        ceiling.contains(&AgentCapability::Read),
        "recovery roles require Read for governed control actions"
    );
    let unique = ceiling
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        ceiling.len(),
        "recovery capability ceiling must be unique"
    );
    assert!(
        ceiling
            .iter()
            .all(|capability| contract.capability_ceiling.contains(capability)),
        "recovery capability ceiling cannot expand its source Definition"
    );
    contract.capability_ceiling = ceiling.to_vec();
    contract
        .approval_required_for
        .retain(|capability| ceiling.contains(capability));
}

#[test]
fn recovery_role_capability_contract_is_a_ceiling() {
    use harness_contract::agent::{AgentCapability, AgentCapabilityContract};
    let original = AgentCapabilityContract {
        capability_ceiling: vec![
            AgentCapability::Read,
            AgentCapability::Search,
            AgentCapability::Write,
            AgentCapability::Test,
        ],
        skill_refs: Vec::new(),
        approval_required_for: vec![AgentCapability::Write],
    };
    let mut reviewer = original.clone();
    set_recovery_capability_ceiling(&mut reviewer, &[AgentCapability::Read]);
    assert_eq!(reviewer.capability_ceiling, vec![AgentCapability::Read]);
    assert!(reviewer.approval_required_for.is_empty());
    let mut producer = original;
    set_recovery_capability_ceiling(
        &mut producer,
        &[AgentCapability::Read, AgentCapability::Write],
    );
    assert_eq!(producer.approval_required_for, vec![AgentCapability::Write]);
    let resolved = runtime::resolve_agent_capability(runtime::AgentCapabilityRequest {
        role_id: "arbitrary-display-role".into(),
        allowed_capabilities: vec!["read".into()],
        evidence_duties: Vec::new(),
    });
    assert_eq!(resolved.permission_mode, runtime::PermissionMode::ReadOnly);
    assert!(resolved.allowed_tools.contains("read_file"));
    assert!(resolved.allowed_tools.contains("evidence_retrieve"));
    assert!(!resolved.allowed_tools.contains("edit"));
    assert!(!resolved.allowed_tools.contains("artifact_materialize"));
    assert_eq!(
        producer.capability_ceiling,
        vec![AgentCapability::Read, AgentCapability::Write]
    );
}

#[test]
#[should_panic(expected = "cannot expand its source Definition")]
fn recovery_role_capability_contract_rejects_expansion() {
    use harness_contract::agent::{AgentCapability, AgentCapabilityContract};
    let mut contract = AgentCapabilityContract {
        capability_ceiling: vec![AgentCapability::Read],
        skill_refs: Vec::new(),
        approval_required_for: Vec::new(),
    };
    set_recovery_capability_ceiling(
        &mut contract,
        &[AgentCapability::Read, AgentCapability::Write],
    );
}

fn install_native_definition(
    services: &RecoveryServices,
    name: &str,
    lane: &LoopbackProvider,
    capability_ceiling: &[harness_contract::agent::AgentCapability],
) -> String {
    use harness_contract::agent::*;
    use runtime::agent::definition::{AgentDefinitionStore, RegisteredAgentDefinitionLayout};
    use sha2::Digest;
    let runtime = services.runtime.runtime_services();
    let registry = runtime.definition_registry();
    let builtin_root = services
        .config_home
        .join("runtime")
        .join("builtin-definitions");
    let workspace_root = runtime.workspace_root();
    let layout = RegisteredAgentDefinitionLayout::from_storage_registry(
        &services.topology.registry,
        &builtin_root,
        workspace_root,
    )
    .unwrap();
    assert_eq!(layout.builtin_root(), builtin_root);
    assert_eq!(
        layout.user_root(),
        services
            .topology
            .registry
            .endpoint(&storage::StorageDomainId::Definitions)
            .unwrap()
            .as_handle()
            .path
    );
    assert_eq!(layout.user_root(), services.config_home.join("definitions"));
    assert_eq!(
        layout.workspace_root(),
        services
            .topology
            .registry
            .endpoint_in_scope(
                &storage::StorageDomainId::Definitions,
                &storage::StorageScope::workspace_for_root(workspace_root),
            )
            .unwrap()
            .as_handle()
            .path
    );
    assert_eq!(
        layout.workspace_root(),
        workspace_root.join(".cowd").join("definitions")
    );
    let builder_registry =
        storage::StorageRegistry::postgres_for_config_home(&services.config_home)
            .with_postgres_workspace(workspace_root)
            .unwrap();
    assert_eq!(
        layout,
        RegisteredAgentDefinitionLayout::from_storage_registry(
            &builder_registry,
            &builtin_root,
            workspace_root,
        )
        .unwrap()
    );
    let store = AgentDefinitionStore::new(layout);
    let base = registry
        .resolve_agent(
            &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
            RevisionSelector::LatestApprovedStable,
        )
        .unwrap();
    let mut manifest = base.revision.manifest.clone();
    manifest.definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, name).unwrap();
    manifest.executor = AgentExecutorPolicy::CowdNative;
    set_recovery_capability_ceiling(&mut manifest.capability_contract, capability_ceiling);
    manifest.model_policy.allowed_models = vec![lane.model.clone()];
    manifest.model_policy.fallback_allowed = false;
    let markdown = format!("{}\n{}\n", base.agent_markdown, lane.marker);
    manifest.instructions_digest = format!("{:x}", sha2::Sha256::digest(markdown.as_bytes()));
    let revision = store.store_revision(manifest, &markdown).unwrap();
    assert_eq!(
        revision
            .revision
            .manifest
            .capability_contract
            .capability_ceiling,
        capability_ceiling
    );
    store
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: revision.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: format!("fixture:{name}"),
            },
            content_digest: revision.revision.content_digest.clone(),
        })
        .unwrap();
    runtime.refresh_definition_catalog().unwrap();
    let entry = runtime
        .agent_runtime()
        .catalog()
        .all()
        .into_iter()
        .find(|entry| entry.definition_ref == revision.revision.revision_ref)
        .expect("new recovery Definition must be present in the production dispatch catalog");
    let expected = capability_ceiling
        .iter()
        .map(|capability| capability.as_str().to_string())
        .collect::<Vec<_>>();
    assert_eq!(entry.capabilities, expected);
    eprintln!(
        "recovery215 definition={} model={} marker={} capabilities={:?}",
        entry.definition_ref.definition_id.as_str(),
        lane.model,
        lane.marker,
        entry.capabilities
    );
    revision
        .revision
        .revision_ref
        .definition_id
        .as_str()
        .to_string()
}

async fn durable_until<T>(mut read: impl FnMut() -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(value) = read() {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("durable business oracle did not converge")
}

struct ReviewedTurn {
    session_id: String,
    request_id: String,
    graph_id: String,
    turn_id: String,
    terminal_id: String,
    program_id: String,
    goal: harness_contract::goal::GoalContract,
    program: runtime::AgenticProgramProjection,
    input: session::SessionRuntimeOutboxRecord,
    observer: crate::session_runtime_bridge::terminal_delivery_faults::Observer,
    effect_receipts: Vec<runtime::RuntimeEventRecord>,
    review_events: Vec<runtime::RuntimeEventRecord>,
    physical_calls: Arc<std::sync::Mutex<Vec<PhysicalCall>>>,
    optional_receipts: Vec<runtime::RuntimeEventRecord>,
    resolution: Option<tokio::sync::oneshot::Receiver<(u64, tokio::sync::oneshot::Sender<()>)>>,
    candidate: Option<
        tokio::sync::oneshot::Receiver<(
            runtime::execution_core::graph::NodeExecutionTicket,
            runtime::SessionTerminalInput,
            tokio::sync::oneshot::Sender<()>,
        )>,
    >,
    commit_loss: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl ReviewedTurn {
    fn request_identity(&self) -> RequestIdentity {
        RequestIdentity {
            session_id: self.session_id.clone(),
            request_id: self.request_id.clone(),
            turn_id: self.turn_id.clone(),
            generation: self.input.session_generation,
            sequence: self.input.sequence as u64,
            terminal_id: self.terminal_id.clone(),
            execution_id: self.graph_id.clone(),
        }
    }
    async fn produce(
        services: &RecoveryServices,
        root: &mut LoopbackProvider,
        producer: &mut LoopbackProvider,
        reviewer: &mut LoopbackProvider,
        before_b: bool,
        lose_commit_response: bool,
    ) -> Self {
        Self::produce_with_optional(
            services,
            root,
            producer,
            reviewer,
            before_b,
            lose_commit_response,
            None,
            false,
            true,
        )
        .await
    }

    async fn produce_with_optional(
        services: &RecoveryServices,
        root: &mut LoopbackProvider,
        producer: &mut LoopbackProvider,
        reviewer: &mut LoopbackProvider,
        before_b: bool,
        lose_commit_response: bool,
        optional: Option<&mut LoopbackProvider>,
        hold_resolution: bool,
        hold_producer_until_a: bool,
    ) -> Self {
        Self::produce_with_optional_for_owner(services, root, producer, reviewer, before_b,
            lose_commit_response, optional, hold_resolution, hold_producer_until_a, "recovery-operator").await
    }

    async fn produce_with_optional_for_owner(
        services: &RecoveryServices,
        root: &mut LoopbackProvider,
        producer: &mut LoopbackProvider,
        reviewer: &mut LoopbackProvider,
        before_b: bool,
        lose_commit_response: bool,
        optional: Option<&mut LoopbackProvider>,
        hold_resolution: bool,
        hold_producer_until_a: bool,
        owner_principal: &str,
    ) -> Self {
        use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
        let runtime = services.runtime.runtime_services();
        let producer_definition = install_native_definition(
            services,
            "recovery/producer",
            producer,
            &[
                harness_contract::agent::AgentCapability::Read,
                harness_contract::agent::AgentCapability::Write,
            ],
        );
        let reviewer_definition = install_native_definition(
            services,
            "recovery/reviewer",
            reviewer,
            &[harness_contract::agent::AgentCapability::Read],
        );
        let session_id = format!("r1-recovery:{}", uuid::Uuid::new_v4());
        let mut request =
            EnsureSessionRequest::new(&session_id, Some(root.model.clone()), SessionSource::Tui);
        request.owner_principal_id = Some(owner_principal.into());
        services.session.create_user_session(request).await.unwrap();
        let envelope = harness_contract::turn::SessionInputEnvelope::text(
            &session_id, harness_contract::turn::InputSourceKind::Tui,
            "Write recovery-effect.txt containing recovery-effect-once, delegate one public Task and independently inspect its physical result and report before satisfying the user_intent criterion. Deliver that reviewed report.",
        );
        let request_id = envelope.idempotency_key.clone();
        let admission = services.session.admit_input(envelope).await.unwrap();
        let graph_id = admission.execution_graph_id;
        let turn_id = admission.turn_id;
        let terminal_id = admission.terminal_id;
        let candidate = before_b.then(|| {
            runtime
                .synthesize_executor()
                .observe_terminal_candidate_for_test(&graph_id)
                .unwrap()
        });
        let commit_loss = lose_commit_response.then(|| {
            runtime
                .commit_service()
                .lose_terminal_response_once_for_test(&terminal_id)
                .unwrap()
        });
        let program_id = harness_contract::agent_action::program_id_for_objective(
            &harness_contract::agent_action::root_objective_id(&session_id, &turn_id),
        );
        eprintln!("recovery188 admitted session={session_id} request={request_id} graph={graph_id} turn={turn_id} terminal={terminal_id} program={program_id}");
        let resolution = hold_resolution.then(|| {
            runtime
                .execution_supervisor()
                .observe_verdict_resolution_for_test(&program_id)
                .unwrap()
        });
        root.tool("recovery-team", "team_create", json!({
            "name":"Recovery production team", "mission":"Produce and independently verify the requested bounded file",
        }), None).await;
        let team = durable_turn_until(services, &graph_id, &program_id, "team_created", || {
            runtime
                .agent_action_service()
                .project_if_exists(&program_id)
                .unwrap()
                .and_then(|program| program.teams.keys().next().cloned())
        })
        .await;
        root.tool("recovery-producer", "agent_invite", json!({
            "team_ref":team,"role":"File producer","mission":"Produce exactly the requested file and report",
            "required_capabilities":["read","write"],"definition_ref":producer_definition,
        }), None).await;
        root.tool("recovery-reviewer", "agent_invite", json!({
            "team_ref":team,"role":"Independent verifier","mission":"Read the actual file and report, accept and review user_intent",
            "required_capabilities":["read"],"definition_ref":reviewer_definition,
        }), None).await;
        root.tool("recovery-task", "task_publish", json!({
            "team_ref":team,"title":"Write bounded recovery result",
            "objective":"Write recovery-effect.txt containing recovery-effect-once and submit a report",
            "acceptance":"The exact file and report must be independently read",
            "required_capabilities":["write","read"],
        }), None).await;
        let task_ref =
            durable_turn_until(services, &graph_id, &program_id, "task_published", || {
                runtime
                    .agent_action_service()
                    .project_if_exists(&program_id)
                    .unwrap()
                    .and_then(|program| program.tasks.keys().next().cloned())
            })
            .await;
        root.tool(
            "recovery-durable-wait",
            "state_inspect",
            json!({
                "scope_ref":program_id,"wait_for_workers":true,
            }),
            None,
        )
        .await;
        durable_turn_until(services, &graph_id, &program_id, "program_wait_external", || {
            let graph = runtime.graph_state_store().load(&graph_id).unwrap();
            graph.nodes.iter().find(|node| node.executor_kind == "agentic_program_wait"
                && graph.node_statuses.get(&node.id) == Some(&harness_contract::execution_graph::ExecutionNodeStatus::WaitingExternal))
                .map(|node| node.id.clone())
        }).await;
        producer
            .tool(
                "recovery-claim",
                "task_claim",
                json!({"task_ref":task_ref,"reason":"Accept exact bounded Task"}),
                None,
            )
            .await;
        let producer_exit_hold = if hold_producer_until_a {
            let packet = durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "producer_exact_attempt",
                || {
                    let program = runtime.agent_action_service().project(&program_id).unwrap();
                    let producer_graph = program.tasks[&task_ref].claim_execution_id.as_deref()?;
                    let graph = runtime.graph_state_store().load(producer_graph).unwrap();
                    let node = graph.nodes.iter().find(|node| {
                        node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    })?;
                    let packet: harness_contract::agent::AgentTaskPacket =
                        serde_json::from_str(&node.payload_ref).unwrap();
                    assert_eq!(
                        graph.recovery_cursor.node_attempts.get(&node.id),
                        Some(&packet.attempt)
                    );
                    Some(packet)
                },
            )
            .await;
            let hold = runtime::InProcessAgentWorker::hold_exit_for_test(&packet).unwrap();
            eprintln!(
                "native_exit_hold registered graph={} node={} attempt={} run={}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt,
                packet.run_id()
            );
            Some((packet, hold))
        } else {
            None
        };
        producer.tool("recovery-write", "edit", json!({
            "action":"write","path":"recovery-effect.txt","content":"recovery-effect-once\n",
        }), None).await;
        producer
            .tool(
                "recovery-producer-read",
                "read_file",
                json!({"path":"recovery-effect.txt","complete":true}),
                None,
            )
            .await;
        producer.tool("recovery-submit", "task_submit", json!({
            "task_ref":task_ref,"deliverable":{"block_index":0,"kind":"report","title":"Verified physical recovery result"},
        }), Some("The actual recovery-effect.txt was written once with recovery-effect-once and read back completely. This report records that bounded physical result; an independent verifier must inspect both it and the file.")).await;
        let submitted =
            durable_turn_until(services, &graph_id, &program_id, "task_submitted", || {
                let program = runtime.agent_action_service().project(&program_id).unwrap();
                let task = &program.tasks[&task_ref];
                (task.status == runtime::AgenticTaskStatus::Submitted)
                    .then(|| (program.clone(), task.artifact_refs.clone()))
            })
            .await;
        let producer_graph = submitted.0.tasks[&task_ref]
            .claim_execution_id
            .as_deref()
            .unwrap();
        for call_id in ["recovery-claim", "recovery-submit"] {
            assert_delegated_control_primary(
                services,
                &graph_id,
                &program_id,
                &session_id,
                call_id,
                producer_graph,
            )
            .await;
        }
        if let Some((packet, hold)) = &producer_exit_hold {
            tokio::time::timeout(Duration::from_secs(30), hold.entered())
                .await
                .expect("real native backend did not enter exit hold")
                .unwrap();
            assert!(!hold.is_released() && !hold.has_exited());
            assert!(services
                .topology
                .runtime_event_store
                .event_by_idempotency_key(
                    &format!("agent:{}", packet.agent_id()),
                    &format!("agent.local_exit:{}", packet.run_id())
                )
                .unwrap()
                .is_none());
            eprintln!(
                "native_exit_hold entered graph={} node={} attempt={} run={}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt,
                packet.run_id()
            );
        }
        if !hold_producer_until_a {
            // TaskExecute closes on Submitted through the production owner;
            // no additional provider request is required for physical exit.
            durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "normal_producer_physically_finished",
                || {
                    let graph = runtime.graph_state_store().load(producer_graph).unwrap();
                    graph
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal())
                        .then_some(())
                },
            )
            .await;
        }
        let result_refs = submitted.1;
        assert!(!result_refs.is_empty());
        let content_ref = submitted.0.artifacts[&result_refs[0]].content_ref.clone();
        reviewer
            .tool(
                "recovery-review-result",
                "evidence_retrieve",
                json!({"evidence_ref":content_ref}),
                None,
            )
            .await;
        reviewer
            .tool(
                "recovery-review-file",
                "read_file",
                json!({"path":"recovery-effect.txt","complete":true}),
                None,
            )
            .await;
        let read_request = reviewer.next().await;
        let read_message = read_request.body["messages"].as_array().into_iter().flatten().rev()
            .find(|message| message["role"] == "tool"
                && message["tool_call_id"] == "recovery-review-result");
        let read_wire_metadata = recovery_evidence_result_metadata(read_message);
        let read_store = &services.topology.runtime_event_store;
        let read_calls = services.physical_calls.lock().unwrap().iter()
            .filter(|call| call.tool_use_id == "recovery-review-result"
                && call.tool_name == "evidence_retrieve")
            .cloned().collect::<Vec<_>>();
        let mut read_acquisition_runs = Vec::new();
        let mut read_originals = Vec::new();
        for call in read_calls.iter().take(4) {
            let primary_lookup = read_store.event_by_idempotency_key(
                &format!("execution-effect:{}", call.idempotency_key),
                &format!("{}:read-receipt", call.idempotency_key),
            );
            let primary_lookup_failed = primary_lookup.is_err();
            let primary = primary_lookup.ok().flatten();
            let index_address = primary.as_ref().and_then(|primary| {
                let seal = &primary.payload["read_acquisition_v1"];
                Some((seal["index_stream"].as_str()?.to_string(), seal["index_key"].as_str()?.to_string()))
            }).or_else(|| {
                let parent = call.parent_execution.as_ref()?;
                Some((format!("execution-agent-receipts:{}:{}:{}", parent.execution_id, parent.node_id, call.attempt?),
                    format!("agent-tool-receipt:{}", call.idempotency_key)))
            });
            let index_lookup = index_address.as_ref().map(|(stream, key)|
                read_store.event_by_idempotency_key(stream, key));
            let index_lookup_failed = index_lookup.as_ref().is_some_and(Result::is_err);
            let index = index_lookup.and_then(Result::ok).flatten();
            if let Some(run) = primary.as_ref().and_then(|primary|
                primary.payload["read_acquisition_v1"]["step"]["run_ref"].as_str()) {
                read_acquisition_runs.push(run.to_string());
            }
            let packet = call.parent_execution.as_ref().and_then(|parent| {
                let graph = runtime.graph_state_store().load(&parent.execution_id).ok()?;
                let node = graph.nodes.iter().find(|node| node.id == parent.node_id)?;
                let packet = serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref).ok()?;
                Some(json!({"graph_id":packet.graph_id(), "node_id":packet.node_id(),
                    "run_id":packet.run_id(), "attempt":packet.attempt,
                    "execution_identity":packet.assignment.execution_identity,
                    "tool_allowed":packet.allowed_tools.contains(&call.tool_name),
                    "tool_bound":packet.binding.as_ref().is_some_and(|binding|
                        binding.tool_contract_refs.contains(&call.tool_name))}))
            });
            read_originals.push(json!({
                "call_id":call.tool_use_id, "idempotency_key":call.idempotency_key,
                "parent_execution":call.parent_execution, "attempt":call.attempt,
                "primary_lookup_failed":primary_lookup_failed,
                "primary":primary.as_ref().map(recovery_read_event_metadata),
                "index_address":index_address, "index_lookup_failed":index_lookup_failed,
                "index":index.as_ref().map(recovery_read_event_metadata), "packet":packet,
            }));
        }
        let delivery_lookup = read_store.latest_for_stream_kind(
            &format!("session:{session_id}"), "context.model_input_observed");
        let delivery_lookup_failed = delivery_lookup.is_err();
        let delivery = delivery_lookup.ok().flatten();
        let delivery_matches_call = delivery.as_ref().is_some_and(|event|
            event.payload["tool_invocation_id"] == "recovery-review-result"
                && event.payload["session_id"] == session_id);
        let delivery_matches_original_primary = delivery.as_ref().is_some_and(|event|
            read_originals.iter().any(|original| original["primary"]["event_id"].is_string()
                && original["primary"]["event_id"] == event.payload["acquisition_event_id"]
                && original["index"]["event_id"] == event.payload["index_event_id"]));
        let task_contents = result_refs.iter().map(|reference| {
            let Some(artifact) = submitted.0.artifacts.get(reference) else {
                return json!({"logical_ref":reference,"logical_mapping_missing":true});
            };
            let selector = &artifact.content_ref;
            let metadata = runtime.artifact_store().resolve(selector);
            json!({"logical_ref":reference,"content_ref":selector,
                "metadata_lookup_failed":metadata.is_err(), "content":metadata.ok()})
        }).collect::<Vec<_>>();
        eprintln!("recovery_result_read_metadata {}", json!({
            "capture_phase":"pre_task_review_provider_guard", "session_id":session_id,
            "task_ref":task_ref,"program_id":program_id,
            "capture_commit_cursor":read_store.current_commit_cursor(),
            "wire_tool_result":read_wire_metadata, "task_contents":task_contents,
            "recorded_call_count":read_calls.len(), "captured_call_limit":4,
            "originals":read_originals,"delivery_lookup_failed":delivery_lookup_failed,
            "delivery_lookup":"latest_native_at_pre_review_guard_not_exhaustive",
            "delivery_matches_call":delivery_matches_call,
            "delivery_matches_original_primary_index":delivery_matches_original_primary,
            "latest_delivery_call_id":delivery.as_ref().map(|event| &event.payload["tool_invocation_id"]),
            "delivery":delivery.as_ref().filter(|_| delivery_matches_call).map(|event| json!({
                "event":recovery_read_event_metadata(event), "material":event.payload,
            })),
        }));
        eprintln!("recovery188 scripted_tool lane={} id=recovery-task-review name=task_review", reviewer.marker);
        read_request.reply.send(json!({"role":"assistant", "content":null,
            "tool_calls":[{"index":0,"id":"recovery-task-review","type":"function","function":{
                "name":"task_review","arguments":json!({
            "task_ref":task_ref,"decision":"accept","reason":"I independently read the immutable report and actual file",
            "evidence_refs":[content_ref],
                }).to_string(),
            }}],
        })).unwrap();
        // The review call ends the reviewer's turn when the review is
        // accepted, so its outcome is read from the durable record. A
        // follow-up request only exists when the call failed and the model
        // still has to react to it, so the fixture waits for the record
        // instead of for a request that may never arrive.
        let review_outcome = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let found = ["tool.invocation.completed", "tool.invocation.failed", "tool.invocation.denied"]
                    .into_iter()
                    .filter_map(|kind| services.topology.runtime_event_store
                        .events_for_root_execution_kind(&graph_id, kind, None, 64).ok())
                    .flatten()
                    .find(|event| event.payload["tool_call_id"] == "recovery-task-review");
                if let Some(event) = found {
                    let selector = event.payload["runtime_raw_content_v1"]["selector"].as_str()
                        .expect("review outcome must retain its original raw content");
                    let artifact = runtime.artifact_store().resolve(selector).unwrap();
                    let bytes = runtime.artifact_store().read(&artifact, &artifact.visibility_scope, None).await.unwrap();
                    break Some(Value::String(String::from_utf8(bytes).unwrap()));
                }
                if std::time::Instant::now() >= deadline {
                    break None;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        let review_result = review_outcome.as_ref().and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| serde_json::to_string(value).ok())
        });
        let observation = review_result
            .as_deref()
            .and_then(|content| {
                serde_json::from_str::<Value>(&content).ok().or_else(|| {
                    let offset = content.find('{')?;
                    serde_json::Deserializer::from_str(&content[offset..])
                        .into_iter::<Value>()
                        .next()
                        .and_then(Result::ok)
                })
            })
            .filter(|value| {
                value["action"] == "task_review"
                    || value["error"]["code"].is_string()
                    || value["error"].is_string()
                    || value["code"].is_string()
                    || value["reason"].is_string()
            });
        // A refused review leaves the model with a tool result it still has
        // to react to, so the follow-up request is taken only in that case.
        // An accepted review ends the reviewer's turn, and there is nothing
        // left to answer.
        let review_request = if observation.as_ref().is_some_and(|value| {
            value["error"]["code"].is_string()
                || value["error"].is_string()
                || value["code"].is_string()
        }) {
            Some(reviewer.next().await)
        } else {
            None
        };
        let redact = |value: Option<&str>| {
            value.map(|text| {
                let mut redacted = Vec::new();
                let mut hide_next = false;
                for word in text.split_whitespace().take(128) {
                    let lowered = word.to_ascii_lowercase();
                    let sensitive = lowered.contains("://")
                        || lowered.starts_with("sk-")
                        || ["token", "secret", "password", "credential", "api_key"]
                            .iter()
                            .any(|marker| {
                                lowered.contains(&format!("{marker}="))
                                    || lowered.contains(&format!("{marker}:"))
                            });
                    redacted.push(if hide_next || sensitive {
                        "[redacted]"
                    } else {
                        word
                    });
                    hide_next = lowered == "bearer"
                        || (sensitive && (word.ends_with(':') || word.ends_with('=')));
                }
                redacted.join(" ").chars().take(512).collect::<String>()
            })
        };
        let review_code = observation.as_ref().and_then(|value| {
            value["error"]["code"].as_str().or_else(|| value["code"].as_str())
        });
        let review_reason = observation.as_ref().and_then(|value| {
            value["error"]["message"].as_str()
                .or_else(|| value["error"].as_str())
                .or_else(|| value["reason"].as_str())
        });
        let review_event = ["tool.invocation.completed", "tool.invocation.failed", "tool.invocation.denied"]
            .into_iter()
            .filter_map(|kind| services.topology.runtime_event_store
                .events_for_root_execution_kind(&graph_id, kind, None, 64).ok())
            .flatten()
            .find(|event| event.payload["tool_call_id"] == "recovery-task-review");
        eprintln!(
            "recovery_task_review_result {}",
            json!({
                "call_id":"recovery-task-review", "session_id":session_id,
                "task_ref":task_ref, "program_id":program_id, "root_execution_id":graph_id,
                "reviewer_lane":reviewer.marker,
                "reviewer_activity":review_event.as_ref().and_then(|event| event.activity_binding()),
                "result_read_same_run_as_reviewer":review_event.as_ref()
                    .and_then(|event| event.activity_binding())
                    .and_then(|binding| binding.agent_run_id)
                    .map(|run| !read_acquisition_runs.is_empty()
                        && read_acquisition_runs.iter().all(|original| original == &run)),
                "result_commit_cursor":review_event.as_ref().map(|event| event.commit_cursor),
                "capture_commit_cursor":services.topology.runtime_event_store.current_commit_cursor(),
                "tool_result_present":review_result.is_some(),
                "structured_result_present":observation.is_some(),
                "code":redact(review_code),
                "reason":redact(review_reason.or(review_result.as_deref())),
                "classification":if review_code.is_some() || review_reason.is_some() {
                    "structured_error"
                } else if observation.is_some() {
                    "action_observation"
                } else if review_result.is_some() {
                    "tool_result_plain_text"
                } else {
                    "missing_tool_result"
                },
                "action_id":redact(observation.as_ref().and_then(|value| value["action_id"].as_str())),
                "revision":observation.as_ref().and_then(|value| value["revision"].as_u64()),
            })
        );
        assert!(review_code.is_none() && review_reason.is_none(), "TaskReview refused: {:?}", redact(review_reason.or(review_result.as_deref())));
        durable_turn_until(
            services,
            &graph_id,
            &program_id,
            "task_independently_accepted",
            || {
                let program = runtime.agent_action_service().project(&program_id).unwrap();
                (program.tasks[&task_ref].status == runtime::AgenticTaskStatus::Accepted)
                    .then_some(())
            },
        )
        .await;
        let goal_id = format!("goal:{graph_id}");
        if let Some(review_request) = review_request {
            review_request
                .reply
                .send(json!({"role":"assistant","content":"The physical result and authored report are independently accepted."}))
                .unwrap();
        }
        let accepted = runtime.agent_action_service().project(&program_id).unwrap();
        let independent_agent = accepted.tasks[&task_ref].reviewed_by.clone().unwrap();
        assert_ne!(
            accepted.tasks[&task_ref].claimant.as_ref(),
            Some(&independent_agent)
        );
        root.tool("recovery-review-team", "team_create", json!({
            "name":"Independent criterion verification", "mission":"Settle only the original user_intent criterion",
        }), None).await;
        let review_team = durable_turn_until(
            services,
            &graph_id,
            &program_id,
            "review_team_created",
            || {
                let program = runtime.agent_action_service().project(&program_id).unwrap();
                program
                    .teams
                    .keys()
                    .find(|reference| **reference != team)
                    .cloned()
            },
        )
        .await;
        root.tool("recovery-review-membership", "agent_invite", json!({
            "team_ref":review_team,"role":"Independent verifier","mission":"Read the accepted result and settle user_intent",
            "required_capabilities":["read"],"existing_agent_ref":independent_agent,
        }), None).await;
        root.tool("recovery-review-control", "task_publish", json!({
            "team_ref":review_team,"title":"Independent criterion verification",
            "objective":"Claim, independently read the accepted report and physical file, then objective_review user_intent",
            "acceptance":"Record the actual independent criterion verdict without creating another deliverable",
            "required_capabilities":["read"],"purpose":"exploration","depends_on":[task_ref],
        }), None).await;
        let control_ref = durable_turn_until(
            services,
            &graph_id,
            &program_id,
            "review_control_published",
            || {
                let program = runtime.agent_action_service().project(&program_id).unwrap();
                program
                    .tasks
                    .keys()
                    .find(|reference| **reference != task_ref)
                    .cloned()
            },
        )
        .await;
        reviewer
            .tool(
                "recovery-goal-claim",
                "task_claim",
                json!({"task_ref":control_ref}),
                None,
            )
            .await;
        let input = services
            .session
            .runtime_input(&request_id)
            .await
            .unwrap()
            .unwrap();
        let observer = observe(RequestIdentity {
            session_id: session_id.clone(),
            request_id: request_id.clone(),
            turn_id: turn_id.clone(),
            generation: input.session_generation,
            sequence: input.sequence as u64,
            terminal_id: terminal_id.clone(),
            execution_id: graph_id.clone(),
        })
        .unwrap();
        reviewer
            .tool(
                "recovery-goal-read",
                "evidence_retrieve",
                json!({"evidence_ref":content_ref}),
                None,
            )
            .await;
        reviewer
            .tool(
                "recovery-goal-file",
                "read_file",
                json!({"path":"recovery-effect.txt","complete":true}),
                None,
            )
            .await;
        let held_optional = if let Some(optional) = optional {
            std::fs::write(
                runtime.workspace_root().join("recovery-optional.txt"),
                "initial separate target\n",
            )
            .unwrap();
            let definition = install_native_definition(
                services,
                "recovery/optional",
                optional,
                &[
                    harness_contract::agent::AgentCapability::Read,
                    harness_contract::agent::AgentCapability::Write,
                ],
            );
            root.tool("recovery-optional-team", "team_create", json!({
                "name":"Optional disjoint exploration","mission":"Only the separate optional file, never the required result",
            }), None).await;
            let optional_team = durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "optional_team_created",
                || {
                    let program = runtime.agent_action_service().project(&program_id).unwrap();
                    program
                        .teams
                        .keys()
                        .find(|reference| **reference != team && **reference != review_team)
                        .cloned()
                },
            )
            .await;
            root.tool("recovery-optional-agent", "agent_invite", json!({
                "team_ref":optional_team,"role":"Optional explorer","mission":"Write only recovery-optional.txt",
                "required_capabilities":["write"],"definition_ref":definition,
            }), None).await;
            root.tool("recovery-optional-task", "task_publish", json!({
                "team_ref":optional_team,"title":"Optional separate observation","purpose":"exploration",
                "objective":"Write only recovery-optional.txt, which is unrelated to the required deliverable",
                "acceptance":"Retire explicitly without a required result","required_capabilities":["write"],
            }), None).await;
            let optional_task = durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "optional_task_published",
                || {
                    let program = runtime.agent_action_service().project(&program_id).unwrap();
                    program
                        .tasks
                        .keys()
                        .find(|reference| **reference != task_ref && **reference != control_ref)
                        .cloned()
                },
            )
            .await;
            let (entered, observed) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            assert!(services
                .physical_holds
                .lock()
                .unwrap()
                .insert(
                    "recovery-optional-write".into(),
                    PhysicalHold { entered, released }
                )
                .is_none());
            optional
                .tool(
                    "recovery-optional-claim",
                    "task_claim",
                    json!({"task_ref":optional_task}),
                    None,
                )
                .await;
            optional.tool("recovery-optional-write", "edit", json!({
                "action":"write","path":"recovery-optional.txt","content":"optional-physical-result\n",
            }), None).await;
            let request = tokio::time::timeout(Duration::from_secs(30), async {
                tokio::select! {
                    request = observed => request.unwrap(),
                    continuation = optional.next() => {
                        let parent = services.physical_calls.lock().unwrap().iter()
                            .find(|call| call.tool_use_id == "recovery-optional-claim")
                            .and_then(|call| call.parent_execution.clone());
                        let packet = parent.and_then(|parent| {
                            let graph = runtime.graph_state_store().load(&parent.execution_id).ok()?;
                            let node = graph.nodes.iter().find(|node| node.id == parent.node_id)?;
                            serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref).ok()
                        });
                        let authority = packet.map(|packet| json!({
                            "allowed_tools":packet.allowed_tools,
                            "permission_ceiling":packet.permission_ceiling,
                            "resource_scopes":packet.resource_scopes,
                            "agentic_binding":packet.agentic_binding,
                            "definition":packet.binding.as_ref().map(|binding| &binding.definition_ref),
                            "capabilities":packet.binding.as_ref().map(|binding| &binding.effective_capabilities),
                        }));
                        let result = continuation.body["messages"].as_array()
                            .into_iter().flatten().rev()
                            .find(|message| message["role"] == "tool"
                                && message["tool_call_id"] == "recovery-optional-write");
                        panic!("optional write continued before physical hold: authority={authority:?} result={result:?}");
                    }
                }
            })
                .await
                .unwrap();
            assert_eq!(request.tool_use_id, "recovery-optional-write");
            assert_eq!(
                std::fs::read(runtime.workspace_root().join("recovery-optional.txt")).unwrap(),
                b"optional-physical-result\n"
            );
            root.tool(
                "recovery-optional-retire",
                "task_withdraw",
                json!({
                    "task_ref":optional_task,"reason_ref":content_ref,"evidence_refs":[content_ref],
                }),
                None,
            )
            .await;
            root.wait_for_tool_result("recovery-optional-retire").await;
            let program = runtime.agent_action_service().project(&program_id).unwrap();
            assert_eq!(
                program.tasks[&optional_task].status,
                runtime::AgenticTaskStatus::CancelRequested
            );
            assert!(
                !program.tasks[&optional_task].active_attempts.is_empty(),
                "a committed withdrawal must still retain the held physical attempt"
            );
            Some((optional_task, request, release))
        } else {
            None
        };
        reviewer.tool("recovery-objective-review", "objective_review", json!({
            "criterion_ref":"user_intent","decision":"satisfied","result_refs":result_refs,
            "evidence_refs":[content_ref],"reason":"The required physical file and submitted report were independently inspected",
        }), None).await;
        reviewer.finish("The physical result and authored report are independently verified for user_intent.").await;
        root.tool(
            "recovery-review-retire",
            "task_withdraw",
            json!({
                "task_ref":control_ref,"reason_ref":content_ref,"evidence_refs":[content_ref],
            }),
            None,
        )
        .await;
        durable_turn_until(
            services,
            &graph_id,
            &program_id,
            "review_control_physically_retired",
            || {
                let program = runtime.agent_action_service().project(&program_id).unwrap();
                (program.tasks[&control_ref].status == runtime::AgenticTaskStatus::Withdrawn
                    && program.tasks[&control_ref].active_attempts.is_empty())
                .then_some(())
            },
        )
        .await;
        let satisfied = durable_turn_until(
            services,
            &graph_id,
            &program_id,
            "durable_a_satisfied",
            || {
                runtime.goal_store().get(&goal_id).unwrap().filter(|goal| {
                    goal.completion == harness_contract::goal::GoalCompletion::Satisfied
                })
            },
        );
        let completion_request = root.tool(
            "recovery-complete",
            "objective_complete_request",
            json!({
                "result_refs":result_refs,"evidence_refs":[content_ref],"unresolved":[],
            }),
            None,
        );
        // The original Program/Goal owner can satisfy A and enter B without
        // another Provider RPC. An already satisfied Goal must not wait for an
        // unnecessary scripted command; a still-open Goal may consume it.
        let goal = await_satisfaction_or_offer_completion(satisfied, completion_request).await;
        let program = runtime.agent_action_service().project(&program_id).unwrap();
        assert_eq!(program.status, runtime::AgenticProgramStatus::Verified);
        let mut optional_receipts = Vec::new();
        if let Some((optional_task, request, release)) = held_optional {
            let task = &program.tasks[&optional_task];
            assert_eq!(task.status, runtime::AgenticTaskStatus::CancelRequested);
            assert!(!task.active_attempts.is_empty());
            let pending = services
                .topology
                .runtime_event_store
                .list_stream(&receipt_owner::stream(
                    request.parent_execution.as_ref().unwrap(),
                    request.parent_execution_attempt.unwrap(),
                ))
                .unwrap();
            let intent = pending
                .iter()
                .find(|event| {
                    event.kind == "execution.agent_tool.intent"
                        && event.payload["idempotency_key"] == request.idempotency_key
                })
                .unwrap();
            assert!(!intent.payload["resolved_effect_scope"].is_null());
            let admitted_scope: harness_contract::goal::ProofEffectScope =
                serde_json::from_value(intent.payload["resolved_effect_scope"].clone()).unwrap();
            let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
            assert_eq!(admitted_scope.version, 2);
            admitted_scope.validate_purpose_mapping(descriptor).unwrap();
            assert!(admitted_scope
                .scope_purposes
                .iter()
                .any(|entry| entry.purpose
                    == harness_contract::goal::ProofEffectPurpose::BusinessEffect));
            assert!(!pending
                .iter()
                .any(|event| event.kind == "execution.agent_tool.receipt"
                    && event.payload["idempotency_key"] == request.idempotency_key));
            let effect = &services
                .topology
                .runtime_event_store
                .list_stream(&format!("execution-effect:{}", request.idempotency_key))
                .unwrap();
            assert!(effect
                .iter()
                .any(|event| event.kind == "execution.effect.intent"));
            assert!(!effect
                .iter()
                .any(|event| event.kind == "execution.effect.receipt"));
            release.send(()).unwrap();
            optional_receipts = durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "optional_effect_receipted",
                || {
                    let receipts = services
                        .topology
                        .runtime_event_store
                        .list_stream(&receipt_owner::stream(
                            request.parent_execution.as_ref().unwrap(),
                            request.parent_execution_attempt.unwrap(),
                        ))
                        .unwrap()
                        .into_iter()
                        .filter(|event| {
                            event.kind == "execution.agent_tool.receipt"
                                && event.payload["idempotency_key"] == request.idempotency_key
                        })
                        .collect::<Vec<_>>();
                    (receipts.len() == 1).then_some(receipts)
                },
            )
            .await;
            assert_eq!(
                optional_receipts[0].payload["outcome"]["status"],
                "executed"
            );
            assert_eq!(
                optional_receipts[0].payload["resolved_effect_scope"],
                intent.payload["resolved_effect_scope"]
            );
            durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "optional_attempt_physically_retired",
                || {
                    let current = runtime.agent_action_service().project(&program_id).unwrap();
                    let task = &current.tasks[&optional_task];
                    let graph = runtime
                        .graph_state_store()
                        .load(
                            request
                                .parent_execution
                                .as_ref()
                                .unwrap()
                                .execution_id
                                .as_str(),
                        )
                        .unwrap();
                    (task.status == runtime::AgenticTaskStatus::Withdrawn
                        && task.active_attempts.is_empty()
                        && graph
                            .node_statuses
                            .values()
                            .all(|status| status.is_terminal()))
                    .then_some(())
                },
            )
            .await;
        }
        if hold_producer_until_a {
            let producer_graph = runtime
                .graph_state_store()
                .load(
                    program.tasks[&task_ref]
                        .claim_execution_id
                        .as_deref()
                        .unwrap(),
                )
                .unwrap();
            assert!(
                producer_graph
                    .node_statuses
                    .values()
                    .any(|status| !status.is_terminal()),
                "A must be real while its already-receipted producer is still physically alive"
            );
            let (packet, hold) = producer_exit_hold.as_ref().unwrap();
            assert!(
                !hold.is_released(),
                "A must precede the explicit physical release"
            );
            assert!(
                !hold.has_exited(),
                "the real backend future must still own the hold at A"
            );
            assert!(
                services
                    .topology
                    .runtime_event_store
                    .event_by_idempotency_key(
                        &format!("agent:{}", packet.agent_id()),
                        &format!("agent.local_exit:{}", packet.run_id())
                    )
                    .unwrap()
                    .is_none(),
                "Graph status is not proof of physical liveness"
            );
            hold.release();
            tokio::time::timeout(Duration::from_secs(30), hold.exited())
                .await
                .expect("native backend exit hold did not close")
                .unwrap();
            durable_turn_until(
                services,
                &graph_id,
                &program_id,
                "producer_original_exit_and_graph_closure",
                || {
                    let exit = services
                        .topology
                        .runtime_event_store
                        .event_by_idempotency_key(
                            &format!("agent:{}", packet.agent_id()),
                            &format!("agent.local_exit:{}", packet.run_id()),
                        )
                        .unwrap()?;
                    assert_eq!(exit.actor.as_deref(), Some("agent_runtime"));
                    assert_eq!(exit.payload["graph_id"], packet.graph_id());
                    assert_eq!(exit.payload["node_id"], packet.node_id());
                    assert_eq!(exit.payload["run_id"], packet.run_id());
                    assert_eq!(exit.payload["attempt"], packet.attempt);
                    assert_eq!(exit.payload["agent_id"], packet.agent_id());
                    assert_eq!(exit.payload["backend"], "in_process");
                    assert_eq!(exit.payload["phase"], "local_returned");
                    use sha2::Digest;
                    assert_eq!(
                        exit.payload["packet_sha256"],
                        format!(
                            "{:x}",
                            sha2::Sha256::digest(serde_json::to_vec(packet).unwrap())
                        )
                    );
                    let graph = runtime.graph_state_store().load(packet.graph_id()).unwrap();
                    graph
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal())
                        .then_some(())
                },
            )
            .await;
            eprintln!(
                "native_exit_hold exited graph={} node={} attempt={} run={}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt,
                packet.run_id()
            );
        }
        assert!(!goal.reviews.is_empty());
        assert!(goal
            .reviews
            .iter()
            .any(|review| review.verification.is_some()));
        for review in &goal.reviews {
            let verification = review.verification.as_ref().unwrap();
            assert!(verification.independence_required);
            assert!(!verification.reviewer_execution_id.is_empty());
            assert!(!verification.producer_execution_ids.is_empty());
            assert!(!verification
                .producer_execution_ids
                .contains(&verification.reviewer_execution_id));
            assert_eq!(verification.goal_spec_revision, Some(goal.spec_revision));
            assert_eq!(
                verification.goal_spec_digest.as_deref(),
                Some(goal.spec_digest.as_str())
            );
            let inputs = verification.input_set.as_ref().unwrap();
            assert_eq!(inputs.version, 2);
            assert!(inputs.required_scope_complete);
            assert!(!inputs.required_scopes.is_empty());
            assert!(
                inputs
                    .required_scopes
                    .iter()
                    .all(|scope| scope.path.workspace_relative_path == "recovery-effect.txt"),
                "dependency manifest must reflect the actual complete producer read, not read:."
            );
            assert_eq!(
                services
                    .physical_calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|call| call.tool_use_id == "recovery-producer-read"
                        && call.tool_name == "read_file"
                        && serde_json::from_str::<serde_json::Value>(&call.input).unwrap()
                            ["complete"]
                            == true)
                    .count(),
                1
            );
            assert!(!inputs.material_events.is_empty());
            assert!(!verification.reads.is_empty());
            for read in &verification.reads {
                assert!(!read.receipt_refs.is_empty());
                assert!(!read.sha256.is_empty());
                assert!(read.bytes > 0);
            }
        }
        let writes = services
            .physical_calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.tool_use_id == "recovery-write")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(writes.len(), 1);
        let effect_receipts =
            receipt_owner::original_receipts(&services.topology.runtime_event_store, &writes[0])
                .unwrap();
        assert_eq!(
            effect_receipts.len(),
            1,
            "one real physical write receipt, not zero-effect nonduplication"
        );
        assert_eq!(effect_receipts[0].payload["outcome"]["status"], "executed");
        assert_eq!(effect_receipts[0].payload["outcome"]["tool_name"], "edit");
        assert_eq!(writes[0].tool_use_id, "recovery-write");
        assert_eq!(
            effect_receipts[0].payload["idempotency_key"],
            writes[0].idempotency_key
        );
        assert!(writes[0].parent_execution.is_some());
        assert!(writes[0].attempt.is_some());
        let review_events = review_events(&services.topology.runtime_event_store, &program_id);
        assert_eq!(review_events.len(), 2);
        assert_eq!(
            std::fs::read(runtime.workspace_root().join("recovery-effect.txt")).unwrap(),
            b"recovery-effect-once\n"
        );
        Self {
            session_id,
            request_id,
            graph_id,
            turn_id,
            terminal_id,
            program_id,
            goal,
            program,
            input,
            observer,
            effect_receipts,
            review_events,
            physical_calls: services.physical_calls.clone(),
            optional_receipts,
            resolution,
            candidate,
            commit_loss,
        }
    }
}

async fn assert_delegated_control_primary(
    services: &RecoveryServices,
    graph_id: &str,
    program_id: &str,
    session_id: &str,
    call_id: &str,
    producer_graph: &str,
) {
    let runtime = services.runtime.runtime_services();
    let original = durable_turn_until(
        services,
        graph_id,
        program_id,
        "original_control_receipt",
        || {
            services
                .topology
                .runtime_event_store
                .list_stream(&format!("session:{session_id}"))
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "tool.invocation.completed"
                        && event.payload["tool_call_id"] == call_id
                })
        },
    )
    .await;
    let events = services
        .topology
        .runtime_event_store
        .list_stream(&format!("session:{session_id}"))
        .unwrap();
    let tasks = events
        .iter()
        .filter(|event| {
            event.kind == "tool.execution_plan.created"
                && event.payload["plan_id"] == original.payload["governed_plan_id"]
                && event.payload["revision"] == original.payload["governed_plan_revision"]
        })
        .flat_map(|event| event.payload["tasks"].as_array().unwrap())
        .filter(|task| task["tool_call_id"] == call_id)
        .collect::<Vec<_>>();
    assert_eq!(tasks.len(), 1);
    let step: runtime::step_snapshot::StepCallBinding =
        serde_json::from_value(tasks[0]["step_binding"].clone()).unwrap();
    let key = step.effect_idempotency_key();
    let stream = format!("execution-effect:{key}");
    let primary = services
        .topology
        .runtime_event_store
        .event_by_idempotency_key(&stream, &format!("{key}:receipt"))
        .unwrap()
        .unwrap();
    let intent = services
        .topology
        .runtime_event_store
        .event_by_idempotency_key(&stream, &format!("{key}:intent"))
        .unwrap()
        .unwrap();
    let producer = runtime.graph_state_store().load(producer_graph).unwrap();
    let nodes = producer
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 1);
    let node = nodes[0];
    let attempt = producer.recovery_cursor.node_attempts[&node.id];
    let parent = json!({"execution_id":producer.id,"node_id":node.id});
    assert_eq!(primary.payload["replay"]["parent_execution"], parent);
    assert_eq!(intent.payload["replay"]["parent_execution"], parent);
    assert!(primary.payload["root_effect_owner"].is_null());
    assert!(intent.payload["root_effect_owner"].is_null());
    assert_eq!(
        original.activity_binding().unwrap().agent_run_id,
        Some(format!("run:{}", producer.id))
    );
    let index_stream = format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        producer.id, node.id
    );
    let index = services
        .topology
        .runtime_event_store
        .event_by_idempotency_key(&index_stream, &format!("agent-tool-receipt:{key}"))
        .unwrap()
        .unwrap();
    assert_eq!(index.transaction_id, primary.transaction_id);
    assert_eq!(index.commit_cursor, primary.commit_cursor);
    assert_eq!(index.payload["outcome"], primary.payload["outcome"]);
    assert_eq!(
        index.payload["resolved_effect_scope"],
        primary.payload["resolved_effect_scope"]
    );
}

async fn durable_turn_until<T>(
    services: &RecoveryServices,
    graph_id: &str,
    program_id: &str,
    phase: &str,
    mut read: impl FnMut() -> Option<T>,
) -> T {
    eprintln!("recovery188 wait phase={phase} graph={graph_id} program={program_id}");
    let outcome = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(value) = read() {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match outcome {
        Ok(value) => {
            eprintln!("recovery188 reached phase={phase} graph={graph_id}");
            value
        }
        Err(error) => {
            let retained = services.topology.preserve_failed_test_namespace();
            eprintln!("recovery188 retained_namespace={}", json!({
                "schema":retained, "status":if retained.is_some() {"Retained"} else {"Unavailable"},
                "owner":"candidate test runner; explicit external evidence collection then schema cleanup",
                "owner_pid":std::process::id(), "graph_id":graph_id, "program_id":program_id,
                "scope":"only this failed fixture namespace", "phase":phase,
                "cleanup":retained.map(|schema| format!("After the owning test process has exited and external evidence is saved, run on its isolated COWD_TEST_POSTGRES_URL database: DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;")),
            }));
            let program_stream = format!("agentic-program:{program_id}");
            let goal_stream = format!("goal:goal:{graph_id}");
            let sources = [graph_id, program_stream.as_str(), goal_stream.as_str()]
                .map(|stream_id| runtime::failure_evidence::FailureEvidenceSource {
                    stream_id, after_sequence:0, through_sequence:u64::MAX,
                });
            let diagnostic = runtime::failure_evidence::collect_failure_evidence(
                &services.topology.runtime_event_store, &sources,
                "durable business oracle did not converge within 120 seconds",
            );
            eprintln!("recovery188 failure_diagnostic={diagnostic}");
            panic!("durable business oracle did not converge phase={phase} graph={graph_id} program={program_id}: {error:?}; recovery188 diagnostics precede this failure");
        }
    }
}


#[tokio::test]
async fn r1_failure_evidence_pg_unavailable_preserves_only_failed_namespace() {
    for preserve in [false, true] {
        let root = fixture_root();
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let topology = SelectedStorageTopology::compose_for_test(&root.path().join("home"), &workspace).unwrap();
        let executor = topology.postgres_executor.clone();
        let schema: String = executor.checkout_online_read().unwrap()
            .query_one("SELECT current_schema()", &[]).unwrap().get(0);
        assert!(schema.starts_with("cowdtest_") && schema.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
        let report = runtime::failure_evidence::collect_failure_evidence(
            &topology.runtime_event_store,
            &[runtime::failure_evidence::FailureEvidenceSource {
                stream_id:"agent:fixture", after_sequence:0, through_sequence:u64::MAX,
            }], "controlled fixture failure",
        );
        assert_eq!(report["status"], "ArchivedDiagnostic");
        let archive = std::path::Path::new(report["ref"].as_str().unwrap());
        let captured: Value = serde_json::from_slice(&std::fs::read(archive).unwrap()).unwrap();
        assert_eq!(captured["sources"][0]["status"], "Unavailable");
        assert_eq!(captured["events_read"], 0);
        std::fs::remove_file(archive).unwrap();
        if preserve {
            assert_eq!(topology.preserve_failed_test_namespace(), Some(schema.as_str()));
        }
        drop(topology);
        let mut connection = executor.checkout_online_read().unwrap();
        let exists: bool = connection.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", &[&schema],
        ).unwrap().get(0);
        if exists {
            connection.batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE")).unwrap();
        }
        assert_eq!(exists, preserve);
    }
}

async fn drive_final_until<T>(
    root: &mut LoopbackProvider,
    future: impl std::future::Future<Output = T>,
) -> T {
    tokio::pin!(future);
    tokio::time::timeout(Duration::from_secs(120), async {
        let mut rounds = 0;
        loop {
            tokio::select! {
                result = &mut future => break result,
                request = root.requests.recv() => {
                    let request = request.unwrap();
                    if request.reply.is_closed() { continue; }
                    assert!(task_route_control_reply(&request.body)
                        .unwrap_or_else(|reason| panic!("recovery206 terminal control RPC: {reason}"))
                        .is_none(), "unexpected late TaskRouter control RPC at terminal delivery");
                    rounds += 1;
                    assert!(rounds <= 8, "the real root did not converge after its accepted proof");
                    let _ = request.reply.send(json!({"role":"assistant",
                        "content":"The independently inspected report is complete. recovery-effect.txt contains recovery-effect-once; the original accepted report and physical read receipts are the evidence. No unresolved requirement remains."
                    }));
                }
            }
        }
    }).await.expect("production terminal caller did not reach the armed fault")
}

impl ReviewedTurn {
    async fn delivery_barrier(
        &mut self,
        services: &RecoveryServices,
        root: &mut LoopbackProvider,
        point: Point,
    ) -> (
        runtime::RuntimeSessionOutboxRecord,
        crate::session_runtime_bridge::terminal_delivery_faults::Barrier,
    ) {
        let mut foreign_observers = Vec::new();
        for field in [
            "session",
            "request",
            "turn",
            "generation",
            "sequence",
            "terminal",
            "execution",
        ] {
            let mut identity = self.request_identity();
            match field {
                "session" => identity.session_id.push_str(":foreign"),
                "request" => identity.request_id.push_str(":foreign"),
                "turn" => identity.turn_id.push_str(":foreign"),
                "generation" => identity.generation += 1,
                "sequence" => identity.sequence += 1,
                "terminal" => identity.terminal_id.push_str(":foreign"),
                _ => identity.execution_id.push_str(":foreign"),
            }
            foreign_observers.push(observe(identity).unwrap());
        }
        let mut observations = 0;
        let (record, worker) = loop {
            observations += 1;
            assert!(
                observations <= 8,
                "production adoption did not converge to its current exact Session claim"
            );
            let (record, worker) = drive_final_until(root, &mut self.observer.observed)
                .await
                .unwrap();
            let input = services
                .session
                .runtime_input(&self.request_id)
                .await
                .unwrap()
                .unwrap();
            if record.input_claim_revision == input.claim_fence_epoch
                && record.input_claim_token == input.claim_token
                && record.input_claim_owner == input.claim_owner
            {
                break (record, worker);
            }
            let replacement = observe(self.request_identity()).unwrap();
            let mut stale = std::mem::replace(&mut self.observer, replacement);
            assert_ne!(record.input_claim_revision, input.claim_fence_epoch);
            stale.bind().unwrap();
        };
        assert_eq!(record.terminal_id, self.terminal_id);
        assert_eq!(record.execution_id.as_deref(), Some(self.graph_id.as_str()));
        assert_eq!(record.request_id.as_deref(), Some(self.request_id.as_str()));
        assert_eq!(record.turn_id.as_deref(), Some(self.turn_id.as_str()));
        assert_eq!(
            record.session_generation,
            Some(self.input.session_generation)
        );
        let input = services
            .session
            .runtime_input(&self.request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.input_claim_revision, input.claim_fence_epoch);
        assert_eq!(record.input_claim_token, input.claim_token);
        assert_eq!(record.input_claim_owner, input.claim_owner);
        assert_eq!(record.input_sequence, Some(self.input.sequence as u64));
        assert_eq!(record.claim_owner.as_deref(), Some(worker.as_str()));
        assert!(worker.starts_with("gateway-delivery:"));
        assert!(record.revision > 0);
        for observer in &mut foreign_observers {
            assert!(matches!(
                observer.observed.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
        }
        let mut barrier = register(&record.terminal_id, &worker, record.revision, point).unwrap();
        self.observer.bind().unwrap();
        let hit = tokio::time::timeout(Duration::from_secs(30), &mut barrier.hit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hit.terminal_id, record.terminal_id);
        assert_eq!(hit.worker_id, worker);
        assert_eq!(hit.revision, record.revision);
        assert_eq!(hit.point, point);
        (record, barrier)
    }

    fn assert_a_and_b(
        &self,
        services: &RecoveryServices,
        original: &runtime::RuntimeSessionOutboxRecord,
    ) {
        let runtime = services.runtime.runtime_services();
        let goal = runtime.goal_store().get(&self.goal.id).unwrap().unwrap();
        assert_eq!(goal, self.goal);
        let program = runtime
            .agent_action_service()
            .project(&self.program_id)
            .unwrap();
        assert_eq!(program.objective_verdict, self.program.objective_verdict);
        assert_eq!(program.completion_request, self.program.completion_request);
        assert_eq!(program.artifacts, self.program.artifacts);
        assert_eq!(
            review_events(&services.topology.runtime_event_store, &self.program_id),
            self.review_events
        );
        let original_writes = self
            .physical_calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.tool_use_id == "recovery-write")
            .count();
        assert_eq!(original_writes, 1);
        assert_eq!(
            self.physical_calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.tool_use_id == "recovery-optional-write")
                .count(),
            self.optional_receipts.len()
        );
        if !Arc::ptr_eq(&self.physical_calls, &services.physical_calls) {
            assert_eq!(
                services
                    .physical_calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|call| call.tool_name == "edit")
                    .count(),
                0,
                "the new production host must not dispatch the old physical write again"
            );
        }
        for (task_ref, task) in &self.program.tasks {
            if task.purpose == harness_contract::agent_action::TaskPurpose::Delivery {
                assert_eq!(task.status, runtime::AgenticTaskStatus::Accepted);
                assert_eq!(program.tasks[task_ref].status, task.status);
                assert_eq!(program.tasks[task_ref].claimant, task.claimant);
                assert_eq!(program.tasks[task_ref].reviewed_by, task.reviewed_by);
            }
        }
        // Session terminal publication owns its original request stream. The
        // root activity index contains activity-bound events, not all Session
        // events that refer to this Graph.
        let events = services
            .topology
            .runtime_event_store
            .list_stream(&format!("session-terminal:{}", self.request_id))
            .unwrap();
        let graph = runtime.graph_state_store().load(&self.graph_id).unwrap();
        let waits = graph
            .nodes
            .iter()
            .filter(|node| node.executor_kind == "agentic_program_wait")
            .collect::<Vec<_>>();
        assert!(
            !waits.is_empty(),
            "the real root must have used the original durable ProgramWait"
        );
        assert!(waits.iter().all(|node| graph.node_statuses.get(&node.id)
            == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Completed)));
        let original_write_calls = self
            .physical_calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.tool_use_id == "recovery-write")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(original_write_calls.len(), 1);
        let writes = receipt_owner::original_receipts(
            &services.topology.runtime_event_store,
            &original_write_calls[0],
        )
        .unwrap();
        assert_eq!(writes, self.effect_receipts);
        assert_eq!(writes.len(), 1);
        let optional_calls = self
            .physical_calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.tool_use_id == "recovery-optional-write")
            .cloned()
            .collect::<Vec<_>>();
        let optional_receipts = optional_calls
            .iter()
            .flat_map(|call| {
                receipt_owner::original_receipts(&services.topology.runtime_event_store, call)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(optional_receipts, self.optional_receipts);
        assert_eq!(
            std::fs::read(runtime.workspace_root().join("recovery-effect.txt")).unwrap(),
            b"recovery-effect-once\n"
        );
        let terminals = events
            .iter()
            .filter(|event| {
                event.kind == "runtime.session.terminal_requested"
                    && event.payload["terminal_id"] == self.terminal_id
            })
            .collect::<Vec<_>>();
        assert_eq!(terminals.len(), 1);
        let terminal = terminals[0];
        assert_eq!(terminal.commit_cursor, original.commit_cursor);
        let verdict = &terminal.payload["objective_verdict"];
        assert_eq!(verdict["program_id"], self.program_id);
        assert_eq!(verdict["goal_spec_revision"], goal.spec_revision);
        assert_eq!(verdict["goal_spec_digest"], goal.spec_digest);
        assert_eq!(
            verdict["proof_input_digests"],
            json!(goal
                .reviews
                .iter()
                .filter_map(|review| review
                    .verification
                    .as_ref()
                    .and_then(|proof| proof.input_set.as_ref())
                    .map(|inputs| inputs.semantic_digest.clone()))
                .collect::<Vec<_>>())
        );
        assert_eq!(
            verdict["verdict"],
            serde_json::to_value(self.program.objective_verdict.as_ref().unwrap()).unwrap()
        );
        let a = &services
            .topology
            .runtime_event_store
            .list_stream(&format!("agentic-program:{}", self.program_id))
            .unwrap()
            .into_iter()
            .find(|event| event.kind == "agentic.objective_verdict_bound")
            .unwrap();
        assert_eq!(verdict["a_commit_cursor"], a.commit_cursor);
        assert!(verdict["program_stream_revision"].as_u64().unwrap() >= a.sequence);
        assert!(verdict["program_stream_revision"].as_u64().unwrap() <= program.revision);
        assert!(a.commit_cursor < terminal.commit_cursor);
        assert_eq!(terminal.payload["payload_ref"], original.payload_ref);
    }

    async fn assert_materialized(
        &self,
        services: &RecoveryServices,
        original: &runtime::RuntimeSessionOutboxRecord,
    ) {
        let store = &services.topology.runtime_event_store;
        let terminal = durable_until(|| {
            store
                .session_terminal(&self.terminal_id)
                .unwrap()
                .filter(|terminal| terminal.status == "materialized")
        })
        .await;
        assert_eq!(terminal.commit_cursor, original.commit_cursor);
        assert_eq!(terminal.message_id, original.message_id);
        assert_eq!(terminal.payload_ref, original.payload_ref);
        self.assert_a_and_b(services, original);
        let payload = crate::session_runtime_bridge::load_terminal_payload(
            &services.topology.artifact_store,
            &terminal,
        )
        .await
        .unwrap();
        let artifact =
            runtime::decode_session_terminal_artifact_ref(&terminal.payload_ref).unwrap();
        let artifact_id = artifact.selector.strip_prefix("artifact://").unwrap();
        let permanent: i64 = services
            .topology
            .postgres_executor
            .checkout_online_read()
            .unwrap()
            .query_one(
                "SELECT until_ms FROM artifact_pins WHERE artifact_id=$1 AND owner=$2",
                &[&artifact_id, &terminal.terminal_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(permanent as u64, runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS);
        assert_eq!(
            payload.goal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        );
        assert_eq!(
            payload.ingress_message_id.as_deref(),
            Some(self.input.message_id.as_str())
        );
        assert_eq!(payload.consumed_input_sequence, Some(self.input.sequence));
        let messages = session_messages(&services.session, &self.session_id).await;
        let stable = messages
            .iter()
            .filter(|message| message.stable_message_id == terminal.message_id)
            .collect::<Vec<_>>();
        assert_eq!(stable.len(), 1);
        let transcript = payload.transcript.unwrap();
        assert!(!transcript.is_empty());
        let mut seen_sequences = std::collections::BTreeSet::new();
        let mut tool_blocks = Vec::new();
        for (index, expected) in transcript.iter().enumerate() {
            let stable_id = if index + 1 == transcript.len() {
                terminal.message_id.clone()
            } else {
                format!("{}:transcript:{index}", terminal.message_id)
            };
            let actual = messages
                .iter()
                .filter(|message| message.stable_message_id == stable_id)
                .collect::<Vec<_>>();
            assert_eq!(actual.len(), 1);
            assert!(seen_sequences.insert(actual[0].sequence));
            assert_eq!(actual[0].role, expected.role);
            let mut actual_body: Value = serde_json::from_str(&actual[0].content_json).unwrap();
            let mut expected_body: Value = serde_json::from_str(&expected.content_json).unwrap();
            for block in actual_body.as_array().unwrap() {
                assert_eq!(block["cowd_turn_id"], self.turn_id);
                assert_eq!(block["cowd_turn_ingress_message_id"], self.input.message_id);
                assert_eq!(block["cowd_execution_id"], self.graph_id);
                if matches!(block["type"].as_str(), Some("tool_use" | "tool_result")) {
                    tool_blocks.push(block.clone());
                }
            }
            for body in [&mut actual_body, &mut expected_body] {
                for block in body.as_array_mut().unwrap() {
                    let object = block.as_object_mut().unwrap();
                    for field in [
                        "cowd_turn_id",
                        "cowd_turn_ingress_message_id",
                        "cowd_execution_id",
                        "cowd_tool_instance_id",
                    ] {
                        object.remove(field);
                    }
                }
            }
            assert_eq!(actual_body, expected_body);
            assert_eq!(actual[0].token_usage_json, expected.token_usage_json);
        }
        terminal_tool_associations(&tool_blocks).unwrap();
        let input = services
            .session
            .runtime_input(&self.request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(input.status, session::SessionRuntimeInputStatus::Completed);
        assert_eq!(input.session_generation, self.input.session_generation);
        assert_eq!(input.turn_id, self.turn_id);
        assert_eq!(input.message_id, self.input.message_id);
        assert_eq!(input.sequence, self.input.sequence);
        assert_eq!(input.runtime_commit_cursor, Some(terminal.commit_cursor));
        let session = services
            .topology
            .session_store
            .get_session(&self.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.message_count, messages.len() as i64);
        let usage = payload
            .token_usage_json
            .as_deref()
            .map(|usage| serde_json::from_str::<Value>(usage).unwrap())
            .unwrap();
        assert!(usage["input_tokens"].as_u64().unwrap() > 0);
        assert_eq!(
            session.input_tokens,
            usage["input_tokens"].as_i64().unwrap()
        );
        assert_eq!(
            session.output_tokens,
            usage["output_tokens"].as_i64().unwrap()
        );
    }
}

fn terminal_tool_associations(blocks: &[Value]) -> Result<(), String> {
    let mut instances = std::collections::BTreeMap::new();
    for block in blocks {
        let (is_use, provider_field, name_field) = match block["type"].as_str() {
            Some("tool_use") => (true, "id", "name"),
            Some("tool_result") => (false, "tool_use_id", "tool_name"),
            _ => return Err("expected a terminal tool block".into()),
        };
        let instance = block["cowd_tool_instance_id"]
            .as_str()
            .filter(|identity| !identity.is_empty())
            .ok_or("missing terminal tool instance identity")?;
        let provider = block[provider_field]
            .as_str()
            .ok_or("missing provider tool identity")?;
        let name = block[name_field].as_str().ok_or("missing tool name")?;
        let entry = instances
            .entry(instance)
            .or_insert((provider, name, false, false));
        if entry.0 != provider || entry.1 != name {
            return Err(format!(
                "terminal tool instance {instance} crosses tool identities"
            ));
        }
        let seen = if is_use { &mut entry.2 } else { &mut entry.3 };
        if *seen {
            return Err(format!(
                "duplicate terminal tool block for instance {instance}"
            ));
        }
        *seen = true;
    }
    Ok(())
}

#[test]
fn recovery_terminal_tool_association_contract() {
    let blocks = vec![
        json!({"type":"tool_use", "id":"reused", "name":"read_file",
            "cowd_tool_instance_id":"opaque:first"}),
        json!({"type":"tool_result", "tool_use_id":"reused", "tool_name":"read_file",
            "cowd_tool_instance_id":"opaque:first"}),
        json!({"type":"tool_use", "id":"reused", "name":"read_file",
            "cowd_tool_instance_id":"opaque:second"}),
        json!({"type":"tool_result", "tool_use_id":"reused", "tool_name":"read_file",
            "cowd_tool_instance_id":"opaque:second"}),
        json!({"type":"tool_result", "tool_use_id":"historical", "tool_name":"read_file",
            "cowd_tool_instance_id":"opaque:historical"}),
    ];
    terminal_tool_associations(&blocks).unwrap();
    let mut reordered = blocks.clone();
    reordered.reverse();
    terminal_tool_associations(&reordered).unwrap();
    for defect in [
        "provider",
        "tool",
        "duplicate_use",
        "duplicate_result",
        "missing",
        "empty",
    ] {
        let mut changed = blocks.clone();
        match defect {
            "provider" => changed[1]["tool_use_id"] = json!("foreign"),
            "tool" => changed[1]["tool_name"] = json!("write_file"),
            "duplicate_use" => changed.push(blocks[0].clone()),
            "duplicate_result" => changed.push(blocks[1].clone()),
            "missing" => {
                changed[1]
                    .as_object_mut()
                    .unwrap()
                    .remove("cowd_tool_instance_id");
            }
            "empty" => changed[1]["cowd_tool_instance_id"] = json!(""),
            _ => unreachable!(),
        }
        assert!(terminal_tool_associations(&changed).is_err(), "{defect}");
    }
}

async fn restart_services(
    root: &Path,
    services: RecoveryServices,
    providers: Arc<runtime::ProviderRegistry>,
) -> RecoveryServices {
    let lease = services.stop().await;
    let topology = lease.rebuild().unwrap();
    drop(lease);
    RecoveryServices::start(root, topology, providers).await
}

async fn assert_staged_artifact(
    services: &RecoveryServices,
    turn: &ReviewedTurn,
    terminal: &runtime::SessionTerminalInput,
) {
    use sha2::Digest;
    assert_eq!(terminal.terminal_id, turn.terminal_id);
    assert_eq!(
        terminal.request_id.as_deref(),
        Some(turn.request_id.as_str())
    );
    assert_eq!(
        terminal.execution_id.as_deref(),
        Some(turn.graph_id.as_str())
    );
    assert_eq!(terminal.session_id, turn.session_id);
    assert_eq!(terminal.turn_id.as_deref(), Some(turn.turn_id.as_str()));
    let artifact = runtime::decode_session_terminal_artifact_ref(&terminal.payload_ref).unwrap();
    let bytes = services
        .topology
        .artifact_store
        .read(&artifact, &format!("session:{}", turn.session_id), None)
        .await
        .unwrap();
    assert_eq!(bytes.len() as u64, artifact.bytes);
    assert_eq!(
        format!("{:x}", sha2::Sha256::digest(&bytes)),
        artifact.sha256.trim_start_matches("sha256:")
    );
    let payload: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload["goal_completion"], "satisfied");
    assert_eq!(payload["ingress_message_id"], turn.input.message_id);
    assert!(!payload["transcript"].as_array().unwrap().is_empty());
    let artifact_id = artifact.selector.strip_prefix("artifact://").unwrap();
    // Prepared artifacts use the catalog's permanent private-prepared pin;
    // the caller identity is carried by typed provenance, not the pin owner.
    let prepared = services
        .topology
        .postgres_executor
        .checkout_online_read()
        .unwrap()
        .query_one(
            "SELECT p.until_ms, r.prepared_origin FROM artifact_pins p \
             JOIN artifact_records r USING (artifact_id) \
             WHERE p.artifact_id=$1 AND p.owner=$2",
            &[&artifact_id, &"cowd:private-prepared:v1"],
        )
        .unwrap();
    let until: i64 = prepared.get(0);
    assert_eq!(until as u64, runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS);
    let origin: Value = prepared.get(1);
    assert_eq!(origin["origin_kind"], "terminal_body");
    assert_eq!(origin["terminal_id"], turn.terminal_id);
    assert_eq!(origin["session_id"], turn.session_id);
    assert_eq!(origin["execution_id"], turn.graph_id);
    assert_eq!(origin["turn_id"], turn.turn_id);
    assert_eq!(
        origin["event_stream"],
        format!("session-terminal:{}", turn.request_id)
    );
    assert!(origin["event_key"]
        .as_str()
        .is_some_and(|key| !key.is_empty()));
    assert!(services
        .topology
        .runtime_event_store
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn r1_normal_business_chain_reaches_real_session() {
    // B layer: the ordinary business chain the framework exists to serve.
    //
    // The producer writes one real file and submits the real artifact, the
    // independent reviewer reads both and issues the verdict, the root opens
    // the criterion team, the reviewer settles the criterion, and the runtime
    // materializes the result into the ORIGINAL Session. There is no optional
    // draining write, no service rebuild, no lost ACK and no reordered
    // stream here: those are the X layer and they have their own cases.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("off,runtime::runtime_event_reactor=warn,runtime::execution_core::services=warn")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers.clone()).await;
    use futures::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        true,
        false,
    )
    .await;
    let candidate = turn.candidate.take().unwrap();
    let (ticket, staged, release) = drive_final_until(&mut provider, candidate).await.unwrap();
    assert_staged_artifact(&services, &turn, &staged).await;
    assert_eq!(ticket.executor_kind, "synthesize");
    // The physical effect happened exactly once, with the real content.
    let physical = services.physical_calls.lock().unwrap().clone();
    let writes = physical
        .iter()
        .filter(|call| call.tool_use_id == "recovery-write")
        .collect::<Vec<_>>();
    assert_eq!(writes.len(), 1, "the real file write must happen exactly once");
    assert!(writes[0].input.contains("recovery-effect-once"));
    // The independent reviewer really read the artifact and the physical file.
    assert!(
        physical
            .iter()
            .any(|call| call.tool_name == "read_file"
                && call.input.contains("recovery-effect.txt")),
        "the reviewer must read the physical file, not only the report"
    );
    // Deliver into the ORIGINAL Session and prove the A/B results landed.
    release.send(()).unwrap();
    let (record, mut barrier) = turn
        .delivery_barrier(&services, &mut provider, Point::BeforeSessionCommit)
        .await;
        // A completion request is applied by the Program owner, and the lane
        // that releases the Agentic barrier is the only thing that drives the
        // Goal afterwards. Read the lane the production health port already
        // exposes, so "the normal chain stopped" cannot silently mean "the
        // lane is wedged".
        let lane = services
            .runtime
            .runtime_services()
            .event_reactor_health()
            .unwrap()
            .lanes
            .into_iter()
            .find(|lane| lane.projection_id == "runtime:agentic-program-wait-resolver:v1")
            .expect("the Agentic barrier lane must be registered");
        eprintln!(
            "recovery188 barrier_lane worker_running={} total_passes={} lag_commits={} consecutive_failures={} last_error={:?}",
            lane.worker_running, lane.total_passes, lane.lag_commits,
            lane.consecutive_failures, lane.last_error
        );
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&services, &record).await;
    assert_eq!(
        services
            .runtime
            .runtime_services()
            .goal_store()
            .get(&turn.goal.id)
            .unwrap()
            .unwrap()
            .completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    }).catch_unwind().await;
    // Preserve identities and physical originals BEFORE attempting cleanup,
    // including when cleanup itself is the first failing consumer.
    let retained_namespace = services
        .topology
        .preserve_failed_test_namespace()
        .map(str::to_owned);
    let retained_root = root.keep();
    eprintln!("normal_b retained_namespace={retained_namespace:?}");
    eprintln!("normal_b retained_fixture_root={}", retained_root.display());
    let panic_message = |error: &(dyn std::any::Any + Send)| {
        error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("non-string fixture panic")
            .chars()
            .take(2048)
            .collect::<String>()
    };
    if let Err(error) = &result {
        eprintln!("normal_b first_failure={}", panic_message(error.as_ref()));
        // Close the owned fixture transports before joining their RPC owner.
        drop(provider);
        drop(producer);
        drop(reviewer);
    }
    let cleanup = std::panic::AssertUnwindSafe(services.stop())
        .catch_unwind()
        .await;
    if let Err(error) = &cleanup {
        eprintln!("normal_b cleanup_failure={}", panic_message(error.as_ref()));
    }
    // A secondary cleanup panic never replaces the original business oracle.
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}


#[tokio::test]
async fn n1_b_reviewed_file_reaches_real_session_without_recovery_faults() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    use futures::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
        let mut turn = ReviewedTurn::produce_with_optional(
            &services,
            &mut provider,
            &mut producer,
            &mut reviewer,
            false,
            false,
            None,
            false,
            false,
        )
        .await;
        assert!(turn.optional_receipts.is_empty());
        let (terminal, _) = drive_final_until(&mut provider, &mut turn.observer.observed)
            .await
            .unwrap();
        turn.observer.bind().unwrap();
        turn.assert_materialized(&services, &terminal).await;
    })
    .catch_unwind()
    .await;
    if result.is_err() {
        eprintln!(
            "normal_b retained_namespace={:?}",
            services.topology.preserve_failed_test_namespace()
        );
    }
    drop(services.stop().await);
    if let Err(error) = result {
        eprintln!("normal_b retained_fixture_root={}", root.keep().display());
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn r1_terminal_recovery_satisfied_a_reaches_real_session() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("off,runtime::runtime_event_reactor=warn,runtime::execution_core::services=warn")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let mut optional = LoopbackProvider::start("optional").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer, &optional]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers.clone()).await;
    let mut turn = ReviewedTurn::produce_with_optional(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        true,
        false,
        Some(&mut optional),
        false,
        true,
    )
    .await;
    let candidate = turn.candidate.take().unwrap();
    let (ticket, staged, held) = drive_final_until(&mut provider, candidate).await.unwrap();
    assert_staged_artifact(&services, &turn, &staged).await;
    assert_eq!(ticket.graph_id, turn.graph_id);
    assert_eq!(ticket.executor_kind, "synthesize");
    let old_binding = services
        .runtime
        .runtime_services()
        .execution_supervisor()
        .active_binding_for_ticket_for_test(&ticket)
        .await
        .unwrap();
    assert!(services
        .topology
        .runtime_event_store
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .is_none());
    assert_eq!(
        services
            .runtime
            .runtime_services()
            .goal_store()
            .get(&turn.goal.id)
            .unwrap()
            .unwrap(),
        turn.goal
    );
    services.supervisor.stop_accepting();
    services
        .runtime
        .runtime_services()
        .shutdown_execution()
        .await;
    assert!(
        held.send(()).is_err(),
        "old Synthesize future must have been destroyed by its production supervisor"
    );
    let lease = services.stop().await;
    let topology = lease.rebuild().unwrap();
    drop(lease);
    let mut rebuilt = RecoveryServices::start_with_notifications(
        root.path(),
        topology,
        providers,
        None,
        Some(turn.graph_id.clone()),
    )
    .await;
    let recovery_candidate = rebuilt.recovered_candidate.take().unwrap();
    let (new_ticket, recovered_staged, release_recovered) =
        drive_final_until(&mut provider, recovery_candidate)
            .await
            .unwrap();
    assert_staged_artifact(&rebuilt, &turn, &recovered_staged).await;
    assert_eq!(new_ticket.graph_id, ticket.graph_id);
    assert_eq!(new_ticket.node_id, ticket.node_id);
    assert_eq!(new_ticket.idempotency_key, ticket.idempotency_key);
    assert_eq!(new_ticket.payload_ref, ticket.payload_ref);
    assert!(new_ticket.attempt >= ticket.attempt);
    let runtime = rebuilt.runtime.runtime_services();
    let before_graph = runtime.graph_state_store().load(&turn.graph_id).unwrap();
    let current_binding = runtime
        .execution_supervisor()
        .active_binding_for_ticket_for_test(&new_ticket)
        .await
        .unwrap();
    assert_ne!(current_binding, old_binding);
    let resources_before = runtime
        .resource_manager()
        .snapshot(&runtime::execution_core::graph::ExecutionResourceKind::Provider)
        .unwrap();
    assert!(matches!(
        runtime
            .execution_supervisor()
            .reject_stale_binding_for_test(&turn.graph_id, &new_ticket.node_id, &old_binding,)
            .await,
        Err(runtime::execution_core::graph::ExecutionRunnerError::CommandSuperseded { .. })
    ));
    assert_eq!(
        runtime.graph_state_store().load(&turn.graph_id).unwrap(),
        before_graph
    );
    assert_eq!(
        runtime
            .execution_supervisor()
            .active_binding_for_ticket_for_test(&new_ticket)
            .await,
        Some(current_binding)
    );
    assert_eq!(
        runtime
            .resource_manager()
            .snapshot(&runtime::execution_core::graph::ExecutionResourceKind::Provider)
            .unwrap()
            .active_leases,
        resources_before.active_leases
    );
    drop(runtime);
    release_recovered.send(()).unwrap();
    let (record, mut barrier) = turn
        .delivery_barrier(&rebuilt, &mut provider, Point::BeforeSessionCommit)
        .await;
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&rebuilt, &record).await;
    drop(rebuilt.stop().await);
    terminal_transaction_rollback_case(false).await;
    terminal_transaction_rollback_case(true).await;
}

struct TerminalCheckWitness {
    sequence: String,
    table: String,
}

impl TerminalCheckWitness {
    fn new(topology: &SelectedStorageTopology, sequence: &str, table: &str) -> Self {
        let namespace: String = topology
            .postgres_executor
            .checkout_critical()
            .unwrap()
            .query_one("SELECT current_schema()", &[])
            .unwrap()
            .get(0);
        assert!(
            namespace.starts_with("cowdtest_")
                && namespace
                    .bytes()
                    .all(|character| character.is_ascii_alphanumeric() || character == b'_')
        );
        assert!(
            !sequence.is_empty()
                && sequence
                    .bytes()
                    .all(|character| character.is_ascii_alphanumeric() || character == b'_')
        );
        assert!(matches!(table, "runtime_events" | "artifact_pins"));
        let witness = Self {
            sequence: format!("\"{namespace}\".\"{sequence}\""),
            table: format!("\"{namespace}\".\"{table}\""),
        };
        topology
            .postgres_executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!(
                "CREATE SEQUENCE {} START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE CACHE 1",
                witness.sequence,
            ))
            .unwrap();
        assert_eq!(witness.state(topology), (1, false));
        witness
    }

    fn state(&self, topology: &SelectedStorageTopology) -> (i64, bool) {
        let row = topology
            .postgres_executor
            .checkout_critical()
            .unwrap()
            .query_one(
                &format!("SELECT last_value, is_called FROM {}", self.sequence),
                &[],
            )
            .unwrap();
        (row.get(0), row.get(1))
    }
}

async fn recovery_delivery_services() -> (
    tempfile::TempDir,
    LoopbackProvider,
    LoopbackProvider,
    LoopbackProvider,
    Arc<runtime::ProviderRegistry>,
    RecoveryServices,
) {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let provider = LoopbackProvider::start("root").await;
    let producer = LoopbackProvider::start("producer").await;
    let reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers.clone()).await;
    (root, provider, producer, reviewer, providers, services)
}

async fn terminal_transaction_rollback_case(source_race: bool) {
    let (root, mut provider, mut producer, mut reviewer, providers, services) =
        recovery_delivery_services().await;
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        true,
        false,
    )
    .await;
    let (ticket, staged, release) =
        drive_final_until(&mut provider, turn.candidate.take().unwrap())
            .await
            .unwrap();
    assert_staged_artifact(&services, &turn, &staged).await;
    assert_eq!(ticket.graph_id, turn.graph_id);
    let runtime = services.runtime.runtime_services();
    let before_graph = runtime.graph_state_store().load(&turn.graph_id).unwrap();
    let before_terminal_revision = services
        .topology
        .runtime_event_store
        .stream_revision(&format!("session-terminal:{}", turn.request_id))
        .unwrap();
    let failed = runtime
        .commit_service()
        .observe_terminal_append_failure_for_test(&turn.terminal_id)
        .unwrap();
    let append_observer = source_race.then(|| {
        runtime
            .commit_service()
            .observe_terminal_append_for_test(&turn.terminal_id)
            .unwrap()
    });
    let check_witness = if !source_race {
        let witness = TerminalCheckWitness::new(
            &services.topology,
            "r1_reject_terminal_hit",
            "runtime_events",
        );
        let terminal = turn.terminal_id.replace('\'', "''");
        services.topology.postgres_executor.checkout_critical().unwrap().batch_execute(&format!(
            "ALTER TABLE {} ADD CONSTRAINT r1_reject_terminal CHECK (CASE WHEN kind = 'runtime.session.terminal_requested' AND payload->>'terminal_id' = '{terminal}' THEN nextval('{}'::regclass) = 0 ELSE TRUE END)",
            witness.table, witness.sequence,
        )).unwrap();
        assert_eq!(witness.state(&services.topology), (1, false));
        Some(witness)
    } else {
        None
    };
    services.supervisor.stop_accepting();
    release.send(()).unwrap();
    let mut raced_source = None;
    if let Some((expected, append_release)) = append_observer {
        let expected = tokio::time::timeout(Duration::from_secs(30), expected)
            .await
            .unwrap()
            .unwrap();
        let material = turn
            .goal
            .reviews
            .iter()
            .filter_map(|review| review.verification.as_ref())
            .filter_map(|verification| verification.input_set.as_ref())
            .flat_map(|inputs| &inputs.material_events)
            .find(|material| {
                material.stream_id.starts_with("execution-effect:")
                    && expected
                        .iter()
                        .any(|source| source.stream_id == material.stream_id)
            })
            .expect("B must actually fence a material event consumed by original A");
        let original = services
            .topology
            .runtime_event_store
            .list_stream(&material.stream_id)
            .unwrap()
            .into_iter()
            .find(|event| event.event_id == material.event_id)
            .unwrap();
        let expected_revision = expected
            .iter()
            .find(|source| source.stream_id == material.stream_id)
            .unwrap()
            .expected_revision;
        assert_eq!(
            services
                .topology
                .runtime_event_store
                .stream_revision(&material.stream_id)
                .unwrap(),
            expected_revision
        );
        let mut corrupted = original.payload.clone();
        corrupted["recovery_source_corruption"] = json!({"original_event_id":original.event_id});
        assert_eq!(
            services
                .topology
                .postgres_executor
                .checkout_critical()
                .unwrap()
                .execute(
                    "UPDATE runtime_events SET payload=$1::text::jsonb WHERE event_id=$2",
                    &[&corrupted.to_string(), &original.event_id],
                )
                .unwrap(),
            1
        );
        let fault = services.topology.runtime_event_store.append(runtime::RuntimeEventInput {
            stream_id:material.stream_id.clone(), scope:original.scope, kind:"test.recovery.source_revision_fault".into(),
            status:Some("corrupted".into()), actor:Some("recovery-source-fault".into()),
            refs:vec![runtime::RuntimeEventRef { kind:"original_material".into(), id:original.event_id.clone() }],
            payload:json!({"original_event_id":original.event_id,"original_sequence":original.sequence}),
        }).unwrap();
        assert_eq!(fault.sequence, expected_revision + 1);
        let changed = services
            .topology
            .runtime_event_store
            .list_stream(&material.stream_id)
            .unwrap()
            .into_iter()
            .find(|event| event.event_id == material.event_id)
            .unwrap();
        assert_eq!(changed.payload, corrupted);
        assert_ne!(changed.payload, original.payload);
        raced_source = Some(material.stream_id.clone());
        append_release.send(()).unwrap();
    }
    let error = tokio::time::timeout(Duration::from_secs(30), failed)
        .await
        .unwrap()
        .unwrap();
    if let Some(source) = &raced_source {
        assert!(error.contains(source), "{error}");
    } else {
        assert!(error.contains("storage failure"), "{error}");
    }
    runtime.shutdown_execution().await;
    if let Some(witness) = &check_witness {
        assert_eq!(witness.state(&services.topology), (1, true));
    }
    assert_eq!(
        runtime.graph_state_store().load(&turn.graph_id).unwrap(),
        before_graph
    );
    assert_eq!(
        runtime.goal_store().get(&turn.goal.id).unwrap().unwrap(),
        turn.goal
    );
    assert_eq!(
        services
            .topology
            .runtime_event_store
            .stream_revision(&format!("session-terminal:{}", turn.request_id))
            .unwrap(),
        before_terminal_revision
    );
    assert!(services
        .topology
        .runtime_event_store
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .is_none());
    assert!(
        session_messages(&services.session, &turn.session_id)
            .await
            .iter()
            .all(|message| message.stable_message_id
                != format!("assistant:{}", turn.input.message_id))
    );
    if let Some(witness) = &check_witness {
        services
            .topology
            .postgres_executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!(
                "ALTER TABLE {} DROP CONSTRAINT r1_reject_terminal",
                witness.table
            ))
            .unwrap();
    }
    drop(runtime);
    if source_race {
        let _lease = services.stop().await;
        return;
    }
    let rebuilt = restart_services(root.path(), services, providers).await;
    let (record, mut barrier) = turn
        .delivery_barrier(&rebuilt, &mut provider, Point::BeforeAck)
        .await;
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&rebuilt, &record).await;
    let _lease = rebuilt.stop().await;
}

async fn delivery_restart_case(point: Point, commit_response_loss: bool) {
    let (root, mut provider, mut producer, mut reviewer, providers, services) =
        recovery_delivery_services().await;
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        false,
        commit_response_loss,
    )
    .await;
    let mut notifications = services
        .session
        .event_bus()
        .subscribe(&turn.session_id, 512)
        .await;
    let (record, mut barrier) = turn.delivery_barrier(&services, &mut provider, point).await;
    turn.assert_a_and_b(&services, &record);
    if let Some(hits) = &turn.commit_loss {
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    let before = session_messages(&services.session, &turn.session_id).await;
    let counters = services
        .topology
        .session_store
        .get_session(&turn.session_id)
        .await
        .unwrap()
        .unwrap();
    if point == Point::BeforeSessionCommit {
        assert!(before
            .iter()
            .all(|message| message.stable_message_id != record.message_id));
    } else {
        assert_eq!(
            before
                .iter()
                .filter(|message| message.stable_message_id == record.message_id)
                .count(),
            1
        );
    }
    if point == Point::AfterAck {
        assert_eq!(
            services
                .topology
                .runtime_event_store
                .session_terminal(&turn.terminal_id)
                .unwrap()
                .unwrap()
                .status,
            "materialized"
        );
    }
    let published = terminal_notifications(&turn, &mut notifications);
    if matches!(point, Point::BeforeAck | Point::AfterAck) {
        assert_eq!(published.len(), 1);
        assert!(!published[0].2);
        assert_eq!(published[0].0, record.message_id);
        assert_eq!(published[0].3, record.commit_cursor);
    } else {
        assert!(published.is_empty());
    }
    services.runtime.stop_accepting_for_recovery();
    services.supervisor.stop_accepting();
    barrier.release(Release::ResponseLost).unwrap();
    if point != Point::AfterAck {
        turn.observer = observe(turn.request_identity()).unwrap();
    }
    let rebuilt = restart_services(root.path(), services, providers).await;
    let mut replay_notifications = rebuilt
        .session
        .event_bus()
        .subscribe(&turn.session_id, 512)
        .await;
    if point != Point::AfterAck {
        let (reclaimed, mut replay) = turn
            .delivery_barrier(&rebuilt, &mut provider, Point::BeforeAck)
            .await;
        assert_ne!(reclaimed.claim_owner, record.claim_owner);
        assert!(reclaimed.revision > record.revision);
        assert!(rebuilt
            .topology
            .runtime_event_store
            .ack_session_terminal(
                &record.terminal_id,
                record.claim_owner.as_deref().unwrap(),
                record.revision,
                recovery_now_ms(),
            )
            .is_err());
        let unchanged = rebuilt
            .topology
            .runtime_event_store
            .session_terminal(&turn.terminal_id)
            .unwrap()
            .unwrap();
        assert_eq!(unchanged, reclaimed);
        replay.release(Release::Continue).unwrap();
    }
    turn.assert_materialized(&rebuilt, &record).await;
    let replayed = terminal_notifications(&turn, &mut replay_notifications);
    if point != Point::AfterAck {
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].0, record.message_id);
        assert_eq!(replayed[0].3, record.commit_cursor);
        assert_eq!(replayed[0].2, point != Point::BeforeSessionCommit);
        let payload = crate::session_runtime_bridge::load_terminal_payload(
            &rebuilt.topology.artifact_store,
            &record,
        )
        .await
        .unwrap();
        assert_eq!(replayed[0].1, payload.text);
    } else {
        assert!(replayed.is_empty());
    }
    if point != Point::BeforeSessionCommit {
        let after = session_messages(&rebuilt.session, &turn.session_id).await;
        let restored = rebuilt
            .topology
            .session_store
            .get_session(&turn.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after, before);
        assert_eq!(restored.message_count, counters.message_count);
        assert_eq!(restored.input_tokens, counters.input_tokens);
        assert_eq!(restored.output_tokens, counters.output_tokens);
    }
    let current = rebuilt
        .topology
        .runtime_event_store
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.commit_cursor, record.commit_cursor);
    let _lease = rebuilt.stop().await;
}

#[tokio::test]
async fn r1_terminal_recovery_commit_response_loss() {
    delivery_restart_case(Point::BeforeSessionCommit, true).await;
    delivery_restart_case(Point::AfterSessionCommit, false).await;
    terminal_artifact_pin_failure_case().await;
}

async fn terminal_artifact_pin_failure_case() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers.clone()).await;
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        true,
        false,
    )
    .await;
    let (_, terminal, release) = drive_final_until(&mut provider, turn.candidate.take().unwrap())
        .await
        .unwrap();
    assert_staged_artifact(&services, &turn, &terminal).await;
    let terminal_id = turn.terminal_id.replace('\'', "''");
    let check_witness = TerminalCheckWitness::new(
        &services.topology,
        "r1_reject_terminal_pin_hit",
        "artifact_pins",
    );
    services.topology.postgres_executor.checkout_critical().unwrap().batch_execute(&format!(
        "ALTER TABLE {} ADD CONSTRAINT r1_reject_terminal_pin CHECK (CASE WHEN owner = '{terminal_id}' THEN nextval('{}'::regclass) = 0 ELSE TRUE END)",
        check_witness.table, check_witness.sequence,
    )).unwrap();
    assert_eq!(check_witness.state(&services.topology), (1, false));
    let mut notifications = services
        .session
        .event_bus()
        .subscribe(&turn.session_id, 512)
        .await;
    release.send(()).unwrap();
    let (record, worker) = drive_final_until(&mut provider, &mut turn.observer.observed)
        .await
        .unwrap();
    assert_eq!(record.terminal_id, turn.terminal_id);
    assert_eq!(record.session_id, turn.session_id);
    assert_eq!(record.request_id.as_deref(), Some(turn.request_id.as_str()));
    assert_eq!(record.turn_id.as_deref(), Some(turn.turn_id.as_str()));
    assert_eq!(record.execution_id.as_deref(), Some(turn.graph_id.as_str()));
    assert_eq!(
        record.session_generation,
        Some(turn.input.session_generation)
    );
    assert_eq!(record.input_sequence, Some(turn.input.sequence as u64));
    assert_eq!(record.claim_owner.as_deref(), Some(worker.as_str()));
    assert_eq!(record.input_claim_token, turn.input.claim_token);
    assert_eq!(record.input_claim_revision, turn.input.claim_fence_epoch);
    assert_eq!(record.attempts, 1);
    services.supervisor.stop_accepting();
    turn.observer.bind().unwrap();
    let failed = durable_until(|| {
        services
            .topology
            .runtime_event_store
            .session_terminal(&turn.terminal_id)
            .unwrap()
            .filter(|record| {
                record
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("retain committed terminal artifact"))
            })
    })
    .await;
    assert_eq!(failed.status, "retry_scheduled");
    assert!(failed.materialized_at_ms.is_none());
    assert_eq!(failed.commit_cursor, record.commit_cursor);
    assert_eq!(failed.message_id, record.message_id);
    assert_eq!(failed.payload_ref, record.payload_ref);
    assert!(session_messages(&services.session, &turn.session_id)
        .await
        .iter()
        .all(|message| message.stable_message_id != record.message_id));
    assert!(terminal_notifications(&turn, &mut notifications).is_empty());
    turn.assert_a_and_b(&services, &record);
    services.supervisor.shutdown().await;
    assert_eq!(services.supervisor.health().forced_aborts, 0);
    let stable_failed = services
        .topology
        .runtime_event_store
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .unwrap();
    assert_eq!(stable_failed.status, "retry_scheduled");
    assert!(stable_failed
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("retain committed terminal artifact")));
    assert!(stable_failed.materialized_at_ms.is_none());
    assert_eq!(stable_failed.terminal_id, record.terminal_id);
    assert_eq!(stable_failed.session_id, record.session_id);
    assert_eq!(stable_failed.request_id, record.request_id);
    assert_eq!(stable_failed.turn_id, record.turn_id);
    assert_eq!(stable_failed.execution_id, record.execution_id);
    assert_eq!(stable_failed.session_generation, record.session_generation);
    assert_eq!(stable_failed.input_sequence, record.input_sequence);
    assert_eq!(stable_failed.input_claim_owner, record.input_claim_owner);
    assert_eq!(stable_failed.input_claim_token, record.input_claim_token);
    assert_eq!(
        stable_failed.input_claim_revision,
        record.input_claim_revision
    );
    assert_eq!(stable_failed.commit_cursor, record.commit_cursor);
    assert_eq!(stable_failed.message_id, record.message_id);
    assert_eq!(stable_failed.payload_ref, record.payload_ref);
    let effective_calls = stable_failed
        .attempts
        .checked_sub(record.attempts)
        .unwrap()
        .checked_add(1)
        .unwrap();
    assert_eq!(
        check_witness.state(&services.topology),
        (i64::from(effective_calls), true)
    );
    assert!(session_messages(&services.session, &turn.session_id)
        .await
        .iter()
        .all(|message| message.stable_message_id != record.message_id));
    assert!(terminal_notifications(&turn, &mut notifications).is_empty());
    turn.assert_a_and_b(&services, &record);
    services
        .topology
        .postgres_executor
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!(
            "ALTER TABLE {} DROP CONSTRAINT r1_reject_terminal_pin",
            check_witness.table
        ))
        .unwrap();
    turn.observer = observe(turn.request_identity()).unwrap();
    let rebuilt = restart_services(root.path(), services, providers).await;
    let (adopted, mut barrier) = turn
        .delivery_barrier(&rebuilt, &mut provider, Point::BeforeSessionCommit)
        .await;
    assert_eq!(adopted.payload_ref, record.payload_ref);
    assert_eq!(adopted.commit_cursor, record.commit_cursor);
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&rebuilt, &record).await;
    let _lease = rebuilt.stop().await;
}

#[tokio::test]
async fn r1_terminal_recovery_outbox_ack_loss() {
    for point in [Point::BeforePublish, Point::BeforeAck, Point::AfterAck] {
        delivery_restart_case(point, false).await;
    }
}

fn recovery_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn r1_terminal_recovery_lost_notify() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let reader = Arc::new(
        runtime_postgres::PostgresRuntimeEventStore::new(topology.postgres_executor.clone())
            .unwrap()
            .into_runtime_event_store(),
    );
    let bell = reader.subscribe_commits();
    let before_cursor = reader.current_commit_cursor();
    let services = RecoveryServices::start_with_notifications(
        root.path(),
        topology,
        providers.clone(),
        Some(reader.clone()),
        None,
    )
    .await;
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        false,
        false,
    )
    .await;
    let lane_hits = services
        .lane_hits
        .as_ref()
        .unwrap()
        .load(std::sync::atomic::Ordering::SeqCst);
    let (record, mut barrier) = turn
        .delivery_barrier(&services, &mut provider, Point::BeforeSessionCommit)
        .await;
    assert!(!bell.has_changed().unwrap());
    assert_eq!(reader.current_commit_cursor(), before_cursor);
    assert!(reader.durable_commit_cursor().unwrap() >= record.commit_cursor);
    let durable_b = reader
        .list_stream(&format!("session-terminal:{}", turn.request_id))
        .unwrap();
    assert_eq!(
        durable_b
            .iter()
            .filter(|event| event.kind == "runtime.session.terminal_requested")
            .count(),
        1
    );
    assert!(
        services
            .delivery_hits
            .as_ref()
            .unwrap()
            .safety_hits
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0,
        "the original delivery fallback, not a manual drain or synthetic notification, must fire"
    );
    durable_until(|| {
        let hits = services
            .lane_hits
            .as_ref()
            .unwrap()
            .load(std::sync::atomic::Ordering::SeqCst);
        let checkpoint = services
            .topology
            .runtime_event_store
            .projection_checkpoint("runtime:agentic-program-wait-resolver:v1")
            .unwrap();
        (hits > lane_hits
            && checkpoint
                .is_some_and(|checkpoint| checkpoint.source_cursor >= record.commit_cursor))
        .then_some(())
    })
    .await;
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&services, &record).await;
    drop(reader);
    let rebuilt = restart_services(root.path(), services, providers).await;
    turn.assert_materialized(&rebuilt, &record).await;
    let _lease = rebuilt.stop().await;
    drop(_lease);
    for after_checkpoint in [false, true] {
        lost_notify_checkpoint_restart(after_checkpoint).await;
    }
}

async fn lost_notify_checkpoint_restart(after_checkpoint: bool) {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let reader = Arc::new(
        runtime_postgres::PostgresRuntimeEventStore::new(topology.postgres_executor.clone())
            .unwrap()
            .into_runtime_event_store(),
    );
    let bell = reader.subscribe_commits();
    let services = RecoveryServices::start_with_notifications(
        root.path(),
        topology,
        providers.clone(),
        Some(reader.clone()),
        None,
    )
    .await;
    let mut turn = ReviewedTurn::produce_with_optional(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        true,
        false,
        None,
        true,
        true,
    )
    .await;
    let (revision, held) =
        tokio::time::timeout(Duration::from_secs(120), turn.resolution.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    let a = reader
        .list_stream(&format!("agentic-program:{}", turn.program_id))
        .unwrap()
        .into_iter()
        .find(|event| event.kind == "agentic.objective_verdict_bound")
        .unwrap();
    assert_eq!(revision, turn.program.revision);
    assert_eq!(
        services
            .runtime
            .runtime_services()
            .goal_store()
            .get(&turn.goal.id)
            .unwrap()
            .unwrap(),
        turn.goal
    );
    assert!(!bell.has_changed().unwrap());
    assert!(
        services
            .lane_hits
            .as_ref()
            .unwrap()
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
    );
    let checkpoint = || {
        reader
            .projection_checkpoint("runtime:agentic-program-wait-resolver:v1")
            .unwrap()
            .map_or(0, |checkpoint| checkpoint.source_cursor)
    };
    assert!(checkpoint() < a.commit_cursor);
    if after_checkpoint {
        held.send(()).unwrap();
        durable_until(|| (checkpoint() >= a.commit_cursor).then_some(())).await;
        let (_, terminal, release) =
            drive_final_until(&mut provider, turn.candidate.take().unwrap())
                .await
                .unwrap();
        assert_staged_artifact(&services, &turn, &terminal).await;
        services.supervisor.stop_accepting();
        services
            .runtime
            .runtime_services()
            .shutdown_execution()
            .await;
        assert!(release.send(()).is_err());
    } else {
        services.supervisor.stop_accepting();
        let runtime = services.runtime.runtime_services();
        let (execution, ()) =
            tokio::join!(runtime.shutdown_execution(), runtime.shutdown_maintenance());
        assert_eq!(execution.forced_aborts, 0);
        assert!(held.send(()).is_err());
        assert!(checkpoint() < a.commit_cursor);
    }
    assert!(reader
        .session_terminal(&turn.terminal_id)
        .unwrap()
        .is_none());
    let saved_checkpoint = checkpoint();
    drop(reader);
    let lease = services.stop().await;
    let topology = lease.rebuild().unwrap();
    drop(lease);
    let reader = Arc::new(
        runtime_postgres::PostgresRuntimeEventStore::new(topology.postgres_executor.clone())
            .unwrap()
            .into_runtime_event_store(),
    );
    assert_eq!(
        reader
            .projection_checkpoint("runtime:agentic-program-wait-resolver:v1")
            .unwrap()
            .map_or(0, |checkpoint| checkpoint.source_cursor),
        saved_checkpoint
    );
    let rebuilt = RecoveryServices::start_with_notifications(
        root.path(),
        topology,
        providers,
        Some(reader.clone()),
        None,
    )
    .await;
    let (record, mut barrier) = turn
        .delivery_barrier(&rebuilt, &mut provider, Point::BeforeSessionCommit)
        .await;
    assert!(a.commit_cursor < record.commit_cursor);
    durable_until(|| {
        reader
            .projection_checkpoint("runtime:agentic-program-wait-resolver:v1")
            .unwrap()
            .filter(|checkpoint| checkpoint.source_cursor >= record.commit_cursor)
    })
    .await;
    barrier.release(Release::Continue).unwrap();
    turn.assert_materialized(&rebuilt, &record).await;
    drop(reader);
    let _lease = rebuilt.stop().await;
}

#[derive(Clone, Copy)]
enum EpochRace {
    Generation,
    PendingInput,
    Claim,
}

async fn epoch_race_case(race: EpochRace) {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers.clone()).await;
    let mut turn = ReviewedTurn::produce(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        false,
        false,
    )
    .await;
    let mut notifications = services
        .session
        .event_bus()
        .subscribe(&turn.session_id, 512)
        .await;
    let (record, mut barrier) = turn
        .delivery_barrier(&services, &mut provider, Point::BeforeSessionCommit)
        .await;
    let current = services
        .session
        .runtime_input(&turn.request_id)
        .await
        .unwrap()
        .unwrap();
    let adoption = runtime::RuntimeSessionTerminalFenceAdoption {
        terminal_id: record.terminal_id.clone(),
        expected_terminal_revision: record.revision,
        request_id: turn.request_id.clone(),
        session_id: turn.session_id.clone(),
        turn_id: turn.turn_id.clone(),
        session_generation: current.session_generation,
        input_sequence: current.sequence as u64,
        claim_owner: current.claim_owner.clone().unwrap(),
        claim_token: current.claim_token.clone().unwrap(),
        claim_revision: current.claim_fence_epoch.unwrap(),
        claim_expires_at_ms: current.claim_expires_at_ms.unwrap(),
        adopted_at_ms: recovery_now_ms(),
    };
    for field in ["generation", "request", "turn", "sequence"] {
        let mut foreign = adoption.clone();
        match field {
            "generation" => foreign.session_generation += 1,
            "request" => foreign.request_id.push_str(":foreign"),
            "turn" => foreign.turn_id.push_str(":foreign"),
            _ => foreign.input_sequence += 1,
        }
        assert!(services
            .topology
            .runtime_event_store
            .adopt_session_terminal_fence(&foreign)
            .is_err());
        assert_eq!(
            services
                .topology
                .runtime_event_store
                .session_terminal(&turn.terminal_id)
                .unwrap()
                .unwrap(),
            record
        );
    }
    match race {
        EpochRace::Generation => {
            let advanced = services
                .topology
                .session_store
                .advance_session_input_generation(
                    &turn.session_id,
                    current.session_generation,
                    true,
                    "recovery-operator",
                    "generation race",
                    recovery_now_ms(),
                )
                .await
                .unwrap();
            assert!(advanced.generation > current.session_generation);
        }
        EpochRace::PendingInput => {
            let envelope = harness_contract::turn::SessionInputEnvelope::text(
                &turn.session_id, harness_contract::turn::InputSourceKind::Tui,
                "Supplement the still-running original turn with a new required exact-file verification.",
            );
            let id = envelope.idempotency_key.clone();
            services.session.admit_input(envelope).await.unwrap();
            let pending = services.session.runtime_input(&id).await.unwrap().unwrap();
            assert!(pending.sequence > current.sequence);
            assert_ne!(
                pending.status,
                session::SessionRuntimeInputStatus::Completed
            );
        }
        EpochRace::Claim => {
            services.supervisor.stop_accepting();
            let mut released = false;
            for _attempt in 0..8 {
                let live = services
                    .session
                    .runtime_input(&turn.request_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(live.claim_token, current.claim_token);
                assert_eq!(live.claim_fence_epoch, current.claim_fence_epoch);
                let result = services
                    .topology
                    .session_store
                    .requeue_claimed_session_runtime_outbox(
                        &turn.request_id,
                        live.claim_owner.as_deref().unwrap(),
                        live.session_generation,
                        live.claim_token.as_deref().unwrap(),
                        live.revision,
                        live.decision,
                        live.target_turn_id.as_deref(),
                        live.classification_json.as_deref(),
                        "release exact previous claim for recovery",
                        recovery_now_ms(),
                    )
                    .await;
                if result.is_ok() {
                    released = true;
                    break;
                }
                assert!(matches!(
                    result,
                    Err(session::SessionError::StaleExecutionFence(_))
                ));
            }
            assert!(
                released,
                "the original claim could not be released with its current exact CAS"
            );
        }
    }
    barrier.release(Release::Continue).unwrap();
    let refused = durable_until(|| {
        services
            .topology
            .runtime_event_store
            .session_terminal(&turn.terminal_id)
            .unwrap()
            .filter(|record| {
                matches!(
                    record.status.as_str(),
                    "blocked" | "suppressed" | "retry_scheduled"
                )
            })
    })
    .await;
    assert_ne!(refused.status, "materialized");
    assert!(refused.materialized_at_ms.is_none());
    assert!(refused
        .last_error
        .as_deref()
        .is_some_and(|error| !error.is_empty()));
    assert_eq!(refused.commit_cursor, record.commit_cursor);
    assert_eq!(refused.message_id, record.message_id);
    assert_eq!(refused.payload_ref, record.payload_ref);
    let messages = session_messages(&services.session, &turn.session_id).await;
    assert!(messages
        .iter()
        .all(|message| message.stable_message_id != record.message_id));
    assert!(terminal_notifications(&turn, &mut notifications).is_empty());
    turn.assert_a_and_b(&services, &record);
    if matches!(race, EpochRace::Claim) {
        turn.observer = observe(turn.request_identity()).unwrap();
        let rebuilt = restart_services(root.path(), services, providers).await;
        let (adopted, mut recovered) = turn
            .delivery_barrier(&rebuilt, &mut provider, Point::BeforeAck)
            .await;
        assert_ne!(adopted.input_claim_token, record.input_claim_token);
        assert_ne!(adopted.input_claim_revision, record.input_claim_revision);
        assert!(adopted.input_claim_revision > record.input_claim_revision);
        assert_eq!(adopted.commit_cursor, record.commit_cursor);
        assert!(rebuilt
            .topology
            .session_store
            .ack_session_runtime_outbox(
                &current.request_id,
                current.claim_owner.as_deref().unwrap(),
                current.session_generation,
                current.claim_token.as_deref().unwrap(),
                current.revision,
                session::SessionRuntimeInputStatus::Completed,
                record.commit_cursor,
                recovery_now_ms(),
            )
            .await
            .is_err());
        recovered.release(Release::Continue).unwrap();
        turn.assert_materialized(&rebuilt, &record).await;
        let _lease = rebuilt.stop().await;
    } else {
        let _lease = services.stop().await;
    }
}

#[tokio::test]
async fn r1_terminal_recovery_session_epoch_race() {
    for race in [
        EpochRace::Generation,
        EpochRace::PendingInput,
        EpochRace::Claim,
    ] {
        epoch_race_case(race).await;
    }
}

fn review_events(
    store: &runtime::RuntimeEventStore,
    program: &str,
) -> Vec<runtime::RuntimeEventRecord> {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, ObjectiveReviewDecision, TaskReviewDecision,
    };
    store.list_stream(&format!("agentic-program:{program}")).unwrap().into_iter().filter(|event| {
        serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone()).is_ok_and(|envelope|
            matches!(envelope.action, AgentAction::TaskReview(ref input) if input.decision == TaskReviewDecision::Accept)
            || matches!(envelope.action, AgentAction::ObjectiveReview(ref input) if input.decision == ObjectiveReviewDecision::Satisfied))
    }).collect()
}

async fn session_messages(
    session: &crate::services::SessionService,
    session_id: &str,
) -> Vec<session::SessionMessage> {
    let mut messages = Vec::new();
    loop {
        let page = session
            .stored_messages(session_id, messages.len(), 128)
            .await
            .unwrap()
            .unwrap();
        if page.is_empty() {
            break;
        }
        messages.extend(page);
    }
    messages
}

fn terminal_notifications(
    turn: &ReviewedTurn,
    subscription: &mut crate::event_bus::SessionProjectionSubscription,
) -> Vec<(String, String, bool, u64)> {
    let mut notifications = Vec::new();
    while let Ok(event) = subscription.try_recv() {
        if let crate::event_bus::SessionProjectionEvent::TerminalCommitted {
            session_id,
            terminal_id,
            message_id,
            response,
            replayed,
            runtime_commit_cursor,
            execution_id,
            turn_id,
            ..
        } = event
        {
            if terminal_id != turn.terminal_id {
                continue;
            }
            assert_eq!(session_id, turn.session_id);
            assert_eq!(execution_id.as_deref(), Some(turn.graph_id.as_str()));
            assert_eq!(turn_id.as_deref(), Some(turn.turn_id.as_str()));
            notifications.push((message_id, response, replayed, runtime_commit_cursor));
        }
    }
    notifications
}

struct RecoveryServices {
    config_home: std::path::PathBuf,
    topology: SelectedStorageTopology,
    runtime: Arc<super::RuntimeService>,
    session: Arc<crate::services::SessionService>,
    supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
    notification_reader: Option<Arc<runtime::RuntimeEventStore>>,
    physical_calls: Arc<std::sync::Mutex<Vec<PhysicalCall>>>,
    physical_holds: Arc<std::sync::Mutex<std::collections::BTreeMap<String, PhysicalHold>>>,
    bounded_missing: Arc<std::sync::atomic::AtomicU8>,
    lane_hits: Option<Arc<std::sync::atomic::AtomicU64>>,
    delivery_hits:
        Option<crate::session_runtime_bridge::terminal_delivery_faults::NotificationWitness>,
    recovered_candidate: Option<
        tokio::sync::oneshot::Receiver<(
            runtime::execution_core::graph::NodeExecutionTicket,
            runtime::SessionTerminalInput,
            tokio::sync::oneshot::Sender<()>,
        )>,
    >,
}

impl RecoveryServices {
    async fn start(
        root: &Path,
        topology: SelectedStorageTopology,
        providers: Arc<runtime::ProviderRegistry>,
    ) -> Self {
        Self::start_with_notifications(root, topology, providers, None, None).await
    }

    async fn start_with_notifications(
        root: &Path,
        topology: SelectedStorageTopology,
        providers: Arc<runtime::ProviderRegistry>,
        notification_reader: Option<Arc<runtime::RuntimeEventStore>>,
        candidate_graph: Option<String>,
    ) -> Self {
        Self::start_composed(
            root,
            topology,
            providers,
            notification_reader,
            candidate_graph,
            None,
        )
        .await
    }

    async fn start_with_single_provider_slot(
        root: &Path,
        topology: SelectedStorageTopology,
        providers: Arc<runtime::ProviderRegistry>,
    ) -> Self {
        let mut config = runtime::ProviderResourceConfig::default();
        config.global = runtime::ProviderQuotaPolicy::new(1, 1, 1, 0);
        config.fallback = runtime::ProviderQuotaPolicy::new(1, 1, 1, 0);
        Self::start_composed(root, topology, providers, None, None, Some(config)).await
    }

    async fn start_composed(
        root: &Path,
        topology: SelectedStorageTopology,
        providers: Arc<runtime::ProviderRegistry>,
        notification_reader: Option<Arc<runtime::RuntimeEventStore>>,
        candidate_graph: Option<String>,
        provider_config: Option<runtime::ProviderResourceConfig>,
    ) -> Self {
        use crate::services::session_service::{
            activation::SessionActivationCoordinator, presence::SessionPresenceLedger,
            repository::SessionRepository,
        };

        let active = Arc::new(crate::active_session::ActiveSessionDirectory::new());
        let event_bus = crate::event_bus::SessionProjectionHub::new();
        let repository = Arc::new(SessionRepository::new(
            active.clone(),
            Some(topology.session_store.clone()),
            event_bus.clone(),
        ));
        let presence = Arc::new(SessionPresenceLedger::with_store(
            topology.session_store.clone(),
        ));
        let port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
        let catalog = crate::runtime_bootstrap::GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .unwrap();
        let tool_host = Arc::new(
            tools::ToolHost::new(
                format!("recovery:{}", root.display()),
                root.join("workspace"),
                tools::ToolHostSnapshot::new(
                    Arc::new(catalog.clone()),
                    Arc::new(tools::lsp_client::LspRegistry::new()),
                    None,
                ),
            )
            .with_authorization_lease_verifier(Arc::new(
                runtime::AuthorizationNegotiator::verify_lease_signature,
            )),
        );
        let executor = Arc::new(
            crate::gateway_tool_executor::GatewayToolExecutor::from_tool_host(
                None,
                false,
                tool_host.clone(),
            ),
        );
        let physical_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let physical_holds = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
        let bounded_missing = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let recording_host = Arc::new(RecordingExecutionHost {
            bounded_missing: bounded_missing.clone(),
            inner: executor.clone(),
            calls: physical_calls.clone(),
            holds: physical_holds.clone(),
        });
        let config_home = root.join("home");
        let builder = runtime::RuntimeServices::builder(&config_home, root.join("workspace"));
        let services = builder
            .provider_resource_config(provider_config.unwrap_or_default())
            .provider_registry(providers.clone())
            .tool_execution_host(recording_host)
            .runtime_event_store(topology.runtime_event_store.clone())
            .artifact_store(topology.artifact_store.clone())
            .task_aggregate_service(topology.task_service.clone())
            .session_ports(port.clone(), port.clone(), port.clone(), port.clone())
            .build()
            .unwrap();
        executor.bind_runtime_services(&services).unwrap();
        let recovered_candidate = candidate_graph.as_ref().map(|graph| {
            services
                .synthesize_executor()
                .observe_terminal_candidate_for_test(graph)
                .unwrap()
        });
        let (lane_hits, delivery_hits) = notification_reader
            .as_ref()
            .map(|reader| {
                (
                    services
                        .use_recovery_lane_notifications_for_test(reader.subscribe_commits())
                        .unwrap(),
                    crate::session_runtime_bridge::terminal_delivery_faults::use_notifications(
                        &topology.artifact_store,
                        reader.subscribe_commits(),
                    )
                    .unwrap(),
                )
            })
            .unzip();
        let runtime = Arc::new(
            super::RuntimeService::new(
                active,
                Arc::new(session::SessionLeaseRegistry::default()),
                port.clone(),
                event_bus.clone(),
                std::time::Instant::now(),
                Some("test-model".to_string()),
                providers,
                Arc::new(runtime::UpgradeCoordinator::new()),
                services.clone(),
            )
            .unwrap()
            .with_permission_mode(runtime::PermissionMode::DangerFullAccess)
            .with_approval_profile(runtime::ApprovalProfile::Autonomous)
            .with_tool_host(tool_host)
            .with_session_bootstrap(
                crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot {
                    feature_config: runtime::RuntimeFeatureConfig::default(),
                    tool_registry: catalog,
                    plugin_registry: plugins::PluginRegistry::default(),
                },
            ),
        );
        let activation = Arc::new(SessionActivationCoordinator::new(
            runtime.clone(),
            repository,
            presence,
            Arc::new(runtime::session_lifecycle::SessionWorkingSetManager::new(
                runtime::session_lifecycle::SessionLifecycleConfig::default(),
            )),
            None,
            runtime::SessionRecoveryConfig::default(),
        ));
        let session = Arc::new(crate::services::SessionService::new_unbound(
            runtime.clone(),
            activation,
        ));
        port.bind(&session).unwrap();
        let supervisor = crate::session_runtime_bridge::SessionWorkerSupervisor::start(
            runtime.clone(),
            session.clone(),
            event_bus,
        )
        .await
        .unwrap();
        session.install_supervisor(supervisor.clone()).unwrap();
        assert!(!supervisor.is_accepting());
        crate::runtime_host::restore_runtime_before_admission(&session, &supervisor, &services)
            .await
            .unwrap();
        assert!(!supervisor.is_accepting());
        services.release_recovered_producers().await.unwrap();
        supervisor.release_recovered_producers();
        assert!(supervisor.is_accepting());
        if let Some(witness) = &delivery_hits {
            durable_until(|| {
                (witness.idle_hits.load(std::sync::atomic::Ordering::SeqCst) > 0).then_some(())
            })
            .await;
        }
        Self {
            config_home,
            topology,
            runtime,
            session,
            supervisor,
            notification_reader,
            physical_calls,
            physical_holds,
            bounded_missing,
            lane_hits,
            delivery_hits,
            recovered_candidate,
        }
    }

    async fn stop(self) -> crate::selected_storage::DetachedTestPostgresRestartLease {
        self.stop_with_original_failure_export(None, None).await
            .unwrap_or_else(|error| panic!("original Gateway stop unresolved: {error}"))
    }

    async fn stop_with_original_failure_export(
        self,
        original: Option<(String, String, Option<String>, String)>,
        mut progress: Option<tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>>,
    ) -> Result<crate::selected_storage::DetachedTestPostgresRestartLease, String> {
        use futures::FutureExt;
        let Self {
            topology,
            runtime,
            session,
            supervisor,
            notification_reader,
            physical_calls,
            ..
        } = self;
        let runtime_witness = Arc::downgrade(&runtime);
        let session_witness = Arc::downgrade(&session);
        let supervisor_witness = Arc::downgrade(&supervisor);
        let services = runtime.runtime_services();
        let services_witness = Arc::downgrade(&services);
        let host_witness = Arc::downgrade(services.tool_execution_host().unwrap());
        let tool_host_witness = Arc::downgrade(&runtime.tool_host());
        let events_witness = Arc::downgrade(&topology.runtime_event_store);
        let artifacts_witness = Arc::downgrade(&topology.artifact_store);
        let sessions_witness = Arc::downgrade(&topology.session_store);
        let tasks_witness = Arc::downgrade(&topology.task_service);
        let memory_witness = Arc::downgrade(&topology.memory_store);
        let knowledge_witness = Arc::downgrade(&topology.knowledge_store);
        let fact_witness = Arc::downgrade(&topology.fact_ledger);
        let matrix_witness = Arc::downgrade(&topology.matrix_store);
        let surface_witness = Arc::downgrade(&topology.surface_messages);
        let connector_witness = Arc::downgrade(&topology.connector_factory);
        let mut cleanup_stage = "stop_accepting";
        let mut execution_report = None;
        let mut first_execution_report = None;
        let shutdown = std::panic::AssertUnwindSafe(async {
            runtime.stop_accepting_for_recovery();
            supervisor.stop_accepting();
            runtime.gateway_tasks.stop_accepting();
            cleanup_stage = "runtime_execution";
            let first_execution = services.shutdown_execution().await;
            first_execution_report = serde_json::to_value(&first_execution).ok();
            let mut completion = services.subscribe_execution_shutdown_completion()
                .ok_or("original Runtime execution finalizer was not registered; custody unresolved")?;
            let deadline = services.execution_shutdown_deadline()
                .ok_or("original Runtime execution deadline unavailable; custody unresolved")?;
            let execution = 'original_completion: loop {
                if let Some(done) = completion.borrow().clone() {
                    match done {
                        runtime::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed } => {
                            execution_report = serde_json::to_value(&report).ok();
                            if !custody_closed { return Err("original Runtime execution custody remains unresolved".to_string()); }
                            break report;
                        }
                    }
                }
                match tokio::time::timeout_at(deadline, completion.changed()).await {
                    Ok(Ok(())) => {},
                    Ok(Err(_)) => return Err("original Runtime execution finalizer ended without a completion; custody unresolved".to_string()),
                    Err(_) => {
                        eprintln!("full private cleanup evidence pending; process exit may interrupt export");
                        let first_report = first_execution_report.as_ref()
                            .ok_or_else(|| "original first execution report could not be serialized".to_string())
                            .and_then(|report| serde_json::to_vec(&json!({
                                "cleanup_stage": cleanup_stage,
                                "first_execution_report": report,
                            })).map_err(|error| error.to_string()));
                        match progress.take() {
                            Some(sender) => { let _ = sender.send(first_report); }
                            None => return Err("original Runtime execution remains unresolved at its first deadline".to_string()),
                        }
                        if let Some((session_id, turn, graph, model)) = original.as_ref() {
                            let route_facts = json!({"cleanup_stage":cleanup_stage, "first_execution_report":first_execution_report});
                            let _ = std::panic::AssertUnwindSafe(c_export_original_failure(
                                &services, &physical_calls, session_id, turn, graph.clone(), model,
                                "original Runtime execution unresolved at its first deadline",
                                "original Session control before cleanup", "original Goal before cleanup", None, Some(route_facts)
                            )).catch_unwind().await;
                        }
                        // This is the original stop task, not a second observer or
                        // a renewed deadline. Keep its dependencies until the
                        // already-started Runtime finalizer actually ends.
                        loop {
                            if let Some(done) = completion.borrow().clone() {
                                match done {
                                    runtime::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed } => {
                                        execution_report = serde_json::to_value(&report).ok();
                                        if !custody_closed { return Err("original Runtime execution custody remains unresolved".to_string()); }
                                        break 'original_completion report;
                                    }
                                }
                            }
                            completion.changed().await.map_err(|_| "original Runtime execution finalizer ended without a completion; custody unresolved".to_string())?;
                        }
                    }
                }
            };
            assert_eq!(first_execution.forced_aborts, 0, "original first shutdown observation required forced abort");
            assert!(first_execution.errors.is_empty(), "original first shutdown observation has {} error(s)", first_execution.errors.len());
            assert_eq!(execution.forced_aborts, 0,
                "runtime execution shutdown: errors={} owners={} remaining={} accepted={} completed={} failed={}",
                execution.errors.len(), execution.owners.len(), execution.remaining_keys,
                execution.accepted, execution.completed, execution.failed);
            assert!(execution.errors.is_empty(),
                "runtime execution shutdown incomplete: {} error(s)", execution.errors.len());
            assert_eq!(execution.remaining_keys, 0, "runtime execution still has registered keys");
            assert!(execution.owners.values().all(|owner| owner.active == 0),
                "runtime execution still has active owners");
            cleanup_stage = "session_supervisor";
            supervisor.shutdown().await;
            let workers = supervisor.health();
            assert!(!workers.accepting);
            assert_eq!(workers.forced_aborts, 0);
            cleanup_stage = "gateway_tasks";
            let tasks = runtime.gateway_tasks.shutdown().await;
            assert_eq!(tasks.panicked, 0);
            assert_eq!(tasks.forced_aborts, 0);
            cleanup_stage = "runtime_maintenance";
            services.shutdown_maintenance().await;
            Ok::<(), String>(())
        }).catch_unwind().await;
        if !matches!(&shutdown, Ok(Ok(()))) {
            if let Some((session_id, turn, graph, model)) = original {
                let failure = match &shutdown {
                    Ok(Err(error)) => error.as_str(),
                    Err(panic) => panic.downcast_ref::<String>().map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("non-string cleanup panic"),
                    Ok(Ok(())) => unreachable!(),
                };
                let route_facts = json!({"cleanup_stage":cleanup_stage, "first_execution_report":first_execution_report, "shutdown_execution_report":execution_report});
                let export = std::panic::AssertUnwindSafe(c_export_original_failure(
                    &services, &physical_calls, &session_id, &turn, graph, &model, failure,
                    "original Session control before cleanup", "original Goal before cleanup", None, Some(route_facts)
                )).catch_unwind().await;
                if export.is_err() { eprintln!("original cleanup failure export panicked"); }
            }
            match shutdown {
                Ok(Err(error)) => return Err(error),
                Err(panic) => std::panic::resume_unwind(panic),
                Ok(Ok(())) => unreachable!(),
            }
        }
        drop(supervisor);
        drop(session);
        drop(runtime);
        drop(services);
        drop(notification_reader);
        durable_until(|| services_witness.upgrade().is_none().then_some(())).await;
        assert!(supervisor_witness.upgrade().is_none());
        assert!(session_witness.upgrade().is_none());
        assert!(runtime_witness.upgrade().is_none());
        assert!(services_witness.upgrade().is_none());
        assert!(host_witness.upgrade().is_none());
        assert!(tool_host_witness.upgrade().is_none());
        let lease = topology.detach_for_test_restart().unwrap();
        assert!(events_witness.upgrade().is_none());
        assert!(artifacts_witness.upgrade().is_none());
        assert!(sessions_witness.upgrade().is_none());
        assert!(tasks_witness.upgrade().is_none());
        assert!(memory_witness.upgrade().is_none());
        assert!(knowledge_witness.upgrade().is_none());
        assert!(fact_witness.upgrade().is_none());
        assert!(matrix_witness.upgrade().is_none());
        assert!(surface_witness.upgrade().is_none());
        assert!(connector_witness.upgrade().is_none());
        Ok(lease)
    }
}

async fn n3_reply_under_single_provider_lease(
    provider: &mut LoopbackProvider,
    services: &RecoveryServices,
) {
    // This original Task leaf sends one non-streaming structured request, not
    // the conversation TaskRouter control RPC consumed by provider.next().
    assert!(provider.pending_request.is_none());
    assert_eq!(provider.task_route_controls, 0);
    let request = tokio::time::timeout(Duration::from_secs(90), provider.requests.recv())
        .await
        .expect("original structured Provider request deadline")
        .expect("original structured Provider channel closed");
    assert_eq!(request.body["model"], "test-model");
    let runtime = services.runtime.runtime_services();
    let capacity = runtime
        .resource_manager()
        .snapshot(&runtime::execution_core::graph::ExecutionResourceKind::Provider)
        .unwrap();
    assert_eq!(capacity.effective_limit, 1);
    assert_eq!(capacity.active_leases, 1);
    assert_eq!(request.body["stream"], false);
    let prompt: Value = serde_json::from_str(
        request.body["messages"].as_array().unwrap().last().unwrap()["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        prompt["untrusted_application_input"],
        json!({"request":"bounded result"})
    );
    assert_eq!(
        prompt["result_contract"],
        json!({"contract_id":"result:structured","schema":{
            "type":"object","properties":{"value":{"type":"integer"}},
            "required":["value"],"additionalProperties":false
        },"max_bytes":1024})
    );
    assert_eq!(
        prompt["instruction"],
        "Return one JSON object with value 37"
    );
    request
        .reply
        .send(json!({"role":"assistant","content":"{\"value\":37}"}))
        .unwrap();
}

// Full Runtime/PG recovery assets: compile during development, execute at N8.
// Each fault destroys old adapters; production recovery, not a second APP start,
// owns registration, the original attempt and the result projection.
async fn n3_structured_recovery_case(point: runtime::task::StructuredTaskFaultPoint) {
    use crate::services::{EnsureSessionRequest, SessionSource};
    use harness_contract::task::*;
    use sha2::{Digest, Sha256};
    // Retain synthetic artifacts together with the original namespace for failure diagnosis.
    let root = fixture_root().keep();
    eprintln!("n3 original recovery artifact root={}", root.display());
    std::fs::create_dir_all(root.as_path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.as_path().join("home"),
        &root.as_path().join("workspace"),
    )
    .unwrap();
    if let Some(schema) = topology.preserve_failed_test_namespace() {
        eprintln!("n3 original recovery evidence schema={schema}");
    }
    let services =
        RecoveryServices::start_with_single_provider_slot(root.as_path(), topology, providers.clone())
            .await;
    let session_id = format!("structured-recovery:{}", uuid::Uuid::new_v4());
    let mut session_request =
        EnsureSessionRequest::new(&session_id, Some("test-model".into()), SessionSource::Tui);
    session_request.owner_principal_id = Some("recovery-operator".into());
    services
        .session
        .create_user_session(session_request)
        .await
        .unwrap();
    let runtime = services.runtime.runtime_services();
    let task_id = format!("task-structured-{}", uuid::Uuid::new_v4());
    let schema = json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false});
    let schema_bytes = cowd_app_protocol::canonical_result_schema_bytes(&schema).unwrap();
    let bodies = [
        (
            "instruction",
            b"Return one JSON object with value 37".to_vec(),
            "text/plain",
        ),
        (
            "input",
            br#"{"request":"bounded result"}"#.to_vec(),
            "application/json",
        ),
        ("schema", schema_bytes.clone(), "application/schema+json"),
    ];
    let mut refs = Vec::new();
    for (name, bytes, media) in bodies {
        let reference = runtime
            .artifact_store()
            .write_bytes_with_identity(
                runtime::ArtifactWriteDescriptor {
                    media_type: media.into(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: Some(name.into()),
                },
                &bytes,
                format!("{task_id}-{name}"),
            )
            .await
            .unwrap();
        runtime
            .artifact_store()
            .pin(
                &reference,
                &format!("runtime.structured-task:{task_id}"),
                runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            )
            .unwrap();
        refs.push(reference);
    }
    let mut spec = TaskSpec::new("Return the original schema-validated application result");
    spec.application_provenance = Some(TaskApplicationProvenance {
        model: "test-model".into(),
        producer_id: "app:structured-recovery".into(),
        workspace_id: "workspace:recovery".into(),
        surface: "panel".into(),
        request_id: format!("request:{task_id}"),
        request_idempotency_key: Some(format!("once:{task_id}")),
        result_contract_id: "result:structured".into(),
        result_schema_id: "schema:structured".into(),
        result_schema_version: 1,
        result_schema_digest: format!("{:x}", Sha256::digest(&schema_bytes)),
        result_max_bytes: 1024,
        instruction_ref: refs[0].clone(),
        input_ref: refs[1].clone(),
        result_schema_ref: refs[2].clone(),
        request_digest: format!("{:x}", Sha256::digest(task_id.as_bytes())),
    });
    let spec = runtime
        .task_runtime_port()
        .bind_task_spec(&session_id, None, spec)
        .unwrap();
    let command = TaskCreateCommand {
        task_id: task_id.clone(),
        mission_id: runtime.mission_runtime().default_mission_id().into(),
        kind: TaskKind::Root,
        origin: TaskOrigin::System,
        origin_session_id: session_id.clone(),
        origin_turn_id: format!("application-turn:{task_id}"),
        root_task_id: task_id.clone(),
        parent_task_id: None,
        predecessor_task_id: None,
        mission_assignment: TaskMissionAssignment::ExplicitLocked,
        mission_assigned_by: "gateway.application_structured_task".into(),
        spec,
        evidence_refs: Vec::new(),
    };
    runtime
        .task_aggregate_service()
        .inject_structured_fault_for_test(&task_id, point);
    let creation = runtime.create_structured_task(command.clone()).await;
    if point == runtime::task::StructuredTaskFaultPoint::AfterTaskCommitBeforeEvidence {
        assert!(creation.unwrap_err().contains("structured_fault_injected"));
    } else {
        creation.unwrap();
        if point == runtime::task::StructuredTaskFaultPoint::BeforeTaskResultProjection {
            runtime.register_structured_task(&task_id).await.unwrap();
            n3_reply_under_single_provider_lease(&mut provider, &services).await;
            durable_until(|| {
                (runtime
                    .task_aggregate_service()
                    .structured_fault_hits_for_test(&task_id)
                    > 0)
                .then_some(())
            })
            .await;
        } else {
            assert!(runtime
                .register_structured_task(&task_id)
                .await
                .unwrap_err()
                .contains("structured_fault_injected"));
        }
    }
    assert!(
        runtime
            .task_aggregate_service()
            .structured_fault_hits_for_test(&task_id)
            > 0,
        "fault must hit its original owner"
    );
    let before = runtime
        .task_aggregate_service()
        .get(&task_id)
        .unwrap()
        .unwrap();
    let before_binding = before.structured_registration.clone().unwrap();
    let original_creation_outbox: String = services
        .topology
        .postgres_executor
        .checkout_online_read()
        .unwrap()
        .query_one(
            "SELECT outbox_id FROM runtime_task_evidence_outbox WHERE task_id=$1 AND revision=1",
            &[&task_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(before.status, TaskStatus::Running);
    if point == runtime::task::StructuredTaskFaultPoint::AfterConfirmationBeforeWake {
        assert!(before_binding.receipt.is_some());
    }
    drop(runtime);
    // stop() drops every old domain adapter. start() runs the real ordered
    // recovery barrier and release_recovered_producers before returning.
    let lease = services.stop().await;
    let topology = lease.rebuild().unwrap();
    drop(lease);
    let services =
        RecoveryServices::start_with_single_provider_slot(root.as_path(), topology, providers).await;
    if point != runtime::task::StructuredTaskFaultPoint::BeforeTaskResultProjection {
        n3_reply_under_single_provider_lease(&mut provider, &services).await;
    }
    let runtime = services.runtime.runtime_services();
    let completed = durable_until(|| {
        runtime
            .task_aggregate_service()
            .get(&task_id)
            .unwrap()
            .filter(|task| task.status == TaskStatus::Completed)
    })
    .await;
    let binding = completed.structured_registration.as_ref().unwrap();
    assert_eq!(binding.graph_id, before_binding.graph_id);
    assert_eq!(binding.binding_digest, before_binding.binding_digest);
    if before_binding.receipt.is_some() {
        assert_eq!(binding.receipt, before_binding.receipt);
    }
    assert!(binding.receipt.is_some());
    let graph = runtime.graph_state_store().load(&binding.graph_id).unwrap();
    assert_eq!(
        graph.recovery_cursor.node_attempts.get(&binding.node_id),
        Some(&1)
    );
    let artifacts = completed
        .phases
        .iter()
        .flat_map(|phase| &phase.artifacts)
        .collect::<Vec<_>>();
    assert_eq!(
        artifacts
            .iter()
            .filter(|artifact| artifact.kind == "application_structured_attempt.v1")
            .count(),
        1
    );
    let result = artifacts
        .iter()
        .find(|artifact| artifact.kind == "application_structured_result.v1")
        .unwrap();
    let value: Value = serde_json::from_str(&result.value).unwrap();
    assert_eq!(value["result"], json!({"value":37}));
    assert_eq!(value["attempt"], 1);
    assert_eq!(value["graph_id"], binding.graph_id);
    assert_eq!(value["usage"]["input_tokens"], 13);
    assert_eq!(value["usage"]["output_tokens"], 7);
    runtime
        .validate_structured_result_identity(
            &completed,
            &binding.graph_id,
            &binding.node_id,
            1,
            &binding.binding_digest,
            &result.value,
        )
        .unwrap();
    let replay = runtime
        .create_structured_task(command.clone())
        .await
        .unwrap();
    assert_eq!(replay.command_receipt.accepted_revision, 1);
    assert_eq!(replay.command_receipt.outbox_id, original_creation_outbox);
    for fault in ["input", "schema", "producer", "source"] {
        let mut changed = command.clone();
        let provenance = changed.spec.application_provenance.as_mut().unwrap();
        match fault {
            "input" => provenance.input_ref.sha256 = format!("sha256:{}", "d".repeat(64)),
            "schema" => provenance.result_schema_digest = "d".repeat(64),
            "producer" => provenance.producer_id = "app:foreign".into(),
            _ => changed.origin_turn_id = "foreign-turn".into(),
        }
        assert!(
            runtime.create_structured_task(changed).await.is_err(),
            "{fault}"
        );
    }
    assert!(
        provider.requests.is_empty(),
        "recovery/replay/foreign inputs must not create another Provider call"
    );
    drop(runtime);
    let _lease = services.stop().await;
}

#[tokio::test]
async fn n3_task_committed_before_evidence_recovers_without_app_resubmission() {
    n3_structured_recovery_case(
        runtime::task::StructuredTaskFaultPoint::AfterTaskCommitBeforeEvidence,
    )
    .await;
}
#[tokio::test]
async fn n3_evidence_projected_before_graph_recovers_without_app_resubmission() {
    n3_structured_recovery_case(runtime::task::StructuredTaskFaultPoint::BeforeGraphRegistration)
        .await;
}
#[tokio::test]
async fn n3_graph_committed_before_confirmation_recovers_same_graph() {
    n3_structured_recovery_case(
        runtime::task::StructuredTaskFaultPoint::AfterGraphRegistrationBeforeConfirmation,
    )
    .await;
}
#[tokio::test]
async fn n3_confirmation_before_wake_recovers_through_original_supervisor() {
    n3_structured_recovery_case(
        runtime::task::StructuredTaskFaultPoint::AfterConfirmationBeforeWake,
    )
    .await;
}
#[tokio::test]
async fn n3_artifact_and_graph_result_restart_projects_without_another_provider() {
    n3_structured_recovery_case(
        runtime::task::StructuredTaskFaultPoint::BeforeTaskResultProjection,
    )
    .await;
}

#[tokio::test]
async fn r1_restart_harness_rebuilds_services_through_production_admission_barrier() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let services =
        RecoveryServices::start(root.path(), topology, super::test_bound_provider_registry()).await;
    let lease = services.stop().await;
    let namespace = lease.namespace().to_string();
    let rebuilt = lease.rebuild().unwrap();
    drop(lease);
    let services =
        RecoveryServices::start(root.path(), rebuilt, super::test_bound_provider_registry()).await;
    let next = services.stop().await;
    assert_eq!(next.namespace(), namespace);
}

#[tokio::test]
async fn r1_delivery_fault_barrier_requires_exact_identity_and_is_one_shot() {
    let terminal = uuid::Uuid::new_v4().to_string();
    let mut barrier = register(&terminal, "owned-worker", 9, Point::BeforeAck).unwrap();
    assert!(register(&terminal, "owned-worker", 9, Point::BeforeAck).is_err());
    checkpoint(&terminal, "foreign-worker", 9, Point::BeforeAck)
        .await
        .unwrap();
    checkpoint(&terminal, "owned-worker", 8, Point::BeforeAck)
        .await
        .unwrap();
    checkpoint(&terminal, "owned-worker", 9, Point::AfterAck)
        .await
        .unwrap();
    assert!(matches!(
        barrier.hit.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let task_terminal = terminal.clone();
    let task = tokio::spawn(async move {
        checkpoint(&task_terminal, "owned-worker", 9, Point::BeforeAck).await
    });
    let hit = tokio::time::timeout(std::time::Duration::from_secs(5), &mut barrier.hit)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hit.terminal_id, terminal);
    assert_eq!(hit.worker_id, "owned-worker");
    assert_eq!(hit.revision, 9);
    assert_eq!(hit.point, Point::BeforeAck);
    assert!(!task.is_finished());
    barrier.release(Release::Continue).unwrap();
    assert!(barrier.release(Release::Continue).is_err());
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    checkpoint(&terminal, "owned-worker", 9, Point::BeforeAck)
        .await
        .unwrap();
}

#[tokio::test]
async fn r1_delivery_fault_response_loss_is_explicit_at_each_real_boundary() {
    for point in [
        Point::BeforeSessionCommit,
        Point::AfterSessionCommit,
        Point::BeforePublish,
        Point::BeforeAck,
        Point::AfterAck,
    ] {
        let terminal = uuid::Uuid::new_v4().to_string();
        let mut barrier = register(&terminal, "owned-worker", 3, point).unwrap();
        let task =
            tokio::spawn(async move { checkpoint(&terminal, "owned-worker", 3, point).await });
        let hit = tokio::time::timeout(std::time::Duration::from_secs(5), &mut barrier.hit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hit.point, point);
        barrier.release(Release::ResponseLost).unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error,
            format!("injected terminal delivery response loss at {point:?}")
        );
    }
}

#[tokio::test]
async fn r1_delivery_fault_drop_unblocks_and_removes_only_owned_registration() {
    let terminal = uuid::Uuid::new_v4().to_string();
    let mut first = register(&terminal, "owned-worker", 1, Point::BeforePublish).unwrap();
    let task_terminal = terminal.clone();
    let task = tokio::spawn(async move {
        checkpoint(&task_terminal, "owned-worker", 1, Point::BeforePublish).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut first.hit)
        .await
        .unwrap()
        .unwrap();
    let mut second = register(&terminal, "owned-worker", 2, Point::BeforePublish).unwrap();
    drop(first);
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(register(&terminal, "owned-worker", 2, Point::BeforePublish).is_err());
    let next = tokio::spawn(async move {
        checkpoint(&terminal, "owned-worker", 2, Point::BeforePublish).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut second.hit)
        .await
        .unwrap()
        .unwrap();
    drop(second);
    tokio::time::timeout(std::time::Duration::from_secs(5), next)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let terminal = uuid::Uuid::new_v4().to_string();
    let unhit = register(&terminal, "owned-worker", 4, Point::AfterAck).unwrap();
    drop(unhit);
    let replacement = register(&terminal, "owned-worker", 4, Point::AfterAck).unwrap();
    drop(replacement);
    checkpoint(&terminal, "owned-worker", 4, Point::AfterAck)
        .await
        .unwrap();
}

#[tokio::test]
async fn r1_delivery_fault_points_are_independent_and_require_nonempty_identities() {
    assert!(register("", "worker", 1, Point::BeforeAck).is_err());
    assert!(register("terminal", "", 1, Point::BeforeAck).is_err());
    let terminal = uuid::Uuid::new_v4().to_string();
    let before = register(&terminal, "worker", 1, Point::BeforeAck).unwrap();
    let after = register(&terminal, "worker", 1, Point::AfterAck).unwrap();
    assert!(register(&terminal, "worker", 1, Point::BeforeAck).is_err());
    assert!(register(&terminal, "worker", 1, Point::AfterAck).is_err());
    drop(before);
    drop(after);
}

fn fixture_root() -> tempfile::TempDir {
    let root = std::env::var_os("COWD_TEST_FIXTURE_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("detached-adapter-restart-")
        .tempdir_in(root)
        .unwrap()
}

#[tokio::test]
async fn r1_restart_fixture_destroys_domain_adapters_and_preserves_namespace() {
    let root = fixture_root();
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let topology = SelectedStorageTopology::compose_for_test(&home, &workspace).unwrap();
    let registry = serde_json::to_value(&topology.registry).unwrap();
    let session_id = format!("restart-fixture:{}", uuid::Uuid::new_v4());
    let timestamp = chrono::Utc::now().to_rfc3339();
    let session = session::SessionRecord {
        session_id: session_id.clone(),
        platform: "test".into(),
        chat_id: session_id.clone(),
        user_id: None,
        model: None,
        created_at: timestamp.clone(),
        last_activity: timestamp,
        message_count: 0,
        reset_policy: "manual".into(),
        metadata_json: None,
        input_tokens: 0,
        output_tokens: 0,
        status: "active".into(),
    };
    topology
        .session_store
        .create_session(&session)
        .await
        .unwrap();
    let event = topology
        .runtime_event_store
        .append(runtime::RuntimeEventInput {
            stream_id: format!("restart-fixture:{session_id}"),
            scope: runtime::RuntimeEventScope::Session,
            kind: "test.adapter_restart.witness.v1".into(),
            status: None,
            actor: None,
            refs: Vec::new(),
            payload: serde_json::json!({"session_id": session_id}),
        })
        .unwrap();
    let bytes = b"adapter restart persistence witness, not a terminal transcript";
    let visibility = format!("session:{session_id}");
    let artifact = topology
        .artifact_store
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".into(),
                visibility_scope: visibility.clone(),
                expected_bytes: Some(bytes.len() as u64),
                original_name: None,
            },
            bytes,
        )
        .await
        .unwrap();
    let old_sessions = Arc::downgrade(&topology.session_store);
    let old_events = Arc::downgrade(&topology.runtime_event_store);
    let old_artifacts = Arc::downgrade(&topology.artifact_store);
    let old_tasks = Arc::downgrade(&topology.task_service);
    let lease = topology.detach_for_test_restart().unwrap();
    let namespace = lease.namespace().to_string();
    assert!(old_sessions.upgrade().is_none());
    assert!(old_events.upgrade().is_none());
    assert!(old_artifacts.upgrade().is_none());
    assert!(old_tasks.upgrade().is_none());
    for _restart in 0..2 {
        let rebuilt = lease.rebuild().unwrap();
        assert_eq!(serde_json::to_value(&rebuilt.registry).unwrap(), registry);
        assert_eq!(lease.namespace(), namespace);
        assert!(lease.rebuild().is_err());
        let restored = rebuilt
            .session_store
            .get_session(&session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.session_id, session.session_id);
        assert_eq!(restored.created_at, session.created_at);
        assert_eq!(restored.message_count, 0);
        assert_eq!(restored.input_tokens, 0);
        assert_eq!(restored.output_tokens, 0);
        assert_eq!(
            rebuilt
                .runtime_event_store
                .list_stream(&event.stream_id)
                .unwrap(),
            vec![event.clone()]
        );
        assert_eq!(
            rebuilt
                .artifact_store
                .read(&artifact, &visibility, None)
                .await
                .unwrap(),
            bytes
        );
        let current_sessions = Arc::downgrade(&rebuilt.session_store);
        let current_events = Arc::downgrade(&rebuilt.runtime_event_store);
        let current_artifacts = Arc::downgrade(&rebuilt.artifact_store);
        drop(rebuilt);
        assert!(current_sessions.upgrade().is_none());
        assert!(current_events.upgrade().is_none());
        assert!(current_artifacts.upgrade().is_none());
    }
}

#[tokio::test]
async fn r1_restart_fixture_separate_adapter_does_not_receive_writer_doorbell() {
    let root = fixture_root();
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let topology =
        SelectedStorageTopology::compose_for_test(&root.path().join("home"), &workspace).unwrap();
    let reader =
        runtime_postgres::PostgresRuntimeEventStore::new(topology.postgres_executor.clone())
            .unwrap()
            .into_runtime_event_store();
    let doorbell = reader.subscribe_commits();
    let before = reader.current_commit_cursor();
    let event = topology
        .runtime_event_store
        .append(runtime::RuntimeEventInput {
            stream_id: format!("restart-fixture:{}", uuid::Uuid::new_v4()),
            scope: runtime::RuntimeEventScope::Session,
            kind: "test.adapter_restart.doorbell.v1".into(),
            status: None,
            actor: None,
            refs: Vec::new(),
            payload: serde_json::json!({}),
        })
        .unwrap();
    assert!(!doorbell.has_changed().unwrap());
    assert_eq!(reader.current_commit_cursor(), before);
    assert!(reader.durable_commit_cursor().unwrap() >= event.commit_cursor);
    assert_eq!(reader.list_stream(&event.stream_id).unwrap(), vec![event]);
}

// ROUND437 replays the retained completed model item, not a reconstructed wire.
// Only original local Team metadata is applied; no paid model or external tool.
#[test]
fn round437_original_six_team_batch_crosses_host_to_next_node_on_default_stack() {
    let threads = crate::gateway_capacity::configure_runtime_threads(&runtime::GatewayCapacityConfig::default()).unwrap();
    eprintln!("round437 shared_runtime workers={} stack_bytes={}", threads.workers, threads.stack_bytes);
    crate::SHARED_RT.block_on(async {
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    let root = fixture_root().keep();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(&root.join("home"), &root.join("workspace")).unwrap();
    eprintln!("round437 replay_root={} schema={:?}", root.display(), topology.preserve_failed_test_namespace());
    let services = RecoveryServices::start(&root, topology, providers).await;
    let session_id = format!("round437:{}", uuid::Uuid::new_v4());
    let mut request = EnsureSessionRequest::new(&session_id, Some("test-model".into()), SessionSource::Tui);
    request.owner_principal_id = Some("round437-local-replay".into());
    services.session.create_user_session(request).await.unwrap();
    let admission = services.session.admit_input(harness_contract::turn::SessionInputEnvelope::text(
        &session_id, harness_contract::turn::InputSourceKind::Tui,
        "Create six real Teams with separate missions for reading the runtime and gateway source. Do not invite Agents or run external tools in this local reproduction.",
    )).await.unwrap();
    let content = r###"{"actions": [{"input": {"mission": "Read and analyze core agentic program structure files with dual-agent verification", "name": "Program Core Readers"}, "tool": "team_create"}, {"input": {"mission": "Read and analyze action service and work market files with dual-agent verification", "name": "Action Market Readers"}, "tool": "team_create"}, {"input": {"mission": "Read and analyze messaging and topic files with dual-agent verification", "name": "Messaging Readers"}, "tool": "team_create"}, {"input": {"mission": "Read and analyze supervision and recovery files with dual-agent verification", "name": "Supervision Recovery Readers"}, "tool": "team_create"}, {"input": {"mission": "Read and analyze worker and gateway files with dual-agent verification", "name": "Worker Gateway Readers"}, "tool": "team_create"}, {"input": {"mission": "Read and analyze runtime routes and services files, then synthesize cross-team findings into final stress report", "name": "Routes Services Synthesis"}, "tool": "team_create"}]}"###;
    let input: Value = serde_json::from_str(content).unwrap();
    assert_eq!(input["actions"].as_array().unwrap().len(), 6);
    eprintln!("round437 admitted graph={} turn={} original_model_content_bytes={}", admission.execution_graph_id, admission.turn_id, content.len());
    provider.tool("round437-original-batch", "agent_action_batch", input, None).await;
    eprintln!("round437 original_model_reply_sent");
    provider.wait_for_tool_result("round437-original-batch").await;
    eprintln!("round437 next_model_input_contains_original_tool_result");
    let program = services.runtime.runtime_services().agent_action_service().project(
        &harness_contract::agent_action::program_id_for_objective(&harness_contract::agent_action::root_objective_id(&session_id, &admission.turn_id)),
    ).unwrap();
    assert_eq!(program.teams.len(), 6, "every original action must commit before the next model input");
    // Check the member indices against the actual action and transaction receipt.
    use runtime::RuntimeEventStoreBackend as _;
    let runtime = services.runtime.runtime_services();
    let program_stream = format!("agentic-program:{}", program.program_id);
    let events = runtime.event_reader().all_events(100_001).unwrap();
    assert!(events.len() < 100_001, "original event history was truncated");
    let mut members: Vec<_> = events.iter().filter(|event| {
        event.kind == "agentic.batch_member.index"
            && event.payload["ticket"]["graph_id"] == admission.execution_graph_id
            && event.payload["program_stream"] == program_stream
    }).collect();
    members.sort_by_key(|event| event.payload["member_index"].as_u64());
    assert_eq!(members.len(), 6, "each original batch member needs one committed index");
    let outer = members[0].payload["outer_key"].as_str().unwrap();
    let backend = runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone()).unwrap();
    for (index, member) in members.iter().enumerate() {
        assert_eq!(member.payload["member_index"].as_u64(), Some(index as u64));
        assert_eq!(member.payload["outer_key"].as_str(), Some(outer));
        let position = member.payload["program_event_index"].as_u64().unwrap() as usize;
        let action = events.iter().find(|event| event.transaction_id == member.transaction_id
            && event.transaction_index as usize == position).expect("original member action missing");
        assert_eq!(action.stream_id, program_stream);
        assert_eq!(action.kind, "agentic.action_applied");
        assert_eq!(member.payload["program_event_kind"].as_str(), Some(action.kind.as_str()));
        assert_eq!(member.payload["program_event_key"].as_str(), action.idempotency_key.as_deref());
        assert_eq!(action.payload["member_admission"]["original_hook"]["outer_request"]["tool_use_id"],
            "round437-original-batch");
        assert_eq!(action.payload["member_admission"]["original_hook"]["outer_request"]["idempotency_key"].as_str(), Some(outer));
        assert_eq!(action.commit_cursor, member.commit_cursor);
        let receipt = backend.original_transaction_receipt(&member.transaction_id).unwrap()
            .expect("original member transaction receipt missing");
        assert_eq!(receipt.commit_cursor, member.commit_cursor);
        assert_eq!(receipt.event_ids.get(position), Some(&action.event_id));
        assert_eq!(receipt.event_ids.get(member.transaction_index as usize), Some(&member.event_id));
    }
    drop(backend);
    drop(runtime);
    drop(provider);
    services.runtime.runtime_services().cancel_execution_tree(&admission.execution_graph_id, "round437 local reproduction complete").await.unwrap();
    services.stop().await;
    });
}

// One zero-cost local gate for the collaboration admission path and the final
// evidence verification chain.
//
// Phase 1 drives six real team creations in one original agent_action_batch on
// the DEFAULT tokio worker stack: the admission path must complete, and the
// session worker must shut down cleanly with zero forced aborts.
// Phase 2 drives the original claim -> artifact submit -> independent review ->
// final verification chain and requires the materialized terminal delivery.
//
// No model call, no paid run, ~10 seconds, repeatable.
async fn collaboration_admission_and_final_evidence_gate_body() {
    // Phase 1 fixture: six real team creations in one original batch. The
    // collaboration admission path must complete on the worker's stack and the
    // session worker must shut down without a forced abort.
    {
        let root = fixture_root();
        std::fs::create_dir_all(root.path().join("workspace")).unwrap();
        let mut provider = LoopbackProvider::start("root").await;
        let providers = provider_registry(&[&provider]);
        let topology = SelectedStorageTopology::compose_for_test(
            &root.path().join("home"),
            &root.path().join("workspace"),
        )
        .unwrap();
        let services = RecoveryServices::start(root.path(), topology, providers).await;
        let session_id = format!("gate:{}", uuid::Uuid::new_v4());
        use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
        let mut request =
            EnsureSessionRequest::new(&session_id, Some("test-model".into()), SessionSource::Tui);
        request.owner_principal_id = Some("gate-local-replay".into());
        services.session.create_user_session(request).await.unwrap();
        let admission = services
            .session
            .admit_input(harness_contract::turn::SessionInputEnvelope::text(
                &session_id,
                harness_contract::turn::InputSourceKind::Tui,
                "Create six real Teams with separate missions for reading the runtime and gateway source.",
            ))
            .await
            .unwrap();
        let actions = (0..6)
            .map(|index| {
                serde_json::json!({
                    "tool": "team_create",
                    "input": {"mission": format!("read lane {index}"), "name": format!("Gate Team {index}")},
                })
            })
            .collect::<Vec<_>>();
        let input = serde_json::json!({"actions": actions});
        provider.tool("gate-six-team-batch", "agent_action_batch", input, None).await;
        provider.wait_for_tool_result("gate-six-team-batch").await;
        let program = services
            .runtime
            .runtime_services()
            .agent_action_service()
            .project(&harness_contract::agent_action::program_id_for_objective(
                &harness_contract::agent_action::root_objective_id(&session_id, &admission.turn_id),
            ))
            .unwrap();
        assert_eq!(
            program.teams.len(),
            6,
            "every original team_create must be admitted before the next model input"
        );
        services
            .runtime
            .runtime_services()
            .cancel_execution_tree(&admission.execution_graph_id, "gate phase one complete")
            .await
            .unwrap();
        let worker_health = services.supervisor.health();
        assert_eq!(
            worker_health.forced_aborts, 0,
            "the collaboration path must not force a session worker abort at shutdown: {worker_health:?}"
        );
        drop(provider);
        drop(services.stop().await);
    }

}

// Component-level reproduction of the *observed action shape* of round601:
// two batches (two Teams, then four invitations), followed by another model
// request. This does not claim to replay its unavailable response or Gateway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "round601 zero-model stack investigation; requires isolated Postgres"]
async fn r604_two_team_four_invite_then_model_continuation() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"), &root.path().join("workspace"),
    ).unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    let session_id = format!("r604:{}", uuid::Uuid::new_v4());
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    let mut request = EnsureSessionRequest::new(&session_id, Some("test-model".into()), SessionSource::Tui);
    request.owner_principal_id = Some("r604-local-replay".into());
    services.session.create_user_session(request).await.unwrap();
    let admission = services.session.admit_input(harness_contract::turn::SessionInputEnvelope::text(
        &session_id, harness_contract::turn::InputSourceKind::Tui,
        "Create two teams, each with two independent reviewers, to examine the workspace.",
    )).await.unwrap();
    let program_id = harness_contract::agent_action::program_id_for_objective(
        &harness_contract::agent_action::root_objective_id(&session_id, &admission.turn_id),
    );
    provider.tool("r604-teams", "agent_action_batch", json!({"actions":[
        {"tool":"team_create","input":{"name":"R604 team A","mission":"Read workspace evidence A"}},
        {"tool":"team_create","input":{"name":"R604 team B","mission":"Read workspace evidence B"}}
    ]}), None).await;
    provider.wait_for_tool_result("r604-teams").await;
    let program = services.runtime.runtime_services().agent_action_service().project(&program_id).unwrap();
    let teams = program.teams.keys().cloned().collect::<Vec<_>>();
    assert_eq!(teams.len(), 2);
    let actions = (0..4).map(|index| json!({"tool":"agent_invite","input":{
        "team_ref":teams[index / 2], "role":format!("Reviewer {index}"),
        "mission":format!("Independently inspect evidence for lane {index}"),
        "required_capabilities":["read","search"], "execution_requirements":[], "expertise_hints":[]
    }})).collect::<Vec<_>>();
    provider.tool("r604-invites", "agent_action_batch", json!({"actions":actions}), None).await;
    provider.wait_for_tool_result("r604-invites").await;
    let program = services.runtime.runtime_services().agent_action_service().project(&program_id).unwrap();
    assert_eq!(program.agents.len(), 4, "all invitations must really be admitted");
    services.runtime.runtime_services().cancel_execution_tree(&admission.execution_graph_id, "r604 local reproduction complete").await.unwrap();
    assert_eq!(services.supervisor.health().forced_aborts, 0);
    drop(provider);
    services.stop().await;
}

/// The class-4 acceptance gate: the same body on the DEFAULT tokio worker
/// stack. It is ignored on purpose because today it aborts the whole test
/// process (deterministic red): the collaboration admission path needs more
/// than the default 2 MiB worker stack. Run it explicitly:
///
///   cargo test --offline -p gateway --lib \
///     'runtime_service::tests::r1_terminal_recovery::collaboration_admission_and_final_evidence_gate' \
///     -- --ignored --exact --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "class-4 acceptance gate: red until the admission path fits the default worker stack"]
async fn collaboration_admission_and_final_evidence_gate() {
    collaboration_admission_and_final_evidence_gate_body().await;
}

/// The same gate on a roomy stack: it proves the three phases themselves are
/// sound (six admitted teams, the claim -> artifact -> independent review ->
/// final verification chain, and a clean session worker shutdown), so the only
/// remaining class-4 failure is the stack depth of the admission path.
#[test]
fn collaboration_admission_and_final_evidence_gate_on_large_stack() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(64 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(collaboration_admission_and_final_evidence_gate_body());
}

#[path = "rc_session_receipt.rs"]
mod rc_session_receipt;

// Real Session ingress supplies the turn control and its original graph
// identity; the local provider only supplies deterministic model responses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run alone in an isolated process with an exclusive Session PG fixture"]
async fn b_scope_refusal_and_independent_read_reach_original_gateway_provider() {
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    use futures::FutureExt;
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value { std::env::set_var(key, value); }
                else { std::env::remove_var(key); }
            }
        }
    }
    let keys = ["COWD_EVAL_HARNESS", "COWD_EVAL_CORPUS_ID", "COWD_MODEL_TEMPERATURE"];
    let _restore = Restore(keys.into_iter().map(|key| (key, std::env::var_os(key))).collect());
    std::env::set_var("COWD_EVAL_HARNESS", "1");
    std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
    std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
    let root = fixture_root();
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("target.txt"), "scope fixture content").unwrap();
    std::fs::write(workspace.join("outside.txt"), "must not be read").unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(&root.path().join("home"), &workspace).unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    let runtime = services.runtime.runtime_services();
    let session = format!("b-scope-ingress:{}", uuid::Uuid::new_v4());
    let identity = std::sync::Mutex::new(None::<(String, String, String)>);
    let run = std::panic::AssertUnwindSafe(async {
        let mut request = EnsureSessionRequest::new(&session, Some(provider.model.clone()), SessionSource::Tui);
        request.owner_principal_id = Some("b-scope-local-fixture".into());
        services.session.create_user_session(request).await.unwrap();
        let control = json!({"corpus_id":"live-scenarios-v1", "workspace_fixture":"none",
            "provider_constraint":"normal", "temperature_milli":0,
            "resource_scopes":["read:target.txt"], "prompt":"Read target.txt."});
        let envelope = harness_contract::turn::SessionInputEnvelope::text(&session,
            harness_contract::turn::InputSourceKind::Tui,
            &format!("COWD_EVAL_CONTROL {control}\nRead target.txt and report its content."));
        let request_id = envelope.idempotency_key.clone();
        let admitted = services.session.admit_input(envelope).await.unwrap();
        let turn = admitted.turn_id;
        let graph = admitted.execution_graph_id;
        *identity.lock().unwrap() = Some((request_id.clone(), turn.clone(), graph.clone()));
        let input = services.session.runtime_input(&request_id).await.unwrap().expect("original Session input");
        assert_eq!(input.session_id, session);
        assert_eq!(input.turn_id, turn);
        eprintln!("b_scope_script stage=await_initial_request");
        provider.tool("b-scope-discover", "tool_search", json!({"query":"select:read_file"}), None).await;
        eprintln!("b_scope_script stage=discovery_reply_submitted");
        let request = provider.next().await;
        eprintln!("b_scope_script stage=awaiting_mixed_call_response");
        assert!(request.body["tools"].as_array().is_some_and(|tools| tools.iter().any(|tool|
            tool["function"]["name"] == "read_file")), "original next Step must expose read_file");
        request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
            {"index":0,"id":"b-scope-outside","type":"function","function":{"name":"read_file","arguments":json!({"path":"outside.txt"}).to_string()}},
            {"index":1,"id":"b-scope-allowed","type":"function","function":{"name":"read_file","arguments":json!({"path":"target.txt"}).to_string()}}
        ]})).unwrap();
        eprintln!("b_scope_script stage=mixed_reply_submitted");
        let next = provider.next().await;
        eprintln!("b_scope_script stage=continuation_received");
        let messages = next.body["messages"].as_array().expect("actual next provider messages");
        let refused = messages.iter().find(|message| message["role"] == "tool"
            && message["tool_call_id"] == "b-scope-outside").expect("original refusal delivered");
        let refused = refused["content"].to_string();
        assert!(refused.contains("evaluation_scope_exceeded") && refused.contains("read:target.txt")
            && refused.contains("policy and lease still apply"), "{refused}");
        let allowed = messages.iter().find(|message| message["role"] == "tool"
            && message["tool_call_id"] == "b-scope-allowed").expect("independent result delivered");
        assert!(allowed["content"].to_string().contains("scope fixture content"));
        let calls = services.physical_calls.lock().unwrap();
        assert!(calls.iter().any(|call| call.tool_use_id == "b-scope-allowed"));
        assert!(calls.iter().all(|call| call.tool_use_id != "b-scope-outside"));
        drop(calls);
        let facts = runtime.event_reader().all_events(4096).unwrap();
        assert!(facts.iter().any(|event| event.kind == "execution.call.refused"
            && event.payload["request"]["tool_use_id"] == "b-scope-outside"));
        assert!(facts.iter().any(|event| event.kind == "execution.read.receipt"
            && event.payload.to_string().contains("b-scope-allowed")));
        assert!(facts.iter().all(|event| event.kind != "execution.effect.intent"
            || !event.payload.to_string().contains("b-scope-outside")));
        let undispatched = facts.iter().filter_map(|event| event.payload.get("no_dispatch"))
            .flat_map(|fact| fact["calls"].as_array().into_iter().flatten()).collect::<Vec<_>>();
        assert_eq!(undispatched.iter().filter(|call| call["call_id"] == "b-scope-outside").count(), 1);
        assert!(undispatched.iter().all(|call| call["call_id"] != "b-scope-allowed"));
        next.reply.send(json!({"role":"assistant","content":"scope fixture content"})).unwrap();
    }).catch_unwind().await;
    if let Some((request_id, turn, graph)) = identity.lock().unwrap_or_else(|e| e.into_inner()).clone() {
        let active = services.runtime.stop_accepting_for_recovery();
        if let Ok(Some(input)) = services.session.runtime_input(&request_id).await {
            let _ = services.session.cancel_input(&session,
                harness_contract::turn::SessionInputId::from_string(input.input_id.to_string()),
                "local scope fixture complete").await;
        }
        let _ = services.session.cancel_active_execution(&session, &turn, &graph, "local scope fixture complete");
        let _ = runtime.cancel_execution_tree(&graph, "local scope fixture complete").await;
        let drain = services.runtime.wait_for_active_turns(active.len(), Duration::from_secs(30)).await;
        assert!(drain.remaining_turn_ids.is_empty(), "original Session turn must drain");
    }
    drop(runtime);
    drop(provider);
    let cleanup = std::panic::AssertUnwindSafe(services.stop()).catch_unwind().await;
    if let Err(panic) = run { std::panic::resume_unwind(panic); }
    if let Err(panic) = cleanup { std::panic::resume_unwind(panic); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run alone in an isolated process with an exclusive Session PG fixture"]
async fn original_host_orchestrated_search_novelty_and_unknown() {
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    use futures::FutureExt;
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value { std::env::set_var(key, value); }
                else { std::env::remove_var(key); }
            }
        }
    }
    let keys = ["COWD_EVAL_HARNESS", "COWD_EVAL_CORPUS_ID", "COWD_MODEL_TEMPERATURE"];
    let _restore = Restore(keys.into_iter().map(|key| (key, std::env::var_os(key))).collect());
    std::env::set_var("COWD_EVAL_HARNESS", "1");
    std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
    std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
    let root = fixture_root();
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("target.txt"), "scope fixture content").unwrap();
    std::fs::create_dir_all(workspace.join("docs")).unwrap();
    std::fs::write(workspace.join("docs/a.txt"), "alpha document").unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(&root.path().join("home"), &workspace).unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    let runtime = services.runtime.runtime_services();
    let session = format!("b-scope-ingress:{}", uuid::Uuid::new_v4());
    let identity = std::sync::Mutex::new(None::<(String, String, String)>);
    let run = std::panic::AssertUnwindSafe(async {
        let mut request = EnsureSessionRequest::new(&session, Some(provider.model.clone()), SessionSource::Tui);
        request.owner_principal_id = Some("b-scope-local-fixture".into());
        services.session.create_user_session(request).await.unwrap();
        let control = json!({"corpus_id":"live-scenarios-v1", "workspace_fixture":"none",
            "provider_constraint":"normal", "temperature_milli":0,
            "resource_scopes":["read:target.txt", "read:docs"], "prompt":"Read target.txt."});
        let envelope = harness_contract::turn::SessionInputEnvelope::text(&session,
            harness_contract::turn::InputSourceKind::Tui,
            &format!("COWD_EVAL_CONTROL {control}\nRead target.txt and report its content."));
        let request_id = envelope.idempotency_key.clone();
        let admitted = services.session.admit_input(envelope).await.unwrap();
        let turn = admitted.turn_id;
        let graph = admitted.execution_graph_id;
        *identity.lock().unwrap() = Some((request_id.clone(), turn.clone(), graph.clone()));
        let input = services.session.runtime_input(&request_id).await.unwrap().expect("original Session input");
        assert_eq!(input.session_id, session);
        assert_eq!(input.turn_id, turn);
        provider.tool("novelty-discover", "tool_search", json!({"query":"select:read_file,tool_orchestrate"}), None).await;
        let request = provider.next().await;
        let tools = request.body["tools"].as_array().expect("original Step catalog");
        for name in ["read_file", "tool_orchestrate"] {
            assert!(tools.iter().any(|tool| tool["function"]["name"] == name), "original Step must expose {name}");
        }
        request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
            {"index":0,"id":"novelty-prior","type":"function","function":{"name":"read_file","arguments":json!({"path":"target.txt","complete":true}).to_string()}}
        ]})).unwrap();
        let request = provider.next().await;
        assert!(request.body["messages"].as_array().unwrap().iter().any(|message|
            message["role"] == "tool" && message["tool_call_id"] == "novelty-prior"));
        let positive = json!({"steps":[{"id":"find","op":"call","tool":"grep_search",
            "input":{"path":"docs","pattern":"alpha"}}]});
        request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
            {"index":0,"id":"novelty-search","type":"function","function":{"name":"tool_orchestrate","arguments":positive.to_string()}}
        ]})).unwrap();
        let request = provider.next().await;
        assert!(request.body["messages"].as_array().unwrap().iter().any(|message|
            message["role"] == "tool" && message["tool_call_id"] == "novelty-search"));
        let mixed = json!({"steps":[{"id":"find","op":"batch","calls":[
            {"tool":"grep_search","input":{"path":"docs","pattern":"alpha"}},
            {"tool":"read_file","input":{"path":"docs/a.txt","complete":true}}
        ]}]});
        request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
            {"index":0,"id":"novelty-mixed","type":"function","function":{"name":"tool_orchestrate","arguments":mixed.to_string()}},
            {"index":1,"id":"novelty-direct","type":"function","function":{"name":"read_file","arguments":json!({"path":"target.txt","complete":true}).to_string()}}
        ]})).unwrap();
        let next = provider.next().await;
        for call in ["novelty-mixed", "novelty-direct"] {
            assert!(next.body["messages"].as_array().unwrap().iter().any(|message|
                message["role"] == "tool" && message["tool_call_id"] == call), "same frame result {call}");
        }
        let observations = runtime.goal_store().observations(&format!("goal:{graph}")).unwrap();
        let find = |id: &str| observations.iter().position(|observation|
            observation.identity.graph_id == graph
                && observation.evidence_refs.contains(&format!("tool_call:{id}")))
            .expect("original same-graph ToolProgress");
        let prior = find("novelty-prior");
        let found = find("novelty-search");
        let mixed_index = find("novelty-mixed");
        assert!(prior < found && found < mixed_index);
        assert!(observations[found].evidence_refs.contains(&"tool_coverage:discovery:docs".to_string()));
        let mixed = &observations[mixed_index];
        assert!(mixed.evidence_refs.contains(&"tool_call:novelty-direct".to_string()), "calls must share original batch");
        assert!(mixed.evidence_refs.contains(&"tool_coverage:evidence:read_file:target.txt".to_string()));
        assert!(!mixed.evidence_refs.contains(&"tool_coverage:discovery:docs".to_string()));
        assert!(mixed.summary.contains("search coverage remains unknown"));
        assert!(!mixed.summary.contains("no new evidence coverage"));
        next.reply.send(json!({"role":"assistant","content":"alpha"})).unwrap();
    }).catch_unwind().await;
    let drain = std::panic::AssertUnwindSafe(async {
    if let Some((request_id, turn, graph)) = identity.lock().unwrap_or_else(|e| e.into_inner()).clone() {
        let active = services.runtime.stop_accepting_for_recovery();
        if let Ok(Some(input)) = services.session.runtime_input(&request_id).await {
            let _ = services.session.cancel_input(&session,
                harness_contract::turn::SessionInputId::from_string(input.input_id.to_string()),
                "local scope fixture complete").await;
        }
        let _ = services.session.cancel_active_execution(&session, &turn, &graph, "local scope fixture complete");
        let _ = runtime.cancel_execution_tree(&graph, "local scope fixture complete").await;
        let drain = services.runtime.wait_for_active_turns(active.len(), Duration::from_secs(30)).await;
        assert!(drain.remaining_turn_ids.is_empty(), "original Session turn must drain");
    }
    }).catch_unwind().await;
    drop(runtime);
    drop(provider);
    let cleanup = std::panic::AssertUnwindSafe(services.stop()).catch_unwind().await;
    if let Err(panic) = run { std::panic::resume_unwind(panic); }
    if let Err(panic) = drain { std::panic::resume_unwind(panic); }
    if let Err(panic) = cleanup { std::panic::resume_unwind(panic); }
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run alone in an isolated process with an exclusive Session PG fixture"]
async fn grep_samecall_original_ingress_receipt_wire_and_projection() {
    use sha2::{Digest, Sha256};
    for mode in ["direct", "batch", "child_call", "child_batch", "child_search_batch", "count", "filenames", "page", "truncated"] {
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    use futures::FutureExt;
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value { std::env::set_var(key, value); }
                else { std::env::remove_var(key); }
            }
        }
    }
    let keys = ["COWD_EVAL_HARNESS", "COWD_EVAL_CORPUS_ID", "COWD_MODEL_TEMPERATURE"];
    let _restore = Restore(keys.into_iter().map(|key| (key, std::env::var_os(key))).collect());
    std::env::set_var("COWD_EVAL_HARNESS", "1");
    std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
    std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
    let root = fixture_root();
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("target.txt"), "alpha\nbeta\n").unwrap();
    std::fs::create_dir_all(workspace.join("docs")).unwrap();
    for name in ["a.txt", "b.txt", "c.txt", "d.txt"] {
        std::fs::write(workspace.join("docs").join(name), "alpha\nbeta\n").unwrap();
    }
    let long_body = (0..5000).map(|i| format!("row {i:05}: original bytes exceed provider presentation capacity\n")).collect::<String>();
    std::fs::write(workspace.join("long.txt"), &long_body).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(&root.path().join("home"), &workspace).unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    let runtime = services.runtime.runtime_services();
    let session = format!("b-scope-ingress:{}", uuid::Uuid::new_v4());
    let identity = std::sync::Mutex::new(None::<(String, String, String)>);
    let run = std::panic::AssertUnwindSafe(async {
        let mut request = EnsureSessionRequest::new(&session, Some(provider.model.clone()), SessionSource::Tui);
        request.owner_principal_id = Some("b-scope-local-fixture".into());
        services.session.create_user_session(request).await.unwrap();
        let control = json!({"corpus_id":"live-scenarios-v1", "workspace_fixture":"none",
            "provider_constraint":"normal", "temperature_milli":0,
            "resource_scopes":["read:target.txt", "read:docs", "read:long.txt"], "prompt":"Read target.txt."});
        let envelope = harness_contract::turn::SessionInputEnvelope::text(&session,
            harness_contract::turn::InputSourceKind::Tui,
            &format!("COWD_EVAL_CONTROL {control}\nRead target.txt and report its content."));
        let request_id = envelope.idempotency_key.clone();
        let admitted = services.session.admit_input(envelope).await.unwrap();
        let turn = admitted.turn_id;
        let graph = admitted.execution_graph_id;
        *identity.lock().unwrap() = Some((request_id.clone(), turn.clone(), graph.clone()));
        let input = services.session.runtime_input(&request_id).await.unwrap().expect("original Session input");
        assert_eq!(input.session_id, session);
        assert_eq!(input.turn_id, turn);
        provider.tool("grep-discover", "tool_search", json!({"query":"select:grep_search,tool_orchestrate"}), None).await;
        let request = provider.next().await;
        let tools = request.body["tools"].as_array().expect("original Step catalog");
        for name in ["grep_search", "tool_orchestrate"] {
            assert!(tools.iter().any(|tool| tool["function"]["name"] == name), "original Step must expose {name}");
        }
        let search = json!({"pattern":".*","path":"docs","glob":"*.txt"});
        let (tool, input, expected) = match mode {
            "direct" => ("grep_search", search.clone(), 4),
            "batch" => ("grep_search", json!({"searches":[search,search]}), 8),
            "child_call" => ("tool_orchestrate", json!({"steps":[{"id":"g","op":"call","tool":"grep_search","input":search}]}), 4),
            "child_batch" => ("tool_orchestrate", json!({"steps":[{"id":"g","op":"batch","calls":[{"tool":"grep_search","input":search,"as":"g"}]}]}), 4),
            "child_search_batch" => ("tool_orchestrate", json!({"steps":[{"id":"g","op":"batch","calls":[{"tool":"grep_search","input":{"searches":[search,search]},"as":"g"}]}]}), 8),
            "count" => ("grep_search", json!({"pattern":".*","path":"docs","output_mode":"count"}), 0),
            "filenames" => ("grep_search", json!({"pattern":".*","path":"docs","output_mode":"files_with_matches"}), 0),
            "page" => ("grep_search", json!({"pattern":".*","path":"docs","head_limit":1}), 0),
            _ => ("grep_search", json!({"pattern":".*","path":"long.txt","head_limit":10000}), 0),
        };
        request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
            {"index":0,"id":"grep-original","type":"function","function":{"name":tool,"arguments":input.to_string()}}
        ]})).unwrap();
        let next = provider.next().await;
        let wire = next.body["messages"].as_array().unwrap().iter().find(|message|
            message["role"] == "tool" && message["tool_call_id"] == "grep-original").expect("original grep provider presentation");
        assert!(!wire.to_string().contains("directory_ancestry"));
        assert!(!wire.to_string().contains("physical_identity"));
        let active = runtime.graph_state_store().load(&graph).unwrap();
        let models = active.nodes.iter().filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::InlineModel
            && active.node_statuses.get(&node.id) == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running))
            .map(|node| node.id.clone()).collect::<Vec<_>>();
        assert!(!models.is_empty(), "original provider Model node must own the pending wire request");
        next.reply.send(json!({"role":"assistant","content":"Read-only search completed."})).unwrap();
        // Negative assertions also wait for the actual consuming Model node to
        // finish confirmation; checking an early empty stream would be vacuous.
        durable_until(|| {
            let settled = runtime.graph_state_store().load(&graph).ok()?;
            models.iter().all(|id| settled.node_statuses.get(id).is_some_and(|status| status.is_terminal())).then_some(())
        }).await;
        let physical = services.physical_calls.lock().unwrap().iter().find(|call| call.tool_use_id == "grep-original").unwrap().clone();
        assert_eq!(physical.tool_name, tool);
        let primary = runtime.event_reader().list_stream(&format!("execution-effect:{}", physical.idempotency_key)).unwrap()
            .into_iter().find(|event| event.payload["child_read_acquisitions_v1"].is_array())
            .or_else(|| runtime.event_reader().list_stream(&format!("execution-effect:{}", physical.idempotency_key)).unwrap().into_iter().find(|event| matches!(event.kind.as_str(), "execution.effect.receipt" | "execution.read.receipt")))
            .expect("original authorized ToolHost primary receipt");
        assert_eq!(primary.payload["outcome"]["status"], "executed", "{mode}: {}", primary.payload);
        assert_eq!(primary.payload["replay"]["call_id"], "grep-original");
        let parent = physical.parent_execution.as_ref().expect("original Model ticket owner");
        assert_eq!(parent.execution_id, graph);
        let index = runtime.event_reader().list_stream(&format!("execution-agent-receipts:{}:{}:{}", parent.execution_id,
            parent.node_id, physical.attempt.unwrap())).unwrap().into_iter()
            .find(|event| event.idempotency_key.as_deref() == Some(format!("agent-tool-receipt:{}",physical.idempotency_key).as_str())).unwrap();
        assert_eq!(primary.transaction_id, index.transaction_id);
        assert_eq!(primary.commit_cursor, index.commit_cursor);
        assert_eq!(primary.payload["outcome"], index.payload["outcome"]);
        assert_eq!(primary.payload["child_read_acquisitions_v1"], index.payload["child_read_acquisitions_v1"]);
        let children = primary.payload["child_read_acquisitions_v1"].as_array().cloned().unwrap_or_default();
        let acquired = if mode == "truncated" { 1 } else { expected };
        assert_eq!(children.len(), acquired, "{mode}: {}", primary.payload);
        if expected > 0 {
            durable_until(|| runtime.event_reader().list_stream(&format!("session:{session}")).unwrap().into_iter()
                .find(|event| event.kind == "context.nested_read_model_observed" && event.payload["child"]["parent_tool_use_id"] == "grep-original")).await;
        }
        let deliveries = runtime.event_reader().list_stream(&format!("session:{session}")).unwrap().into_iter()
            .filter(|event| event.kind == "context.nested_read_model_observed" && event.payload["child"]["parent_tool_use_id"] == "grep-original").collect::<Vec<_>>();
        assert_eq!(deliveries.len(), expected, "{mode}");
        let mut ids = std::collections::BTreeSet::new();
        for delivery in &deliveries {
            assert!(ids.insert(delivery.idempotency_key.clone()));
            let child = &delivery.payload["child"];
            assert!(children.contains(child), "delivery must reference its original primary child");
            assert_eq!(child["body"]["source_sha256"], format!("sha256:{:x}", Sha256::digest(b"alpha\nbeta\n")));
            assert_eq!(child["body"]["source_bytes"], 11);
            assert_eq!(child["directory_ancestry"][0], "docs");
            assert_eq!(delivery.payload["delivered_extent"]["line_count"], 2);
            assert_eq!(delivery.payload["delivered_extent"]["source_omitted"], false);
            assert_eq!(delivery.payload["parent"]["step"]["turn_id"], turn);
            assert_eq!(delivery.payload["parent"]["graph_id"], graph);
        }
        let context = harness_contract::projection::ProjectionQueryContext {
            principal:"grep-original-ingress".into(), workspace_id:runtime.workspace_key().into(),
            session_scopes:vec![session.clone()], mission_scopes:vec![],
            visibility_grants:vec![format!("workspace:{}",runtime.workspace_key()),"resource:*".into()],
            detail_scope:harness_contract::projection::ProjectionDetailScope::Full, authorization_revision:1,
        };
        let page = runtime::execution_projection::read_evidence(&runtime, &graph, &context, Default::default()).await.unwrap();
        assert!(!page.incomplete, "{mode}: {page:?}");
        let reads = page.reads.iter().filter(|read| read.tool_name == "grep_search").collect::<Vec<_>>();
        assert_eq!(reads.len(), expected, "{mode}: {page:?}");
        assert!(reads.iter().all(|read| read.body_delivered));
    }).catch_unwind().await;
    let drain = std::panic::AssertUnwindSafe(async {
    if let Some((request_id, turn, graph)) = identity.lock().unwrap_or_else(|e| e.into_inner()).clone() {
        let active = services.runtime.stop_accepting_for_recovery();
        if let Ok(Some(input)) = services.session.runtime_input(&request_id).await {
            let _ = services.session.cancel_input(&session,
                harness_contract::turn::SessionInputId::from_string(input.input_id.to_string()),
                "local scope fixture complete").await;
        }
        let _ = services.session.cancel_active_execution(&session, &turn, &graph, "local scope fixture complete");
        let _ = runtime.cancel_execution_tree(&graph, "local scope fixture complete").await;
        let drain = services.runtime.wait_for_active_turns(active.len(), Duration::from_secs(30)).await;
        assert!(drain.remaining_turn_ids.is_empty(), "original Session turn must drain");
    }
    }).catch_unwind().await;
    drop(runtime);
    drop(provider);
    let cleanup = std::panic::AssertUnwindSafe(services.stop()).catch_unwind().await;
    if let Err(panic) = run { std::panic::resume_unwind(panic); }
    if let Err(panic) = drain { std::panic::resume_unwind(panic); }
    if let Err(panic) = cleanup { std::panic::resume_unwind(panic); }
    }
}

// Metadata-only fallback for either a missing private directory or a failed
// private write. It never prints request/response bodies or raw error text.
fn c_bounded_original_failure_summary(
    services: &runtime::RuntimeServices, session: &str, turn: &str, root: Option<&String>,
) -> Value {
    let reader = services.event_reader();
    let program = harness_contract::agent_action::program_id_for_objective(
        &harness_contract::agent_action::root_objective_id(session, turn));
    let program_stream = format!("agentic-program:{program}");
    let records = reader.all_events(100_001);
    let mut facts = Vec::new();
    let mut team_events = Vec::new();
    let mut first_failure: Option<Value> = None;
    if let Ok(events) = &records {
        let mut ordered = events.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        for event in ordered {
            let same_scope = event.stream_id == program_stream
        || root.is_some_and(|root| event.stream_id == *root)
        || event.refs.iter().any(|reference|
            (reference.kind == "turn" && reference.id == turn)
            || (reference.kind == "execution_graph" && root == Some(&reference.id)))
        || event.activity_binding().is_some_and(|activity|
            activity.session_id == session && activity.turn_id == turn);
            let call_id = event.payload["tool_use_id"].as_str()
        .or_else(|| event.payload["outcome"]["tool_use_id"].as_str())
        .or_else(|| event.payload["request"]["tool_use_id"].as_str());
            let plan_matches = event.payload["tasks"].as_array().is_some_and(|tasks|
        tasks.iter().any(|task| task["tool_call_id"].as_str() == Some("c-ingress-team")));
            if !same_scope { continue; }
            let has_error = !event.payload["error"].is_null() || !event.payload["outcome"]["error"].is_null();
            let kind = match event.kind.as_str() {
                "agentic.action_applied" => "agentic.action_applied",
                "execution.call.refused" => "execution.call.refused",
                "execution.effect.intent" => "execution.effect.intent",
                "execution.effect.receipt" => "execution.effect.receipt",
                "execution.read.receipt" => "execution.read.receipt",
                "execution.agent_tool.receipt" => "execution.agent_tool.receipt",
                "tool.execution_plan.created" => "tool.execution_plan.created",
                _ => "other",
            };
            let status = match event.status.as_deref() {
                Some("failed") => "failed", Some("error") => "error", Some("refused") => "refused",
                Some("rejected") => "rejected", Some("completed") => "completed",
                Some("applied") => "applied", Some("executed") => "executed",
                Some("running") => "running", _ => "other_or_absent",
            };
            let fact = json!({"sequence":event.sequence,"kind":kind,"status":status,
                "cursor":event.commit_cursor,
                "call":if call_id == Some("c-ingress-team") || plan_matches { Some("c-ingress-team") } else { None },
                "error_present":has_error});
            if first_failure.is_none() && (has_error || event.status.as_deref().is_some_and(|status|
        matches!(status, "failed" | "error" | "refused" | "rejected"))) {
        first_failure = Some(fact.clone());
            }
            if (call_id == Some("c-ingress-team") || plan_matches) && team_events.len() < 16 {
        team_events.push(fact.clone());
            }
            if facts.len() < 64 { facts.push(fact); }
        }
    }
    json!({"session_id":session,"turn_id":turn,"root_graph_id":root,
        "program_id":program,"events":facts,"limit":64,
        "first_failure_candidate":first_failure,"team_call_events":team_events,
        "association":"original stream/ref/activity candidate; owner binding still requires verification",
        "source_read_ok":records.is_ok(),
        "source_possibly_truncated":records.as_ref().is_ok_and(|events| events.len() >= 100_001)})
}

// Shared v3 original-facts export, before either fixture shuts down.
async fn c_export_original_failure(
    services: &runtime::RuntimeServices,
    calls: &std::sync::Mutex<Vec<PhysicalCall>>,
    session: &str, turn: &str, root: Option<String>, model: &str,
    failure: &str, turn_control: &str, goal_source: &str, root_task: Option<&str>,
    route_facts: Option<Value>,
) {
    // Read original facts before cleanup; no new observation or receipt is
    // written to Runtime. tmpfs is mandatory; never fall back to disk.
    #[cfg(target_os = "linux")]
    let tmpfs_ok = std::fs::read_to_string("/proc/self/mountinfo").ok().is_some_and(|mounts| {
        mounts.lines().filter_map(|line| line.split_once(" - ")).any(|(left, right)| {
            left.split_whitespace().nth(4) == Some("/dev/shm")
                && right.split_whitespace().next() == Some("tmpfs")
        })
    });
    #[cfg(not(target_os = "linux"))]
    let tmpfs_ok = false;
    // The PG owner deliberately puts compile TMPDIR on the data disk. It
    // separately provides an owned, mode-0700 tmpfs evidence directory.
    let private_parent = std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR")
        .map(std::path::PathBuf::from).unwrap_or_else(std::env::temp_dir);
    #[cfg(target_os = "linux")]
    let private_parent_ok = {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::symlink_metadata(&private_parent);
        let own_uid = std::fs::metadata("/proc/self").ok().map(|m| m.uid());
        let on_tmpfs = std::process::Command::new("stat")
            .args(["-f", "-c", "%T"]).arg(&private_parent).output()
            .is_ok_and(|output| output.status.success() && output.stdout == b"tmpfs\n");
        on_tmpfs && private_parent.canonicalize().is_ok()
            && metadata.is_ok_and(|m| m.file_type().is_dir()
                && m.permissions().mode() & 0o777 == 0o700 && Some(m.uid()) == own_uid)
    };
    #[cfg(not(target_os = "linux"))]
    let private_parent_ok = false;
    let directory = if tmpfs_ok && private_parent_ok {
        tempfile::Builder::new().prefix("cowd-delegated-evidence-")
            .tempdir_in(&private_parent).and_then(|dir| {
                #[cfg(unix)] {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
                }
                Ok(dir)
            })
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "verified private tmpfs unavailable"))
    };
    match directory {
        Ok(directory) => {
            let reader = services.event_reader();
            let root = root.clone();
            let program = harness_contract::agent_action::program_id_for_objective(
                &harness_contract::agent_action::root_objective_id(session, turn));
            let program_stream = format!("agentic-program:{program}");
            let mut program_events = reader.list_stream(&program_stream);
            if let Ok(events) = &mut program_events {
                events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
            }
            let mut graph_ids = std::collections::BTreeSet::new();
            if let Some(root) = root.as_ref() { graph_ids.insert(root.clone()); }
            if let Ok(events) = &program_events {
                for event in events {
                    if let Some(id) = event.payload["envelope"]["action"]["input"]["execution_id"].as_str() {
                        graph_ids.insert(id.to_string());
                    }
                }
            }
            let graphs = graph_ids.iter().map(|id| {
                let graph = services.graph_state_store().load(id).map_err(|error| error.to_string());
                json!({"graph_id":id,"graph":graph.as_ref().ok(),"error":graph.as_ref().err()})
            }).collect::<Vec<_>>();
            let all = reader.all_events(100_001);
            let mut matching = all.as_ref().ok().map(|events| events.iter().filter(|event| {
                event.stream_id == program_stream
                    || event.stream_id == format!("session:{session}")
                    || root.as_ref().is_some_and(|root| event.stream_id == format!("goal:goal:{root}"))
                    || (event.kind.starts_with("goal.") && event.payload["session_id"].as_str() == Some(session))
                    || graph_ids.contains(&event.stream_id)
                    || event.refs.iter().any(|reference| graph_ids.contains(&reference.id))
                    || event.activity_binding().is_some_and(|activity|
                        activity.session_id == session && activity.turn_id == turn)
            }).collect::<Vec<_>>());
            if let Some(events) = &mut matching {
                events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
            }
            let timeline = reader.session_timeline_events(session, None, 100);
            let captured_calls = calls.lock().unwrap_or_else(|error| error.into_inner()).iter().map(|call| json!({
                "tool_use_id":call.tool_use_id,"tool_name":call.tool_name,"input":call.input,
                "idempotency_key":call.idempotency_key,"parent_execution":call.parent_execution,"attempt":call.attempt,
            })).collect::<Vec<_>>();
            // Use the public, session-scoped reader on each original event's
            // artifact reference. A prepared body may be rejected; preserve
            // that error instead of bypassing the owner or changing scope.
            let mut transcript_bodies = Vec::new();
            if let Some(events) = matching.as_ref() {
                for (index, event) in events.iter().filter(|event| matches!(event.kind.as_str(), "execution.host.transcript" | "goal.observation")).enumerate() {
                    let mut fact = json!({"event_id":event.event_id,"stream_id":event.stream_id,
                        "kind":event.kind,
                        "body_status":"unknown", "truncated":"unknown",
                        "association":"candidate from original event; owner and node binding require independent verification"});
                    let pointer = if event.kind == "goal.observation" { "/observation_body/body/artifact" } else { "/body/body/artifact" };
                    match event.payload.pointer(pointer) {
                        None => {
                            fact["error"] = json!("artifact reference missing or body inline; inspect original event");
                        },
                        Some(value) => match serde_json::from_value::<runtime::ArtifactRef>(value.clone()) {
                            Err(error) => fact["error"] = json!(format!("invalid original ArtifactRef: {error}")),
                            Ok(artifact) => {
                                fact["artifact"] = json!(artifact);
                                match services.artifact_store().read(&artifact, &format!("session:{session}"), None).await {
                                    Err(error) => fact["error"] = json!(format!("ArtifactStore::read: {error}")),
                                    Ok(bytes) => {
                                        let name = format!("original-body-{index}.body");
                                        let path = directory.path().join(&name);
                                        #[cfg(unix)] use std::os::unix::fs::OpenOptionsExt;
                                        use std::io::Write;
                                        let write = (|| -> std::io::Result<()> {
                                            let mut options = std::fs::OpenOptions::new();
                                            options.write(true).create_new(true);
                                            #[cfg(unix)] options.mode(0o600);
                                            options.open(&path)?.write_all(&bytes)
                                        })();
                                        fact["bytes_read"] = json!(bytes.len());
                                        fact["truncated"] = json!(false);
                                        match write {
                                            Ok(()) => { fact["body_status"] = json!("exported"); fact["file"] = json!(name); }
                                            Err(error) => fact["error"] = json!(format!("private tmpfs write: {error}")),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    transcript_bodies.push(fact);
                }
            }
            let data = json!({
                "session_id":session,"turn_id":turn,"root_graph_id":root,"program_id":program,
                "failure":failure,
                "provider_route_facts":route_facts,
                "preconditions":{
                    "session_policy":services.session_execution_policy(session),
                    "root_task":root_task.map(|id| format!("{:?}", services.task_aggregate_service().get(id))),
                    "goal":root.as_ref().map(|root| services.goal_store().get(&format!("goal:{root}"))),
                    "gateway_host_installed":services.tool_execution_host().is_some(),
                    "gateway_catalog_installed":services.tool_execution_host().and_then(|host| host.pin_step_catalog()).is_some(),
                    "provider_model_resolves":services.provider_registry().pin().resolve(model).is_some(),
                    "provider_configured_context_window":services.provider_registry().pin().configured_context_window(model),
                    "builtin_direct_definition":format!("{:?}", services.definition_registry().resolve_agent(
                        &harness_contract::agent::AgentDefinitionId::new(harness_contract::agent::DefinitionScope::Builtin, "cowd/direct").unwrap(),
                        harness_contract::agent::RevisionSelector::LatestApprovedStable)),
                    "session_input_router_installed":services.session_input_router().is_some(),
                    "session_history_reader_installed":services.session_history_reader().is_some(),
                    "turn_control":turn_control,
                    "root_causal_bus":"unknown: no public inspection port",
                    "root_step_authorization":"Gateway follows existing per-call policy and Step validation; exact signed authorization not retained by existing call recorder",
                    "goal_source":goal_source
                },
                "program_events":program_events.as_ref().ok(),"program_events_error":program_events.as_ref().err(),
                "graphs":graphs,"agent_runs":services.agent_runtime().list().into_iter().filter(|run| graph_ids.contains(&run.graph_id)).collect::<Vec<_>>(),
                "events":matching,"all_events_error":all.as_ref().err(),
                "event_selection":"original graph refs or same fixture session/turn; nested candidates require separate graph binding verification",
                "all_events_limit":100001,"all_events_possibly_truncated":all.as_ref().ok().is_some_and(|events| events.len() >= 100001),
                "event_order":"commit_cursor, transaction_index",
                "earliest_event_conclusion":if all.is_err() || all.as_ref().is_ok_and(|events| events.len() >= 100001) { "unknown: global read failed or may be truncated" } else { "global read within limit; earliest matching event is first after sort, subject to run correlation" },
                "session_timeline":timeline.as_ref().ok().map(|page| json!({"events":page.events,"has_more":page.has_more,"complete_through_cursor":page.complete_through_cursor})),
                "session_timeline_error":timeline.as_ref().err(),
                "existing_host_call_recorder":captured_calls,
                "host_transcript_event_count":matching.as_ref().map(|events| events.iter().filter(|event| event.kind == "execution.host.transcript").count()),
                "original_body_candidates":transcript_bodies,
                "host_transcript_reader":"public ArtifactStore::read; committed_host_transcript verification is crate-private"
            });
            let path = directory.path().join("original-facts.json");
            #[cfg(unix)] use std::os::unix::fs::OpenOptionsExt;
            use std::io::Write;
            let write = (|| -> Result<(), String> {
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)] options.mode(0o600);
                let mut file = options.open(&path).map_err(|error| error.to_string())?;
                let bytes = serde_json::to_vec_pretty(&data).map_err(|error| error.to_string())?;
                file.write_all(&bytes).map_err(|error| error.to_string())
            })();
            let kept = directory.keep();
            match write {
                Ok(()) => eprintln!("private tmpfs original facts: {}", kept.display()),
                Err(error) => eprintln!("private tmpfs fact export failed at {}: {error}; bounded_original_summary={}",
                    kept.display(), c_bounded_original_failure_summary(services, session, turn, root.as_ref())),
            }
        }
        Err(error) => eprintln!("private tmpfs fact export unavailable: {error}; bounded_original_summary={}",
            c_bounded_original_failure_summary(services, session, turn, root.as_ref())),
    }
}

// Correlate exact received wire bytes with original Session packed and Runtime
// step facts. Missing, truncated or non-unique evidence fails closed.
async fn c_route_owner(
    services: &RecoveryServices, request: &ProviderRequest, session: &str,
    turn: &str, root: &str, program: &str, task: Option<&str>,
) -> Result<bool, String> {
    let rows = services.topology.session_store.get_session_domain_events_by_kind_limited(
        session, "context.provider_request_packed", 0, 10_001).await.map_err(|e| e.to_string())?;
    if rows.len() >= 10_001 { return Err("packed Session events possibly truncated".into()); }
    let mut packed = Vec::new();
    for row in rows {
        let event = session::SessionDomainEvent::from_session_event(&row).map_err(|e| e.to_string())?;
        if event.session_id == session && event.payload["turn_id"] == turn
            && event.payload["body_sha256"] == request.body_sha256 { packed.push(event); }
    }
    if packed.len() != 1 { return Err(format!("packed wire match count {}", packed.len())); }
    let payload = &packed[0].payload;
    let step_id = payload.pointer("/step_render/step_id").and_then(Value::as_str).ok_or("packed step missing")?;
    let execution = payload["execution_id"].as_str().ok_or("packed execution missing")?;
    let events = services.runtime.runtime_services().event_reader().all_events(100_001)?;
    if events.len() >= 100_001 { return Err("Runtime step events possibly truncated".into()); }
    let steps = events.iter().filter(|event| event.kind == "model.step_bound"
        && event.payload["identity"]["step_id"] == step_id).collect::<Vec<_>>();
    if steps.len() != 1 { return Err(format!("step match count {}", steps.len())); }
    let render_attempt = payload.pointer("/step_render/attempt_id").and_then(Value::as_str)
        .ok_or("packed provider attempt missing")?;
    let render_catalog = payload.pointer("/step_render/catalog_digest").and_then(Value::as_str)
        .ok_or("packed catalog missing")?;
    if steps[0].payload["provider_attempt"]["attempt_id"] != render_attempt
        || steps[0].payload["catalog_digest"] != render_catalog {
        return Err("packed/step provider attempt or catalog mismatch".into());
    }
    let identity = &steps[0].payload["identity"];
    if identity["session_id"] != session || identity["turn_id"] != turn || identity["execution_id"] != execution {
        return Err("packed/step session, turn or execution mismatch".into());
    }
    if execution == root && identity["run_ref"].is_null() { return Ok(false); }
    let task = task.ok_or("delegated request before Task is known")?;
    let run_id = identity["run_ref"].as_str().ok_or("delegated run missing")?;
    let runtime = services.runtime.runtime_services();
    let runs = runtime.agent_runtime().list().into_iter().filter(|run| run.run_id == run_id && run.session_id == session).collect::<Vec<_>>();
    if runs.len() != 1 { return Err(format!("original run match count {}", runs.len())); }
    let run = &runs[0];
    let binding = run.agentic_binding.as_ref().ok_or("run Program binding missing")?;
    let mode = match &binding.focus {
        harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } if task_ref == task =>
            harness_contract::agent_action::AgentAttemptMode::Execute,
        harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } if task_ref == task =>
            harness_contract::agent_action::AgentAttemptMode::Review,
        _ => return Err("run Program/Task owner mismatch".into()),
    };
    if binding.program_id != program { return Err("run Program owner mismatch".into()); }
    // Native Host step execution is the original run id, not the
    // AgentTask graph id. Verify the run's frozen physical coordinates.
    let run_identity = serde_json::to_value(&run.execution_identity).map_err(|e| e.to_string())?;
    if execution != run_id || identity["task_ref"] != run.task_id
        || run_identity["agent_run_id"] != run_id || run_identity["graph_id"] != run.graph_id
        || run_identity["node_id"] != run.node_id || run_identity["turn_id"] != turn
        || run_identity["session_id"] != session {
        return Err("step/run physical execution identity mismatch".into());
    }
    let graph = runtime.graph_state_store().load(&run.graph_id).map_err(|e| e.to_string())?;
    let node = graph.nodes.iter().find(|node| node.id == run.node_id).ok_or("AgentTask node missing")?;
    let packet: harness_contract::agent::AgentTaskPacket = serde_json::from_str(&node.payload_ref).map_err(|e| e.to_string())?;
    if packet.run_id() != run_id || packet.graph_id() != run.graph_id || packet.node_id() != run.node_id
        || packet.session_id() != session || packet.attempt != run.attempt { return Err("run/packet physical identity mismatch".into()); }
    let projection = runtime.agent_action_service().project(program).map_err(|e| e.to_string())?;
    let active = projection.tasks.get(task).and_then(|task| task.active_attempts.get(&run.graph_id))
        .is_some_and(|attempt| attempt.mode == mode
            && attempt.agent_id == binding.agent_id && attempt.membership_id == binding.membership_id);
    if !active {
        // Only the original executor may finish its response after its own
        // submit removed it from active_attempts. Never route it as a reviewer
        // or use this historical evidence to authorize another action.
        let has_submit_result = request.body["messages"].as_array().is_some_and(|messages|
            messages.iter().any(|message| message["role"] == "tool" && message["tool_call_id"] == "d-original-submit"));
        if mode != harness_contract::agent_action::AgentAttemptMode::Execute || !has_submit_result
            || packet.agentic_binding.as_ref() != Some(binding) {
            return Err("Program original attempt owner mismatch".into());
        }
        let program_stream = format!("agentic-program:{program}");
        let mut dispatches = Vec::new();
        let mut submissions = Vec::new();
        for event in &events {
            if event.stream_id != program_stream || event.kind != "agentic.action_applied" { continue; }
            let envelope = &event.payload["envelope"];
            let action = &envelope["action"];
            let input = &action["input"];
            if action["kind"] == "task_attempt_dispatch" && input["task_ref"] == task
                && input["execution_id"] == run.graph_id && input["mode"] == "execute"
                && input["agent_ref"] == binding.agent_id && input["membership_id"] == binding.membership_id {
                dispatches.push(event);
            }
            if action["kind"] == "task_submit" && input["task_ref"] == task
                && envelope["actor"]["program_id"] == program && envelope["actor"]["session_id"] == session
                && envelope["actor"]["turn_id"] == turn && envelope["actor"]["execution_id"] == run.graph_id
                && envelope["actor"]["agent_id"] == binding.agent_id {
                submissions.push(event);
            }
        }
        if dispatches.len() != 1 || submissions.len() != 1
            || dispatches[0].commit_cursor >= submissions[0].commit_cursor
            || submissions[0].commit_cursor >= steps[0].commit_cursor {
            return Err("submitted original execution evidence missing or ambiguous".into());
        }
        let action_key = submissions[0].payload["envelope"]["action_id"].as_str()
            .ok_or("submitted original action identity missing")?;
        let primary_stream = format!("execution-effect:{action_key}");
        let index_stream = format!("execution-agent-receipts:{}:{}:{}", run.graph_id, run.node_id, run.attempt);
        let primaries = events.iter().filter(|event| event.stream_id == primary_stream
            && event.kind == "execution.effect.receipt" && event.payload["idempotency_key"] == action_key).collect::<Vec<_>>();
        let indices = events.iter().filter(|event| event.stream_id == index_stream
            && event.kind == "execution.agent_tool.receipt" && event.payload["idempotency_key"] == action_key).collect::<Vec<_>>();
        if primaries.len() != 1 || indices.len() != 1 { return Err("submitted original receipt missing or ambiguous".into()); }
        let (primary, index) = (primaries[0], indices[0]);
        let outcome = &primary.payload["outcome"];
        let original_output = outcome["output"].as_str().ok_or("submitted original output missing")?;
        let messages = request.body["messages"].as_array().ok_or("original provider messages missing")?;
        let results = messages.iter().filter(|message| message["role"] == "tool"
            && message["tool_call_id"] == "d-original-submit").collect::<Vec<_>>();
        if results.len() != 1 || results[0]["content"].as_str() != Some(original_output)
            || outcome["status"] != "executed" || !outcome["error"].is_null()
            || outcome["tool_use_id"] != "d-original-submit" || outcome["tool_name"] != "task_submit"
            || primary.payload["replay"]["call_id"] != "d-original-submit"
            || primary.payload["replay"]["session_id"] != session
            || primary.payload["replay"]["parent_execution"]["execution_id"] != run.graph_id
            || primary.payload["replay"]["parent_execution"]["node_id"] != run.node_id
            || primary.payload["replay"]["parent_execution_attempt"].as_u64() != Some(u64::from(run.attempt))
            || primary.transaction_id != index.transaction_id || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index || primary.payload["outcome"] != index.payload["outcome"]
            || submissions[0].commit_cursor >= primary.commit_cursor || primary.commit_cursor >= steps[0].commit_cursor {
            return Err("submitted original call outcome is failed, unknown or mismatched".into());
        }
    }
    let parent = harness_contract::execution_graph::ExecutionParentBinding { execution_id: run.graph_id.clone(), node_id: run.node_id.clone() };
    let actor = runtime.resolve_agent_action_actor_for_attempt(&parent, run.attempt, None).await?;
    if actor.session_id != session || actor.turn_id != turn || actor.program_id != program {
        return Err("original NodesStarted actor scope mismatch".into());
    }
    Ok(true)
}

async fn c_root_tool(
    services: &RecoveryServices, provider: &mut LoopbackProvider, session: &str, turn: &str,
    graph: &str, program: &str, id: &str, name: &str, input: Value,
) {
    let request = provider.next().await;
    assert!(!c_route_owner(services, &request, session, turn, graph, program, None).await.expect("unique root wire owner"));
    request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[{
        "index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}
    }]})).expect("root provider reply");
}

// Explicit PG integration gate: all model responses are served by the private
// loopback fixture, while Session admission and worker ownership are real.
#[tokio::test]
#[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
async fn c_delegated_gateway_session_ingress_native_claim_original_attempt() {
    c_original_session_worker_fixture(None, false, false).await;
}

// The original member uses the real Gateway directory reader and its own
// same-call state_inspect authorization before its ordinary task submission.
#[tokio::test]
#[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
async fn a8_member_read_ref_uses_original_directory_authority() {
    c_original_session_worker_fixture(None, false, true).await;
}

// Reuse the real Session/worker/review chain with the provider spelling seen
// in #2; its existing receipt and conclusion assertions remain unchanged.
#[tokio::test]
#[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
async fn a9_delegated_review_normalizes_original_read_before_source_registration() {
    c_original_session_worker_fixture(None, true, false).await;
}

// Same production Session/Program/Task producer as the claim test. The only
// variant is the capability requested and the local provider's next call.
#[tokio::test]
#[ignore = "requires isolated COWD_TEST_POSTGRES_URL and a working bounded process sandbox"]
async fn b_delegated_conversation_ticket_bounded_compute_original_producer() {
    c_original_session_worker_fixture(Some(0), false, false).await;
}

#[tokio::test]
#[ignore = "run alone with isolated Session PG and the real producer fixture"]
async fn b_delegated_conversation_ticket_missing_bounded_capabilities_fail_closed() {
    for missing in 1..=3 { c_original_session_worker_fixture(Some(missing), false, false).await; }
}

// Inspect only the original Session plan and the corresponding durable receipt
// pair for this exact root call. A tool message by itself is not a read.
fn c_original_lifecycle_plan_task<'a>(
    events: &'a [runtime::DurableRuntimeEvent], call: &str, tool: &str,
) -> Result<&'a Value, String> {
    let completions = events.iter().filter(|event| event.kind == "tool.invocation.completed"
        && event.payload["tool_call_id"] == call && event.payload["tool_name"] == tool
        && event.payload["status"] == "completed" && event.payload["is_error"] == false).collect::<Vec<_>>();
    let [completion] = completions.as_slice() else { return Err("original completed tool lifecycle missing or ambiguous".into()); };
    let plan_id = completion.payload["governed_plan_id"].as_str().filter(|value| !value.is_empty())
        .ok_or("original completed tool lifecycle plan missing")?;
    let plans = events.iter().filter(|event| event.kind == "tool.execution_plan.created"
        && event.actor.as_deref() == Some("conversation_runtime") && event.stream_id == completion.stream_id
        && event.payload["plan_id"] == plan_id && event.commit_cursor < completion.commit_cursor).collect::<Vec<_>>();
    let [plan] = plans.as_slice() else { return Err("original completed tool plan missing or ambiguous".into()); };
    let tasks = plan.payload["tasks"].as_array().ok_or("original completed tool plan tasks missing")?
        .iter().filter(|task| task["tool_call_id"] == call && task["tool_name"] == tool).collect::<Vec<_>>();
    let [task] = tasks.as_slice() else { return Err("original completed tool plan call missing or ambiguous".into()); };
    Ok(task)
}

fn c_verified_root_artifact_page(
    services: &RecoveryServices, request: &ProviderRequest, session: &str, turn: &str,
    root: &str, call: &str, selector: &str, expected_start: u64, expected_total: Option<u64>,
    expected_source: Option<(&str, u64)>,
) -> Result<(Option<Value>, u64, u64, String, u64), String> {
    let runtime = services.runtime.runtime_services();
    let events = runtime.event_reader().list_stream(&format!("session:{session}"))?;
    let refusals: Vec<_> = events.iter().filter(|event| event.kind == "tool.call.rejected_before_dispatch"
        && event.payload["tool_call_id"] == call).collect();
    if !refusals.is_empty() { return Err(format!("original root read refused before dispatch: {}", refusals[0].payload["reason"].as_str().unwrap_or("unknown"))); }
    let plan_task = c_original_lifecycle_plan_task(&events, call, "read_ref")?;
    let step: runtime::step_snapshot::StepCallBinding = serde_json::from_value(plan_task["step_binding"].clone())
        .map_err(|_| "original root read Step binding malformed")?;
    if step.origin.call_id != call || step.snapshot.identity.session_id != session
        || step.snapshot.identity.turn_id != turn || step.snapshot.identity.execution_id.as_deref() != Some(root)
        || step.snapshot.identity.run_ref.is_some() {
        return Err("original root read Step owner differs".into());
    }
    let key = step.effect_idempotency_key();
    let primary = runtime.event_reader().list_stream(&format!("execution-effect:{key}"))?
        .into_iter().filter(|event| event.kind == "execution.read.receipt"
            && event.idempotency_key.as_deref() == Some(format!("{key}:read-receipt").as_str())).collect::<Vec<_>>();
    if primary.len() != 1 { return Err("original root read receipt missing or ambiguous".into()); }
    let primary = &primary[0];
    let replay = &primary.payload["replay"];
    let context = &primary.payload["root_effect_context"];
    let owner = &primary.payload["root_effect_owner"];
    if replay["session_id"] != session || replay["call_id"] != call {
        return Err("root read receipt call differs".into());
    }
    let (node, attempt, ticket_owner) = if owner.is_null() && context.is_null() {
        let parent = &replay["parent_execution"];
        let node = parent["node_id"].as_str().filter(|value| !value.is_empty()).ok_or("original root ticket node missing")?;
        let attempt = replay["parent_execution_attempt"].as_u64().filter(|value| *value > 0).ok_or("original root ticket attempt missing")?;
        if parent["execution_id"] != root { return Err("original root ticket graph differs".into()); }
        (node, attempt, true)
    } else {
        let binding = &owner["binding"];
        let node = binding["node_id"].as_str().filter(|value| !value.is_empty()).ok_or("root read original node missing")?;
        let attempt = owner["attempt"].as_u64().filter(|value| *value > 0).ok_or("root read original attempt missing")?;
        if !replay["parent_execution"].is_null() || !replay["parent_execution_attempt"].is_null()
            || context["execution_id"] != root || context["session_id"] != session || context["turn_id"] != turn
            || binding["root_execution_id"] != root || binding["session_id"] != session || binding["turn_id"] != turn {
            return Err("root read receipt owner differs".into());
        }
        (node, attempt, false)
    };
    let index = runtime.event_reader().list_stream(&format!("execution-agent-receipts:{root}:{node}:{attempt}"))?
        .into_iter().filter(|event| event.kind == "execution.agent_tool.receipt"
            && event.idempotency_key.as_deref() == Some(format!("agent-tool-receipt:{key}").as_str())).collect::<Vec<_>>();
    if index.len() != 1 || primary.transaction_id != index[0].transaction_id
        || primary.commit_cursor != index[0].commit_cursor || primary.transaction_index >= index[0].transaction_index
        || primary.payload["outcome"] != index[0].payload["outcome"]
        || primary.payload["root_effect_owner"] != index[0].payload["root_effect_owner"]
        || primary.payload["root_effect_context"] != index[0].payload["root_effect_context"] {
        return Err("original root read receipt pair differs".into());
    }
    for event in [primary, &index[0]] {
        if event.scope != runtime::RuntimeEventScope::ExecutionNode || event.actor.as_deref() != Some("governed_tool")
            || event.status.as_deref() != Some("completed") || event.payload["idempotency_key"] != key
            || ![("execution_graph", root), ("execution_node", node), ("agent_attempt", &attempt.to_string())].iter()
                .all(|(kind, id)| event.refs.iter().any(|reference| reference.kind == *kind && reference.id == *id)) {
            return Err("original root read receipt references differ".into());
        }
    }
    use runtime::RuntimeEventStoreBackend as _;
    let backend = runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone())
        .map_err(|error| error.to_string())?;
    if ticket_owner {
        let graph = runtime.graph_state_store().load(root).map_err(|error| error.to_string())?;
        let lineage = graph.lineage.as_ref().ok_or("original root ticket lineage missing")?;
        if graph.parent_execution.is_some() || lineage.session_id != session || lineage.turn_id != turn {
            return Err("original root ticket lineage differs".into());
        }
        let attempt_u32 = u32::try_from(attempt).map_err(|_| "original root ticket attempt out of range")?;
        let (ticket, position) = runtime.graph_state_store()
            .original_binding_start_for_test(&graph, node, attempt_u32).map_err(|error| error.to_string())?;
        if ticket.graph_id != root || ticket.node_id != node || ticket.attempt != attempt_u32
            || position.1 >= primary.commit_cursor {
            return Err("original root ticket identity or order differs".into());
        }
        let batch = match ticket.executor_kind.as_str() {
            "tool_batch" => runtime::step_snapshot::decode_persisted_tool_batch_for_test(&ticket.payload_ref)
                .map_err(|error| error.to_string())?,
            "inline_model" => {
                // The original provider frame can execute a read before its
                // model ticket publishes the successor ToolBatch. Authenticate
                // that exact frame and then the same-call successor; never
                // pretend that the earlier receipt belongs to the batch.
                let frames = events.iter().filter(|event| event.kind == "model.item_completed"
                    && event.payload["tool_call_id"] == call && event.payload["tool_name"] == "read_ref")
                    .collect::<Vec<_>>();
                let [frame] = frames.as_slice() else { return Err("original early model frame missing or ambiguous".into()); };
                let activity = frame.activity_binding().ok_or("original early model frame owner missing")?;
                if activity.root_execution_id != root || activity.session_id != session || activity.turn_id != turn
                    || activity.node_id.as_deref() != Some(node) || frame.commit_cursor <= position.1
                    || frame.commit_cursor >= primary.commit_cursor {
                    return Err("original early model frame owner or order differs".into());
                }
                let frame_input = frame.payload["content"].as_str().ok_or("original early model frame input missing")?;
                let original_input = primary.payload["original_input"].as_str().ok_or("original early read input missing")?;
                let original_value = harness_contract::step::parse_strict_json(original_input).map_err(|error| error.to_string())?;
                use sha2::{Digest as _, Sha256};
                let mut step_digest = Sha256::new();
                step_digest.update(b"tool-effect-step-snapshot-v1\0");
                step_digest.update(serde_json::to_vec(&step.snapshot).map_err(|error| error.to_string())?);
                if frame.scope != runtime::RuntimeEventScope::Session || frame.actor.as_deref() != Some("conversation_runtime.model_stream")
                    || frame.status.as_deref() != Some("completed")
                    || harness_contract::step::parse_strict_json(frame_input).map_err(|error| error.to_string())?
                        != plan_task["normalized_input"] || original_value != plan_task["normalized_input"]
                    || replay["origin"] != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
                    || replay["step_snapshot_digest"] != format!("sha256:{:x}", step_digest.finalize())
                    || replay["payload_digest"] != harness_contract::step::canonical_digest("tool-effect-payload", &original_value)
                        .map_err(|error| error.to_string())? {
                    return Err("original early model frame, Step or input differs".into());
                }
                let frame_receipt = backend.original_transaction_receipt(&frame.transaction_id).map_err(|error| error.to_string())?
                    .ok_or("original early model frame transaction missing")?;
                if frame_receipt.transaction_id != frame.transaction_id || frame_receipt.commit_cursor != frame.commit_cursor
                    || frame_receipt.event_ids.get(frame.transaction_index as usize) != Some(&frame.event_id)
                    || !frame_receipt.stream_revisions.iter().any(|source| source.stream_id == frame.stream_id
                        && source.expected_revision < frame.sequence && frame.sequence <= source.committed_revision) {
                    return Err("original early model frame transaction differs".into());
                }
                let effect_stream = format!("execution-effect:{key}");
                let intents = runtime.event_reader().list_stream(&effect_stream)?.into_iter()
                    .filter(|event| event.kind == "execution.read.intent"
                        && event.idempotency_key.as_deref() == Some(format!("{key}:read-intent").as_str()))
                    .collect::<Vec<_>>();
                let [intent] = intents.as_slice() else { return Err("original early read intent missing or ambiguous".into()); };
                let intent_receipt = backend.original_transaction_receipt(&intent.transaction_id).map_err(|error| error.to_string())?
                    .ok_or("original early read intent transaction missing")?;
                if intent.scope != runtime::RuntimeEventScope::ExecutionNode || intent.actor.as_deref() != Some("governed_tool")
                    || intent.status.as_deref() != Some("inflight") || intent.schema_version != 1
                    || intent.refs != primary.refs || intent.payload["idempotency_key"] != key
                    || intent.payload["tool_use_id"] != call || intent.payload["tool_name"] != "read_ref"
                    || intent.payload["replay"] != *replay || intent.payload["effect"] != primary.payload["effect"]
                    || intent.payload["input_sha256"] != primary.payload["input_sha256"]
                    || intent.commit_cursor <= frame.commit_cursor || intent.commit_cursor > primary.commit_cursor || intent.sequence >= primary.sequence
                    || intent_receipt.transaction_id != intent.transaction_id || intent_receipt.commit_cursor != intent.commit_cursor
                    || intent_receipt.event_ids.get(intent.transaction_index as usize) != Some(&intent.event_id)
                    || !intent_receipt.stream_revisions.iter().any(|source| source.stream_id == intent.stream_id
                        && source.expected_revision < intent.sequence && intent.sequence <= source.committed_revision) {
                    return Err("original early read intent differs".into());
                }
                let mut successor = Vec::new();
                for candidate in graph.nodes.iter().filter(|candidate| candidate.executor_kind == "tool_batch"
                    && graph.edges.iter().any(|edge| edge.from == node && edge.to == candidate.id && edge.kind.is_dependency())) {
                    let value = runtime::step_snapshot::decode_persisted_tool_batch_for_test(&candidate.payload_ref)
                        .map_err(|error| error.to_string())?;
                    if value["calls"].as_array().is_some_and(|calls| calls.iter().any(|item| item["id"] == call)) {
                        successor.push((candidate, value));
                    }
                }
                let [(successor_node, value)] = successor.as_slice() else { return Err("original early read successor missing or ambiguous".into()); };
                let successor_attempt = *graph.recovery_cursor.node_attempts.get(&successor_node.id)
                    .ok_or("original early read successor attempt missing")?;
                let (successor_ticket, successor_position) = runtime.graph_state_store()
                    .original_binding_start_for_test(&graph, &successor_node.id, successor_attempt).map_err(|error| error.to_string())?;
                if successor_ticket.graph_id != root || successor_ticket.node_id != successor_node.id
                    || successor_ticket.executor_kind != "tool_batch" || successor_ticket.payload_ref != successor_node.payload_ref
                    || successor_position.1 <= primary.commit_cursor {
                    return Err("original early read successor owner or order differs".into());
                }
                let successor_starts = runtime.event_reader().list_stream(root)?.into_iter().filter(|event|
                    event.sequence == successor_position.0 && event.commit_cursor == successor_position.1
                        && event.transaction_index == successor_position.2).collect::<Vec<_>>();
                let [successor_start] = successor_starts.as_slice() else { return Err("original early read successor start missing or ambiguous".into()); };
                let successor_receipt = backend.original_transaction_receipt(&successor_start.transaction_id).map_err(|error| error.to_string())?
                    .ok_or("original early read successor transaction missing")?;
                if successor_receipt.transaction_id != successor_start.transaction_id
                    || successor_receipt.commit_cursor != successor_start.commit_cursor
                    || successor_receipt.event_ids.get(successor_start.transaction_index as usize) != Some(&successor_start.event_id)
                    || !successor_receipt.stream_revisions.iter().any(|source| source.stream_id == successor_start.stream_id
                        && source.expected_revision < successor_start.sequence && successor_start.sequence <= source.committed_revision) {
                    return Err("original early read successor transaction differs".into());
                }
                value.clone()
            }
            _ => return Err("original root ticket executor differs".into()),
        };
        let matching_calls = batch["calls"].as_array().ok_or("original root ticket calls missing")?
            .iter().filter(|item| item["id"] == call).collect::<Vec<_>>();
        let [original_call] = matching_calls.as_slice() else { return Err("original root ticket call missing or ambiguous".into()); };
        let original_input = original_call["input"].as_str().ok_or("original root ticket input missing")?;
        if batch["session_id"] != session || original_call["name"] != "read_ref"
            || original_call["origin"] != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
            || batch["step_snapshots"].get(step.snapshot.identity.step_id.as_str())
                != Some(&serde_json::to_value(&*step.snapshot).map_err(|error| error.to_string())?)
            || harness_contract::step::parse_strict_json(original_input).map_err(|error| error.to_string())?
                != plan_task["normalized_input"] {
            return Err("original root ticket call, Step or input differs".into());
        }
        let graph_events = runtime.event_reader().list_stream(root)?;
        let starts = graph_events.iter().filter(|event| event.sequence == position.0
            && event.commit_cursor == position.1 && event.transaction_index == position.2).collect::<Vec<_>>();
        let [start] = starts.as_slice() else { return Err("original root ticket start missing or ambiguous".into()); };
        let start_receipt = backend.original_transaction_receipt(&start.transaction_id).map_err(|error| error.to_string())?
            .ok_or("original root ticket transaction receipt missing")?;
        if start_receipt.transaction_id != start.transaction_id
            || start_receipt.commit_cursor != start.commit_cursor
            || start_receipt.event_ids.get(start.transaction_index as usize) != Some(&start.event_id)
            || !start_receipt.stream_revisions.iter().any(|source| source.stream_id == start.stream_id
                && source.expected_revision < start.sequence && start.sequence <= source.committed_revision) {
            return Err("original root ticket transaction differs".into());
        }
    }
    let receipt = backend.original_transaction_receipt(&primary.transaction_id).map_err(|error| error.to_string())?
        .ok_or("original root read transaction receipt missing")?;
    if receipt.transaction_id != primary.transaction_id || receipt.commit_cursor != primary.commit_cursor
        || receipt.event_ids.get(primary.transaction_index as usize) != Some(&primary.event_id)
        || receipt.event_ids.get(index[0].transaction_index as usize) != Some(&index[0].event_id)
        || ![primary, &index[0]].iter().all(|event| receipt.stream_revisions.iter().any(|source|
            source.stream_id == event.stream_id && source.expected_revision < event.sequence && event.sequence <= source.committed_revision)) {
        return Err("original root read transaction receipt differs".into());
    }
    let acquisition = &primary.payload["read_acquisition_v1"];
    if acquisition.is_null() || acquisition != &index[0].payload["read_acquisition_v1"]
        || acquisition["primary_stream"] != primary.stream_id || acquisition["index_stream"] != index[0].stream_id
        || acquisition["session_id"] != session || acquisition["invocation_id"] != call
        || acquisition["tool_name"] != "read_ref" || acquisition["graph_id"] != root
        || acquisition["node_id"] != node || acquisition["attempt"].as_u64() != Some(attempt)
        || acquisition["step"] != serde_json::to_value(&step.snapshot.identity).map_err(|error| error.to_string())?
        || acquisition["origin"] != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
        || acquisition["body"]["source_ref"] != selector {
        return Err("original root read acquisition differs".into());
    }
    let source_hash = acquisition["body"]["source_sha256"].as_str().filter(|value| !value.is_empty())
        .ok_or("root read source digest missing")?;
    let source_bytes = acquisition["body"]["source_bytes"].as_u64().ok_or("root read source length missing")?;
    if expected_source.is_some_and(|(hash, bytes)| hash != source_hash || bytes != source_bytes) {
        return Err("root read continuation changed source".into());
    }
    let outcome = &primary.payload["outcome"];
    if outcome["tool_use_id"] != call || outcome["tool_name"] != "read_ref"
        || outcome["status"] != "executed" || !outcome["error"].is_null() {
        return Err(format!("original root read did not succeed: {}", outcome["error"].as_str().unwrap_or("unknown")));
    }
    let output = outcome["output"].as_str().ok_or("original root read output missing")?;
    if primary.payload["outcome_truncated"] != false { return Err("original root read receipt output truncated or unknown".into()); }
    let completions: Vec<_> = events.iter().filter(|event| event.kind == "tool.invocation.completed"
        && event.payload["tool_call_id"] == call && event.payload["tool_name"] == "read_ref"
        && event.payload["is_error"] == false && event.commit_cursor > primary.commit_cursor).collect();
    if completions.len() != 1 { return Err("original root read completion missing or ambiguous".into()); }
    let completion = completions[0];
    let raw_ref = completion.payload["full_output_ref"].as_str().filter(|value| value.starts_with("tool://"))
        .ok_or("original root read feedback reference missing")?;
    let messages: Vec<_> = request.body["messages"].as_array().ok_or("provider messages missing")?.iter()
        .filter(|message| message["role"] == "tool" && message["tool_call_id"] == call).collect();
    let feedback = messages.first().and_then(|message| message["content"].as_str()).ok_or("root read feedback missing")?;
    if messages.len() != 1 || !feedback.starts_with("Tool `read_ref` completed. Evidence: ") || !feedback.contains(raw_ref)
        || completion.payload["input_hash"].as_str().is_none_or(|hash| primary.payload["input_sha256"] != format!("sha256:{hash}")) {
        return Err("original root read feedback differs from owner receipt".into());
    }
    let page: Value = serde_json::from_str(output).map_err(|_| "original root read output malformed")?;
    if page["available"] != true || page["evidence_ref"] != selector || page.get("next_request").is_none()
        || page["coverage"]["selection"] != "sequential_content" {
        return Err("original root artifact page unavailable or unbound".into());
    }
    let start = page["coverage"]["byte_start"].as_u64().ok_or("root read page start missing")?;
    let end = page["coverage"]["byte_end"].as_u64().ok_or("root read page end missing")?;
    let total = page["coverage"]["total_bytes"].as_u64().ok_or("root read page total missing")?;
    if total != source_bytes || start != expected_start || end < start || end > total || (start == end && end < total)
        || expected_total.is_some_and(|value| value != total) {
        return Err("root artifact page coverage is discontinuous".into());
    }
    if page["next_request"].is_null() {
        if end != total { return Err("root artifact coverage incomplete without continuation".into()); }
        return Ok((None, end, total, source_hash.to_string(), source_bytes));
    }
    if end == total { return Err("root artifact continuation follows complete coverage".into()); }
    if !page["next_request"]["query"].is_null() || page["next_request"]["cursor"] != page["next_cursor"] {
        return Err("root read continuation differs from original page".into());
    }
    let next = page["next_request"].as_object().ok_or("root read continuation malformed")?;
    let reference = next.get("reference").or_else(|| next.get("evidence_ref")).and_then(Value::as_str)
        .filter(|value| *value == selector).ok_or("root read continuation changes artifact")?;
    let cursor = next.get("cursor").and_then(Value::as_str).filter(|value| !value.is_empty())
        .ok_or("root read continuation cursor missing")?;
    let mut input = json!({"reference": reference, "cursor": cursor});
    if let Some(limit) = next.get("limit") { input["limit"] = limit.clone(); }
    Ok((Some(input), end, total, source_hash.to_string(), source_bytes))
}

// Check the original per-call outcome before scripting the next model response.
fn c_require_original_tool_success(
    services: &RecoveryServices, request: &ProviderRequest, session: &str, turn: &str,
    program: &str, call: &str, tool: &str, action: Option<&str>,
) -> Result<(), String> {
    let runtime = services.runtime.runtime_services();
    let events = runtime.event_reader().list_stream(&format!("session:{session}"))?;
    let plan_task = c_original_lifecycle_plan_task(&events, call, tool)?;
    let step: runtime::step_snapshot::StepCallBinding = serde_json::from_value(plan_task["step_binding"].clone())
        .map_err(|_| "original tool Step missing")?;
    if step.origin.call_id != call || step.snapshot.identity.session_id != session || step.snapshot.identity.turn_id != turn {
        return Err("original tool Step owner differs".into());
    }
    let key = step.effect_idempotency_key();
    let suffix = if action.is_some() { "receipt" } else { "read-receipt" };
    let primaries: Vec<_> = runtime.event_reader().list_stream(&format!("execution-effect:{key}"))?.into_iter()
        .filter(|event| event.idempotency_key.as_deref() == Some(format!("{key}:{suffix}").as_str())).collect();
    if primaries.len() != 1 { return Err("original tool receipt missing or ambiguous".into()); }
    let primary = &primaries[0];
    let outcome = &primary.payload["outcome"];
    if outcome["tool_use_id"] != call || outcome["tool_name"] != tool
        || outcome["status"] != "executed" || !outcome["error"].is_null() {
        return Err(format!("original {tool} failed or unknown: {}", outcome["error"].as_str().unwrap_or("unknown")));
    }
    let parent = &primary.payload["replay"]["parent_execution"];
    let graph = parent["execution_id"].as_str().ok_or("original tool graph missing")?;
    let node = parent["node_id"].as_str().ok_or("original tool node missing")?;
    let attempt = primary.payload["replay"]["parent_execution_attempt"].as_u64().ok_or("original tool attempt missing")?;
    let indices: Vec<_> = runtime.event_reader().list_stream(&format!("execution-agent-receipts:{graph}:{node}:{attempt}"))?
        .into_iter().filter(|event| event.kind == "execution.agent_tool.receipt"
            && event.idempotency_key.as_deref() == Some(format!("agent-tool-receipt:{key}").as_str())).collect();
    if indices.len() != 1 || primary.transaction_id != indices[0].transaction_id
        || primary.commit_cursor != indices[0].commit_cursor || primary.transaction_index >= indices[0].transaction_index
        || primary.payload["outcome"] != indices[0].payload["outcome"] {
        return Err("original tool primary/index differ".into());
    }
    use runtime::RuntimeEventStoreBackend as _;
    let backend = runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone())
        .map_err(|error| error.to_string())?;
    let receipt = backend.original_transaction_receipt(&primary.transaction_id).map_err(|error| error.to_string())?
        .ok_or("original tool transaction receipt missing")?;
    if receipt.transaction_id != primary.transaction_id || receipt.commit_cursor != primary.commit_cursor
        || receipt.event_ids.get(primary.transaction_index as usize) != Some(&primary.event_id)
        || receipt.event_ids.get(indices[0].transaction_index as usize) != Some(&indices[0].event_id) {
        return Err("original tool transaction differs".into());
    }
    let completed = events.iter().filter(|event| event.kind == "tool.invocation.completed"
        && event.payload["tool_call_id"] == call && event.payload["tool_name"] == tool
        && event.payload["status"] == "completed" && event.payload["is_error"] == false
        && event.commit_cursor > primary.commit_cursor).count();
    let messages = request.body["messages"].as_array().ok_or("provider messages missing")?;
    if completed != 1 || messages.iter().filter(|message| message["role"] == "tool" && message["tool_call_id"] == call).count() != 1 {
        return Err("original completed tool feedback missing or ambiguous".into());
    }
    if let Some(action) = action {
        let applied = runtime.event_reader().list_stream(&format!("agentic-program:{program}"))?.into_iter()
            .filter(|event| event.kind == "agentic.action_applied" && event.payload["envelope"]["action_id"] == key
                && event.payload["envelope"]["action"]["kind"] == action && event.commit_cursor < primary.commit_cursor).count();
        if applied != 1 { return Err("original applied Program action missing or ambiguous".into()); }
    }
    Ok(())
}

// Called only by the original read fault once its exact post-action scope is
// active. It writes through the real Goal owner and retains the transaction
// identities needed to distinguish a missed hook from an actual update.
fn c_original_review_for_external_goal(
    store: &runtime::RuntimeEventStore,
    graph_store: &runtime::ExecutionGraphStateStore,
    backend: &runtime_postgres::PostgresRuntimeEventStore,
    ticket_graph: &str, ticket_node: &str, ticket_attempt: u32,
    session: &str, turn: &str, root: &str, program: &str, goal_id: &str,
) -> Result<(String, u64, Value), String> {
    use runtime::RuntimeEventStoreBackend as _;
    let program_stream = format!("agentic-program:{program}");
    let goal_stream = format!("goal:{goal_id}");
    let mut matches = Vec::new();
    for action in store.list_stream(&program_stream)? {
        let envelope = &action.payload["envelope"];
        if action.kind != "agentic.action_applied"
            || envelope["action"]["kind"] != "objective_review"
            || envelope["actor"]["session_id"] != session
            || envelope["actor"]["turn_id"] != turn
            || envelope["actor"]["root_execution_id"] != root {
            continue;
        }
        let Some(key) = envelope["action_id"].as_str() else { continue; };
        let source = format!("execution-effect:{key}");
        let Some(primary) = store.event_by_idempotency_key(&source, &format!("{key}:receipt"))
            .map_err(|error| error.to_string())? else { continue; };
        let outcome = &primary.payload["outcome"];
        if outcome["tool_use_id"] != "d-objective-review" || outcome["tool_name"] != "objective_review"
            || outcome["status"] != "executed" || !outcome["error"].is_null()
            || primary.commit_cursor <= action.commit_cursor { continue; }
        let Some(output) = outcome["output"].as_str() else { continue; };
        let Ok(observation) = serde_json::from_str::<harness_contract::agent_action::AgentActionObservation>(output) else { continue; };
        if observation.action_id != key || observation.program_id != program || observation.revision != action.sequence
            || observation.status != harness_contract::agent_action::AgentActionStatus::Applied
            || observation.duplicate || observation.error.is_some() { continue; }
        let replay = &primary.payload["replay"];
        let parent = &replay["parent_execution"];
        let Some(node) = parent["node_id"].as_str() else { continue; };
        let Some(attempt) = replay["parent_execution_attempt"].as_u64() else { continue; };
        if replay["session_id"] != session || replay["call_id"] != "d-objective-review"
            || parent["execution_id"] != root || ticket_graph != root || ticket_node != node
            || u64::from(ticket_attempt) != attempt || attempt == 0 { continue; }
        let graph = graph_store.load(root).map_err(|error| error.to_string())?;
        let lineage = graph.lineage.as_ref().ok_or("original root review lineage missing")?;
        if graph.parent_execution.is_some() || lineage.session_id != session || lineage.turn_id != turn { continue; }
        let (ticket, position) = graph_store.original_binding_start_for_test(&graph, node, ticket_attempt)
            .map_err(|error| error.to_string())?;
        if ticket.graph_id != root || ticket.node_id != node || ticket.attempt != ticket_attempt
            || ticket.executor_kind != "tool_batch" || position.1 >= action.commit_cursor { continue; }
        let batch = runtime::step_snapshot::decode_persisted_tool_batch_for_test(&ticket.payload_ref)
            .map_err(|error| error.to_string())?;
        let plan_events = store.list_stream(&format!("session:{session}"))?;
        let plans = plan_events.iter().filter(|event| event.kind == "tool.execution_plan.created")
            .flat_map(|event| event.payload["tasks"].as_array().into_iter().flatten())
            .filter(|task| task["tool_call_id"] == "d-objective-review" && task["tool_name"] == "objective_review")
            .collect::<Vec<_>>();
        let [plan] = plans.as_slice() else { continue; };
        let step: runtime::step_snapshot::StepCallBinding = serde_json::from_value(plan["step_binding"].clone())
            .map_err(|error| error.to_string())?;
        let calls = batch["calls"].as_array().ok_or("original objective review ticket calls missing")?
            .iter().filter(|item| item["id"] == "d-objective-review").collect::<Vec<_>>();
        let [call] = calls.as_slice() else { continue; };
        let Some(input) = call["input"].as_str() else { continue; };
        if batch["session_id"] != session || call["name"] != "objective_review"
            || step.effect_idempotency_key() != key || step.snapshot.identity.session_id != session
            || step.snapshot.identity.turn_id != turn || step.snapshot.identity.execution_id.as_deref() != Some(root)
            || step.snapshot.identity.run_ref.is_some()
            || call["origin"] != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
            || batch["step_snapshots"].get(step.snapshot.identity.step_id.as_str())
                != Some(&serde_json::to_value(&*step.snapshot).map_err(|error| error.to_string())?)
            || harness_contract::step::parse_strict_json(input).map_err(|error| error.to_string())?
                != plan["normalized_input"] { continue; }
        let starts: Vec<_> = store.list_stream(root)?.into_iter().filter(|event|
            event.sequence == position.0 && event.commit_cursor == position.1 && event.transaction_index == position.2
        ).collect();
        let [start] = starts.as_slice() else { continue; };
        let Some(start_receipt) = backend.original_transaction_receipt(&start.transaction_id)
            .map_err(|error| error.to_string())? else { continue; };
        if start_receipt.transaction_id != start.transaction_id || start_receipt.commit_cursor != start.commit_cursor
            || start_receipt.event_ids.get(start.transaction_index as usize) != Some(&start.event_id)
            || !start_receipt.stream_revisions.iter().any(|source| source.stream_id == start.stream_id
                && source.expected_revision < start.sequence && start.sequence <= source.committed_revision) { continue; }
        let index_stream = format!("execution-agent-receipts:{root}:{node}:{attempt}");
        let Some(index) = store.event_by_idempotency_key(&index_stream, &format!("agent-tool-receipt:{key}"))
            .map_err(|error| error.to_string())? else { continue; };
        let Some(physical) = backend.original_transaction_receipt(&primary.transaction_id)
            .map_err(|error| error.to_string())? else { continue; };
        if primary.kind != "execution.effect.receipt" || index.kind != "execution.agent_tool.receipt"
            || primary.scope != runtime::RuntimeEventScope::ExecutionNode || index.scope != primary.scope
            || primary.actor.as_deref() != Some("governed_tool") || index.actor != primary.actor
            || primary.transaction_id != index.transaction_id || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index || primary.payload["outcome"] != index.payload["outcome"]
            || physical.transaction_id != primary.transaction_id || physical.commit_cursor != primary.commit_cursor
            || physical.event_ids.get(primary.transaction_index as usize) != Some(&primary.event_id)
            || physical.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
            || ![&primary, &index].iter().all(|event| physical.stream_revisions.iter().any(|source|
                source.stream_id == event.stream_id && source.expected_revision < event.sequence && event.sequence <= source.committed_revision)) { continue; }
        let goal_events: Vec<_> = store.list_stream(&goal_stream)?.into_iter().filter(|event|
            event.kind == "goal.revised" && event.transaction_id == action.transaction_id
                && event.commit_cursor == action.commit_cursor).collect();
        let [original_goal] = goal_events.as_slice() else { continue; };
        let Some(action_receipt) = backend.original_transaction_receipt(&action.transaction_id)
            .map_err(|error| error.to_string())? else { continue; };
        if action_receipt.transaction_id != action.transaction_id || action_receipt.commit_cursor != action.commit_cursor
            || action_receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
            || action_receipt.event_ids.get(original_goal.transaction_index as usize) != Some(&original_goal.event_id)
            || !action_receipt.stream_revisions.iter().any(|source| source.stream_id == program_stream
                && source.expected_revision < action.sequence && action.sequence <= source.committed_revision)
            || !action_receipt.stream_revisions.iter().any(|source| source.stream_id == goal_stream
                && source.committed_revision == original_goal.sequence
                && source.expected_revision.checked_add(1) == Some(original_goal.sequence)) { continue; }
        matches.push((action.transaction_id.clone(), original_goal.sequence, original_goal.payload["goal"].clone()));
    }
    let [(transaction_id, original_head, original_goal)] = matches.as_slice() else {
        return Err("same-call original review and receipt missing or ambiguous".into());
    };
    Ok((transaction_id.clone(), *original_head, original_goal.clone()))
}

fn c_append_external_goal(
    store: &runtime::RuntimeEventStore,
    goals: &runtime::execution_core::goal::GoalStore,
    backend: &runtime_postgres::PostgresRuntimeEventStore,
    goal_id: &str,
    original: &(String, u64, Value),
) -> Result<(String, u64, u64), String> {
    use runtime::RuntimeEventStoreBackend as _;
    let goal_stream = format!("goal:{goal_id}");
    let (transaction_id, original_head, original_goal) = original;
    let current = goals.get(goal_id)?.ok_or("original Goal missing")?;
    if serde_json::to_value(&current).map_err(|error| error.to_string())? != *original_goal {
        return Err("Goal changed before external update".into());
    }
    goals.revise(goal_id, current.revision, current.user_sequence + 1, "external concurrent constraint", |goal| {
        goal.constraints.push("external concurrent constraint".into());
        vec!["constraints".into()]
    })?;
    let updates: Vec<_> = store.list_stream(&goal_stream)?.into_iter().filter(|event|
        event.sequence == original_head + 1 && event.kind == "goal.revised"
            && event.transaction_id != *transaction_id
            && event.payload["goal"]["constraints"].as_array().is_some_and(|constraints|
                constraints.iter().any(|constraint| constraint == "external concurrent constraint"))).collect();
    let [update] = updates.as_slice() else { return Err("external Goal revision missing or ambiguous".into()); };
    let update_receipt = backend.original_transaction_receipt(&update.transaction_id)
        .map_err(|error| error.to_string())?.ok_or("external Goal transaction receipt missing")?;
    if update_receipt.transaction_id != update.transaction_id || update_receipt.commit_cursor != update.commit_cursor
        || update_receipt.event_ids.get(update.transaction_index as usize) != Some(&update.event_id)
        || !update_receipt.stream_revisions.iter().any(|source| source.stream_id == goal_stream
            && source.expected_revision == *original_head && source.committed_revision == *original_head + 1) {
        return Err("external Goal revision transaction differs".into());
    }
    Ok((transaction_id.clone(), *original_head, original_head + 1))
}

// Observe the existing owner phase trace, not a second commit path. Missing,
// incomplete or mismatched trace remains unknown and cannot satisfy the gate.
fn c_observed_external_goal_cas(graph: &str, node: &str, attempt: u32, goal_stream: &str, old: u64, new: u64) -> bool {
    use sha2::{Digest as _, Sha256};
    let Some(dir) = std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR") else { return false; };
    let Ok(log) = std::fs::read_to_string(std::path::PathBuf::from(dir).join("callback-complete.log")) else { return false; };
    let graph = format!("graph_sha256={:x}", Sha256::digest(graph.as_bytes()));
    let node = format!("node_sha256={:x}", Sha256::digest(node.as_bytes()));
    let source = format!("source_sha256=Some(\"{:x}\")", Sha256::digest(goal_stream.as_bytes()));
    let attempt = format!("attempt={attempt}");
    let expected = format!("expected=Some({old})");
    let actual = format!("actual=Some({new})");
    log.split_inclusive('\n').filter(|line| line.ends_with('\n')).any(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        line.starts_with("k05_runner_cas_error ") && fields.contains(&graph.as_str()) && fields.contains(&node.as_str())
            && fields.contains(&attempt.as_str()) && fields.contains(&"category=event_store_stale_revision")
            && fields.contains(&"source_kind=goal") && fields.contains(&source.as_str())
            && fields.contains(&expected.as_str()) && fields.contains(&actual.as_str())
    })
}

async fn a8_original_member_coordinates(
    services: &RecoveryServices, request: &ProviderRequest, session: &str, turn: &str,
) -> (String, String, u32) {
    let rows = services.topology.session_store.get_session_domain_events_by_kind_limited(
        session, "context.provider_request_packed", 0, 10_001).await.unwrap();
    assert!(rows.len() < 10_001, "original packed events are complete");
    let packed: Vec<_> = rows.iter().map(|row| session::SessionDomainEvent::from_session_event(row).unwrap())
        .filter(|event| event.session_id == session && event.payload["turn_id"] == turn
            && event.payload["body_sha256"] == request.body_sha256).collect();
    assert_eq!(packed.len(), 1, "one original packed request");
    let execution = packed[0].payload["execution_id"].as_str().expect("original member execution");
    let runs: Vec<_> = services.runtime.runtime_services().agent_runtime().list().into_iter()
        .filter(|run| run.run_id == execution && run.session_id == session).collect();
    assert_eq!(runs.len(), 1, "one original member run");
    (runs[0].graph_id.clone(), runs[0].node_id.clone(), runs[0].attempt)
}

async fn c_original_session_worker_fixture(missing: Option<u8>, native_colon_read: bool, directory_read: bool) {
    let compute = missing.is_some();
    use futures::FutureExt;
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    struct RestoreEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for RestoreEnvironment {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value { std::env::set_var(key, value); }
                else { std::env::remove_var(key); }
            }
        }
    }
    // This ignored integration test must run alone in a child process.
    let _environment = if compute {
        let keys = ["COWD_EVAL_HARNESS", "COWD_EVAL_CORPUS_ID", "COWD_MODEL_TEMPERATURE"];
        let restore = RestoreEnvironment(keys.into_iter().map(|key| (key, std::env::var_os(key))).collect());
        std::env::set_var("COWD_EVAL_HARNESS", "1");
        std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
        std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
        Some(restore)
    } else { None };
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    if compute {
        std::fs::write(root.path().join("workspace/target.txt"), "bounded-ticket-proof").unwrap();
        std::fs::write(root.path().join("workspace/outside.txt"), "must-not-be-read").unwrap();
    }
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry_with_early(&[&provider], native_colon_read);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"), &root.path().join("workspace"),
    ).unwrap();
    let services = RecoveryServices::start(root.path(), topology, providers).await;
    services.bounded_missing.store(missing.unwrap_or(0), std::sync::atomic::Ordering::SeqCst);
    let runtime = services.runtime.runtime_services();
    let session = format!("c-delegated-ingress:{}", uuid::Uuid::new_v4());
    let identity = std::sync::Mutex::new(None::<(String, String)>);
    let ingress_admission = std::sync::Mutex::new(None::<Value>);
    let run = std::panic::AssertUnwindSafe(async {
        let mut request = EnsureSessionRequest::new(&session, Some(provider.model.clone()), SessionSource::Tui);
        request.owner_principal_id = Some("c-delegated-test-operator".into());
        services.session.create_user_session(request).await.unwrap();
        let prompt = if compute {
            let control = json!({"corpus_id":"live-scenarios-v1", "workspace_fixture":"none",
                "provider_constraint":"normal", "temperature_milli":0,
                "resource_scopes":["read:target.txt"], "prompt":"Delegate one bounded task."});
            format!("COWD_EVAL_CONTROL {control}\nDelegate one bounded task and record the assigned agent's claim.")
        } else {
            "Delegate one bounded task and record the assigned agent's claim.".to_string()
        };
        let envelope = harness_contract::turn::SessionInputEnvelope::text(
            &session, harness_contract::turn::InputSourceKind::Tui, &prompt,
        );
        let request_id = envelope.idempotency_key.clone();
        let admission = services.session.admit_input(envelope).await.unwrap();
        *ingress_admission.lock().unwrap() = Some(json!({
            "session_matches":admission.receipt.session_id == session,
            "input_id_present":!admission.receipt.input_id.to_string().is_empty(),
            "input_status":admission.receipt.status,
            "turn_present":!admission.turn_id.is_empty(),
            "execution_present":!admission.execution_graph_id.is_empty(),
            "terminal_id_present":!admission.terminal_id.is_empty(),
            "materialized_present":admission.materialized.is_some(),
        }));
        let graph_id = admission.execution_graph_id;
        let turn = admission.turn_id;
        *identity.lock().unwrap() = Some((turn.clone(), graph_id.clone()));
        let program_id = harness_contract::agent_action::program_id_for_objective(
            &harness_contract::agent_action::root_objective_id(&session, &turn));
        c_root_tool(&services, &mut provider, &session, &turn, &graph_id, &program_id, "c-ingress-team", "team_create", json!({
            "name":"Claim team", "mission":"Claim the bounded task"
        })).await;
        // Observe the original Program and root terminal together. The
        // failure exporter reads bounded call candidates once after failure;
        // unrelated event volume is never a business precondition.
        let team = tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                if let Some(team) = runtime.agent_action_service().project_if_exists(&program_id)
                    .expect("original Program projection must be readable")
                    .and_then(|program| program.teams.keys().next().cloned()) { break team; }
                if let Ok(graph) = runtime.graph_state_store().load(&graph_id) {
                    use harness_contract::execution_graph::ExecutionNodeStatus;
                    let statuses = graph.node_statuses.values().copied().collect::<Vec<_>>();
                    if !statuses.is_empty() && statuses.iter().all(|status| status.is_terminal())
                        && statuses.iter().any(|status| matches!(status,
                            ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked | ExecutionNodeStatus::Cancelled)) {
                        panic!("original root execution terminal before team_create; inspect original event export");
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }).await.expect("original team_create did not settle before deadline");
        c_root_tool(&services, &mut provider, &session, &turn, &graph_id, &program_id, "c-ingress-invite", "agent_invite", json!({
            "team_ref":team, "role":"Reader", "mission":"Claim the bounded task",
            "required_capabilities":if compute { json!(["read", "test", "search"]) } else { json!(["read"]) },
            "definition_ref":if compute { "builtin/cowd/execute" } else { "builtin/cowd/direct" }
        })).await;
        if !compute {
            c_root_tool(&services, &mut provider, &session, &turn, &graph_id, &program_id, "d-ingress-reviewer", "agent_invite", json!({
                "team_ref":team, "role":"Independent reviewer", "mission":"Independently review the submitted result",
                "required_capabilities":["read"], "definition_ref":"builtin/cowd/direct"
            })).await;
        }
        c_root_tool(&services, &mut provider, &session, &turn, &graph_id, &program_id, "c-ingress-publish", "task_publish", json!({
            "team_ref":team, "title":"Claim task", "objective":"Claim the bounded task",
            "acceptance":"Claim is recorded by Runtime",
            "required_capabilities":if compute { json!(["read", "test", "search"]) } else { json!(["read"]) }
        })).await;
        let task = durable_until(|| runtime.agent_action_service().project_if_exists(&program_id)
            .unwrap().and_then(|program| program.tasks.keys().next().cloned())).await;
        let mut root_wait_sent = false;
        let mut claim_sent = false;
        let mut directory_sent = false;
        let mut directory_owner = None;
        let mut claimed_continuation = None;
        let mut compute_stage = 0_u8;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let request = provider.next().await;
                let child = c_route_owner(&services, &request, &session, &turn, &graph_id, &program_id, Some(&task))
                    .await.expect("unique original provider owner");
                if child && !claim_sent {
                    assert!(request.body["tools"].as_array().is_some_and(|tools| tools.iter().any(|tool|
                        tool["function"]["name"] == "task_claim")), "Agent must be offered original task_claim");
                    request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[{
                        "index":0,"id":"c-ingress-claim","type":"function",
                        "function":{"name":"task_claim","arguments":json!({"task_ref":task,"reason":"Accept the assigned task"}).to_string()}
                    }]})).unwrap();
                    claim_sent = true;
                } else if child && claim_sent {
                    if (!compute && !directory_sent) || (compute && compute_stage == 0) {
                        assert!(request.body["messages"].as_array().is_some_and(|messages| messages.iter().any(|message|
                            message["role"] == "tool" && message["tool_call_id"] == "c-ingress-claim")));
                    }
                    if !compute {
                        if directory_read && !directory_sent {
                            assert!(request.body["tools"].as_array().is_some_and(|tools| tools.iter().any(|tool|
                                tool["function"]["name"] == "read_ref")));
                            directory_sent = true;
                            directory_owner = Some(a8_original_member_coordinates(&services, &request, &session, &turn).await);
                            request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[{
                                "index":0,"id":"a8-member-directory","type":"function",
                                "function":{"name":"read_ref","arguments":json!({"reference":format!("task://{task}")}).to_string()}
                            }]})).unwrap();
                            continue;
                        }
                        if directory_read {
                            assert_eq!(Some(a8_original_member_coordinates(&services, &request, &session, &turn).await),
                                directory_owner, "directory feedback belongs to the original member attempt");
                            c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                                "a8-member-directory", "read_ref", None).expect("original member directory read and feedback");
                            let events = runtime.event_reader().list_stream(&format!("session:{session}")).unwrap();
                            let decisions: Vec<_> = events.iter().filter(|event|
                                event.kind == "authorization.capability_assessed"
                                && event.payload["capability"] == "state_inspect"
                                && event.payload["_call_refusal_binding"]["call_id"] == "a8-member-directory"
                                && event.payload["_call_refusal_binding"]["derived_target"]["input"]["entry_ref"] == task).collect();
                            assert_eq!(decisions.len(), 1, "one same-call target assessment");
                            let lease = decisions[0].payload["lease"]["lease_id"].as_str().expect("target lease");
                            let transitions = runtime.event_reader().list_stream(&format!("authorization-lease:{lease}")).unwrap();
                            assert_eq!(transitions.iter().filter(|event| event.kind == "authorization.lease_transition"
                                && event.status.as_deref() == Some("consumed")).count(), 1, "one target consumption");
                            let ticket = &decisions[0].payload["_call_refusal_binding"]["owner"]["ticket"];
                            let task_fact = c_original_lifecycle_plan_task(&events, "a8-member-directory", "read_ref").unwrap();
                            let step: runtime::step_snapshot::StepCallBinding = serde_json::from_value(task_fact["step_binding"].clone()).unwrap();
                            let key = step.effect_idempotency_key();
                            let receipts: Vec<_> = runtime.event_reader().list_stream(&format!("execution-effect:{key}")).unwrap()
                                .into_iter().filter(|event| event.idempotency_key.as_deref() == Some(format!("{key}:read-receipt").as_str())).collect();
                            assert_eq!(receipts.len(), 1, "one original read receipt");
                            let replay = &receipts[0].payload["replay"];
                            let parent = &replay["parent_execution"];
                            let (graph, node, attempt) = directory_owner.as_ref().expect("original member owner");
                            assert_eq!(parent["execution_id"], *graph);
                            assert_eq!(parent["node_id"], *node);
                            assert_eq!(replay["parent_execution_attempt"].as_u64(), Some(u64::from(*attempt)));
                            assert_ne!((ticket["graph_id"].as_str(), ticket["node_id"].as_str()),
                                (parent["execution_id"].as_str(), parent["node_id"].as_str()), "ToolBatch and AgentTask are distinct owners");
                        }
                        // Hold the real worker at its original provider request while
                        // the claim/receipt assertions below inspect the live owner.
                        claimed_continuation = Some(request);
                        break;
                    }
                    match compute_stage {
                        0 => {
                            assert!(request.body["tools"].as_array().is_some_and(|tools| tools.iter().any(|tool|
                                tool["function"]["name"] == "tool_search")), "original Agent Step must expose tool_search");
                            request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[{
                                "index":0,"id":"b-native-search","type":"function",
                                "function":{"name":"tool_search","arguments":json!({"query":"select:execute_code"}).to_string()}
                            }]})).unwrap();
                        }
                        1 => {
                            assert!(request.body["tools"].as_array().is_some_and(|tools| tools.iter().any(|tool|
                                tool["function"]["name"] == "execute_code")), "original Agent Step must expose execute_code");
                            request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[
                                {"index":0,"id":"b-native-outside","type":"function",
                                 "function":{"name":"execute_code","arguments":json!({"language":"python",
                                    "code":"print(open('/project/outside.txt').read())",
                                    "input_files":["outside.txt"],"timeout_ms":5000}).to_string()}},
                                {"index":1,"id":"b-native-compute","type":"function",
                                 "function":{"name":"execute_code","arguments":json!({"language":"python",
                                    "code":"print(open('/project/target.txt').read())",
                                    "input_files":["target.txt"],"timeout_ms":5000}).to_string()}}
                            ]})).unwrap();
                        }
                        2 => {
                            let message = request.body["messages"].as_array().unwrap().iter().find(|message|
                                message["role"] == "tool" && message["tool_call_id"] == "b-native-compute")
                                .expect("original next provider request must contain compute result");
                            if missing == Some(0) {
                                assert!(message.to_string().contains("bounded-ticket-proof"),
                                    "original bounded compute did not return the declared file: {message}");
                            } else {
                                assert!(!message.to_string().contains("bounded-ticket-proof"),
                                    "missing bounded capability must not return the declared content");
                                assert!(message.to_string().contains("bound") || message.to_string().contains("unavailable")
                                    || message.to_string().contains("error"), "missing capability must be reported: {message}");
                            }
                            let rejected = request.body["messages"].as_array().unwrap().iter().find(|message|
                                message["role"] == "tool" && message["tool_call_id"] == "b-native-outside")
                                .expect("original next provider request must report the out-of-scope call");
                            assert!(!rejected.to_string().contains("must-not-be-read"),
                                "out-of-scope content cannot reach the provider");
                            assert_eq!(rejected["is_error"], true, "out-of-scope tool feedback must be an error");
                            let text = rejected["content"].as_str().expect("original refusal feedback text");
                            assert!(text.contains("scope") || text.contains("denied") || text.contains("refus")
                                || (text.contains("outside") && text.contains("resource lease")),
                                "out-of-scope feedback must explicitly identify the refusal");
                            request.reply.send(json!({"role":"assistant","content":"The bounded calculation completed."})).unwrap();
                            break;
                        }
                        _ => panic!("unexpected Agent compute stage"),
                    }
                    compute_stage += 1;
                } else if !root_wait_sent {
                    assert!(request.body["messages"].as_array().is_some_and(|messages| messages.iter().any(|message|
                        message["role"] == "tool" && message["tool_call_id"] == "c-ingress-publish")));
                    request.reply.send(json!({"role":"assistant","content":null,"tool_calls":[{
                        "index":0,"id":"c-ingress-wait","type":"function",
                        "function":{"name":"state_inspect","arguments":json!({"scope_ref":program_id,"wait_for_workers":true}).to_string()}
                    }]})).unwrap();
                    root_wait_sent = true;
                } else {
                    panic!("unexpected extra root provider request while waiting for Agent claim");
                }
            }
        }).await.expect("real delegated Agent claim continuation deadline");
        let stream = format!("agentic-program:{program_id}");
        let event = durable_until(|| runtime.event_reader().list_stream(&stream).unwrap().into_iter().find(|event|
            event.kind == "agentic.action_applied" && event.payload["envelope"]["action"]["kind"] == "task_claim"
                && event.payload["envelope"]["action"]["input"]["task_ref"] == task)).await;
        let actor = &event.payload["envelope"]["actor"];
        let actual = services.physical_calls.lock().unwrap().iter()
            .find(|call| call.tool_use_id == "c-ingress-claim").cloned().expect("same Gateway saw Agent request");
        let parent = actual.parent_execution.as_ref().expect("AgentTask parent");
        let attempt = actual.attempt.expect("original AgentTask attempt");
        if compute {
            let agent_graph = runtime.graph_state_store().load(&parent.execution_id).expect("original AgentTask graph");
            let node = agent_graph.nodes.iter().find(|node| node.id == parent.node_id).expect("original AgentTask node");
            let packet: harness_contract::agent::AgentTaskPacket = serde_json::from_str(&node.payload_ref).expect("original AgentTask packet");
            assert!(packet.resource_scopes.iter().any(|scope| scope == "read:target.txt"),
                "original packet must carry the root Session's bounded read scope");
            let events = runtime.event_reader().all_events(100_001).expect("original compute receipts");
            assert!(events.len() < 100_001, "receipt evidence may be truncated");
            let receipts = |call: &str| events.iter().filter(|event| match event.kind.as_str() {
                "execution.read.receipt" => event.payload["tool_use_id"] == call,
                "execution.effect.receipt" => event.payload["outcome"]["tool_use_id"] == call,
                _ => false,
            }).count();
            assert_eq!(receipts("b-native-compute"), usize::from(missing == Some(0)),
                "a valid call requires its original receipt; a missing capability cannot produce one");
            assert_eq!(receipts("b-native-outside"), 0, "refused call cannot produce an execution receipt");
            if missing == Some(0) {
                let primary = events.iter().find(|event| event.kind == "execution.effect.receipt"
                    && event.payload["outcome"]["tool_use_id"] == "b-native-compute")
                    .expect("original compute effect primary");
                let key = primary.payload["idempotency_key"].as_str().filter(|key| !key.is_empty())
                    .expect("original compute effect key");
                assert_eq!(primary.stream_id, format!("execution-effect:{key}"));
                assert_eq!(primary.schema_version, 1);
                assert_eq!(primary.payload["replay"]["call_id"], "b-native-compute");
                assert_eq!(primary.payload["replay"]["parent_execution"]["execution_id"], parent.execution_id);
                assert_eq!(primary.payload["replay"]["parent_execution"]["node_id"], parent.node_id);
                let indexes: Vec<_> = events.iter().filter(|event| event.kind == "execution.agent_tool.receipt"
                    && event.payload["idempotency_key"] == key).collect();
                assert_eq!(indexes.len(), 1, "unique original compute index");
                let index = indexes[0];
                assert_eq!(index.schema_version, 2);
                assert_eq!(index.stream_id, format!("execution-agent-receipts:{}:{}:{}",
                    parent.execution_id, parent.node_id, attempt));
                assert_eq!(primary.transaction_id, index.transaction_id);
                assert_eq!(primary.commit_cursor, index.commit_cursor);
                assert!(primary.transaction_index < index.transaction_index);
                assert_eq!(primary.payload["outcome"], index.payload["outcome"]);
                assert_eq!(primary.payload["effect"], index.payload["effect"]);
                assert_eq!(primary.payload["outcome"]["status"], "executed");
                assert_eq!(primary.payload["outcome_truncated"], false);
                assert_eq!(index.payload["outcome_truncated"], false);
                let attempt_ref = attempt.to_string();
                for event in [primary, index] {
                    for (kind, id) in [("tool_invocation", "b-native-compute"),
                        ("execution_graph", parent.execution_id.as_str()),
                        ("execution_node", parent.node_id.as_str()), ("agent_attempt", attempt_ref.as_str())] {
                        assert!(event.refs.iter().any(|reference| reference.kind == kind && reference.id == id),
                            "original compute receipt reference missing");
                    }
                }
                use runtime::RuntimeEventStoreBackend as _;
                let backend = runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone())
                    .expect("original compute receipt backend");
                let receipt = backend.original_transaction_receipt(&primary.transaction_id)
                    .expect("read original compute transaction").expect("original compute transaction receipt");
                assert_eq!(receipt.transaction_id, primary.transaction_id);
                assert_eq!(receipt.commit_cursor, primary.commit_cursor);
                for event in [primary, index] {
                    assert_eq!(receipt.event_ids.get(event.transaction_index as usize), Some(&event.event_id));
                    assert!(receipt.stream_revisions.iter().any(|source| source.stream_id == event.stream_id
                        && source.expected_revision < event.sequence && event.sequence <= source.committed_revision));
                }
            }
            let no_dispatch: Vec<_> = events.iter().filter(|event| event.kind == "execution_node.transitioned")
                .filter_map(|event| event.payload.get("no_dispatch").map(|fact| (event, fact)))
                .flat_map(|(event, fact)| fact["calls"].as_array().into_iter().flatten()
                    .filter(|call| call["call_id"] == "b-native-outside").map(move |call| (event, fact, call)))
                .collect();
            assert_eq!(no_dispatch.len(), 1, "original outside refusal must bind one node outcome");
            let (node_event, fact, outside) = no_dispatch[0];
            assert_eq!(outside["reason"], "committed_call_refusal");
            let refusal_id = outside["refusal_event_id"].as_str().expect("durable refusal id");
            let effect_key = outside["effect_key"].as_str().expect("original outside effect key");
            let ticket = &fact["ticket"];
            assert_eq!(ticket["attempt"].as_u64(), Some(1), "original node attempt");
            assert_eq!(node_event.payload["graph_id"], ticket["graph_id"], "original node graph");
            assert_eq!(node_event.payload["node_id"], ticket["node_id"], "original node identity");
            let refusals: Vec<_> = events.iter().filter(|event| event.event_id == refusal_id
                && event.kind == "execution.call.refused" && event.status.as_deref() == Some("refused"))
                .collect();
            assert_eq!(refusals.len(), 1, "original committed refusal event");
            let refusal = refusals[0];
            assert_eq!(refusal.payload["ticket"], *ticket, "refusal binds the original node ticket");
            assert_eq!(refusal.payload["request"]["tool_use_id"], "b-native-outside", "refusal binds the original call");
            assert_eq!(refusal.payload["request"]["idempotency_key"], effect_key, "refusal binds the original effect");
            assert!(refusal.commit_cursor < node_event.commit_cursor, "refusal precedes node outcome");
            assert!(refusal.stream_id == format!("execution-call-decision:{effect_key}"), "same original effect decision");
            use runtime::RuntimeEventStoreBackend as _;
            let backend = runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone())
                .expect("original receipt backend");
            for event in [refusal, node_event] {
                let receipt = backend.original_transaction_receipt(&event.transaction_id)
                    .expect("read original transaction receipt").expect("original transaction receipt");
                assert_eq!(receipt.transaction_id, event.transaction_id);
                assert_eq!(receipt.commit_cursor, event.commit_cursor);
                assert_eq!(receipt.event_ids.get(event.transaction_index as usize), Some(&event.event_id));
                assert!(receipt.stream_revisions.iter().any(|source| source.stream_id == event.stream_id
                    && source.expected_revision < event.sequence && source.committed_revision >= event.sequence));
                if event.event_id == refusal.event_id {
                    let effect_stream = format!("execution-effect:{effect_key}");
                    let effect_sources: Vec<_> = receipt.stream_revisions.iter()
                        .filter(|source| source.stream_id == effect_stream).collect();
                    assert_eq!(effect_sources.len(), 1, "original refusal transaction effect fence");
                    assert_eq!(effect_sources[0].expected_revision, 0, "effect absent at refusal");
                    assert_eq!(effect_sources[0].committed_revision, 0, "refusal did not append an effect");
                }
            }
            assert_eq!(runtime.event_reader().list_stream(&format!("execution-effect:{effect_key}"))
                .expect("original outside effect stream").len(), 0, "refused outside call has no effect history");
        }
        assert_eq!(actor["original_execution"]["graph_id"], parent.execution_id);
        assert_eq!(actor["original_execution"]["node_id"], parent.node_id);
        assert_eq!(actor["original_execution"]["attempt"], attempt);
        assert_eq!(actor["execution_id"], parent.execution_id);
        let projection = runtime.agent_action_service().project(&program_id).unwrap();
        let registered = projection.tasks[&task].active_attempts.get(&parent.execution_id)
            .expect("original Program dispatch owner");
        assert_eq!(registered.mode, harness_contract::agent_action::AgentAttemptMode::Execute);
        assert_eq!(projection.tasks[&task].claim_execution_id.as_deref(), Some(parent.execution_id.as_str()));
        let input = services.session.runtime_input(&request_id).await.unwrap().expect("original Session input");
        assert_eq!(input.turn_id, turn);
        assert_eq!(input.session_id, session);
        assert!(input.claim_owner.is_some() && input.claim_token.is_some() && input.claim_fence_epoch.is_some());
        {
            let state = services.runtime.active_turns.state.lock().unwrap();
            let control = state.controls.get(&turn).expect("Gateway installed original turn control");
            assert_eq!(control.session_id, session);
            assert_eq!(control.execution_id.as_deref(), Some(graph_id.as_str()));
        }
        let authenticated = runtime.resolve_agent_action_actor_for_attempt(parent, attempt, None).await
            .expect("original NodesStarted ticket and fence");
        assert_eq!(serde_json::to_value(authenticated).unwrap(), *actor);
        let index = runtime.event_reader().list_stream(&format!("execution-agent-receipts:{}:{}:{}", parent.execution_id, parent.node_id, attempt)).unwrap();
        let receipt = index.iter().find(|record| record.kind == "execution.agent_tool.receipt"
            && record.payload["idempotency_key"] == actual.idempotency_key
            && record.payload["outcome"]["tool_use_id"] == actual.tool_use_id).expect("original Agent receipt index");
        let primary = runtime.event_reader().list_stream(&format!("execution-effect:{}", actual.idempotency_key)).unwrap();
        let primary = primary.iter().find(|record| matches!(record.kind.as_str(), "execution.effect.receipt" | "execution.read.receipt"))
            .expect("original Gateway receipt");
        assert_eq!(primary.transaction_id, receipt.transaction_id);
        assert_eq!(primary.commit_cursor, receipt.commit_cursor);
        assert!(primary.transaction_index < receipt.transaction_index);
        assert_eq!(primary.payload["outcome"], receipt.payload["outcome"]);
        let graph = runtime.graph_state_store().load(&graph_id).expect("original ingress graph");
        let lineage = graph.lineage.as_ref().expect("original Task lineage");
        assert_eq!(lineage.session_id, session);
        assert_eq!(lineage.turn_id, turn);
        assert!(runtime.task_aggregate_service().get(&lineage.root_task_id).unwrap().is_some(), "original root Task owner");
        let goal = runtime.goal_store().get(&format!("goal:{graph_id}")).unwrap().expect("original root Goal");
        let binding = goal.execution_binding.as_ref().unwrap();
        assert_eq!(binding.root_execution_id, graph_id);
        assert_eq!(binding.turn_id, turn);
        assert_eq!(goal.session_id, session);
        // Retain only the original bound facts needed to review this claim;
        // do not manufacture a Goal conclusion or export the session history.
        let evidence_dir = std::path::PathBuf::from(std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR")
            .expect("private fixture evidence directory"));
        let facts = json!({"session_id":session,"turn_id":turn,"root_graph_id":graph_id,
            "program_id":program_id,"task_id":task,"claim_event":event,
            "primary_receipt":primary,"index_receipt":receipt});
        use std::io::Write;
        #[cfg(unix)] use std::os::unix::fs::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)] options.mode(0o600);
        let evidence_name = match missing { None => "claim-original-facts.json".to_string(),
            Some(case) => format!("bounded-original-facts-{case}.json") };
        let mut output = options.open(evidence_dir.join(evidence_name))
            .expect("create private claim facts");
        serde_json::to_writer(&mut output, &facts).expect("write original claim facts");
        output.flush().expect("flush original claim facts");
        output.sync_all().expect("sync original claim facts");
        let mut original_conclusion_observed = false;
        let cas_probe = std::env::var_os("COWD_TEST_ORIGINAL_CONCLUSION_CAS").is_some();
        let external_goal_probe = std::env::var_os("COWD_TEST_ORIGINAL_EXTERNAL_GOAL").is_some();
        let external_review_match = Arc::new(std::sync::Mutex::new(None::<(String, u64, Value)>));
        let external_goal_result = Arc::new(std::sync::Mutex::new(None::<Result<(String, u64, u64, String, u32), String>>));
        let mut cas_observed = false;
        let conclusion_hook_result = Arc::new(std::sync::Mutex::new(None::<Result<(String, u64, u64), String>>));
        let conclusion_attempt_result = Arc::new(std::sync::Mutex::new(None::<(bool, Option<runtime::execution_core::services::ConclusionAttemptObservation>)>));
        if native_colon_read {
            assert!(claimed_continuation.is_some(), "original worker continuation is required");
        }
        if let Some(request) = claimed_continuation.take() {
            request.reply.send(json!({"role":"assistant", "content":"The bounded task is complete.", "tool_calls":[{
                "index":0, "id":"d-original-submit", "type":"function", "function":{
                    "name":"task_submit", "arguments":json!({"task_ref":task, "artifact_refs":[], "evidence_refs":[],
                        "unresolved":[], "deliverable":{"block_index":0,"kind":"note","title":"Bounded task result"}}).to_string()
                }
            }]})).expect("send original worker submission over loopback");
            let mut reviewer_read_sent = false;
            let mut task_review_sent = false;
            let mut root_read_sent = false;
            let mut root_read_complete = false;
            let mut root_read_page = 0_u32;
            let mut root_read_call = String::new();
            let mut root_read_source: Option<(String, u64)> = None;
            let mut root_read_end = 0_u64;
            let mut root_read_total: Option<u64> = None;
            let mut external_goal_rejection_observed = false;
            let mut objective_review_sent = false;
            let mut completion_sent = false;
            let mut wait_index = 0_u32;
            let chain_result = tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    if external_goal_probe {
                        let hit = external_goal_result.lock().unwrap().clone();
                        if let Some(Ok((_, old, new, node, attempt))) = hit {
                            if c_observed_external_goal_cas(&graph_id, &node, attempt,
                                &format!("goal:goal:{graph_id}"), old, new) {
                                external_goal_rejection_observed = true;
                                break;
                            }
                        }
                    }
                    let current = runtime.agent_action_service().project(&program_id).expect("original Program projection");
                    let goal = runtime.goal_store().get(&format!("goal:{graph_id}")).unwrap().expect("original Goal");
                    if cas_probe && conclusion_attempt_result.lock().unwrap().is_some() {
                        cas_observed = true;
                        break;
                    }
                    if current.status == runtime::AgenticProgramStatus::Verified
                        && goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
                        assert_eq!(current.objective_verdict.as_ref().expect("original Program verdict").terminal_fence,
                            goal.terminal.as_ref().expect("original Goal terminal").terminal_fence);
                        let reviewer = current.tasks[&task].reviewed_by.as_deref().expect("original independent Task reviewer");
                        assert_ne!(Some(reviewer), actor["actor_id"].as_str(), "reviewer must differ from original claimant");
                        let request_revision = current.completion_request.as_ref().expect("original completion request").program_revision;
                        let program_key = format!("program-conclusion:{request_revision}");
                        let program_event = runtime.event_reader().list_stream(&format!("agentic-program:{program_id}"))
                            .unwrap().into_iter().find(|event| event.idempotency_key.as_deref() == Some(program_key.as_str()))
                            .expect("original Program conclusion event");
                        let goal_key = format!("program-conclusion:agentic-objective:{program_id}:request:{request_revision}");
                        let goal_event = runtime.event_reader().list_stream(&format!("goal:{}", goal.id))
                            .unwrap().into_iter().find(|event| event.idempotency_key.as_deref() == Some(goal_key.as_str()))
                            .expect("original Goal conclusion event");
                        assert_eq!(program_event.transaction_id, goal_event.transaction_id, "Goal and Program must share the original conclusion transaction");
                        assert_eq!(program_event.commit_cursor, goal_event.commit_cursor);
                        original_conclusion_observed = true;
                    }
                    let request = tokio::select! {
                        request = provider.next() => request,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    let is_child = c_route_owner(&services, &request, &session, &turn, &graph_id, &program_id, Some(&task))
                        .await.expect("original request owner for review chain");
                    let has_result = |call: &str| request.body["messages"].as_array().is_some_and(|messages|
                        messages.iter().any(|message| message["role"] == "tool" && message["tool_call_id"] == call));
                    let selected = current.tasks.get(&task).and_then(|task| task.artifact_refs.first())
                        .and_then(|reference| current.artifacts.get(reference).map(|artifact| (reference.clone(), artifact.content_ref.clone())));
                    let tool_reply = |id: &str, name: &str, input: Value| json!({"role":"assistant", "content":null, "tool_calls":[{
                        "index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}
                    }]});
                    let finish_original_turn = original_conclusion_observed && !is_child && has_result("d-objective-review");
                    let answer = if finish_original_turn {
                        c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                            "d-objective-review", "objective_review", Some("objective_review")).expect("original objective review outcome");
                        json!({"role":"assistant","content":"The bounded task was independently reviewed."})
                    } else if is_child && has_result("d-original-submit") {
                        json!({"role":"assistant","content":"I submitted the bounded result."})
                    } else if is_child && !reviewer_read_sent {
                        let (_, selector) = selected.as_ref().expect("original submitted artifact before independent review");
                        reviewer_read_sent = true;
                        let provider_ref = if native_colon_read {
                            format!("artifact:{}", selector.strip_prefix("artifact://")
                                .expect("original artifact selector"))
                        } else { selector.clone() };
                        tool_reply("d-review-read", "evidence_retrieve", json!({"evidence_ref":provider_ref}))
                    } else if is_child && has_result("d-review-read") && !task_review_sent {
                        c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                            "d-review-read", "evidence_retrieve", None).expect("original reviewer read outcome");
                        if native_colon_read {
                            let session_events = runtime.event_reader().list_stream(&format!("session:{session}")).unwrap();
                            let read_plan = c_original_lifecycle_plan_task(&session_events, "d-review-read", "evidence_retrieve")
                                .expect("original read lifecycle plan");
                            let step: runtime::step_snapshot::StepCallBinding =
                                serde_json::from_value(read_plan["step_binding"].clone()).unwrap();
                            let key = step.effect_idempotency_key();
                            let receipts = runtime.event_reader().list_stream(&format!("execution-effect:{key}"))
                                .unwrap().into_iter().filter(|event| event.kind == "execution.read.receipt")
                                .collect::<Vec<_>>();
                            let [primary] = receipts.as_slice() else { panic!("one original read receipt required") };
                            assert_eq!(session_events.iter().filter(|event| event.kind == "model.item_completed"
                                && event.payload["tool_call_id"] == "d-review-read").count(), 1,
                                "the provider supplied exactly one original read");
                            let parent = &primary.payload["replay"]["parent_execution"];
                            let graph = runtime.graph_state_store().load(parent["execution_id"].as_str().unwrap()).unwrap();
                            let parent_node = graph.nodes.iter().find(|node| node.id == parent["node_id"].as_str().unwrap())
                                .expect("original AgentTask node");
                            assert_eq!(parent_node.executor_kind, "agent_task");
                            let packet: harness_contract::agent::AgentTaskPacket =
                                serde_json::from_str(&parent_node.payload_ref).unwrap();
                            let run_id = step.snapshot.identity.run_ref.as_deref().expect("original delegated run");
                            assert_eq!(step.snapshot.identity.execution_id.as_deref(), Some(run_id));
                            assert_eq!(packet.run_id(), run_id);
                            assert_eq!(packet.graph_id(), graph.id);
                            assert_eq!(packet.node_id(), parent_node.id);
                            assert_eq!(packet.session_id(), session);
                            assert_eq!(packet.assignment.execution_identity.turn_id(), Some(turn.as_str()));
                            let owner = packet.agentic_binding.as_ref().expect("original Program owner");
                            assert_eq!(owner.program_id, program_id);
                            assert!(matches!(&owner.focus,
                                harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } if task_ref == &task));
                            assert_eq!(Some(u64::from(packet.attempt)), primary.payload["replay"]["parent_execution_attempt"].as_u64());
                            let events = runtime.event_reader().all_events(100_001).unwrap();
                            assert!(events.len() < 100_001, "original Runtime events may be truncated");
                            let deferrals = events.iter().filter(|event| event.kind == "execution.tool_early_deferred"
                                && event.payload["tool_call_id"] == "d-review-read"
                                && event.payload["reason"] == "early_repaired_input_requires_original_tool_batch")
                                .collect::<Vec<_>>();
                            let [deferral] = deferrals.as_slice() else { panic!("one original early deferral required") };
                            assert!(deferral.commit_cursor < primary.commit_cursor);
                            let graph_refs = deferral.refs.iter().filter(|r| r.kind == "execution_graph").collect::<Vec<_>>();
                            let node_refs = deferral.refs.iter().filter(|r| r.kind == "execution_node").collect::<Vec<_>>();
                            let [child_graph_ref] = graph_refs.as_slice() else { panic!("one original child graph required") };
                            let [model_ref] = node_refs.as_slice() else { panic!("one original model node required") };
                            let child = runtime.graph_state_store().load(&child_graph_ref.id).unwrap();
                            assert_eq!(serde_json::to_value(&child.parent_execution).unwrap(), *parent);
                            assert_eq!(child.nodes.iter().find(|node| node.id == model_ref.id)
                                .expect("original child model node").executor_kind, "inline_model");
                            let batches = child.nodes.iter().filter(|node| node.executor_kind == "tool_batch"
                                && child.edges.iter().any(|edge| edge.from == model_ref.id
                                    && edge.to == node.id && edge.kind.is_dependency())).collect::<Vec<_>>();
                            let [batch] = batches.as_slice() else { panic!("one original ToolBatch required") };
                            let payload = runtime::step_snapshot::decode_persisted_tool_batch_for_test(&batch.payload_ref).unwrap();
                            assert_eq!(payload["session_id"], session);
                            let calls = payload["calls"].as_array().expect("original ToolBatch calls");
                            let [call] = calls.as_slice() else { panic!("one original ToolBatch call required") };
                            assert_eq!(call["id"], "d-review-read");
                            assert_eq!(call["name"], "evidence_retrieve");
                            assert_eq!(call["origin"], serde_json::to_value(&step.origin).unwrap());
                            assert_eq!(payload["step_snapshots"].get(step.snapshot.identity.step_id.as_str()).unwrap(),
                                &serde_json::to_value(&*step.snapshot).unwrap());
                            let input = harness_contract::step::parse_strict_json(call["input"].as_str().unwrap()).unwrap();
                            assert_eq!(input, read_plan["normalized_input"]);
                        }
                        let (_, selector) = selected.as_ref().expect("original reviewed artifact");
                        task_review_sent = true;
                        tool_reply("d-task-review", "task_review", json!({"task_ref":task,"decision":"accept",
                            "reason":"The submitted result satisfies the bounded task.","evidence_refs":[selector]}))
                    } else if is_child && has_result("d-task-review") {
                        c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                            "d-task-review", "task_review", Some("task_review")).expect("original task review outcome");
                        json!({"role":"assistant","content":"The independent task review is complete."})
                    } else if !is_child && current.tasks.get(&task).is_none_or(|task|
                        task.status != runtime::AgenticTaskStatus::Accepted) {
                        wait_index += 1;
                        tool_reply(&format!("d-wait-{wait_index}"), "state_inspect", json!({"scope_ref":program_id,"wait_for_workers":true}))
                    } else if !is_child && !root_read_sent {
                        let (_, selector) = selected.as_ref().expect("accepted original artifact");
                        root_read_sent = true;
                        root_read_call = "d-root-read".into();
                        tool_reply(&root_read_call, "read_ref", json!({"reference":selector}))
                    } else if !is_child && root_read_sent && !root_read_complete && has_result(&root_read_call) {
                        let (reference, selector) = selected.as_ref().expect("accepted original artifact");
                        let (next, end, total, source_hash, source_bytes) = c_verified_root_artifact_page(&services, &request, &session, &turn, &graph_id,
                            &root_read_call, selector, root_read_end, root_read_total,
                            root_read_source.as_ref().map(|(hash, bytes)| (hash.as_str(), *bytes)))
                            .expect("original root artifact read and receipt");
                        root_read_source = Some((source_hash, source_bytes));
                        root_read_end = end;
                        root_read_total = Some(total);
                        match next {
                            Some(next) => {
                                root_read_page = root_read_page.checked_add(1).expect("root read page overflow");
                                root_read_call = format!("d-root-read-{root_read_page}");
                                tool_reply(&root_read_call, "read_ref", next)
                            }
                            None => {
                                root_read_complete = true;
                                if external_goal_probe {
                                    let backend = Arc::new(runtime_postgres::PostgresRuntimeEventStore::new(services.topology.postgres_executor.clone())
                                        .expect("original PG runtime backend"));
                                    let predicate_backend = Arc::clone(&backend);
                                    let predicate_match = Arc::clone(&external_review_match);
                                    let predicate_session = session.clone();
                                    let predicate_turn = turn.clone();
                                    let predicate_root = graph_id.clone();
                                    let predicate_program = program_id.clone();
                                    let goal_id = goal.id.clone();
                                    let predicate_goal = goal_id.clone();
                                    let predicate_graph_store = runtime.graph_state_store().clone();
                                    let goals = Arc::clone(runtime.goal_store());
                                    let action_result = Arc::clone(&external_goal_result);
                                    let action_match = Arc::clone(&external_review_match);
                                    let goal_stream = format!("goal:{goal_id}");
                                    runtime.fixture_arm_original_goal_read(graph_id.clone(), "d-objective-review".into(), goal_stream,
                                        move |store, graph, node, attempt| {
                                            match c_original_review_for_external_goal(store, &predicate_graph_store, &predicate_backend, graph, node, attempt,
                                                &predicate_session, &predicate_turn, &predicate_root, &predicate_program, &predicate_goal) {
                                                Ok(original) => { *predicate_match.lock().unwrap() = Some(original); true }
                                                Err(_) => false,
                                            }
                                        },
                                        move |store, _, node, attempt| {
                                            let result = action_match.lock().unwrap().take().ok_or_else(|| "original review match missing".to_string())
                                                .and_then(|original| c_append_external_goal(store, &goals, &backend, &goal_id, &original))
                                                .map(|(tx, old, new)| (tx, old, new, node.to_owned(), attempt));
                                            match &result {
                                                Ok((_, old, new, _, _)) => eprintln!("EXTERNAL_GOAL_HOOK hit=true update=committed old_head={old} new_head={new}"),
                                                Err(_) => eprintln!("EXTERNAL_GOAL_HOOK hit=true update=unknown"),
                                            }
                                            *action_result.lock().unwrap() = Some(result);
                                        });
                                }
                                if cas_probe {
                                    let goal_id = goal.id.clone();
                                    let goal_stream = format!("goal:{goal_id}");
                                    let program_stream = format!("agentic-program:{program_id}");
                                    let goals = Arc::clone(runtime.goal_store());
                                    let hook_result = Arc::clone(&conclusion_hook_result);
                                    let attempt_result = Arc::clone(&conclusion_attempt_result);
                                    runtime.fixture_arm_original_conclusion(program_id.clone(), goal_id.clone(),
                                        move |transaction| {
                                            let update = (|| -> Result<(String, u64, u64), String> {
                                                let matches: Vec<_> = transaction.expected_streams.iter().filter(|source| source.stream_id == goal_stream).collect();
                                                let [source] = matches.as_slice() else { return Err("original Goal source missing or ambiguous".into()); };
                                                if transaction.expected_streams.iter().filter(|source| source.stream_id == program_stream).count() != 1 {
                                                    return Err("original Program source missing or ambiguous".into());
                                                }
                                                let before = goals.get(&goal_id).map_err(|e| e.to_string())?.ok_or("original Goal missing")?;
                                                goals.revise(&goal_id, before.revision, before.user_sequence + 1, "concurrent user constraint", |goal| {
                                                    goal.constraints.push("concurrent user constraint".into());
                                                    vec!["constraints".into()]
                                                }).map_err(|e| e.to_string())?;
                                                Ok((transaction.transaction_id.clone(), source.expected_revision, source.expected_revision + 1))
                                            })();
                                            *hook_result.lock().unwrap() = Some(update);
                                        },
                                        move |reached, observation| { *attempt_result.lock().unwrap() = Some((reached, observation)); }
                                    ).expect("arm original conclusion owner probe");
                                }
                                let criterion = goal.user_intent_criterion_id.as_ref().expect("original Goal criterion");
                                objective_review_sent = true;
                                tool_reply("d-objective-review", "objective_review", json!({"criterion_ref":criterion,"decision":"satisfied",
                                    "result_refs":[reference],"evidence_refs":[selector],"reason":"The independent result meets the original objective."}))
                            }
                        }
                    } else if !is_child && has_result("d-objective-review") && !completion_sent {
                        c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                            "d-objective-review", "objective_review", Some("objective_review")).expect("original objective review outcome");
                        let (reference, selector) = selected.as_ref().expect("reviewed original artifact");
                        completion_sent = true;
                        tool_reply("d-completion", "objective_complete_request", json!({"result_refs":[reference],"evidence_refs":[selector],"unresolved":[],"blocked":false}))
                    } else if !is_child && has_result("d-completion") {
                        c_require_original_tool_success(&services, &request, &session, &turn, &program_id,
                            "d-completion", "objective_complete_request", Some("objective_complete_request")).expect("original completion request outcome");
                        json!({"role":"assistant","content":"The bounded task was independently reviewed."})
                    } else {
                        panic!("unexpected original provider request in review chain");
                    };
                    request.reply.send(answer).expect("send original loopback response");
                    if finish_original_turn { break; }
                }
            }).await;
            if external_goal_probe {
                let hook = external_goal_result.lock().unwrap();
                match hook.as_ref() {
                    None => eprintln!("EXTERNAL_GOAL_RESULT hit=false result=unknown"),
                    Some(Err(_)) => eprintln!("EXTERNAL_GOAL_RESULT hit=true update=unknown"),
                    Some(Ok((_, old, new, _, _))) => eprintln!("EXTERNAL_GOAL_RESULT hit=true update=committed old_head={old} new_head={new}"),
                }
                if original_conclusion_observed {
                    panic!("external Goal mutation must not be treated as a successful original conclusion");
                }
                let (original_tx, _, _, _, _) = hook.as_ref().and_then(|result| result.as_ref().ok())
                    .expect("external Goal probe must hit and commit the original Goal owner update");
                let events = runtime.event_reader().list_stream(&format!("agentic-program:{program_id}"))
                    .expect("original Program events after external Goal update");
                let original_actions: Vec<_> = events.iter().filter(|event| event.kind == "agentic.action_applied"
                    && event.transaction_id == *original_tx
                    && event.payload["envelope"]["action"]["kind"] == "objective_review").collect();
                assert_eq!(original_actions.len(), 1, "the original review must remain committed once");
                let original_key = original_actions[0].payload["envelope"]["action_id"].as_str()
                    .expect("original review action ID");
                assert_eq!(events.iter().filter(|event| event.kind == "agentic.action_applied"
                    && event.payload["envelope"]["action_id"] == original_key).count(), 1,
                    "the original review action must not be applied twice");
                let current = runtime.agent_action_service().project(&program_id).expect("original Program after external update");
                let goal = runtime.goal_store().get(&format!("goal:{graph_id}")).expect("Goal after external update")
                    .expect("original Goal after external update");
                assert_ne!(current.status, runtime::AgenticProgramStatus::Verified, "external Goal change cannot produce a verified Program");
                assert_ne!(goal.completion, harness_contract::goal::GoalCompletion::Satisfied, "external Goal change cannot produce a satisfied Goal");
                eprintln!("EXTERNAL_GOAL_RESULT original_review_count=1 program_status={:?} goal_completion={:?}",
                    current.status, goal.completion);
            }
            chain_result.expect("original review-to-conclusion deadline");
            if external_goal_probe {
                assert!(external_goal_rejection_observed, "external Goal rejection was not observed at the original node CAS");
            } else if cas_probe {
                assert!(cas_observed, "original Goal CAS probe must reach the checked commit");
            } else {
                assert!(original_conclusion_observed, "real Goal and Program conclusion must be observed");
                if native_colon_read {
                    assert!(reviewer_read_sent && task_review_sent,
                        "the original delegated read must complete before review");
                }
            }
        }
        if cas_probe {
            let (transaction_id, old, new) = conclusion_hook_result.lock().unwrap().take()
                .expect("original conclusion hook did not execute").expect("original Goal owner update failed");
            let (reached, observation) = conclusion_attempt_result.lock().unwrap().take().expect("original attempt observation missing");
            assert!(reached, "original conclusion append hook was not reached");
            let observation = observation.expect("first original checked append observation missing");
            assert_eq!(observation.transaction_id, transaction_id);
            match observation.outcome {
                runtime::execution_core::services::ConclusionAttemptOutcome::Stale { stream_id, expected, actual } => {
                    assert_eq!(stream_id, format!("goal:goal:{graph_id}")); assert_eq!(expected, old); assert_eq!(actual, new);
                }
                other => panic!("first original Goal CAS did not reject as expected: {other:?}"),
            }
            assert!(observation.original_receipt.expect("original receipt lookup failed").is_none(), "target transaction receipt already exists at observation");
            assert_eq!(observation.events.len(), 2, "original Goal/Program events required");
            let revision = transaction_id.strip_prefix(&format!("program-conclusion:{program_id}:")).and_then(|value| value.parse::<u64>().ok()).expect("original conclusion transaction identity");
            let goal_key = format!("program-conclusion:agentic-objective:{program_id}:request:{revision}");
            let program_key = format!("program-conclusion:{revision}");
            assert!(observation.events.iter().any(|event| event.stream_id == format!("goal:goal:{graph_id}") && event.idempotency_key.as_deref() == Some(goal_key.as_str())));
            assert!(observation.events.iter().any(|event| event.stream_id == format!("agentic-program:{program_id}") && event.idempotency_key.as_deref() == Some(program_key.as_str())));
            for event in observation.events { assert!(event.observed.expect("original event lookup failed").is_none(), "target event exists at observation"); }
            eprintln!("ORIGINAL_CONCLUSION_CAS reached=true stale=true goal_head={old}->{new} receipt=absent goal_event=absent program_event=absent");
        }
        // The non-conclusion variants stop after observing a claim. Close admission, request
        // cancellation through the original Session owner, and drain the same
        // turn before stopping its supervisor; do not synthesize a Goal result.
        let active = services.runtime.stop_accepting_for_recovery();
        let signalled = if original_conclusion_observed {
            false
        } else {
            let mut cancellation_handed_off = false;
            let mut cancellation_error = None;
            for _ in 0..3 {
                let current = services.session.runtime_input(&request_id).await
                    .expect("read original Session input before cancellation")
                    .expect("original Session input missing before cancellation");
                assert_eq!(current.session_id, session, "cancellation input Session changed");
                assert_eq!(current.turn_id, turn, "cancellation input turn changed");
                assert_eq!(current.input_id, input.input_id, "cancellation input identity changed");
                let application_settled = current.application_receipt.as_ref().is_none_or(|receipt| {
                    use harness_contract::input_disposition::{InputApplicationCancellationState, InputApplicationState};
                    match receipt.state {
                        InputApplicationState::Failed => receipt.cancellation.as_ref().is_none_or(|cancel|
                            cancel.state == InputApplicationCancellationState::Settled),
                        InputApplicationState::Applied => receipt.cancellation.as_ref().is_some_and(|cancel|
                            cancel.state == InputApplicationCancellationState::Settled),
                        InputApplicationState::Prepared | InputApplicationState::Materializing => false,
                    }
                });
                if matches!(current.status, session::SessionRuntimeInputStatus::Completed | session::SessionRuntimeInputStatus::Cancelled)
                    && application_settled {
                    cancellation_handed_off = true;
                    break;
                }
                match services.session.cancel_input(
                    &session,
                    harness_contract::turn::SessionInputId::from_string(input.input_id.to_string()),
                    "local claim fixture completed",
                ).await {
                    Ok(_) => { cancellation_handed_off = true; break; }
                    Err(error) if error.starts_with("stale session execution fence:") => cancellation_error = Some(error),
                    Err(error) => panic!("original Session input cancellation failed: {error}"),
                }
            }
            assert!(cancellation_handed_off, "original Session input cancellation remains unknown: {cancellation_error:?}");
            let signalled = services.session.cancel_active_execution(
                &session, &turn, &graph_id, "local claim fixture completed")
                .expect("original bound turn cancellation signal");
            runtime.cancel_execution_tree(&graph_id, "local claim fixture completed").await
                .expect("original execution tree cancellation");
            signalled
        };
        let drain = services.runtime.wait_for_active_turns(active.len(), Duration::from_secs(30)).await;
        eprintln!("claim fixture cancellation: bound_turn_signal={signalled} remaining_turns={:?} workers={:?}",
            drain.remaining_turn_ids, services.supervisor.health().workers);
        assert!(drain.remaining_turn_ids.is_empty(), "original Session turn did not drain");
        if original_conclusion_observed {
            let persisted = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let terminal = services.topology.runtime_event_store
                        .session_terminal_for_execution(&session, &graph_id, &turn)
                        .expect("read original Runtime terminal outbox");
                    if let Some(terminal) = terminal.filter(|terminal| terminal.status == "materialized") {
                        assert_eq!(terminal.session_id, session);
                        assert_eq!(terminal.execution_id.as_deref(), Some(graph_id.as_str()));
                        assert_eq!(terminal.turn_id.as_deref(), Some(turn.as_str()));
                        assert_eq!(terminal.request_id.as_deref(), Some(request_id.as_str()));
                        let payload = crate::session_runtime_bridge::load_terminal_payload(
                            &services.topology.artifact_store, &terminal).await.expect("original terminal payload");
                        assert_eq!(payload.ingress_message_id.as_deref(), Some(input.message_id.as_str()));
                        assert_eq!(payload.goal_completion, harness_contract::goal::GoalCompletion::Satisfied);
                        assert_eq!(payload.text, "The bounded task was independently reviewed.", "original terminal payload text differs");
                        let messages = services.topology.session_store.get_messages(&session, 0, 10_001)
                            .await.expect("read original Session messages");
                        assert!(messages.len() < 10_001, "original Session messages exceed assertion bound");
                        let matches: Vec<_> = messages.iter().filter(|message|
                            message.stable_message_id == terminal.message_id).collect();
                        let [message] = matches.as_slice() else { panic!("original terminal message identity missing or duplicate"); };
                        assert_eq!(message.session_id, session);
                        assert_eq!(message.role, "assistant");
                        let body: Value = serde_json::from_str(&message.content_json).expect("original terminal message JSON");
                        let blocks = body.as_array().expect("original terminal content blocks");
                        let text_blocks: Vec<&str> = blocks.iter().filter(|block| block["type"] == "text")
                            .map(|block| block["text"].as_str().expect("original terminal text block malformed"))
                            .collect();
                        assert!(!text_blocks.is_empty(), "original terminal text is missing");
                        assert_eq!(text_blocks.concat(), payload.text, "original terminal message text differs from payload");
                        for block in blocks {
                            assert_eq!(block["cowd_turn_id"], turn);
                            assert_eq!(block["cowd_turn_ingress_message_id"], input.message_id.to_string());
                            assert_eq!(block["cowd_execution_id"], graph_id);
                        }
                        break (message.sequence, terminal.commit_cursor);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }).await.expect("original Session terminal answer was not materialized");
            eprintln!("ORIGINAL_SESSION_TERMINAL materialized=true unique=true sequence={} runtime_cursor={}", persisted.0, persisted.1);
        }
        if external_goal_probe {
            let hit = external_goal_result.lock().unwrap().clone()
                .expect("external Goal hook result missing")
                .expect("external Goal original update is unknown");
            let events = runtime.event_reader().list_stream(&format!("agentic-program:{program_id}"))
                .expect("original Program events after drain");
            let original: Vec<_> = events.iter().filter(|event| event.kind == "agentic.action_applied"
                && event.transaction_id == hit.0
                && event.payload["envelope"]["action"]["kind"] == "objective_review").collect();
            let [original] = original.as_slice() else { panic!("original review action missing or ambiguous after drain"); };
            let key = original.payload["envelope"]["action_id"].as_str().expect("original review action ID");
            let calls = services.physical_calls.lock().unwrap();
            let matching: Vec<_> = calls.iter().filter(|call|
                call.tool_use_id == "d-objective-review" && call.tool_name == "objective_review").collect();
            let [call] = matching.as_slice() else {
                panic!("original objective review physical execution count was {}, expected one", matching.len());
            };
            assert_eq!(call.idempotency_key, key, "physical call must match original applied action");
            assert_eq!(call.attempt, Some(hit.4), "physical call attempt must match original ticket");
            assert!(call.parent_execution.as_ref().is_some_and(|parent|
                parent.execution_id == graph_id && parent.node_id == hit.3),
                "physical call must match original graph and node");
            eprintln!("EXTERNAL_GOAL_RESULT original_review_physical_calls=1 after_drain=true");
        }
    }).catch_unwind().await;
    if let Err(panic) = &run {
        let identity = identity.lock().unwrap_or_else(|error| error.into_inner()).clone();
        let (turn, graph) = identity.map(|(turn, graph)| (turn, Some(graph)))
            .unwrap_or_else(|| ("unknown-before-admission".into(), None));
        let failure = panic.downcast_ref::<String>().map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("non-string panic");
        let packed = services.topology.session_store.get_session_domain_events_by_kind_limited(
            &session, "context.provider_request_packed", 0, 10_001).await;
        let packed_facts = packed.as_ref().ok().map(|rows| rows.iter().filter_map(|row|
            session::SessionDomainEvent::from_session_event(row).ok().map(|event| json!({
                "sequence":event.sequence, "session_id":event.session_id,
                "execution_id":event.payload["execution_id"], "turn_id":event.payload["turn_id"],
                "request_id":event.payload["request_id"], "body_sha256":event.payload["body_sha256"],
                "step_render":event.payload["step_render"]
            }))).collect::<Vec<_>>());
        // Preserve only the original admission already returned to this fixture;
        // do not launch a blocking backend query while unwinding the failed run.
        let ingress_admission = ingress_admission.try_lock().ok().and_then(|value| value.clone());
        let active_turn = match services.runtime.active_turns.state.try_lock() {
            Ok(state) => match state.controls.get(&turn) {
                Some(control) if control.session_id == session && control.execution_id.as_deref() == graph.as_deref() => "matching_active_owner",
                Some(_) => "owner_identity_differs",
                None => "not_found",
            },
            Err(_) => "unknown_lock_busy",
        };
        let route_facts = json!({"wire":provider.wire_facts.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            "original_admission":ingress_admission, "active_turn_owner":active_turn,
            "original_input_current_status":"unknown_not_queried",
            "original_terminal_current_status":"unknown_not_queried",
            "session_packed":packed_facts, "packed_error":packed.as_ref().err().map(ToString::to_string),
            "packed_limit":10001, "packed_possibly_truncated":packed.as_ref().ok().is_some_and(|rows| rows.len() >= 10001)});
        c_export_original_failure(&runtime, &services.physical_calls, &session, &turn, graph, &provider.model,
            failure, "Gateway Session turn binds original claim control; installation unobserved on early failure",
            "Gateway Session ingress owns Goal; inspect original Goal event", None, Some(route_facts)).await;
    }
    let cleanup_identity = identity.lock().unwrap_or_else(|error| error.into_inner()).clone();
    let (cleanup_turn, cleanup_graph) = cleanup_identity.map(|(turn, graph)| (turn, Some(graph)))
        .unwrap_or_else(|| ("unknown-before-admission".into(), None));
    let cleanup_model = provider.model.clone();
    drop(runtime);
    drop(provider);
    let (progress_tx, progress_rx) = tokio::sync::oneshot::channel();
    let mut cleanup_task = tokio::spawn(async move {
        std::panic::AssertUnwindSafe(services.stop_with_original_failure_export(Some(
            (session, cleanup_turn, cleanup_graph, cleanup_model)
        ), Some(progress_tx))).catch_unwind().await
    });
    let (cleanup, first_report) = tokio::select! {
        result = &mut cleanup_task => (Some(result), None),
        signal = progress_rx => match signal {
            Ok(first_report) => (None, Some(first_report)),
            Err(_) => (Some(cleanup_task.await), None),
        },
    };
    if let Some(first_report) = first_report {
        // The original report is already available at the deadline. Preserve
        // it on the existing private tmpfs path before this observer exits;
        // the larger artifact export remains explicitly best-effort.
        let saved = first_report.and_then(|bytes| {
            let root = std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR")
                .ok_or("private fixture evidence directory missing".to_string())?;
            let path = std::path::PathBuf::from(root).join("shutdown-first-report.json");
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
            use std::io::Write;
            let mut file = options.open(&path).map_err(|error| error.to_string())?;
            file.write_all(&bytes).map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        });
        match saved {
            Ok(()) => eprintln!("original first execution report saved to private fixture evidence"),
            Err(error) => eprintln!("original first execution report unavailable: {error}"),
        }
    }
    if let Err(panic) = run { std::panic::resume_unwind(panic); }
    match cleanup {
        Some(Ok(Ok(Ok(_)))) => {},
        Some(Ok(Ok(Err(error)))) => panic!("original Gateway stop unresolved: {error}"),
        Some(Ok(Err(panic))) => std::panic::resume_unwind(panic),
        Some(Err(error)) => panic!("original Gateway stop task failed: {error}"),
        None => panic!("original Gateway stop remains unresolved at its first deadline"),
    }
}
