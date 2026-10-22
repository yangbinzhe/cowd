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
}

struct PhysicalHold {
    entered: tokio::sync::oneshot::Sender<runtime::RuntimeToolExecutionRequest>,
    released: tokio::sync::oneshot::Receiver<()>,
}

#[async_trait::async_trait]
impl runtime::RuntimeExecutionHost for RecordingExecutionHost {
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
}

struct ProviderRequest {
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
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (mut socket, peer) = connection.unwrap();
                        assert!(peer.ip().is_loopback());
                        let sender = sender.clone();
                        let routing = routing.clone();
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
                                if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..offset]);
                                    let length = headers.lines().find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse::<usize>().unwrap())
                                    }).unwrap();
                                    if bytes.len() >= offset + 4 + length {
                                        break serde_json::from_slice::<Value>(&bytes[offset + 4..offset + 4 + length]).unwrap();
                                    }
                                }
                            };
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
                            if sender.send(ProviderRequest { body, reply }).await.is_err() { return; }
                            let Ok(delta) = response.await else { return; };
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
                    name: lane.model.clone(),
                    base_url: lane.endpoint.clone(),
                    api_key: "loopback-only".into(),
                    models,
                    protocol: Some("completions".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
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
        request.owner_principal_id = Some("recovery-operator".into());
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
                    assert!(block["cowd_tool_instance_id"]
                        .as_str()
                        .is_some_and(|value| value.contains("#cowd-")));
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

async fn terminal_transaction_rollback_case(source_race: bool) {
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
        let recording_host = Arc::new(RecordingExecutionHost {
            inner: executor.clone(),
            calls: physical_calls.clone(),
            holds: physical_holds.clone(),
        });
        let config_home = root.join("home");
        let services = runtime::RuntimeServices::builder(&config_home, root.join("workspace"))
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
            lane_hits,
            delivery_hits,
            recovered_candidate,
        }
    }

    async fn stop(self) -> crate::selected_storage::DetachedTestPostgresRestartLease {
        let Self {
            topology,
            runtime,
            session,
            supervisor,
            notification_reader,
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
        runtime.stop_accepting_for_recovery();
        supervisor.shutdown().await;
        let workers = supervisor.health();
        assert!(!workers.accepting);
        assert_eq!(workers.forced_aborts, 0);
        let tasks = runtime.gateway_tasks.shutdown().await;
        assert_eq!(tasks.panicked, 0);
        assert_eq!(tasks.forced_aborts, 0);
        let execution = services.shutdown_execution().await;
        assert_eq!(execution.forced_aborts, 0);
        services.shutdown_maintenance().await;
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
        lease
    }
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
