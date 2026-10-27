//! Explicit engineering gate. The controller must own a finite local provider;
//! this never chooses a personal account, a live model, or a replacement grader.
use super::SessionActor;
use crate::live_scenario_observer::LiveScenarioObserver;
use harness_contract::live::{SessionMessageReceiptContext, SessionReceiptOutboxObservation};
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "../../../tests/interactive/src/server.rs"]
mod original_server;

fn private_record(path: &Path, value: &Value) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    out.write_all(&serde_json::to_vec_pretty(value)?)?;
    out.sync_all()?;
    Ok(())
}

// SessionActor's general observation contract also permits unknown outboxes.
// This particular Goal gate requires the original known Graph, never null or
// a value substituted from the expected execution.
fn exact_graph_context(context: &SessionMessageReceiptContext, graph: &str, turn: &str) -> bool {
    match context {
        SessionMessageReceiptContext::Available {
            session_source,
            runtime_commit_cursor,
            outbox_observation:
                SessionReceiptOutboxObservation::RuntimeOutbox {
                    execution_id: Some(actual),
                    turn_id,
                    request_id,
                    terminal_id,
                    runtime_commit_cursor: outbox_cursor,
                    ..
                },
            ..
        } => {
            actual == graph
                && turn_id == turn
                && session_source.turn_id == turn
                && request_id == &session_source.request_id
                && terminal_id == &format!("turn-terminal:{}", session_source.request_id)
                && outbox_cursor == runtime_commit_cursor
        }
        _ => false,
    }
}

fn get_owned_json(
    client: &Client,
    base: &str,
    path: &str,
    deadline: Instant,
) -> anyhow::Result<Value> {
    use std::io::Read;
    anyhow::ensure!(path.starts_with("/api/"), "owned fixed API path");
    let left = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("R6 observation deadline"))?;
    let mut response = client
        .get(format!("{base}{path}"))
        .header("x-cowd-surface-id", "harness-eval-live")
        .header("x-cowd-observer-id", "harness-eval-live")
        .header("x-cowd-requested-capabilities", "mission.observe")
        .timeout(left.min(Duration::from_secs(10)))
        .send()?;
    let status = response.status();
    let mut body = Vec::new();
    response
        .by_ref()
        .take(256 * 1024 + 1)
        .read_to_end(&mut body)?;
    anyhow::ensure!(body.len() <= 256 * 1024, "R6 API response bound");
    anyhow::ensure!(status.is_success(), "R6 API read refused status={status}");
    Ok(serde_json::from_slice(&body)?)
}

#[test]
#[ignore = "manual isolated real Gateway/finite owned loopback-provider engineering gate; not AI/N8 acceptance"]
fn admitted_background_pending_allows_real_goal_and_public_ack() -> anyhow::Result<()> {
    use original_server::tests::{
        background_broker_request, background_http_command, background_http_post, growth_test_sql,
    };
    let run_deadline = Instant::now() + Duration::from_secs(180);
    // All values belong to this pre-registered run. No external-provider fallbacks.
    let postgres = std::env::var("COWD_TEST_POSTGRES_URL")?;
    let image = PathBuf::from(
        std::env::var_os("COWD_BIN")
            .ok_or_else(|| anyhow::anyhow!("NotStarted: frozen CLI required"))?,
    )
    .canonicalize()?;
    let root =
        PathBuf::from(std::env::var_os("COWD_R6_OWNED_ROOT").ok_or_else(|| {
            anyhow::anyhow!("NotStarted: fresh registered private root required")
        })?);
    // The endpoint is owned here, never supplied by an account/config fallback.
    let model = "r6-finite-local".to_owned();
    let mut provider = FiniteProvider::start(&root, &model)?;
    let token = {
        use std::io::Read;
        let mut value = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut value)?;
        value.iter().map(|v| format!("{v:02x}")).collect::<String>()
    };
    let mut server = original_server::ServerProcess::start_with_controlled_provider(
        postgres,
        model.clone(),
        image,
        root.clone(),
        &token,
        &provider.endpoint,
        prepare_recovery_definitions,
    )?;
    let base = server.base_url().to_owned();
    let result = (|| -> anyhow::Result<Value> {
        let before =
            background_broker_request(&server, &token, json!({"kind":"get_human_entitlements"}))?;
        let before = &before["entitlement"];
        let preview = background_broker_request(
            &server,
            &token,
            json!({"kind":"preview_human_entitlements","core_profile_id":"core_manager","app_profiles":before["app_profiles"]}),
        )?;
        let applied = background_broker_request(
            &server,
            &token,
            json!({"kind":"set_human_entitlements","core_profile_id":"core_manager","app_profiles":before["app_profiles"],"expected_credential_epoch":before["credential_epoch"],"expected_profile_revision":before["profile_revision"],"confirmation_digest":preview["confirmation_digest"]}),
        )?;
        anyhow::ensure!(
            applied["kind"] == "human_entitlements",
            "original human profile CAS failed"
        );
        for domain in ["evolution", "growth"] {
            let (status, value) = background_http_command(&server, &token, domain, "status")?;
            anyhow::ensure!(
                status == 200 && value["data"]["desired"][domain] == true,
                "R6 specialty must actually be on/on"
            );
        }
        // Same audited target cut as R5; new exclusively owned database only.
        growth_test_sql(
            &root,
            include_str!("../../../tests/interactive/src/growth_receipt_fault.sql"),
        )?;
        let (status, risk) = background_http_post(
            &server,
            &token,
            "/api/approval/risk-receipt",
            &json!({"tool_name":"edit","input":{"action":"write","path":"r6-risk-not-executed.txt","content":"risk assessment only"},"session_id":"r326-growth-A"}),
        )?;
        private_record(
            &root.join("r6-source-risk.json"),
            &json!({"status":status,"body":risk}),
        )?;
        anyhow::ensure!(
            status == 200 && risk["growth"]["status"] == "queued",
            "R6 original source was not admitted"
        );
        let snapshot_sql = r#"SELECT jsonb_build_object('memory',(SELECT count(*) FROM memory_entries WHERE session_id='r326-growth-A' AND source_agent='growth-service'),'receipts',(SELECT count(*) FROM growth_promotions WHERE event_id IN(SELECT event_id FROM growth_events WHERE session_id='r326-growth-A') AND target='memory.entry'),'pending',(SELECT payload ? 'pending_page' FROM runtime_projection_checkpoints WHERE projection_id='projector:growth:v1'),'hits',(SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM r326_receipt_fault_hits))::text;"#;
        let snapshot = || -> anyhow::Result<Value> {
            Ok(serde_json::from_str(&growth_test_sql(
                &root,
                snapshot_sql,
            )?)?)
        };
        let cut_deadline = (Instant::now() + Duration::from_secs(35)).min(run_deadline);
        let pending = loop {
            let value = snapshot()?;
            if value["memory"] == 2
                && value["receipts"] == 0
                && value["pending"] == true
                && value["hits"].as_u64().unwrap_or(0) > 0
            {
                break value;
            }
            anyhow::ensure!(
                Instant::now() < cut_deadline,
                "background receipt cut was not hit"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        private_record(&root.join("r6-pending-before-foreground.json"), &pending)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse()?,
        );
        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()?;
        let deadline = run_deadline;
        let mut actor =
            SessionActor::create(&client, &base, Some(&model), "harness-eval-live", deadline)
                .map_err(anyhow::Error::msg)?;
        let session_id = actor.session_id().to_owned();
        let foreground = (|| -> anyhow::Result<Value> {
            // Independent original task/physical oracle; never inferred from the response.
            let accepted=actor.post_mutation(&format!("/api/sessions/{session_id}/messages"),json!({"content":"Write recovery-effect.txt containing recovery-effect-once, delegate one public Task and independently inspect its physical result and report before satisfying the user_intent criterion. Deliver that reviewed report.","idempotency_key":"r6-foreground-original"})).map_err(anyhow::Error::msg)?;
            private_record(&root.join("r6-foreground-admission.json"), &accepted)?;
            let graph = accepted
                .pointer("/execution/graph_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing original Graph admission"))?;
            let turn = accepted
                .pointer("/execution/turn_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing original turn admission"))?;
            provider.bind(ProviderBinding {
                client: client.clone(),
                base: base.clone(),
                session: session_id.clone(),
                turn: turn.to_owned(),
                graph: graph.to_owned(),
                deadline,
            })?;
            let mut observer = LiveScenarioObserver::default();
            let started = Instant::now();
            let message_id = loop {
                provider.ensure_healthy()?;
                anyhow::ensure!(Instant::now() < deadline, "foreground terminal deadline");
                let path = observer.message_path(&session_id);
                let response =
                    get_owned_json(&client, &base, &path, deadline).map_err(|e| e.to_string());
                observer
                    .observe_message_page(&path, &response, started.elapsed().as_millis() as u64)
                    .map_err(anyhow::Error::msg)?;
                if let Some(id) = actor
                    .select_terminal_message(&observer.assistant_message_ids(), turn, graph)
                    .map_err(anyhow::Error::msg)?
                {
                    let context = actor
                        .terminal_receipt_context(&id)
                        .map_err(anyhow::Error::msg)?;
                    // Extra strict witness, without changing general receipt policy.
                    if exact_graph_context(&context, graph, turn) {
                        break id;
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            let delivered = actor
                .confirm_terminal_delivery(&message_id, turn, graph)
                .map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                delivered.observation["execution_id"] == graph,
                "unknown outbox cannot prove the admitted Graph"
            );
            anyhow::ensure!(
                delivered.observation["fresh_context_confirmed"] == true,
                "missing ACK reread"
            );
            anyhow::ensure!(
                std::fs::read(root.join("workspace/recovery-effect.txt"))?
                    == b"recovery-effect-once\n",
                "independent physical artifact differs"
            );
            let program = get_owned_json(
                &client,
                &base,
                &format!(
                    "/api/runtime/agentic/programs/root?session_id={session_id}&turn_id={turn}"
                ),
                deadline,
            )?;
            let program: runtime::AgenticProgramProjection = serde_json::from_value(program)?;
            anyhow::ensure!(
                program.session_id == session_id
                    && program.turn_id == turn
                    && program.root_execution_id.as_deref() == Some(graph),
                "foreign Program"
            );
            anyhow::ensure!(
                program.status == runtime::AgenticProgramStatus::Verified,
                "real Goal-backed Program not Verified"
            );
            let verdict = program
                .objective_verdict
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing original Goal terminal verdict"))?;
            anyhow::ensure!(
                !verdict.goal_id.is_empty()
                    && !verdict.terminal_fence.is_empty()
                    && verdict.authority_revision > 0,
                "missing Goal authority/fence"
            );
            anyhow::ensure!(
                serde_json::to_value(verdict.kind)? == json!("satisfied"),
                "Goal verdict not Satisfied"
            );
            let still_pending = snapshot()?;
            anyhow::ensure!(
                still_pending["pending"] == true && still_pending["receipts"] == 0,
                "background unexpectedly succeeded; coexistence fault not held"
            );
            Ok(
                json!({"session_id":session_id,"graph_id":graph,"turn_id":turn,"program":program,"terminal_message_id":message_id,"delivery":delivered.observation,"blocks":delivered.blocks,"background_pending":still_pending,"cancel_recovery_proven":false,"AI_proven":false}),
            )
        })();
        let detach = actor.finish();
        let value = foreground?;
        detach.map_err(anyhow::Error::msg)?;
        Ok(value)
    })();
    // The primary original is committed before cleanup, even on test Err.
    private_record(
        &root.join("r6-before-cleanup.json"),
        &match &result {
            Ok(value) => value.clone(),
            Err(error) => {
                json!({"status":"failed","first_error":error.to_string(),"cancel_recovery_proven":false})
            }
        },
    )?;
    let close = server.close();
    let provider_close = provider.close();
    let value = result?;
    close?;
    provider_close?;
    private_record(&root.join("r6-goal-ack-result.json"), &value)?;
    Ok(())
}

// Only fixture setup: the same immutable Definition store/release owner used by
// the original Gateway sequence. No runtime projection or grant is fabricated.
fn prepare_recovery_definitions(
    config: &Path,
    workspace: &Path,
    model: &str,
) -> anyhow::Result<()> {
    use harness_contract::agent::*;
    use runtime::agent::definition::{
        AgentDefinitionResolver, AgentDefinitionStore, RegisteredAgentDefinitionLayout,
    };
    use sha2::{Digest, Sha256};
    let store = AgentDefinitionStore::new(RegisteredAgentDefinitionLayout::new(
        config.join("runtime/builtin-definitions"),
        config.join("definitions"),
        workspace,
    )?);
    for (name, marker, capabilities) in [
        (
            "producer",
            "R6_PRODUCER_LANE",
            vec![AgentCapability::Read, AgentCapability::Write],
        ),
        ("reviewer", "R6_REVIEWER_LANE", vec![AgentCapability::Read]),
    ] {
        let markdown = format!(
            "# R6 {name}\n\nPerform only the claimed bounded Task and acquire its real evidence.\n{marker}\n"
        );
        let manifest = AgentDefinitionManifest {
            api_version: "cowd.agent/v1".into(),
            definition_id: AgentDefinitionId::new(
                DefinitionScope::Workspace,
                format!("recovery/{name}"),
            )?,
            revision: 1,
            name: format!("R6 {name}"),
            description: "Finite deterministic engineering fixture".into(),
            lifecycle: RevisionLifecycle::Published,
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "default".into(),
                allowed_models: vec![model.into()],
                fallback_allowed: false,
            },
            cognitive_policy: AgentCognitivePolicy {
                context_profile: "default".into(),
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
            },
            capability_contract: AgentCapabilityContract {
                capability_ceiling: capabilities.clone(),
                skill_refs: vec![],
                approval_required_for: capabilities
                    .contains(&AgentCapability::Write)
                    .then_some(AgentCapability::Write)
                    .into_iter()
                    .collect(),
            },
            output_contract: AgentOutputContract::reviewable(),
            evaluation: AgentEvaluationContract::single_release_gate("r6-engineering", "evidence"),
            instructions_digest: format!("{:x}", Sha256::digest(markdown.as_bytes())),
        };
        let stored = store.store_revision(manifest, &markdown)?;
        store.record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: format!("isolated-r6-fixture:{name}"),
            },
            content_digest: stored.revision.content_digest.clone(),
        })?;
        let resolved = AgentDefinitionResolver::new(&store).resolve(
            &stored.revision.revision_ref.definition_id,
            RevisionSelector::LatestApprovedStable,
        )?;
        anyhow::ensure!(
            resolved.revision == stored.revision && resolved.agent_markdown == markdown,
            "Definition readback differs"
        );
    }
    Ok(())
}

#[derive(Clone)]
struct ProviderBinding {
    client: Client,
    base: String,
    session: String,
    turn: String,
    graph: String,
    deadline: Instant,
}

struct ScriptRequest {
    body: Value,
    reply: std::sync::mpsc::SyncSender<Result<Value, String>>,
}

// One case-specific bounded local service. Its threads, sockets, request count
// and stop token all have this single test owner; no scheduler or model adapter.
struct FiniteProvider {
    endpoint: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    binding: std::sync::Arc<std::sync::Mutex<Option<ProviderBinding>>>,
    error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl FiniteProvider {
    fn start(root: &Path, model: &str) -> anyhow::Result<Self> {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
            mpsc,
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}/v1", listener.local_addr()?);
        let stop = Arc::new(AtomicBool::new(false));
        let binding = Arc::new(Mutex::new(None::<ProviderBinding>));
        let error = Arc::new(Mutex::new(None::<String>));
        let (send, recv) = mpsc::sync_channel::<ScriptRequest>(8);
        let service_stop = stop.clone();
        let service_error = error.clone();
        let owned_model = model.to_owned();
        let archive = root.to_path_buf();
        let acceptor = std::thread::spawn(move || {
            let mut workers: Vec<std::thread::JoinHandle<()>> = vec![];
            let mut count = 0;
            while !service_stop.load(Ordering::Acquire) {
                // Reap completed connection owners before allowing another.
                let mut i = 0;
                while i < workers.len() {
                    if workers[i].is_finished() {
                        if workers.swap_remove(i).join().is_err() {
                            *service_error.lock().unwrap() =
                                Some("provider connection panicked".into());
                        }
                    } else {
                        i += 1;
                    }
                }
                match listener.accept() {
                    Ok((socket, peer)) => {
                        count += 1;
                        if !peer.ip().is_loopback() || count > 64 || workers.len() >= 8 {
                            *service_error.lock().unwrap() =
                                Some("finite provider request/connection bound".into());
                            break;
                        }
                        let tx = send.clone();
                        let st = service_stop.clone();
                        let er = service_error.clone();
                        let model = owned_model.clone();
                        let path = archive.clone();
                        workers.push(std::thread::spawn(move || {
                            if let Err(e) =
                                serve_script_request(socket, &model, &path, count, &tx, &st)
                            {
                                *er.lock().unwrap() = Some(e.to_string());
                            }
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(e) => {
                        *service_error.lock().unwrap() = Some(e.to_string());
                        break;
                    }
                }
            }
            service_stop.store(true, Ordering::Release);
            for worker in workers {
                if worker.join().is_err() {
                    *service_error.lock().unwrap() = Some("provider connection panicked".into());
                }
            }
        });
        let script_stop = stop.clone();
        let script_binding = binding.clone();
        let script_error = error.clone();
        let script = std::thread::spawn(move || {
            let mut state = RecoveryReplies::default();
            let mut pending = Vec::<ScriptRequest>::new();
            while !script_stop.load(Ordering::Acquire) {
                match recv.recv_timeout(Duration::from_millis(20)) {
                    Ok(request) => pending.push(request),
                    Err(mpsc::RecvTimeoutError::Timeout) => (),
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                let mut i = 0;
                while i < pending.len() {
                    let current = script_binding.lock().unwrap().clone();
                    let reply = state.reply(&pending[i].body, current.as_ref());
                    match reply {
                        Ok(None) => i += 1,
                        Ok(Some(delta)) => {
                            let request = pending.remove(i);
                            let _ = request.reply.send(Ok(delta));
                        }
                        Err(e) => {
                            let reason = e.to_string();
                            let request = pending.remove(i);
                            let _ = request.reply.send(Err(reason.clone()));
                            *script_error.lock().unwrap() = Some(reason);
                            script_stop.store(true, Ordering::Release);
                            break;
                        }
                    }
                }
            }
        });
        Ok(Self {
            endpoint,
            stop,
            binding,
            error,
            threads: vec![acceptor, script],
        })
    }

    fn bind(&mut self, value: ProviderBinding) -> anyhow::Result<()> {
        let mut binding = self.binding.lock().unwrap();
        anyhow::ensure!(
            binding.is_none(),
            "provider cannot change admitted identity"
        );
        *binding = Some(value);
        Ok(())
    }
    fn ensure_healthy(&self) -> anyhow::Result<()> {
        if let Some(error) = self.error.lock().unwrap().as_ref() {
            anyhow::bail!("finite provider: {error}");
        }
        Ok(())
    }
    fn close(&mut self) -> anyhow::Result<()> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        for thread in self.threads.drain(..) {
            anyhow::ensure!(thread.join().is_ok(), "finite provider owner panicked");
        }
        self.ensure_healthy()
    }
}
impl Drop for FiniteProvider {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn serve_script_request(
    mut socket: std::net::TcpStream,
    model: &str,
    root: &Path,
    ordinal: usize,
    send: &std::sync::mpsc::SyncSender<ScriptRequest>,
    stop: &std::sync::atomic::AtomicBool,
) -> anyhow::Result<()> {
    use std::io::{Read, Write};
    use std::sync::{atomic::Ordering, mpsc};
    socket.set_read_timeout(Some(Duration::from_millis(200)))?;
    socket.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut bytes = vec![];
    let read_deadline = Instant::now() + Duration::from_secs(3);
    let body = loop {
        anyhow::ensure!(
            Instant::now() < read_deadline,
            "provider request read deadline"
        );
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut buffer = [0; 8192];
        match socket.read(&mut buffer) {
            Ok(0) => anyhow::bail!("provider request EOF before complete body"),
            Ok(n) => bytes.extend_from_slice(&buffer[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        anyhow::ensure!(bytes.len() <= 16 * 1024 * 1024, "provider input bound");
        if let Some(split) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..split])?;
            anyhow::ensure!(
                header.lines().next() == Some("POST /v1/chat/completions HTTP/1.1"),
                "unexpected provider route"
            );
            let lengths = header
                .lines()
                .filter_map(|v| v.split_once(':'))
                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.trim().parse::<usize>())
                .collect::<Result<Vec<_>, _>>()?;
            anyhow::ensure!(
                lengths.len() == 1 && lengths[0] <= 16 * 1024 * 1024,
                "provider length contract"
            );
            if bytes.len() >= split + 4 + lengths[0] {
                anyhow::ensure!(
                    bytes.len() == split + 4 + lengths[0],
                    "provider trailing input"
                );
                break serde_json::from_slice::<Value>(&bytes[split + 4..])?;
            }
        }
    };
    anyhow::ensure!(body["model"] == model, "foreign provider model");
    let stream = body["stream"]
        .as_bool()
        .ok_or_else(|| anyhow::anyhow!("explicit stream required"))?;
    let output = body
        .get("max_completion_tokens")
        .or_else(|| body.get("max_tokens"))
        .and_then(Value::as_u64);
    anyhow::ensure!(
        output.is_some_and(|n| (1..=64000).contains(&n)),
        "bounded output required"
    );
    private_record(
        &root.join(format!("r6-provider-request-{ordinal}.json")),
        &body,
    )?;
    let (reply, response) = mpsc::sync_channel(1);
    send.try_send(ScriptRequest { body, reply })
        .map_err(|_| anyhow::anyhow!("finite provider queue bound"))?;
    let deadline = Instant::now() + Duration::from_secs(180);
    let delta = loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        anyhow::ensure!(Instant::now() < deadline, "provider reply deadline");
        match response.recv_timeout(Duration::from_millis(20)) {
            Ok(reply) => break reply.map_err(anyhow::Error::msg)?,
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
    };
    private_record(
        &root.join(format!("r6-provider-reply-{ordinal}.json")),
        &json!({"delta":delta,"synthetic_usage":true}),
    )?;
    let finish = if delta.get("tool_calls").is_some() {
        "tool_calls"
    } else {
        "stop"
    };
    let usage = json!({"prompt_tokens":13,"completion_tokens":7,"total_tokens":20});
    let (content_type, wire) = if stream {
        let chunk = |delta: Value, finish: Value| json!({"id":format!("r6-{ordinal}"),"object":"chat.completion.chunk","model":model,"choices":[{"index":0,"delta":delta,"finish_reason":finish}],"usage":usage});
        (
            "text/event-stream",
            format!(
                "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                chunk(delta, Value::Null),
                chunk(json!({}), json!(finish))
            ),
        )
    } else {
        anyhow::ensure!(
            delta.get("tool_calls").is_none(),
            "control cannot execute tools"
        );
        ("application/json",json!({"id":format!("r6-{ordinal}"),"object":"chat.completion","model":model,"choices":[{"index":0,"message":delta,"finish_reason":"stop"}],"usage":usage}).to_string())
    };
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{wire}",wire.len()).as_bytes())?;
    Ok(())
}

#[derive(Default)]
struct RecoveryReplies {
    router_used: bool,
    root: usize,
    producer: usize,
    reviewer: usize,
    final_replies: usize,
}

fn scripted_tool(id: &str, name: &str, input: Value, text: Option<&str>) -> Value {
    json!({"role":"assistant","content":text,"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}}]})
}

fn previous_result(body: &Value, id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        body["messages"].as_array().is_some_and(|messages| messages
            .iter()
            .any(|m| m["role"] == "tool" && m["tool_call_id"] == id)),
        "script continuation lacks actual tool result for {id}"
    );
    Ok(())
}

#[test]
fn finite_provider_owns_control_rpc_and_rejects_a_foreign_model() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let mut provider = FiniteProvider::start(root.path(), "r6-finite-local")?;
    let client = Client::builder().timeout(Duration::from_secs(3)).build()?;
    // Literal fresh-route contract, independent of the decoder under test.
    let body = json!({"model":"r6-finite-local","stream":false,"max_tokens":512,
        "messages":[{"role":"system","content":"Route one user input to durable Tasks. Return one strict JSON object only."},
        {"role":"user","content":json!({"input":"Write the bounded file and independently verify it.","mission_hint":null,"candidates":[],
            "schema":{"action":"continue | create_compound | create_root","task_id":"string or null","additional_objectives":[],"reason":"string"}}).to_string()}]});
    let response: Value = client
        .post(format!("{}/chat/completions", provider.endpoint))
        .json(&body)
        .send()?
        .error_for_status()?
        .json()?;
    let answer: Value = serde_json::from_str(
        response["choices"][0]["message"]["content"]
            .as_str()
            .unwrap(),
    )?;
    assert_eq!(answer["action"], "create_root");
    assert!(answer["task_id"].is_null());
    provider.ensure_healthy()?;
    let mut wrong = body;
    wrong["model"] = json!("unregistered-external-account");
    assert!(
        client
            .post(format!("{}/chat/completions", provider.endpoint))
            .json(&wrong)
            .send()
            .is_err()
    );
    assert!(provider.close().is_err());
    assert!(provider.threads.is_empty());
    Ok(())
}

#[test]
fn controlled_definitions_are_scoped_released_and_do_not_expand_reviewer() -> anyhow::Result<()> {
    use harness_contract::agent::*;
    use runtime::agent::definition::{
        AgentDefinitionResolver, AgentDefinitionStore, RegisteredAgentDefinitionLayout,
    };
    let root = tempfile::tempdir()?;
    let config = root.path().join("config");
    let workspace = root.path().join("workspace");
    prepare_recovery_definitions(&config, &workspace, "r6-finite-local")?;
    let store = AgentDefinitionStore::new(RegisteredAgentDefinitionLayout::new(
        config.join("runtime/builtin-definitions"),
        config.join("definitions"),
        &workspace,
    )?);
    for (name, expected) in [
        (
            "producer",
            vec![AgentCapability::Read, AgentCapability::Write],
        ),
        ("reviewer", vec![AgentCapability::Read]),
    ] {
        let id = AgentDefinitionId::new(DefinitionScope::Workspace, format!("recovery/{name}"))?;
        let resolved = AgentDefinitionResolver::new(&store)
            .resolve(&id, RevisionSelector::LatestApprovedStable)?;
        assert_eq!(
            resolved
                .revision
                .manifest
                .capability_contract
                .capability_ceiling,
            expected
        );
        assert_eq!(
            resolved.revision.manifest.model_policy.allowed_models,
            vec!["r6-finite-local"]
        );
        assert!(!resolved.revision.manifest.model_policy.fallback_allowed);
        assert_eq!(resolved.revision.revision_ref.definition_id, id);
        // The same local name in a different scope gains no approval/alias.
        let foreign = AgentDefinitionId::new(DefinitionScope::User, format!("recovery/{name}"))?;
        assert!(
            AgentDefinitionResolver::new(&store)
                .resolve(&foreign, RevisionSelector::LatestApprovedStable)
                .is_err()
        );
    }
    Ok(())
}

impl RecoveryReplies {
    fn reply(
        &mut self,
        body: &Value,
        binding: Option<&ProviderBinding>,
    ) -> anyhow::Result<Option<Value>> {
        if let Some(control) = r6_task_route_control_reply(body).map_err(anyhow::Error::msg)? {
            anyhow::ensure!(!self.router_used, "repeated fresh Task control");
            self.router_used = true;
            return Ok(Some(control));
        }
        let Some(binding) = binding else {
            return Ok(None);
        };
        anyhow::ensure!(
            Instant::now() < binding.deadline,
            "R6 script absolute foreground deadline"
        );
        let messages = body["messages"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing messages"))?;
        // Only the Definition's real system instructions choose a role. A
        // quoted marker in a user message/tool result cannot choose authority.
        let markers = ["R6_PRODUCER_LANE", "R6_REVIEWER_LANE"].map(|marker| {
            messages
                .iter()
                .filter(|m| m["role"] == "system")
                .filter_map(|m| m["content"].as_str())
                .any(|content| content.lines().any(|line| line == marker))
        });
        anyhow::ensure!(!(markers[0] && markers[1]), "ambiguous Definition lane");
        if !markers[0] && !markers[1] && self.root == 0 {
            self.root = 1;
            return Ok(Some(scripted_tool(
                "recovery-team",
                "team_create",
                json!({
                    "name":"Recovery production team", "mission":"Produce and independently verify the requested bounded file"
                }),
                None,
            )));
        }
        let program_value = get_owned_json(
            &binding.client,
            &binding.base,
            &format!(
                "/api/runtime/agentic/programs/root?session_id={}&turn_id={}",
                binding.session, binding.turn
            ),
            binding.deadline,
        )?;
        let p: runtime::AgenticProgramProjection = serde_json::from_value(program_value)?;
        anyhow::ensure!(
            p.session_id == binding.session
                && p.turn_id == binding.turn
                && p.root_execution_id.as_deref() == Some(binding.graph.as_str()),
            "foreign Program in finite reply driver"
        );
        let production_team = p
            .teams
            .iter()
            .find(|(_, team)| team.name == "Recovery production team")
            .map(|(id, _)| id.clone());
        let review_team = p
            .teams
            .iter()
            .find(|(_, team)| team.name == "Independent criterion verification")
            .map(|(id, _)| id.clone());
        let production_task = p
            .tasks
            .iter()
            .find(|(_, task)| task.title == "Write bounded recovery result");
        let control_task = p
            .tasks
            .iter()
            .find(|(_, task)| task.title == "Independent criterion verification");
        let artifact = production_task
            .and_then(|(_, task)| task.artifact_refs.first())
            .and_then(|reference| {
                p.artifacts
                    .get(reference)
                    .map(|artifact| (reference.clone(), artifact.content_ref.clone()))
            });
        let delta = if markers[0] {
            let Some((task, _)) = production_task else {
                return Ok(None);
            };
            let value = match self.producer {
                0 => scripted_tool(
                    "recovery-claim",
                    "task_claim",
                    json!({"task_ref":task,"reason":"Accept exact bounded Task"}),
                    None,
                ),
                1 => {
                    previous_result(body, "recovery-claim")?;
                    scripted_tool(
                        "recovery-write",
                        "edit",
                        json!({"action":"write","path":"recovery-effect.txt","content":"recovery-effect-once\n"}),
                        None,
                    )
                }
                2 => {
                    previous_result(body, "recovery-write")?;
                    scripted_tool(
                        "recovery-producer-read",
                        "read_file",
                        json!({"path":"recovery-effect.txt","complete":true}),
                        None,
                    )
                }
                3 => {
                    previous_result(body, "recovery-producer-read")?;
                    scripted_tool(
                        "recovery-submit",
                        "task_submit",
                        json!({"task_ref":task,"deliverable":{"block_index":0,"kind":"report","title":"Verified physical recovery result"}}),
                        Some(
                            "The actual recovery-effect.txt was written once with recovery-effect-once and read back completely. This report records that bounded physical result; an independent verifier must inspect both it and the file.",
                        ),
                    )
                }
                _ => anyhow::bail!("producer continued after original Submitted exit"),
            };
            self.producer += 1;
            value
        } else if markers[1] {
            let Some((task, production)) = production_task else {
                return Ok(None);
            };
            let Some((result, content)) = artifact else {
                return Ok(None);
            };
            let value = match self.reviewer {
                0 => {
                    if production.status != runtime::AgenticTaskStatus::Submitted {
                        return Ok(None);
                    }
                    scripted_tool(
                        "recovery-review-result",
                        "evidence_retrieve",
                        json!({"evidence_ref":content}),
                        None,
                    )
                }
                1 => {
                    previous_result(body, "recovery-review-result")?;
                    scripted_tool(
                        "recovery-review-file",
                        "read_file",
                        json!({"path":"recovery-effect.txt","complete":true}),
                        None,
                    )
                }
                2 => {
                    previous_result(body, "recovery-review-file")?;
                    scripted_tool(
                        "recovery-task-review",
                        "task_review",
                        json!({"task_ref":task,"decision":"accept","reason":"I independently read the immutable report and actual file","evidence_refs":[content]}),
                        None,
                    )
                }
                3 => {
                    let Some((control, _)) = control_task else {
                        return Ok(None);
                    };
                    anyhow::ensure!(
                        production.status == runtime::AgenticTaskStatus::Accepted
                            && production.reviewed_by.is_some()
                            && production.reviewed_by != production.claimant,
                        "review not independently accepted"
                    );
                    scripted_tool(
                        "recovery-goal-claim",
                        "task_claim",
                        json!({"task_ref":control}),
                        None,
                    )
                }
                4 => {
                    previous_result(body, "recovery-goal-claim")?;
                    scripted_tool(
                        "recovery-goal-read",
                        "evidence_retrieve",
                        json!({"evidence_ref":content}),
                        None,
                    )
                }
                5 => {
                    previous_result(body, "recovery-goal-read")?;
                    scripted_tool(
                        "recovery-goal-file",
                        "read_file",
                        json!({"path":"recovery-effect.txt","complete":true}),
                        None,
                    )
                }
                6 => {
                    previous_result(body, "recovery-goal-file")?;
                    scripted_tool(
                        "recovery-objective-review",
                        "objective_review",
                        json!({"criterion_ref":"user_intent","decision":"satisfied","result_refs":[result],"evidence_refs":[content],"reason":"The required physical file and submitted report were independently inspected"}),
                        None,
                    )
                }
                7 => {
                    previous_result(body, "recovery-objective-review")?;
                    json!({"role":"assistant","content":"The physical result and authored report are independently verified for user_intent."})
                }
                _ => anyhow::bail!("unexpected independent reviewer continuation"),
            };
            self.reviewer += 1;
            value
        } else {
            let Some(team) = production_team else {
                return Ok(None);
            };
            let value = match self.root {
                1 => {
                    previous_result(body, "recovery-team")?;
                    scripted_tool(
                        "recovery-producer",
                        "agent_invite",
                        json!({"team_ref":team,"role":"File producer","mission":"Produce exactly the requested file and report","required_capabilities":["read","write"],"definition_ref":"workspace/recovery/producer"}),
                        None,
                    )
                }
                2 => {
                    previous_result(body, "recovery-producer")?;
                    scripted_tool(
                        "recovery-reviewer",
                        "agent_invite",
                        json!({"team_ref":team,"role":"Independent verifier","mission":"Read the actual file and report, accept and review user_intent","required_capabilities":["read"],"definition_ref":"workspace/recovery/reviewer"}),
                        None,
                    )
                }
                3 => {
                    previous_result(body, "recovery-reviewer")?;
                    scripted_tool(
                        "recovery-task",
                        "task_publish",
                        json!({"team_ref":team,"title":"Write bounded recovery result","objective":"Write recovery-effect.txt containing recovery-effect-once and submit a report","acceptance":"The exact file and report must be independently read","required_capabilities":["write","read"]}),
                        None,
                    )
                }
                4 => {
                    previous_result(body, "recovery-task")?;
                    scripted_tool(
                        "recovery-durable-wait",
                        "state_inspect",
                        json!({"scope_ref":p.program_id,"wait_for_workers":true}),
                        None,
                    )
                }
                5 => {
                    let Some((_, task)) = production_task else {
                        return Ok(None);
                    };
                    if task.status != runtime::AgenticTaskStatus::Accepted {
                        return Ok(None);
                    }
                    anyhow::ensure!(
                        task.reviewed_by.is_some() && task.reviewed_by != task.claimant,
                        "review must be independent"
                    );
                    scripted_tool(
                        "recovery-review-team",
                        "team_create",
                        json!({"name":"Independent criterion verification","mission":"Settle only the original user_intent criterion"}),
                        None,
                    )
                }
                6 => {
                    previous_result(body, "recovery-review-team")?;
                    let Some(review_team) = review_team else {
                        return Ok(None);
                    };
                    let reviewer = production_task
                        .and_then(|(_, t)| t.reviewed_by.clone())
                        .ok_or_else(|| anyhow::anyhow!("missing actual reviewer"))?;
                    scripted_tool(
                        "recovery-review-membership",
                        "agent_invite",
                        json!({"team_ref":review_team,"role":"Independent verifier","mission":"Read the accepted result and settle user_intent","required_capabilities":["read"],"existing_agent_ref":reviewer}),
                        None,
                    )
                }
                7 => {
                    previous_result(body, "recovery-review-membership")?;
                    let Some(team) = review_team else {
                        return Ok(None);
                    };
                    let Some((task, _)) = production_task else {
                        return Ok(None);
                    };
                    scripted_tool(
                        "recovery-review-control",
                        "task_publish",
                        json!({"team_ref":team,"title":"Independent criterion verification","objective":"Claim, independently read the accepted report and physical file, then objective_review user_intent","acceptance":"Record the actual independent criterion verdict without creating another deliverable","required_capabilities":["read"],"purpose":"exploration","depends_on":[task]}),
                        None,
                    )
                }
                8 => {
                    if self.reviewer < 8 {
                        return Ok(None);
                    }
                    let Some((control, _)) = control_task else {
                        return Ok(None);
                    };
                    let Some((_, content)) = artifact else {
                        return Ok(None);
                    };
                    scripted_tool(
                        "recovery-review-retire",
                        "task_withdraw",
                        json!({"task_ref":control,"reason_ref":content,"evidence_refs":[content]}),
                        None,
                    )
                }
                9 => {
                    let Some((_, control)) = control_task else {
                        return Ok(None);
                    };
                    if control.status != runtime::AgenticTaskStatus::Withdrawn
                        || !control.active_attempts.is_empty()
                    {
                        return Ok(None);
                    }
                    if p.status == runtime::AgenticProgramStatus::Verified {
                        self.root = 10;
                        return self.final_reply().map(Some);
                    }
                    previous_result(body, "recovery-review-retire")?;
                    let Some((result, content)) = artifact else {
                        return Ok(None);
                    };
                    scripted_tool(
                        "recovery-complete",
                        "objective_complete_request",
                        json!({"result_refs":[result],"evidence_refs":[content],"unresolved":[]}),
                        None,
                    )
                }
                _ => return self.final_reply().map(Some),
            };
            self.root += 1;
            value
        };
        Ok(Some(delta))
    }
    fn final_reply(&mut self) -> anyhow::Result<Value> {
        self.final_replies += 1;
        anyhow::ensure!(
            self.final_replies <= 8,
            "original bounded Synthesize continuation budget"
        );
        Ok(
            json!({"role":"assistant","content":"The independently reviewed recovery result is complete: recovery-effect.txt contains recovery-effect-once."}),
        )
    }
}

#[test]
fn exact_goal_witness_rejects_unknown_null_and_foreign_outbox_identity() {
    let mut context = super::tests::context();
    assert!(!exact_graph_context(&context, "execution-1", "turn-1"));
    if let SessionMessageReceiptContext::Available {
        outbox_observation,
        runtime_commit_cursor,
        ..
    } = &mut context
    {
        *outbox_observation = SessionReceiptOutboxObservation::RuntimeOutbox {
            terminal_id: "turn-terminal:request-1".into(),
            revision: "1".into(),
            status: "materialized".into(),
            materialized_at_ms: Some(100),
            runtime_commit_cursor: runtime_commit_cursor.clone(),
            execution_id: Some("execution-1".into()),
            turn_id: "turn-1".into(),
            request_id: "request-1".into(),
        };
    }
    assert!(exact_graph_context(&context, "execution-1", "turn-1"));
    for which in [
        "null_execution",
        "foreign_execution",
        "foreign_turn",
        "foreign_request",
        "foreign_terminal",
        "cursor",
    ] {
        let mut changed = context.clone();
        if let SessionMessageReceiptContext::Available {
            outbox_observation:
                SessionReceiptOutboxObservation::RuntimeOutbox {
                    execution_id,
                    turn_id,
                    request_id,
                    terminal_id,
                    runtime_commit_cursor,
                    ..
                },
            ..
        } = &mut changed
        {
            match which {
                "null_execution" => *execution_id = None,
                "foreign_execution" => *execution_id = Some("execution-foreign".into()),
                "foreign_turn" => *turn_id = "turn-foreign".into(),
                "foreign_request" => *request_id = "request-foreign".into(),
                "foreign_terminal" => *terminal_id = "turn-terminal:foreign".into(),
                "cursor" => {
                    *runtime_commit_cursor =
                        harness_contract::live::ReceiptCursor::parse("9007199254740994").unwrap()
                }
                _ => unreachable!(),
            }
        }
        assert!(
            !exact_graph_context(&changed, "execution-1", "turn-1"),
            "{which}"
        );
    }
}

// Same strict fresh-Task control contract as the original no-hold sequence.
fn r6_task_route_control_reply(body: &Value) -> Result<Option<Value>, String> {
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
    if messages.len() != 2 || !is_router_system(&messages[0]) || messages[1]["role"] != "user" {
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
