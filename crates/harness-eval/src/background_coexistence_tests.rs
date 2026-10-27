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
    goal_ack_coexistence(false, false)
}

#[test]
#[ignore = "separate manually registered retained-root pre-ACK recovery gate; no model; not covered by normal Goal/ACK result"]
fn pending_background_preserves_terminal_delivery_across_retained_gateway_epoch()
-> anyhow::Result<()> {
    goal_ack_coexistence(true, false)
}

#[test]
#[ignore = "separate registered local active-response cancellation gate; original public cancellation and terminal settlement required"]
fn pending_background_allows_owned_active_response_cancellation() -> anyhow::Result<()> {
    goal_ack_coexistence(false, true)
}

fn goal_ack_coexistence(restart_before_ack: bool, cancel_after_open: bool) -> anyhow::Result<()> {
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
    let mut provider = FiniteProvider::start_mode(&root, &model, cancel_after_open)?;
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
            if cancel_after_open {
                let cancellation = observe_owned_active_cancel(
                    &client,
                    &base,
                    &root,
                    &mut actor,
                    &session_id,
                    graph,
                    turn,
                    deadline,
                )?;
                let still_pending = snapshot()?;
                anyhow::ensure!(
                    still_pending["pending"] == true && still_pending["receipts"] == 0,
                    "background cut was lost during foreground cancellation"
                );
                anyhow::ensure!(
                    !root.join("workspace/recovery-effect.txt").exists(),
                    "held root response unexpectedly executed the physical write"
                );
                return Ok(
                    json!({"foreground_cancellation":cancellation,"background_pending":still_pending,
                    "Goal_satisfied":false,"AI_proven":false,"inflight_recovery_proven":false}),
                );
            }
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
            let recovery = if restart_before_ack {
                use std::os::unix::fs::MetadataExt;
                let file = root.join("workspace/recovery-effect.txt");
                let identity = || -> anyhow::Result<Value> {
                    let m = std::fs::metadata(&file)?;
                    Ok(json!({"device":m.dev(),"inode":m.ino(),"length":m.len(),
                        "mtime":m.mtime(),"mtime_nsec":m.mtime_nsec(),"ctime":m.ctime(),"ctime_nsec":m.ctime_nsec()}))
                };
                let requests = || -> anyhow::Result<Vec<String>> {
                    let mut records = std::fs::read_dir(&root)?
                        .filter_map(Result::ok)
                        .map(|entry| entry.file_name().to_string_lossy().to_string())
                        .filter(|n| n.starts_with("r6-provider-request-") && n.ends_with(".json"))
                        .collect::<Vec<_>>();
                    records.sort();
                    Ok(records)
                };
                let before = actor
                    .terminal_receipt_context(&message_id)
                    .map_err(anyhow::Error::msg)?;
                if let SessionMessageReceiptContext::Available { receipt, .. } = &before {
                    anyhow::ensure!(
                        receipt.is_none(),
                        "recovery cut must precede this client's ACK"
                    );
                }
                private_record(
                    &root.join("r6-terminal-before-restart.json"),
                    &serde_json::to_value(&before)?,
                )?;
                let file_before = identity()?;
                let requests_before = requests()?;
                actor.finish().map_err(anyhow::Error::msg)?;
                // Original F2 closes the old child; restart keeps the original
                // root/config/database/port and creates a fresh process epoch.
                server.restart()?;
                actor = SessionActor::resume(
                    &client,
                    &base,
                    &session_id,
                    "harness-eval-live",
                    deadline,
                )
                .map_err(anyhow::Error::msg)?;
                let after = actor
                    .terminal_receipt_context(&message_id)
                    .map_err(anyhow::Error::msg)?;
                private_record(
                    &root.join("r6-terminal-after-restart.json"),
                    &serde_json::to_value(&after)?,
                )?;
                anyhow::ensure!(
                    r6_same_recovered_terminal(&before, &after, graph, turn),
                    "original terminal payload/identity did not recover"
                );
                let file_after = identity()?;
                let requests_after = requests()?;
                anyhow::ensure!(
                    file_before == file_after,
                    "physical output was rewritten during delivery recovery"
                );
                anyhow::ensure!(
                    requests_before == requests_after,
                    "delivery recovery requested another provider continuation"
                );
                Some(
                    json!({"cut":"durable terminal before client ACK","fresh_attachment":true,"same_immutable_terminal":true,
                    "file_identity_before":file_before,"file_identity_after":file_after,"provider_requests_before":requests_before,"provider_requests_after":requests_after,
                    "inflight_graph_or_forced_crash_proven":false}),
                )
            } else {
                None
            };
            let actual_context = actor
                .terminal_receipt_context(&message_id)
                .map_err(anyhow::Error::msg)?;
            private_record(
                &root.join("r6-terminal-before-ack.json"),
                &serde_json::to_value(&actual_context)?,
            )?;
            // A public error terminal can be deliverable too. It is not the
            // business success oracle. Verify original work/Goal before ACK.
            anyhow::ensure!(
                std::fs::read(root.join("workspace/recovery-effect.txt")).map_err(|e|anyhow::anyhow!("foreground_business_artifact_missing_or_unreadable; actual terminal saved before ACK: {e}"))?
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
            let still_pending = snapshot()?;
            anyhow::ensure!(
                still_pending["pending"] == true && still_pending["receipts"] == 0,
                "background unexpectedly succeeded; coexistence fault not held"
            );
            Ok(
                json!({"session_id":session_id,"graph_id":graph,"turn_id":turn,"program":program,"terminal_message_id":message_id,"delivery":delivered.observation,"blocks":delivered.blocks,"background_pending":still_pending,"pre_ack_delivery_recovery":recovery,"cancel_recovery_proven":false,"AI_proven":false}),
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
        Self::start_mode(root, model, false)
    }

    fn start_mode(root: &Path, model: &str, hold_stream: bool) -> anyhow::Result<Self> {
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
                            if let Err(e) = serve_script_request(
                                socket,
                                &model,
                                &path,
                                count,
                                &tx,
                                &st,
                                hold_stream,
                            ) {
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
    hold_stream: bool,
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
    if hold_stream && stream {
        // A real HTTP response and first valid SSE chunk establish an active
        // response, rather than merely a queued connection with no headers.
        let first = json!({"id":format!("r6-hold-{ordinal}"),"object":"chat.completion.chunk","model":model,
            "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]});
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {first}\n\n").as_bytes())?;
        socket.flush()?;
        private_record(
            &root.join(format!("r6-provider-response-opened-{ordinal}.json")),
            &json!({"ordinal":ordinal,"local_http_only":true,"Runtime_opened_evidence_required_separately":true}),
        )?;
        socket.set_nonblocking(true)?;
        let hold_deadline = Instant::now() + Duration::from_secs(180);
        while !stop.load(Ordering::Acquire) {
            anyhow::ensure!(
                Instant::now() < hold_deadline,
                "finite active-response hold deadline"
            );
            match socket.peek(&mut [0u8; 1]) {
                Ok(0) => {
                    private_record(
                        &root.join(format!("r6-provider-peer-closed-{ordinal}.json")),
                        &json!({"local_peer_closed":true,"Runtime_cancel_settlement_required_separately":true}),
                    )?;
                    return Ok(());
                }
                Ok(_) => anyhow::bail!("unexpected data after bounded provider request"),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(e) => return Err(e.into()),
            }
        }
        return Ok(());
    }
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

#[test]
fn controlled_model_window_is_explicit_and_resolves_through_original_loader() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let config_home = root.path().join("config");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&config_home)?;
    std::fs::create_dir(&workspace)?;
    let mut config = json!({"model":"r6-finite-local","providers":{"r5-no-provider":{
        "baseUrl":"http://127.0.0.1:9/v1","apiKey":"unit-synthetic-only","models":["r6-finite-local"],"protocol":"completions"}}});
    let path = config_home.join("config.yaml");
    let resolve = || -> anyhow::Result<runtime::ProviderRegistry> {
        let loaded = runtime::ConfigLoader::new(&workspace, &config_home).load()?;
        Ok(runtime::ProviderRegistry::new_with_context_windows(
            loaded.providers().clone(),
            loaded.model_context_windows().clone(),
        )
        .map_err(|e| anyhow::anyhow!("controlled registry rejected: {e:?}"))?)
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    // Valid routing is insufficient for admission without a verified window.
    let old = resolve()?;
    let old = old.pin();
    assert!(old.resolve("r6-finite-local").is_some());
    assert_eq!(old.configured_context_window("r6-finite-local"), None);
    original_server::configure_controlled_provider(&mut config, "r6-finite-local");
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    let fixed = resolve()?;
    let fixed = fixed.pin();
    assert_eq!(
        fixed.configured_context_window("r6-finite-local"),
        Some(128_000)
    );
    assert_eq!(fixed.configured_context_window("unregistered-model"), None);
    assert!(fixed.resolve("unregistered-model").is_none());
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

// ROUND336: selected-source oracle only; the caller owns admission, cancellation,
// retained-root epochs and physical-return evidence. Never derive principal from
// the cancellation response itself: use the original owned session lease.
fn r6_cancel_settled(
    session: &str,
    turn: &str,
    graph: &str,
    principal: &str,
    cancellation_id: &str,
    receipt: &harness_contract::turn::CancellationReceipt,
    projection: &Value,
) -> bool {
    use harness_contract::{
        projection::{ExecutionTerminalAxes, TerminalEffectSettlement, TerminalExecutionPhase},
        turn::{CancellationCause, CancellationStatus},
    };
    let Ok(axes) = serde_json::from_value::<ExecutionTerminalAxes>(
        projection
            .get("terminal_axes")
            .cloned()
            .unwrap_or(Value::Null),
    ) else {
        return false;
    };
    !session.is_empty()
        && !turn.is_empty()
        && !graph.is_empty()
        && !cancellation_id.is_empty()
        && principal
            .strip_prefix("principal:")
            .is_some_and(|s| !s.is_empty())
        && receipt.cancellation_id == cancellation_id
        && receipt.session_id == session
        && receipt.turn_id == turn
        && receipt.execution_id == graph
        && receipt.actor_id == principal
        && receipt.status == CancellationStatus::Cancelled
        && receipt.cause == CancellationCause::UserRequested
        && receipt.journal_sequence > 0
        && receipt
            .effective_at_ms
            .is_some_and(|t| t >= receipt.requested_at_ms)
        && projection["execution_id"] == graph
        && projection["session_id"] == session
        && projection["turn_id"] == turn
        && axes.execution.phase == TerminalExecutionPhase::Terminal
        && axes.execution.settlement == TerminalEffectSettlement::ProvenSettled
        && axes.execution.coverage_complete
        && !axes.source_race
        && axes.source_cursor > 0
        && axes.outcome.outcome != Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied)
}

fn r6_session_events(
    client: &Client,
    base: &str,
    session: &str,
    deadline: Instant,
) -> anyhow::Result<Vec<Value>> {
    let mut rows = vec![];
    let mut next = 0u64;
    let mut selected = None;
    for _ in 0..32 {
        let limit = 100usize.min(512 - rows.len());
        anyhow::ensure!(
            limit > 0 && Instant::now() < deadline,
            "bounded R6 Session event prefix exhausted"
        );
        let page = get_owned_json(
            client,
            base,
            &format!(
                "/api/sessions/{session}/events?from_seq={next}&limit={limit}&include_payload=true"
            ),
            deadline,
        )?;
        anyhow::ensure!(
            page["session_id"] == session
                && page["from_seq"] == next
                && page["limit"] == limit
                && page["include_payload"] == true,
            "foreign R6 event page"
        );
        let total = page["total"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("missing event count"))?;
        let cut = *selected.get_or_insert(total);
        anyhow::ensure!(cut <= 512, "R6 event prefix bound");
        let events = page["events"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing events"))?;
        anyhow::ensure!(
            events.len() <= limit
                && page["has_more"].as_bool() == Some((events.len() as u64) < total),
            "event pagination contract"
        );
        for event in events {
            let position = event["sequence"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("missing event position"))?;
            anyhow::ensure!(
                event["session_id"] == session && position >= next,
                "foreign/nonmonotonic Session event"
            );
            next = position
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("event cursor overflow"))?;
            if rows.len() < cut as usize {
                rows.push(event.clone());
            }
        }
        if rows.len() == cut as usize {
            return Ok(rows);
        }
        anyhow::ensure!(!events.is_empty(), "selected event prefix incomplete");
    }
    anyhow::bail!("R6 event pagination bound")
}

fn observe_owned_active_cancel(
    client: &Client,
    base: &str,
    root: &Path,
    actor: &mut SessionActor<'_>,
    session: &str,
    graph: &str,
    turn: &str,
    deadline: Instant,
) -> anyhow::Result<Value> {
    use runtime::provider_runtime_client::ProviderPhysicalObservation;
    // The actor is independently read before cancellation, never inferred from
    // the returned receipt (which would make the identity check circular).
    let leases = get_owned_json(client, base, "/api/runtime/session-leases", deadline)?;
    let owned = leases["leases"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing original leases"))?
        .iter()
        .filter(|r| r["session_id"] == session)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        leases["status"] == "available"
            && leases["attached"] == true
            && owned.len() == 1
            && owned[0]["mode"] == "collaborative",
        "owned lease missing/ambiguous"
    );
    let principal = owned[0]["owner"]
        .as_str()
        .and_then(|o| o.strip_suffix(":observer:harness-eval-live"))
        .filter(|p| p.strip_prefix("principal:").is_some_and(|s| !s.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("original principal binding missing"))?
        .to_owned();
    private_record(&root.join("r6-cancel-original-lease.json"), &leases)?;
    let (opened_event, physical) = loop {
        anyhow::ensure!(Instant::now() < deadline, "original ResponseOpened not hit");
        actor
            .maintain_receipt_authority()
            .map_err(anyhow::Error::msg)?;
        let events = r6_session_events(client, base, session, deadline)?;
        let found = events.iter().rev().find_map(|event| {
            if event["type"] != "context.provider_physical_observation"
                || event["status"] != "opened"
                || event["sequence"].as_u64().is_none()
            {
                return None;
            }
            let ProviderPhysicalObservation::ResponseOpened { identity } =
                serde_json::from_value(event["payload"].clone()).ok()?
            else {
                return None;
            };
            (identity.session_id == session
                && identity.turn_id == turn
                && identity.execution_id == graph
                && identity.step.session_id == session
                && identity.step.turn_id == turn
                && identity.step.execution_id.as_deref() == Some(graph)
                && !identity.step.step_id.is_empty()
                && identity.provider_attempt.origin_step_id == identity.step.step_id
                && !identity.provider_attempt.attempt_id.is_empty()
                && !identity.request_id.is_empty()
                && !identity.response_id.is_empty()
                && identity.opened_at_ms > 0
                && !identity.resource_lease.is_empty()
                && !events.iter().any(|later| {
                    later["sequence"].as_u64() > event["sequence"].as_u64()
                        && later["type"] == "context.provider_attempt_outcome"
                        && later["payload"]["request_id"] == identity.request_id
                }))
            .then(|| (event.clone(), identity))
        });
        if let Some(value) = found {
            break value;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    private_record(&root.join("r6-cancel-original-opened.json"), &opened_event)?;
    let before = get_owned_json(
        client,
        base,
        &format!("/api/runtime/executions/{graph}?detail_scope=summary"),
        deadline,
    )?;
    anyhow::ensure!(
        before.pointer("/terminal_axes/execution/phase") != Some(&json!("terminal")),
        "AlreadyTerminal cut does not prove active cancellation"
    );
    let cancellation_id = format!("r6-owned-cancel:{graph}");
    let opened_digest = harness_contract::step::canonical_digest(
        "provider-physical-observation",
        &opened_event["payload"],
    )
    .map_err(anyhow::Error::msg)?;
    for poll in 0..64 {
        anyhow::ensure!(
            Instant::now() < deadline,
            "active cancellation remains unproven"
        );
        let raw=actor.post_mutation(&format!("/api/sessions/{session}/cancel"),json!({"cancellation_id":cancellation_id,"expected_execution_id":graph,"expected_turn_id":turn})).map_err(anyhow::Error::msg)?;
        private_record(&root.join(format!("r6-cancel-receipt-{poll}.json")), &raw)?;
        let receipt: harness_contract::turn::CancellationReceipt = serde_json::from_value(raw)?;
        anyhow::ensure!(
            receipt.status != harness_contract::turn::CancellationStatus::AlreadyTerminal,
            "active cancel lost to terminal"
        );
        let projection = get_owned_json(
            client,
            base,
            &format!("/api/runtime/executions/{graph}?detail_scope=summary"),
            deadline,
        )?;
        if r6_cancel_settled(
            session,
            turn,
            graph,
            &principal,
            &cancellation_id,
            &receipt,
            &projection,
        ) {
            let events = r6_session_events(client, base, session, deadline)?;
            for event in &events {
                if event["type"] != "context.provider_physical_observation"
                    || event["status"] != "cancel_observed"
                {
                    continue;
                }
                let Ok(ProviderPhysicalObservation::CancellationObserved {
                    identity,
                    observed_at_ms,
                    opened,
                    requested,
                }) = serde_json::from_value(event["payload"].clone())
                else {
                    continue;
                };
                if identity != physical
                    || opened.identity != physical
                    || opened.observation_digest != opened_digest
                    || Some(opened.session_sequence as u64) != opened_event["sequence"].as_u64()
                    || observed_at_ms < physical.opened_at_ms
                    || requested.cancellation_id != cancellation_id
                    || requested.stream_id != format!("cancellation:{cancellation_id}")
                    || requested.event_sequence != 1
                    || requested.commit_cursor == 0
                {
                    continue;
                }
                let requested_json = serde_json::to_value(&requested)?;
                let auth = events
                    .iter()
                    .filter(|a| {
                        a["type"] == "session.claim.cancellation_authorized.v1"
                            && a["status"] == "authorized"
                            && a["sequence"].as_u64() < event["sequence"].as_u64()
                            && a["payload"]["requested_ref"] == requested_json
                            && a["payload"]["receipt"]["actor_id"] == principal
                            && a["payload"]["receipt"]["session_id"] == session
                            && a["payload"]["receipt"]["turn_id"] == turn
                            && a["payload"]["receipt"]["execution_id"] == graph
                            && a["payload"]["receipt"]["cancellation_id"] == cancellation_id
                    })
                    .collect::<Vec<_>>();
                if auth.len() != 1 || !events.iter().any(|e| e == &opened_event) {
                    continue;
                }
                private_record(&root.join("r6-cancel-settled-events.json"), &json!(events))?;
                return Ok(
                    json!({"receipt":receipt,"projection":projection,"opened_event":opened_event,"cancel_observed":event,
                    "authorization_event":auth[0],"local_response_cancellation_observed":true,"physical_release":"requires_original_F2_and_owned_resource_return",
                    "remote_inference_stop":"unknown","remote_billing_stop":"unknown"}),
                );
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("bounded original cancel receipt/physical evidence did not settle")
}

// Before-ACK durable-delivery recovery only. A fresh attachment has a NEW client
// identity/grant; compare immutable message content, not the entire context.
// Both contexts must be actually fetched, one in each independently proved epoch.
fn r6_same_recovered_terminal(
    before: &SessionMessageReceiptContext,
    after: &SessionMessageReceiptContext,
    graph: &str,
    turn: &str,
) -> bool {
    if !exact_graph_context(before, graph, turn) || !exact_graph_context(after, graph, turn) {
        return false;
    }
    match (before, after) {
        (
            SessionMessageReceiptContext::Available {
                session_id: a_session,
                message_id: a_message,
                message_sequence: a_sequence,
                payload_kind: a_kind,
                payload_json: a_payload,
                content_digest: a_digest,
                ..
            },
            SessionMessageReceiptContext::Available {
                session_id: b_session,
                message_id: b_message,
                message_sequence: b_sequence,
                payload_kind: b_kind,
                payload_json: b_payload,
                content_digest: b_digest,
                ..
            },
        ) => {
            !a_session.is_empty()
                && !a_message.is_empty()
                && !a_digest.is_empty()
                && a_session == b_session
                && a_message == b_message
                && a_sequence == b_sequence
                && a_kind == b_kind
                && a_payload == b_payload
                && a_digest == b_digest
        }
        _ => false,
    }
}

#[test]
fn r6_cancel_oracle_rejects_foreign_and_unsettled_receipts() {
    let receipt = json!({"cancellation_id":"cancel-a","session_id":"session-a",
        "turn_id":"turn-a","execution_id":"graph-a","actor_id":"principal:fixture",
        "cause":"user_requested","requested_at_ms":10,"effective_at_ms":20,
        "status":"cancelled","journal_sequence":2,"projection_revision":3});
    let projection = json!({"session_id":"session-a","turn_id":"turn-a","execution_id":"graph-a",
        "terminal_axes":{
            "outcome":{"goal_id":null,"goal_revision":null,"outcome":null,"terminal_fence":null},
            "execution":{"phase":"terminal","settlement":"proven_settled","coverage_complete":true,"evidence_refs":["fixture:closure"]},
            "delivery":{"runtime_terminal_ref":null,"session_client_evidence":"requires_fresh_context"},
            "source_cursor":30,"source_race":false}});
    let accepts = |r: Value, p: &Value| {
        let r = serde_json::from_value(r).unwrap();
        r6_cancel_settled(
            "session-a",
            "turn-a",
            "graph-a",
            "principal:fixture",
            "cancel-a",
            &r,
            p,
        )
    };
    assert!(accepts(receipt.clone(), &projection));
    for (key, value) in [
        ("cancellation_id", json!("foreign")),
        ("session_id", json!("foreign")),
        ("turn_id", json!("foreign")),
        ("execution_id", json!("foreign")),
        ("actor_id", json!("principal:foreign")),
        ("cause", json!("system")),
        ("status", json!("requested")),
        ("status", json!("already_terminal")),
        ("journal_sequence", json!(0)),
        ("effective_at_ms", Value::Null),
        ("effective_at_ms", json!(9)),
    ] {
        let mut changed = receipt.clone();
        changed[key] = value;
        assert!(!accepts(changed, &projection), "receipt {key}");
    }
    for (path, value) in [
        ("/session_id", json!("foreign")),
        ("/turn_id", json!("foreign")),
        ("/execution_id", json!("foreign")),
        ("/terminal_axes", Value::Null),
        ("/terminal_axes/execution/coverage_complete", json!(false)),
        ("/terminal_axes/execution/settlement", json!("unknown")),
        ("/terminal_axes/source_race", json!(true)),
        ("/terminal_axes/source_cursor", json!(0)),
        ("/terminal_axes/outcome/outcome", json!("satisfied")),
    ] {
        let mut changed = projection.clone();
        *changed.pointer_mut(path).unwrap() = value;
        assert!(!accepts(receipt.clone(), &changed), "projection {path}");
    }
}

#[test]
fn r6_recovery_oracle_allows_fresh_client_but_rejects_changed_content() {
    let mut before = super::tests::context();
    if let SessionMessageReceiptContext::Available {
        session_source,
        runtime_commit_cursor,
        outbox_observation,
        ..
    } = &mut before
    {
        *outbox_observation = SessionReceiptOutboxObservation::RuntimeOutbox {
            terminal_id: format!("turn-terminal:{}", session_source.request_id),
            revision: "1".into(),
            status: "materialized".into(),
            materialized_at_ms: Some(100),
            runtime_commit_cursor: runtime_commit_cursor.clone(),
            execution_id: Some("graph-a".into()),
            turn_id: session_source.turn_id.clone(),
            request_id: session_source.request_id.clone(),
        };
    }
    let turn = match &before {
        SessionMessageReceiptContext::Available { session_source, .. } => {
            session_source.turn_id.clone()
        }
        _ => unreachable!(),
    };
    let mut after = before.clone();
    if let SessionMessageReceiptContext::Available {
        client_id, receipt, ..
    } = &mut after
    {
        *client_id = "fresh-epoch-client".into();
        *receipt = None;
    }
    assert!(r6_same_recovered_terminal(
        &before, &after, "graph-a", &turn
    ));
    for field in [
        "session", "message", "sequence", "payload", "digest", "unknown",
    ] {
        let mut changed = after.clone();
        if let SessionMessageReceiptContext::Available {
            session_id,
            message_id,
            message_sequence,
            payload_json,
            content_digest,
            outbox_observation,
            ..
        } = &mut changed
        {
            match field {
                "session" => session_id.push_str("-foreign"),
                "message" => message_id.push_str("-foreign"),
                "sequence" => message_sequence.push('0'),
                "payload" => payload_json.push(' '),
                "digest" => content_digest.push('0'),
                "unknown" => {
                    *outbox_observation = SessionReceiptOutboxObservation::RuntimeOutboxUnknown
                }
                _ => unreachable!(),
            }
        }
        assert!(
            !r6_same_recovered_terminal(&before, &changed, "graph-a", &turn),
            "{field}"
        );
    }
}
