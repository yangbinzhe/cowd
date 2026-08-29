use super::*;
use crate::{
    active_session::ActiveSessionDirectory,
    services::session_service::{presence::SessionPresenceLedger, repository::SessionRepository},
};
use session::SessionRecord;

fn test_backend_reporter(name: &'static str) -> WorkerBackendReporter {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    set_worker_state(&states, name, SessionWorkerState::Starting);
    WorkerBackendReporter { name, states }
}

#[test]
fn concurrent_session_owner_conflict_remains_retryable() {
    assert_eq!(
        classify_ingress_failure(SESSION_RUNTIME_BUSY_ERROR),
        OutboxFailureClass::Retryable
    );
}

#[tokio::test]
async fn checkpoint_consumed_supplement_remains_attached_until_terminal_commit() {
    let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&SessionRecord {
            session_id: "supplement-session".to_string(),
            platform: "test".to_string(),
            chat_id: "supplement-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    store
        .append_ingress_with_runtime_outbox(
            "supplement-session",
            "user",
            Some(r#"[{"type":"text","text":"late supplement"}]"#),
            1,
            &session::SessionRuntimeOutboxRequest {
                input_id: "supplement-input".to_string(),
                request_id: "supplement-request".to_string(),
                turn_id: "supplement-message-turn".to_string(),
                message_id: "supplement-message".to_string(),
                session_generation: 1,
                decision: InputRoutingDecision::SupplementCurrentTurn,
                target_turn_id: Some("turn-active".to_string()),
                classification_json: None,
                task_route_hint: None,
                created_at_ms: 1,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let session_service = test_session_service(Arc::clone(&store), SessionProjectionHub::new());
    let record = session_service
        .claim_ingress_work("checkpoint-worker", now_ms(), LEASE_MS, 1)
        .await
        .unwrap()
        .pop()
        .expect("claimed supplement");
    let claim_token = record.claim_token.clone().expect("claim token");

    acknowledge_checkpoint_consumed_ingress(
        &session_service,
        "checkpoint-worker",
        &record,
        &claim_token,
    )
    .await;

    let persisted = session_service
        .runtime_input("supplement-request")
        .await
        .unwrap()
        .expect("persisted input");
    assert_eq!(persisted.status, SessionRuntimeInputStatus::Attached);
    assert_eq!(
        persisted.decision,
        InputRoutingDecision::SupplementCurrentTurn
    );
    assert_eq!(persisted.target_turn_id.as_deref(), Some("turn-active"));
    assert_eq!(persisted.runtime_commit_cursor, None);
}

#[tokio::test]
async fn terminal_primary_failure_rolls_attached_input_into_a_new_turn() {
    let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&SessionRecord {
            session_id: "roll-forward-session".to_string(),
            platform: "test".to_string(),
            chat_id: "roll-forward-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let primary = store
        .append_ingress_with_runtime_outbox(
            "roll-forward-session",
            "user",
            Some(r#"[{"type":"text","text":"primary"}]"#),
            1,
            &session::SessionRuntimeOutboxRequest {
                input_id: "primary-input".to_string(),
                request_id: "primary-request".to_string(),
                turn_id: "primary-turn".to_string(),
                message_id: "primary-message".to_string(),
                session_generation: 1,
                decision: InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                task_route_hint: None,
                created_at_ms: 1,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("failure-worker", 2, LEASE_MS, 1)
        .await
        .unwrap()
        .pop()
        .expect("claim primary");
    let claim_token = claimed.claim_token.clone().expect("claim token");
    let running = store
        .mark_session_runtime_outbox_running(
            &primary.request_id,
            "failure-worker",
            primary.session_generation,
            &claim_token,
            claimed.revision,
            3,
        )
        .await
        .unwrap();
    let supplement = store
        .append_ingress_with_runtime_outbox(
            "roll-forward-session",
            "user",
            Some(r#"[{"type":"text","text":"finish this even if primary fails"}]"#),
            4,
            &session::SessionRuntimeOutboxRequest {
                input_id: "roll-forward-input".to_string(),
                request_id: "roll-forward-request".to_string(),
                turn_id: "supplement-turn".to_string(),
                message_id: "supplement-message".to_string(),
                session_generation: 1,
                decision: InputRoutingDecision::SupplementCurrentTurn,
                target_turn_id: Some(primary.turn_id.clone()),
                classification_json: None,
                task_route_hint: None,
                created_at_ms: 4,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let attached = store
        .attach_session_runtime_outbox(
            &supplement.input_id,
            supplement.session_generation,
            supplement.revision,
            &primary.turn_id,
            "test",
            "delivered",
            5,
        )
        .await
        .unwrap();
    assert_eq!(attached.status, SessionRuntimeInputStatus::Attached);
    let failed = store
        .fail_session_runtime_outbox(
            &running.request_id,
            "failure-worker",
            running.session_generation,
            &claim_token,
            running.revision,
            OutboxFailureClass::Permanent,
            "terminal failure",
            6,
            1,
            6,
        )
        .await
        .unwrap();
    assert_eq!(failed.status, SessionRuntimeInputStatus::Failed);

    let session_service = test_session_service(Arc::clone(&store), SessionProjectionHub::new());
    roll_forward_unapplied_inputs(&session_service, &failed, "terminal failure").await;

    let rolled = store
        .get_session_runtime_outbox("roll-forward-request")
        .await
        .unwrap()
        .expect("rolled input remains auditable");
    assert_eq!(rolled.status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(rolled.decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(rolled.target_turn_id, None);
}

fn test_session_service(
    store: Arc<UnifiedSessionStore>,
    event_bus: Arc<SessionProjectionHub>,
) -> Arc<SessionService> {
    let repository = Arc::new(SessionRepository::new(
        Arc::new(ActiveSessionDirectory::new()),
        Some(Arc::clone(&store)),
        event_bus,
    ));
    Arc::new(SessionService::for_tests(
        repository,
        Arc::new(SessionPresenceLedger::with_store(store)),
    ))
}

async fn delivery_fixture() -> (
    Arc<runtime::RuntimeEventStore>,
    runtime::SessionTerminalDeliveryPort,
    Arc<runtime::ArtifactStore>,
    Arc<SessionService>,
    Arc<UnifiedSessionStore>,
    Arc<SessionProjectionHub>,
    Arc<runtime::RuntimeServices>,
    crate::event_bus::SessionProjectionSubscription,
) {
    let runtime_event_store = Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
    let fixture_root = std::env::temp_dir()
        .join("cowd-terminal-delivery-fixtures")
        .join(uuid::Uuid::new_v4().to_string());
    let home = fixture_root.join("home");
    let workspace = fixture_root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let runtime_services = runtime::RuntimeServices::builder(&home, &workspace)
        .runtime_event_store(Arc::clone(&runtime_event_store))
        .build()
        .unwrap();
    let event_store = runtime_services.session_terminal_delivery();
    let artifacts = Arc::clone(runtime_services.artifact_store());
    let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&SessionRecord {
            session_id: "s1".to_string(),
            platform: "test".to_string(),
            chat_id: "chat".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let event_bus = SessionProjectionHub::new();
    let rx = event_bus.subscribe("s1", 8).await;
    let session_service = test_session_service(Arc::clone(&store), Arc::clone(&event_bus));
    (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        runtime_services,
        rx,
    )
}

async fn enqueue_fenced_terminal(
    runtime_event_store: &runtime::RuntimeEventStore,
    store: &UnifiedSessionStore,
    terminal_id: &str,
    message_id: &str,
    request_id: &str,
    turn_id: &str,
    ingress_message_id: &str,
    artifacts: &runtime::ArtifactStore,
    terminal_payload: serde_json::Value,
) -> u64 {
    store
        .append_ingress_with_runtime_outbox(
            "s1",
            "user",
            Some(r#"[{"type":"text","text":"fixture ingress"}]"#),
            1,
            &session::SessionRuntimeOutboxRequest {
                input_id: request_id.to_string(),
                request_id: request_id.to_string(),
                turn_id: turn_id.to_string(),
                message_id: ingress_message_id.to_string(),
                session_generation: 1,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                task_route_hint: None,
                created_at_ms: 1,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let now = now_ms();
    let claimed = store
        .claim_session_runtime_outbox("session-worker", now, 30_000, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim_token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            request_id,
            "session-worker",
            claimed.session_generation,
            &claim_token,
            claimed.revision,
            now,
        )
        .await
        .unwrap();
    let terminal_payload = serde_json::to_vec(&terminal_payload).unwrap();
    let artifact = artifacts
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.session-terminal+json".to_string(),
                visibility_scope: "session:s1".to_string(),
                expected_bytes: Some(terminal_payload.len() as u64),
                original_name: Some(format!("{terminal_id}.json")),
            },
            &terminal_payload,
        )
        .await
        .unwrap();
    artifacts
        .pin(
            &artifact,
            terminal_id,
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .unwrap();
    let payload_ref = runtime::encode_session_terminal_artifact_ref(&artifact).unwrap();
    runtime_event_store
        .append_transaction_with_terminal(
            runtime::AppendTransactionRequest {
                transaction_id: format!("terminal-fixture:{terminal_id}"),
                expected_streams: vec![runtime::ExpectedStreamRevision {
                    stream_id: format!("turn:{turn_id}"),
                    expected_revision: 0,
                }],
                events: vec![runtime::RuntimeTransactionEventInput {
                    event: runtime::RuntimeEventInput {
                        stream_id: format!("turn:{turn_id}"),
                        scope: runtime::RuntimeEventScope::SessionInput,
                        kind: "turn.terminal_committed".to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("terminal-delivery-fixture".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"terminal_id": terminal_id}),
                    },
                    idempotency_key: Some(format!("terminal-event:{terminal_id}")),
                    schema_version: 1,
                }],
            },
            runtime::SessionTerminalInput {
                terminal_id: terminal_id.to_string(),
                message_id: message_id.to_string(),
                session_id: "s1".to_string(),
                execution_id: Some(format!("execution:{request_id}")),
                turn_id: Some(turn_id.to_string()),
                request_id: Some(request_id.to_string()),
                session_generation: Some(running.session_generation),
                input_sequence: Some(running.sequence as u64),
                input_claim_owner: running.claim_owner,
                input_claim_token: running.claim_token,
                input_claim_revision: running.claim_fence_epoch,
                controlled_recovery_claim_fingerprints: Vec::new(),
                payload_ref,
            },
        )
        .unwrap()
        .commit_cursor
}

#[test]
fn terminal_payload_requires_the_canonical_artifact_schema() {
    let payload = decode_terminal_payload(
            br#"{"schema_version":1,"text":"done","ingress_message_id":"ingress-1","consumed_input_sequence":0,"token_usage":{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"transcript":[{"role":"assistant","blocks":[{"type":"text","text":"done"}]}]}"#,
        )
        .unwrap();
    assert_eq!(payload.text, "done");
    assert!(payload
        .token_usage_json
        .as_deref()
        .is_some_and(|usage| usage.contains("\"input_tokens\":12")));
    assert!(decode_terminal_payload(
            br#"{"schema_version":1,"text":"done","ingress_message_id":"ingress-1","consumed_input_sequence":0,"token_usage":{"input_tokens":"12","output_tokens":3},"transcript":[]}"#
        )
        .is_err());
    assert!(decode_terminal_payload(b"not-json").is_err());
}

#[test]
fn terminal_payload_schema_three_round_trips_collaboration_terminal() {
    let terminal_presentation = serde_json::json!({
        "presentation_id": "presentation-1",
        "attempt_id": "attempt-1",
        "envelope_id": "envelope-1",
        "envelope_revision": 3,
        "state": "committed",
        "answer_origin": "terminal_narrator",
        "models_attempted": [],
        "validation": {"status": "valid", "findings": []},
        "generated_at_ms": 10,
        "committed_at_ms": 11
    });
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema_version": runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,
        "text": "complete collaboration answer.",
        "collaboration_evidence": "{\"kind\":\"cowd.runtime.collaboration_evidence.v1\"}",
        "terminal_presentation": terminal_presentation,
        "ingress_message_id": "ingress-1",
        "consumed_input_sequence": 0,
        "token_usage": {"input_tokens": 12, "output_tokens": 3},
        "transcript": [{
            "role": "assistant",
            "blocks": [{"type": "text", "text": "complete collaboration answer."}]
        }]
    }))
    .unwrap();

    let decoded = decode_terminal_payload(&payload).unwrap();
    assert_eq!(decoded.text, "complete collaboration answer.");
    assert_eq!(
        decoded
            .terminal_presentation
            .as_ref()
            .map(|presentation| presentation.state),
        Some(harness_contract::outcome::TerminalPresentationState::Committed)
    );
}

#[test]
fn terminal_payload_schema_three_fails_closed_on_partial_migration() {
    let common = serde_json::json!({
        "text": "done.",
        "ingress_message_id": "ingress-1",
        "consumed_input_sequence": 0,
        "token_usage": {"input_tokens": 1, "output_tokens": 1},
        "transcript": [{
            "role": "assistant",
            "blocks": [{"type": "text", "text": "done."}]
        }],
        "terminal_presentation": {
            "presentation_id": "presentation-1",
            "attempt_id": "attempt-1",
            "envelope_id": "envelope-1",
            "envelope_revision": 3,
            "state": "committed",
            "answer_origin": "terminal_narrator",
            "models_attempted": [],
            "validation": {"status": "valid", "findings": []},
            "generated_at_ms": 10,
            "committed_at_ms": 11
        }
    });
    let mut missing_evidence = common.clone();
    missing_evidence["schema_version"] = serde_json::json!(3);
    assert!(decode_terminal_payload(&serde_json::to_vec(&missing_evidence).unwrap()).is_err());

    let mut future_schema = common;
    future_schema["schema_version"] = serde_json::json!(4);
    future_schema["collaboration_evidence"] = serde_json::Value::Null;
    assert!(decode_terminal_payload(&serde_json::to_vec(&future_schema).unwrap()).is_err());
}

#[tokio::test]
async fn stop_accepting_does_not_stop_supervised_workers() {
    let supervisor = SessionWorkerSupervisor::for_tests();

    supervisor.stop_accepting();

    assert!(!supervisor.is_accepting());
    let health = supervisor.health();
    assert!(!health.accepting);
    assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
        health
            .workers
            .get(*name)
            .is_some_and(|worker| worker.state == SessionWorkerState::Running)
    }));

    supervisor.shutdown().await;
    assert!(*supervisor.shutdown.borrow());
}

#[test]
fn recovery_health_is_updated_after_background_restoration() {
    let supervisor = SessionWorkerSupervisor::for_tests();
    let recovery = crate::services::session_service::activation::SessionRecoverySummary {
        discovered: 7,
        required: 2,
        recovered: 2,
        ..Default::default()
    };

    supervisor.record_recovery(recovery);

    let health = supervisor.health();
    assert_eq!(health.recovery.discovered, 7);
    assert_eq!(health.recovery.required, 2);
    assert_eq!(health.recovery.recovered, 2);
    assert!(health.recovery_completed_at_ms > 0);
}

#[test]
fn terminal_annotation_preserves_causality_on_non_tool_blocks() {
    let mut transcript = vec![DecodedTerminalTranscriptMessage {
        role: "assistant".to_string(),
        content_json: serde_json::json!([
            {"type": "thinking", "thinking": "reason"},
            {"type": "text", "text": "done"}
        ])
        .to_string(),
        blocks_count: 2,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
    }];

    annotate_terminal_tool_instances(
        &mut transcript,
        Some("execution-1"),
        Some("turn-1"),
        Some("ingress-1"),
    );

    let blocks =
        serde_json::from_str::<Vec<serde_json::Value>>(transcript[0].content_json.as_str())
            .unwrap();
    assert_eq!(blocks.len(), 2);
    for block in blocks {
        assert_eq!(
            block
                .get("cowd_execution_id")
                .and_then(serde_json::Value::as_str),
            Some("execution-1")
        );
        assert_eq!(
            block
                .get("cowd_turn_id")
                .and_then(serde_json::Value::as_str),
            Some("turn-1")
        );
        assert_eq!(
            block
                .get("cowd_turn_ingress_message_id")
                .and_then(serde_json::Value::as_str),
            Some("ingress-1")
        );
    }
}

#[tokio::test]
async fn append_success_ack_failure_replays_notification_without_duplicate_message() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        _runtime_services,
        mut rx,
    ) = delivery_fixture().await;
    let private_reasoning = "private-provider-reasoning";
    let provider_signature = "provider-signature";
    let sealed_reasoning =
        runtime::provider_transcript::seal_provider_transcript(private_reasoning).unwrap();
    let sealed_signature =
        runtime::provider_transcript::seal_provider_transcript(provider_signature).unwrap();
    let terminal_payload = serde_json::json!({
        "schema_version": 1,
        "text": "done",
        "ingress_message_id": "ingress-1",
        "consumed_input_sequence": 0,
        "token_usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0
        },
        "transcript": [{
            "role": "assistant",
            "blocks": [
                {"type": "reasoning_summary", "text": "public summary"},
                {
                    "type": "thinking",
                    "thinking": sealed_reasoning,
                    "signature": sealed_signature
                },
                {"type": "text", "text": "done"}
            ]
        }]
    });
    let commit_cursor = enqueue_fenced_terminal(
        &runtime_event_store,
        &store,
        "t1",
        "m1",
        "request-1",
        "turn-1",
        "ingress-1",
        &artifacts,
        terminal_payload,
    )
    .await;
    let claim_at = now_ms();
    let record = event_store
        .claim("owner-a", claim_at, 10, 1)
        .unwrap()
        .pop()
        .unwrap();

    deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        None,
        "wrong-owner",
        record,
    )
    .await
    .unwrap();
    let persisted = store.get_messages("s1", 0, 10).await.unwrap();
    let terminal_state = event_store.get("t1").unwrap().unwrap();
    let terminal_content = persisted
        .iter()
        .find(|message| message.stable_message_id == "m1")
        .map(|message| message.content_json.as_str())
        .unwrap();
    assert!(terminal_content.contains("public summary"));
    assert!(terminal_content.contains("cowd-provider-transcript:v1:"));
    assert!(!terminal_content.contains(private_reasoning));
    assert!(!terminal_content.contains(provider_signature));
    assert_eq!(
        persisted
            .iter()
            .filter(|message| message.stable_message_id == "m1")
            .count(),
        1,
        "terminal_state={terminal_state:?}, persisted={persisted:?}"
    );
    let terminal_event = rx.try_recv().unwrap().to_transport_value();
    assert_eq!(terminal_event["type"], "TerminalCommitted");
    assert_eq!(terminal_event["terminal_id"], "t1");
    assert_eq!(terminal_event["message_id"], "m1");
    assert_eq!(terminal_event["runtime_commit_cursor"], commit_cursor);
    assert_eq!(terminal_event["replayed"], false);
    assert_eq!(event_store.get("t1").unwrap().unwrap().status, "claimed");

    let reclaimed = event_store
        .claim("owner-b", claim_at + 11, 10, 1)
        .unwrap()
        .pop()
        .unwrap();
    deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        None,
        "owner-b",
        reclaimed,
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .get_messages("s1", 0, 10)
            .await
            .unwrap()
            .iter()
            .filter(|message| message.stable_message_id == "m1")
            .count(),
        1
    );
    let replayed = rx
        .try_recv()
        .expect("retry must rebroadcast")
        .to_transport_value();
    assert_eq!(replayed["terminal_id"], "t1");
    assert_eq!(replayed["message_id"], "m1");
    assert_eq!(replayed["replayed"], true);
    assert!(rx.try_recv().is_err(), "one retry emits one notification");
    assert_eq!(
        event_store.get("t1").unwrap().unwrap().status,
        "materialized"
    );
}

#[tokio::test]
async fn generation_change_after_delivery_claim_rejects_terminal_without_projection() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        _runtime_services,
        mut rx,
    ) = delivery_fixture().await;
    enqueue_fenced_terminal(
        &runtime_event_store,
        &store,
        "terminal-stale-generation",
        "message-stale-generation",
        "request-stale-generation",
        "turn-stale-generation",
        "ingress-stale-generation",
        &artifacts,
        serde_json::json!({
            "schema_version": 1,
            "text": "must not commit",
            "ingress_message_id": "ingress-stale-generation",
            "consumed_input_sequence": 0,
            "token_usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            },
            "transcript": [{
                "role": "assistant",
                "blocks": [{"type":"text","text":"must not commit"}]
            }]
        }),
    )
    .await;
    let claim_at = now_ms();
    let record = event_store
        .claim("delivery-stale-generation", claim_at, 30_000, 1)
        .unwrap()
        .pop()
        .unwrap();
    store
        .advance_session_input_generation(
            "s1",
            1,
            true,
            "test",
            "invalidate terminal after delivery claim",
            claim_at + 1,
        )
        .await
        .unwrap();

    let result = deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        None,
        "delivery-stale-generation",
        record,
    )
    .await;
    assert!(result.is_err());
    assert!(store
        .get_all_messages("s1")
        .await
        .unwrap()
        .iter()
        .all(|message| message.stable_message_id != "message-stale-generation"));
    assert!(
        rx.try_recv().is_err(),
        "a rejected stale terminal must not reach Surface projections"
    );
    let terminal = event_store
        .get("terminal-stale-generation")
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, "blocked");
    assert!(
        terminal
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("stale terminal fence")),
        "blocked terminal: {terminal:?}"
    );
}

#[tokio::test]
async fn corrupt_terminal_is_poisoned_and_visible_to_operations() {
    let (
        _runtime_event_store,
        event_store,
        artifacts,
        session_service,
        _store,
        event_bus,
        _runtime_services,
        _rx,
    ) = delivery_fixture().await;
    event_store
        .enqueue("poison", "m2", "s1", 8, "not-typed")
        .unwrap();
    let record = event_store
        .claim("worker", 100, 10, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert!(deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        None,
        "worker",
        record
    )
    .await
    .is_err());
    let poison = event_store.blocked(10).unwrap();
    assert_eq!(poison.len(), 1);
    assert_eq!(poison[0].terminal_id, "poison");
    assert_eq!(poison[0].failure_class.as_deref(), Some("corrupt_payload"));
}

#[tokio::test]
async fn typed_terminal_atomically_materializes_usage_and_session_counters_before_ack() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        _runtime_services,
        _rx,
    ) = delivery_fixture().await;
    enqueue_fenced_terminal(
        &runtime_event_store,
        &store,
        "usage-terminal",
        "usage-message",
        "usage-request",
        "usage-turn",
        "usage-ingress",
        &artifacts,
        serde_json::json!({
            "schema_version": 1,
            "text": "done",
            "ingress_message_id": "usage-ingress",
            "consumed_input_sequence": 0,
            "token_usage": {
                "input_tokens": 12,
                "output_tokens": 3,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            },
            "transcript": [{
                "role": "assistant",
                "blocks": [{"type":"text","text":"done"}]
            }]
        }),
    )
    .await;
    let record = event_store
        .claim("worker", now_ms(), 30_000, 1)
        .unwrap()
        .pop()
        .unwrap();

    deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        None,
        "worker",
        record,
    )
    .await
    .unwrap();

    let session = store.get_session("s1").await.unwrap().unwrap();
    let messages = store.get_messages("s1", 0, 10).await.unwrap();
    assert_eq!(session.message_count, 2);
    assert_eq!(session.input_tokens, 12);
    assert_eq!(session.output_tokens, 3);
    let terminal = messages
        .iter()
        .find(|message| message.stable_message_id == "usage-message")
        .unwrap();
    assert_eq!(
        terminal
            .token_usage_json
            .as_deref()
            .and_then(|usage| serde_json::from_str::<serde_json::Value>(usage).ok())
            .and_then(|usage| usage["output_tokens"].as_u64()),
        Some(3)
    );
}

#[tokio::test]
async fn cancelled_execution_fence_suppresses_late_terminal_materialization() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        runtime_services,
        _rx,
    ) = delivery_fixture().await;
    let request_id = "cancel-wins-request";
    let execution_id = format!("execution:{request_id}");
    runtime_services.record_live_execution(
        "s1",
        execution_id.clone(),
        "cancel-wins-turn".to_string(),
    );
    assert!(runtime_services
        .try_cancel_live_execution(
            &execution_id,
            "user cancelled before terminal materialization".to_string(),
        )
        .unwrap());
    enqueue_fenced_terminal(
        &runtime_event_store,
        &store,
        "cancel-wins-terminal",
        "cancel-wins-message",
        request_id,
        "cancel-wins-turn",
        "cancel-wins-ingress",
        &artifacts,
        serde_json::json!({
            "schema_version": 1,
            "text": "late answer",
            "goal_completion": "satisfied",
            "ingress_message_id": "cancel-wins-ingress",
            "consumed_input_sequence": 0,
            "token_usage": {"input_tokens": 0, "output_tokens": 2},
            "transcript": [{"role":"assistant","blocks":[{"type":"text","text":"late answer"}]}]
        }),
    )
    .await;
    let record = event_store
        .claim("cancel-fence-worker", now_ms(), 30_000, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert!(!deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        Some(runtime_services.as_ref()),
        "cancel-fence-worker",
        record,
    )
    .await
    .unwrap());
    let messages = store.get_all_messages("s1").await.unwrap();
    assert!(messages.iter().all(|message| message.role != "assistant"));
    let suppressed = event_store
        .get("cancel-wins-terminal")
        .unwrap()
        .expect("suppressed terminal remains auditable");
    assert_eq!(suppressed.status, "suppressed");
    assert_eq!(
        suppressed.failure_class.as_deref(),
        Some("terminal_fence_conflict")
    );
    assert!(suppressed
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("terminal fence")));
    assert!(!event_store.has_unsettled_for_session("s1").unwrap());
    assert_eq!(event_store.health().unwrap().suppressed, 1);
    assert!(event_store
        .materialized_after("s1", 0, 10)
        .unwrap()
        .is_empty());
    assert!(event_store
        .claim("cancel-fence-retry", now_ms(), 30_000, 10)
        .unwrap()
        .is_empty());
    assert_eq!(
        runtime_services
            .execution_live(&execution_id)
            .unwrap()
            .status,
        harness_contract::projection::ExecutionLiveStatus::Cancelled
    );
}

#[tokio::test]
async fn partial_terminal_claims_error_instead_of_complete() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        runtime_services,
        _rx,
    ) = delivery_fixture().await;
    let request_id = "partial-terminal-request";
    let execution_id = format!("execution:{request_id}");
    runtime_services.record_live_execution(
        "s1",
        execution_id.clone(),
        "partial-terminal-turn".to_string(),
    );
    enqueue_fenced_terminal(
            &runtime_event_store,
            &store,
            "partial-terminal",
            "partial-message",
            request_id,
            "partial-terminal-turn",
            "partial-ingress",
            &artifacts,
            serde_json::json!({
                "schema_version": 1,
                "text": "partial answer with preserved findings",
                "goal_completion": "partial",
                "ingress_message_id": "partial-ingress",
                "consumed_input_sequence": 0,
                "token_usage": {"input_tokens": 1, "output_tokens": 5},
                "transcript": [{"role":"assistant","blocks":[{"type":"text","text":"partial answer with preserved findings"}]}]
            }),
        )
        .await;
    let record = event_store
        .claim("partial-worker", now_ms(), 30_000, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert!(deliver_terminal(
        &event_store,
        &artifacts,
        &session_service,
        &event_bus,
        Some(runtime_services.as_ref()),
        "partial-worker",
        record,
    )
    .await
    .unwrap());
    let live = runtime_services.execution_live(&execution_id).unwrap();
    assert_eq!(
        live.status,
        harness_contract::projection::ExecutionLiveStatus::Error
    );
    assert_eq!(live.terminal_ref.as_deref(), Some("partial-terminal"));
    assert!(store
        .get_all_messages("s1")
        .await
        .unwrap()
        .iter()
        .any(|message| message.role == "assistant"));
}

#[tokio::test]
async fn delivery_worker_wakes_on_commit_and_shuts_down_gracefully() {
    let (
        runtime_event_store,
        event_store,
        artifacts,
        session_service,
        store,
        event_bus,
        _runtime_services,
        _rx,
    ) = delivery_fixture().await;
    let (shutdown, receiver) = watch::channel(false);
    let (ready, ready_rx) = oneshot::channel();
    let mut commit_observer = event_store.subscribe_commits();
    let handle = tokio::spawn(run_delivery_worker(
        event_store.clone(),
        Arc::clone(&artifacts),
        session_service,
        event_bus,
        None,
        test_backend_reporter("terminal_delivery"),
        receiver,
        ready,
    ));
    ready_rx.await.unwrap().unwrap();
    enqueue_fenced_terminal(
        &runtime_event_store,
        &store,
        "wake-terminal",
        "wake-message",
        "wake-request",
        "wake-turn",
        "wake-ingress",
        &artifacts,
        serde_json::json!({
            "schema_version": 1,
            "text": "awake",
            "ingress_message_id": "wake-ingress",
            "consumed_input_sequence": 0,
            "token_usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            },
            "transcript": [{
                "role": "assistant",
                "blocks": [{"type":"text","text":"awake"}]
            }]
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), commit_observer.changed())
        .await
        .expect("terminal transaction must publish a commit notification")
        .expect("terminal commit signal remains open");
    let delivered = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if store.get_message_count("s1").await.unwrap() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "commit notification must wake terminal delivery before fallback polling; terminal={:?}",
        event_store.get("wake-terminal").unwrap()
    );
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker must observe graceful shutdown")
        .unwrap()
        .unwrap();
}

fn fast_supervisor_config() -> WorkerSupervisorConfig {
    WorkerSupervisorConfig {
        restart_base: Duration::from_millis(2),
        restart_max: Duration::from_millis(8),
        startup_timeout: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(25),
    }
}

#[test]
fn backend_reporter_exposes_failure_threshold_and_resets_on_success() {
    let reporter = test_backend_reporter("ingress");
    assert!(!reporter.failure("failure-1"));
    assert!(!reporter.failure("failure-2"));
    assert!(reporter.failure("failure-3"));
    let failed = worker_observation(&reporter.states, "ingress").unwrap();
    assert_eq!(failed.consecutive_backend_failures, 3);
    assert_eq!(failed.last_backend_error.as_deref(), Some("failure-3"));

    reporter.success(Some(42));
    let recovered = worker_observation(&reporter.states, "ingress").unwrap();
    assert_eq!(recovered.consecutive_backend_failures, 0);
    assert_eq!(recovered.oldest_queue_age_ms, Some(42));
    assert!(recovered.last_backend_error.is_none());
    assert!(recovered.last_backend_success_at_ms.is_some());
}

#[test]
fn permanent_reconciliation_failure_restarts_after_three_failed_rounds() {
    let reporter = test_backend_reporter("lifecycle_reconciliation");
    for round in 1..BACKEND_FAILURE_RESTART_THRESHOLD {
        finish_reconciliation_backend_round(
            &reporter,
            Some(100),
            Some("permanent operation failure".to_string()),
        )
        .unwrap_or_else(|error| panic!("round {round} restarted too early: {error}"));
        assert_eq!(
            worker_observation(&reporter.states, "lifecycle_reconciliation")
                .unwrap()
                .consecutive_backend_failures,
            round
        );
    }

    let error = finish_reconciliation_backend_round(
        &reporter,
        Some(100),
        Some("permanent operation failure".to_string()),
    )
    .expect_err("the third failed reconciliation round must restart the worker");
    assert_eq!(error, "permanent operation failure");
    assert_eq!(
        worker_observation(&reporter.states, "lifecycle_reconciliation")
            .unwrap()
            .consecutive_backend_failures,
        BACKEND_FAILURE_RESTART_THRESHOLD
    );
}

async fn wait_for_worker(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    predicate: impl Fn(&SessionWorkerObservation) -> bool,
) -> SessionWorkerObservation {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(observation) = worker_observation(states, name) {
                if predicate(&observation) {
                    break observation;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervised worker did not reach the expected state")
}

#[tokio::test]
async fn supervisor_restarts_panics_and_error_returns_with_bounded_backoff() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let release_error = Arc::new(Notify::new());
    let release_restart_readiness = Arc::new(Notify::new());
    let factory: WorkerFactory = Arc::new({
        let attempts = Arc::clone(&attempts);
        let release_error = Arc::clone(&release_error);
        let release_restart_readiness = Arc::clone(&release_restart_readiness);
        move |mut shutdown, ready| {
            let attempts = Arc::clone(&attempts);
            let release_error = Arc::clone(&release_error);
            let release_restart_readiness = Arc::clone(&release_restart_readiness);
            Box::pin(async move {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if attempt == 0 {
                    panic!("deterministic supervised worker panic");
                }
                if attempt >= 2 {
                    release_restart_readiness.notified().await;
                }
                signal_worker_ready(ready)?;
                match attempt {
                    1 => {
                        release_error.notified().await;
                        Err("deterministic worker error".to_string())
                    }
                    _ => {
                        let _ = shutdown.changed().await;
                        Ok(())
                    }
                }
            })
        }
    });
    let (shutdown, receiver) = watch::channel(false);
    let mut supervised = spawn_supervised(
        "deterministic",
        Arc::clone(&states),
        Arc::clone(&forced_aborts),
        receiver,
        factory,
        fast_supervisor_config(),
    );

    let after_panic = wait_for_worker(&states, "deterministic", |observation| {
        observation.state == SessionWorkerState::Running
            && observation.restart_count == 1
            && attempts.load(std::sync::atomic::Ordering::SeqCst) == 2
    })
    .await;
    assert!(after_panic
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("panicked")));

    release_error.notify_one();
    let restarting = wait_for_worker(&states, "deterministic", |observation| {
        observation.state == SessionWorkerState::Starting
            && observation.restart_count == 2
            && attempts.load(std::sync::atomic::Ordering::SeqCst) == 3
    })
    .await;
    assert_eq!(
        restarting.last_error.as_deref(),
        Some("deterministic worker error")
    );
    release_restart_readiness.notify_one();
    let after_error = wait_for_worker(&states, "deterministic", |observation| {
        observation.state == SessionWorkerState::Running
            && observation.restart_count == 2
            && attempts.load(std::sync::atomic::Ordering::SeqCst) == 3
    })
    .await;
    assert_eq!(
        after_error.last_error.as_deref(),
        Some("deterministic worker error")
    );
    assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 0);

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
        .await
        .expect("supervisor must join after shutdown")
        .unwrap();
    let stopped = worker_observation(&states, "deterministic").unwrap();
    assert_eq!(stopped.state, SessionWorkerState::Stopped);
    assert_eq!(stopped.restart_count, 2);
}

#[tokio::test]
async fn graceful_shutdown_does_not_restart_worker() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let factory: WorkerFactory = Arc::new({
        let attempts = Arc::clone(&attempts);
        move |mut shutdown, ready| {
            let attempts = Arc::clone(&attempts);
            Box::pin(async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                signal_worker_ready(ready)?;
                let _ = shutdown.changed().await;
                Ok(())
            })
        }
    });
    let (shutdown, receiver) = watch::channel(false);
    let mut supervised = spawn_supervised(
        "graceful",
        Arc::clone(&states),
        Arc::clone(&forced_aborts),
        receiver,
        factory,
        fast_supervisor_config(),
    );
    wait_for_worker(&states, "graceful", |observation| {
        observation.state == SessionWorkerState::Running
    })
    .await;

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
        .await
        .expect("graceful supervisor must join")
        .unwrap();

    let observation = worker_observation(&states, "graceful").unwrap();
    assert_eq!(observation.state, SessionWorkerState::Stopped);
    assert_eq!(observation.restart_count, 0);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_aborts_and_joins_worker_that_refuses_to_drain() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let factory: WorkerFactory = Arc::new(|_, ready| {
        Box::pin(async move {
            signal_worker_ready(ready)?;
            std::future::pending::<()>().await;
            Ok(())
        })
    });
    let (shutdown, receiver) = watch::channel(false);
    let mut supervised = spawn_supervised(
        "hung",
        Arc::clone(&states),
        Arc::clone(&forced_aborts),
        receiver,
        factory,
        fast_supervisor_config(),
    );
    wait_for_worker(&states, "hung", |observation| {
        observation.state == SessionWorkerState::Running
    })
    .await;

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
        .await
        .expect("hung child must be aborted and supervisor joined")
        .unwrap();

    assert_eq!(
        worker_observation(&states, "hung").unwrap().state,
        SessionWorkerState::Aborted
    );
    assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn restart_delay_is_exponential_and_bounded() {
    let config = WorkerSupervisorConfig {
        restart_base: Duration::from_millis(10),
        restart_max: Duration::from_millis(25),
        startup_timeout: Duration::from_secs(1),
        shutdown_timeout: Duration::from_secs(1),
    };
    assert_eq!(
        supervisor_restart_delay(1, config),
        Duration::from_millis(10)
    );
    assert_eq!(
        supervisor_restart_delay(2, config),
        Duration::from_millis(20)
    );
    assert_eq!(
        supervisor_restart_delay(3, config),
        Duration::from_millis(25)
    );
    assert_eq!(
        supervisor_restart_delay(64, config),
        Duration::from_millis(25)
    );

    let states = Mutex::new(BTreeMap::new());
    let recorded_at_ms = now_ms();
    let delay = record_worker_restart(&states, "observed", "deterministic failure", config);
    let observation = worker_observation(&states, "observed").unwrap();
    assert_eq!(delay, Duration::from_millis(10));
    assert_eq!(observation.state, SessionWorkerState::Failed);
    assert_eq!(observation.restart_count, 1);
    assert_eq!(
        observation.last_error.as_deref(),
        Some("deterministic failure")
    );
    assert!(observation
        .next_retry_at_ms
        .is_some_and(|retry_at| retry_at >= recorded_at_ms.saturating_add(10)));
}

#[tokio::test]
async fn worker_remains_starting_until_child_signals_readiness() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let release_readiness = Arc::new(Notify::new());
    let factory: WorkerFactory = Arc::new({
        let release_readiness = Arc::clone(&release_readiness);
        move |mut shutdown, ready| {
            let release_readiness = Arc::clone(&release_readiness);
            Box::pin(async move {
                release_readiness.notified().await;
                signal_worker_ready(ready)?;
                let _ = shutdown.changed().await;
                Ok(())
            })
        }
    });
    let (shutdown, receiver) = watch::channel(false);
    let mut supervised = spawn_supervised(
        "readiness-gated",
        Arc::clone(&states),
        Arc::clone(&forced_aborts),
        receiver,
        factory,
        fast_supervisor_config(),
    );

    tokio::task::yield_now().await;
    assert_eq!(
        worker_observation(&states, "readiness-gated")
            .unwrap()
            .state,
        SessionWorkerState::Starting
    );
    release_readiness.notify_one();
    let ready = supervised.initial_ready.take().unwrap();
    tokio::time::timeout(Duration::from_secs(1), ready)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        worker_observation(&states, "readiness-gated")
            .unwrap()
            .state,
        SessionWorkerState::Running
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn startup_failure_rolls_back_all_started_workers() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (shutdown, receiver) = watch::channel(false);
    let mut workers = Vec::new();
    for name in REQUIRED_SESSION_WORKERS {
        let should_fail = name == "working_set_cleanup";
        let factory: WorkerFactory = Arc::new(move |mut shutdown, ready| {
            Box::pin(async move {
                if should_fail {
                    let _ = ready.send(Err("deterministic startup failure".to_string()));
                    return Err("deterministic startup failure".to_string());
                }
                signal_worker_ready(ready)?;
                let _ = shutdown.changed().await;
                Ok(())
            })
        });
        workers.push(spawn_supervised(
            name,
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver.clone(),
            factory,
            fast_supervisor_config(),
        ));
    }

    let error = await_initial_worker_readiness(&mut workers, fast_supervisor_config())
        .await
        .expect_err("one failed worker must fail Session supervisor startup");
    assert!(error.contains("working_set_cleanup"));
    rollback_started_workers(
        &shutdown,
        &mut workers,
        &states,
        &forced_aborts,
        fast_supervisor_config(),
    )
    .await;

    assert!(*shutdown.borrow());
    assert!(workers.iter().all(|worker| worker.handle.is_finished()));
    assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
        worker_observation(&states, name).is_some_and(|observation| {
            observation.state != SessionWorkerState::Running
                && observation.state != SessionWorkerState::Starting
        })
    }));
}

#[tokio::test]
async fn startup_timeout_rolls_back_all_six_started_workers() {
    let states = Arc::new(Mutex::new(BTreeMap::new()));
    let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (shutdown, receiver) = watch::channel(false);
    let mut workers = Vec::new();
    let config = WorkerSupervisorConfig {
        startup_timeout: Duration::from_millis(20),
        ..fast_supervisor_config()
    };
    for name in REQUIRED_SESSION_WORKERS {
        let should_timeout = name == "terminal_delivery";
        let factory: WorkerFactory = Arc::new(move |mut shutdown, ready| {
            Box::pin(async move {
                if should_timeout {
                    let readiness_sender = ready;
                    std::future::pending::<()>().await;
                    drop(readiness_sender);
                    return Ok(());
                }
                signal_worker_ready(ready)?;
                let _ = shutdown.changed().await;
                Ok(())
            })
        });
        workers.push(spawn_supervised(
            name,
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver.clone(),
            factory,
            config,
        ));
    }

    let error = await_initial_worker_readiness(&mut workers, config)
        .await
        .expect_err("one readiness timeout must fail Session supervisor startup");
    assert!(error.contains("terminal_delivery"));
    assert!(error.contains("timed out"));
    rollback_started_workers(&shutdown, &mut workers, &states, &forced_aborts, config).await;

    assert!(*shutdown.borrow());
    assert!(workers.iter().all(|worker| worker.handle.is_finished()));
    assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
        worker_observation(&states, name).is_some_and(|observation| {
            observation.state != SessionWorkerState::Running
                && observation.state != SessionWorkerState::Starting
        })
    }));
}

#[tokio::test]
async fn reconciliation_workers_publish_continuous_runtime_progress() {
    let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
    let service = test_session_service(store, SessionProjectionHub::new());
    let progress = Arc::new(Mutex::new(reconciliation_progress_map()));
    let (shutdown, receiver) = watch::channel(false);
    let (lifecycle_ready, lifecycle_ready_rx) = oneshot::channel();
    let lifecycle = tokio::spawn(run_lifecycle_reconciliation_worker(
        Arc::clone(&service),
        None,
        None,
        None,
        Arc::clone(&progress),
        test_backend_reporter("lifecycle_reconciliation"),
        receiver.clone(),
        lifecycle_ready,
    ));
    let (branch_ready, branch_ready_rx) = oneshot::channel();
    let branch = tokio::spawn(run_branch_activation_reconciliation_worker(
        Arc::clone(&service),
        Arc::clone(&progress),
        test_backend_reporter("branch_activation_reconciliation"),
        receiver,
        branch_ready,
    ));
    lifecycle_ready_rx.await.unwrap().unwrap();
    branch_ready_rx.await.unwrap().unwrap();
    service.lifecycle_work_wake().notify_one();
    service.branch_work_wake().notify_one();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if snapshot
                .values()
                .all(|observation| observation.scan_count >= 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both reconciliation workers must update progress after an explicit wake");

    shutdown.send(true).unwrap();
    lifecycle.await.unwrap().unwrap();
    branch.await.unwrap().unwrap();
    let snapshot = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    for name in [
        "lifecycle_reconciliation",
        "branch_activation_reconciliation",
    ] {
        let observation = snapshot.get(name).unwrap();
        assert!(observation.scan_count >= 2);
        assert_eq!(observation.pending_count, 0);
        assert_eq!(observation.oldest_pending_age_ms, None);
        assert!(observation.last_scan_at_ms.is_some());
        assert!(observation.last_success_at_ms.is_some());
        assert!(observation.last_error.is_none());
    }
}

#[test]
fn reconciliation_progress_preserves_pending_age_cursor_and_failure() {
    let progress = Mutex::new(reconciliation_progress_map());
    begin_reconciliation_scan(
        &progress,
        "lifecycle_reconciliation",
        WORKER_BATCH + 1,
        true,
        Some(250),
        1_000,
    );
    let failure = Err("deterministic reconcile failure".to_string());
    record_reconciliation_outcome(
        &progress,
        "lifecycle_reconciliation",
        "operation-7",
        &failure,
        1_010,
    );
    finish_reconciliation_scan(&progress, "lifecycle_reconciliation", false, 1_020);

    let snapshot = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = snapshot.get("lifecycle_reconciliation").unwrap();
    assert_eq!(observation.scan_count, 1);
    assert_eq!(observation.pending_count, (WORKER_BATCH + 1) as u64);
    assert!(observation.pending_count_truncated);
    assert_eq!(observation.oldest_pending_age_ms, Some(750));
    assert_eq!(
        observation.last_operation_id.as_deref(),
        Some("operation-7")
    );
    assert_eq!(
        observation.last_error.as_deref(),
        Some("deterministic reconcile failure")
    );
    assert_eq!(observation.last_error_at_ms, Some(1_010));
    assert_eq!(observation.consecutive_failures, 1);
    drop(snapshot);

    finish_reconciliation_scan(&progress, "lifecycle_reconciliation", true, 1_030);
    let snapshot = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = snapshot.get("lifecycle_reconciliation").unwrap();
    assert_eq!(observation.consecutive_failures, 0);
    assert_eq!(
        observation.last_error.as_deref(),
        Some("deterministic reconcile failure"),
        "recovery must retain the most recent error as historical evidence"
    );
}
