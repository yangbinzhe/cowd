use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn terminal_delivery_parser_preserves_owner_and_typed_cancellation_receipt() {
    let started = serde_json::json!({
        "type": "TerminalDelivery",
        "session_id": "session-root",
        "execution_id": "execution-root",
        "turn_id": "turn-root",
        "delivery": {
            "event": "terminal_presentation_started",
            "presentation_id": "presentation-root",
            "attempt_id": "attempt-1",
            "envelope_id": "envelope-1",
            "envelope_revision": 3,
            "objective_scope": "root"
        }
    });
    assert!(matches!(
        gateway_sse_json_to_cowd_event_for_session(&started, Some("session-root")),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalDelivery {
                delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                    presentation_id,
                    ..
                },
                ..
            }
        }) if presentation_id == "presentation-root"
    ));

    let cancelled = serde_json::json!({
        "type": "TerminalDelivery",
        "session_id": "session-root",
        "execution_id": "execution-root",
        "turn_id": "turn-root",
        "delivery": {
            "event": "cancellation_committed",
            "receipt": {
                "cancellation_id": "cancel-1",
                "session_id": "session-root",
                "turn_id": "turn-root",
                "execution_id": "execution-root",
                "actor_id": "principal:user",
                "cause": "user_requested",
                "requested_at_ms": 100,
                "effective_at_ms": 110,
                "status": "cancelled",
                "journal_sequence": 8,
                "projection_revision": 1
            }
        }
    });
    assert!(matches!(
        gateway_sse_json_to_cowd_event_for_session(&cancelled, Some("session-root")),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalDelivery {
                delivery: harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                    receipt,
                },
                ..
            }
        }) if receipt.cancellation_id == "cancel-1" && receipt.journal_sequence == 8
    ));
}

#[test]
fn session_text_delta_preserves_runtime_causal_and_projection_identities() {
    let frame = concat!(
        "id: 7\n",
        "data: {",
        "\"type\":\"TextDelta\",",
        "\"session_id\":\"session-causal\",",
        "\"execution_id\":\"execution-causal\",",
        "\"turn_id\":\"turn-causal\",",
        "\"model_step_id\":\"step-causal\",",
        "\"item_id\":\"text-causal\",",
        "\"segment_id\":\"text-causal:text:0\",",
        "\"part_id\":\"execution-causal:assistant-output\",",
        "\"causal_sequence\":3,",
        "\"delta_sequence\":1,",
        "\"text\":\"ok\",",
        "\"start_bytes\":0,",
        "\"end_bytes\":2,",
        "\"stream_revision\":2",
        "}\n\n"
    );
    let event = strict_gateway_sse_frame_to_cowd_event_for_session(frame, "session-causal")
        .expect("valid causal frame")
        .expect("typed event");
    let CowdEvent::GatewaySession {
        event: GatewaySessionEvent::TextDelta {
            correlation, text, ..
        },
    } = event
    else {
        panic!("expected causal text delta");
    };
    assert_eq!(text, "ok");
    assert_eq!(
        correlation.part_id.as_deref(),
        Some("execution-causal:assistant-output")
    );
    assert_eq!(correlation.item_id.as_deref(), Some("text-causal"));
    assert_eq!(
        correlation.segment_id.as_deref(),
        Some("text-causal:text:0")
    );
}

#[test]
fn tui_consumes_the_canonical_live_envelope_fixture() {
    let envelope: harness_contract::live::LiveEnvelope =
        serde_json::from_str(harness_contract::live::LIVE_ENVELOPE_CANONICAL_FIXTURE_JSON)
            .expect("TUI must consume the canonical live fixture");
    assert_eq!(envelope.subscription_id, "subscription-contract");
    assert_eq!(envelope.subscription_revision, 7);
    assert_eq!(envelope.source_cursor, Some(42));
    assert_eq!(envelope.event, "TerminalCommitted");
    assert_eq!(
        harness_contract::live::live_envelope_schema_hash(),
        "53ccc1bb8fb6896f1e648035dad6985aba8754b2e5d88e47b7687ddc492a346c"
    );
}

#[test]
fn live_source_selector_recomputes_detail_without_regressing_cursor() {
    let (summary_tx, _) = mpsc::channel(4);
    let (full_tx, _) = mpsc::channel(4);
    let mut source = LiveSourceState {
        selector: harness_contract::live::LiveSourceSelector {
            kind: harness_contract::live::LiveSourceKind::Execution,
            id: "execution-1".to_string(),
            cursor: 11,
            revision: 1,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
        },
        subscribers: BTreeMap::from([
            (
                "summary".to_string(),
                LiveSubscriber {
                    selector: harness_contract::live::LiveSourceSelector {
                        kind: harness_contract::live::LiveSourceKind::Execution,
                        id: "execution-1".to_string(),
                        cursor: 5,
                        revision: 1,
                        detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
                    },
                    tx: summary_tx,
                },
            ),
            (
                "full".to_string(),
                LiveSubscriber {
                    selector: harness_contract::live::LiveSourceSelector {
                        kind: harness_contract::live::LiveSourceKind::Execution,
                        id: "execution-1".to_string(),
                        cursor: 7,
                        revision: 1,
                        detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
                    },
                    tx: full_tx,
                },
            ),
        ]),
        pending_previews: BTreeMap::new(),
    };

    refresh_tui_live_source_selector(&mut source);
    assert_eq!(
        source.selector.detail_scope,
        harness_contract::projection::ProjectionDetailScope::Full
    );
    assert_eq!(source.selector.cursor, 11);

    source.subscribers.remove("full");
    refresh_tui_live_source_selector(&mut source);
    assert_eq!(
        source.selector.detail_scope,
        harness_contract::projection::ProjectionDetailScope::Summary
    );
    assert_eq!(source.selector.cursor, 11);
}

#[tokio::test]
async fn live_transport_interruption_is_visible_to_every_source() {
    let (session_tx, mut session_rx) = mpsc::channel(4);
    let (mission_tx, mut mission_rx) = mpsc::channel(4);
    let session_selector = harness_contract::live::LiveSourceSelector {
        kind: harness_contract::live::LiveSourceKind::Session,
        id: "session-1".to_string(),
        cursor: 13,
        revision: 0,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
    };
    let mission_selector = harness_contract::live::LiveSourceSelector {
        kind: harness_contract::live::LiveSourceKind::Mission,
        id: "mission-1".to_string(),
        cursor: 0,
        revision: 0,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
    };
    let mut sources = BTreeMap::from([
        (
            session_selector.key(),
            LiveSourceState {
                selector: session_selector.clone(),
                subscribers: BTreeMap::from([(
                    "session-view".to_string(),
                    LiveSubscriber {
                        selector: session_selector,
                        tx: session_tx,
                    },
                )]),
                pending_previews: BTreeMap::new(),
            },
        ),
        (
            mission_selector.key(),
            LiveSourceState {
                selector: mission_selector.clone(),
                subscribers: BTreeMap::from([(
                    "mission-view".to_string(),
                    LiveSubscriber {
                        selector: mission_selector,
                        tx: mission_tx,
                    },
                )]),
                pending_previews: BTreeMap::new(),
            },
        ),
    ]);

    deliver_tui_live_resync(&mut sources, &None, "gateway unavailable").await;

    let session = session_rx.recv().await.expect("session resync");
    let mission = mission_rx.recv().await.expect("mission resync");
    assert_eq!(
        session.source_health,
        harness_contract::live::SourceHealth::ResyncRequired
    );
    assert_eq!(
        mission.source_health,
        harness_contract::live::SourceHealth::ResyncRequired
    );
    assert_eq!(session.source_cursor, Some(13));
    assert_eq!(session.payload["reason"], "gateway unavailable");
    assert_eq!(mission.payload["origin"], "tui_live_transport");
}

#[tokio::test]
async fn snapshot_reconstructable_events_use_reliable_bounded_delivery() {
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(harness_contract::live::canonical_live_envelope_fixture())
        .await
        .expect("prefill subscriber queue");
    let selector = harness_contract::live::LiveSourceSelector {
        kind: harness_contract::live::LiveSourceKind::Session,
        id: "session-contract".to_string(),
        cursor: 42,
        revision: 0,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
    };
    let mut sources = BTreeMap::from([(
        selector.key(),
        LiveSourceState {
            selector: selector.clone(),
            subscribers: BTreeMap::from([(
                "execution-view".to_string(),
                LiveSubscriber { selector, tx },
            )]),
            pending_previews: BTreeMap::new(),
        },
    )]);
    let mut envelope = harness_contract::live::canonical_live_envelope_fixture();
    envelope.delivery_class = harness_contract::live::DeliveryClass::SnapshotReconstructable;
    envelope.event = "ExecutionPhase".to_string();

    let mut delivery = Box::pin(deliver_tui_live_envelope(&mut sources, envelope));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut delivery)
            .await
            .is_err(),
        "reconstructable event must wait instead of replacing queued causal data"
    );
    assert_eq!(
        rx.recv().await.expect("prefilled event").event,
        "TerminalCommitted"
    );
    tokio::time::timeout(Duration::from_secs(1), delivery)
        .await
        .expect("reliable delivery resumes when bounded capacity is available");
    assert_eq!(
        rx.recv().await.expect("reconstructable event").event,
        "ExecutionPhase"
    );
}

#[tokio::test]
async fn session_text_delta_backpressures_instead_of_restarting_a_saturated_source() {
    let (tx, mut rx) = crate::cowd_event_channel();
    for index in 0..256 {
        tx.send(CowdEvent::ReasoningSummaryDelta {
            summary: format!("queued-{index}"),
        })
        .expect("fill primary event queue");
    }
    let event = CowdEvent::GatewaySession {
        event: GatewaySessionEvent::TextDelta {
            correlation: GatewayEventCorrelation {
                session_id: "session-1".to_string(),
                execution_id: Some("execution-1".to_string()),
                turn_id: Some("turn-1".to_string()),
                part_id: Some("item-text-1:text:0".to_string()),
                ..Default::default()
            },
            text: "visible before terminal".to_string(),
            start_bytes: 0,
            end_bytes: 23,
            stream_revision: 23,
        },
    };

    let mut delivery = Box::pin(deliver_session_stream_event(&tx, "session-1", event, 1));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut delivery)
            .await
            .is_err(),
        "a saturated render queue must apply backpressure instead of failing the live source"
    );
    assert!(matches!(
        rx.try_recv().expect("free one queue slot"),
        CowdEvent::ReasoningSummaryDelta { .. }
    ));
    tokio::time::timeout(Duration::from_secs(1), delivery)
        .await
        .expect("delta delivery resumes when one bounded slot is available")
        .expect("healthy live source remains connected");

    let delivered = std::iter::from_fn(|| rx.try_recv().ok()).any(|event| {
        matches!(
            event,
            CowdEvent::SessionScoped {
                session_id,
                authority_generation: 1,
                event,
            } if session_id == "session-1"
                && matches!(
                    event.as_ref(),
                    CowdEvent::GatewaySession {
                        event: GatewaySessionEvent::TextDelta { text, .. }
                    } if text == "visible before terminal"
                )
        )
    });
    assert!(
        delivered,
        "accepted preview must reach the TUI reducer exactly once"
    );
}

#[test]
fn normalize_base_url_trims_trailing_slashes() {
    assert_eq!(
        normalize_base_url(" http://127.0.0.1:8642/// ".to_string()).unwrap(),
        "http://127.0.0.1:8642"
    );
    assert!(normalize_base_url("127.0.0.1:8642".to_string()).is_err());
}

#[test]
fn url_encode_encodes_session_ids() {
    assert_eq!(url_encode("session a/b"), "session%20a%2Fb");
}

#[test]
fn generic_app_transport_accepts_only_local_paths_and_non_reserved_metadata() {
    assert_eq!(app_method("post").expect("method"), reqwest::Method::POST);
    assert!(validate_app_path("/api/apps/fixture/read").is_ok());
    assert!(validate_app_path("https://example.invalid/api/apps/fixture/read").is_err());
    assert!(validate_app_path("/api/apps/fixture/../admin").is_err());
    assert!(validate_app_path("/api/apps/fixture/%2e%2e/admin").is_err());
    assert!(app_headers(&BTreeMap::from([(
        "x-fixture-cursor".to_string(),
        "42".to_string(),
    )]))
    .is_ok());
    assert!(app_headers(&BTreeMap::from([(
        "authorization".to_string(),
        "Bearer leaked".to_string(),
    )]))
    .is_err());
}

#[test]
fn declarative_app_stream_uses_the_gateway_view_namespace() {
    assert_eq!(
        app_view_stream_path("reference", "detail:42"),
        "/api/apps/reference/tui/views/detail:42/stream"
    );
}

fn reference_app_detail() -> GatewayAppDetailResponseV1 {
    let manifest: AppManifestV1 = serde_json::from_str(include_str!(
        "../../../../app-protocol/contracts/v1/golden/app-manifest.json"
    ))
    .expect("manifest");
    let handshake: cowd_app_protocol::AppHandshakeV1 = serde_json::from_str(include_str!(
        "../../../../app-protocol/contracts/v1/golden/handshake-success.json"
    ))
    .expect("handshake");
    let entry = serde_json::from_value(serde_json::json!({
        "app_id": "reference-app",
        "display_name": "Reference APP",
        "artifact_version": "1.0.0",
        "generation": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "required": false,
        "activation": "lazy",
        "lifecycle": {"state": "mounted", "retryable": false},
        "compatibility": {
            "status": "compatible",
            "gateway_supported_minimum": 1,
            "gateway_supported_maximum": 1,
            "app_required_minimum": 1,
            "app_required_maximum": 1
        },
        "web_surface": {"available": false, "bridge_revision": 1},
        "effective_capabilities": ["reference-app.read", "reference-app.write"],
        "effective_authorization_profile": "default"
    }))
    .expect("entry");
    GatewayAppDetailResponseV1 {
        schema_version: 1,
        entry,
        manifest,
        operations: handshake.operations,
    }
}

#[test]
fn sanitized_app_detail_binds_catalog_manifest_operations_and_signed_tui_roles() {
    let detail = reference_app_detail();
    detail
        .validate_against_catalog_entry(&detail.entry)
        .expect("valid sanitized detail");
    let encoded = serde_json::to_value(&detail).expect("encode detail");
    assert!(encoded.get("handshake").is_none());
    assert!(encoded.get("worker_nonce").is_none());
    assert!(encoded.get("worker_pid").is_none());
    assert!(!detail
        .manifest
        .presentation
        .as_ref()
        .expect("presentation")
        .result_contracts
        .is_empty());

    let mut generation_tamper = detail.entry.clone();
    generation_tamper.generation.0 =
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned();
    assert!(detail
        .validate_against_catalog_entry(&generation_tamper)
        .is_err());

    let mut digest_tamper = detail.clone();
    digest_tamper.operations.pop();
    assert!(digest_tamper
        .validate_against_catalog_entry(&detail.entry)
        .is_err());

    let mut role_tamper = detail.clone();
    let stream = role_tamper
        .operations
        .iter_mut()
        .find(|operation| operation.operation_id.ends_with(".stream"))
        .expect("stream operation");
    stream.output_schema_digest = stream.input_schema_digest.clone();
    role_tamper.manifest.operation_catalog_digest =
        app_operation_catalog_digest_v1(&role_tamper.manifest.app_id, &role_tamper.operations)
            .expect("tampered catalog digest");
    assert!(role_tamper
        .validate_against_catalog_entry(&detail.entry)
        .is_err());

    let mut unknown = encoded;
    unknown["worker_pid"] = serde_json::json!(9);
    assert!(serde_json::from_value::<GatewayAppDetailResponseV1>(unknown).is_err());
}

#[tokio::test]
async fn declarative_app_stream_posts_typed_request_and_cancels_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept stream");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = socket.read(&mut buffer).await.expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&request);
            let Some(header_end) = text.find("\r\n\r\n") else {
                continue;
            };
            let content_length = text[..header_end]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or_default();
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
        let text = String::from_utf8(request).expect("utf8 request");
        let _ = request_tx.send(text);
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n: ready\n\n",
                )
                .await
                .expect("write SSE headers");
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (event_tx, _event_rx) = crate::events::cowd_event_channel();
    let stream = tokio::spawn(async move {
        client
            .subscribe_app_view_stream(
                AppViewStreamRequest {
                    app_id: "reference-app".to_owned(),
                    view_id: "main".to_owned(),
                    request: AppTuiViewStreamRequestV1 {
                        schema_version: 1,
                        view_id: "main".to_owned(),
                        document_revision: "revision-7".to_owned(),
                        cursor: Some("cursor-3".to_owned()),
                    },
                    session_id: "session-1".to_owned(),
                    authority_generation: 4,
                },
                cancel_rx,
                event_tx,
            )
            .await
    });
    let request = tokio::time::timeout(Duration::from_secs(1), request_rx)
        .await
        .expect("request timeout")
        .expect("captured request");
    assert!(request.starts_with("POST /api/apps/reference-app/tui/views/main/stream HTTP/1.1"));
    assert!(request.contains("\"view_id\":\"main\""));
    assert!(request.contains("\"document_revision\":\"revision-7\""));
    assert!(request.contains("\"cursor\":\"cursor-3\""));
    cancel_tx.send(true).expect("cancel stream");
    tokio::time::timeout(Duration::from_secs(1), stream)
        .await
        .expect("stream cancel timeout")
        .expect("stream task")
        .expect("clean cancellation");
    server.abort();
}

#[test]
fn tui_gateway_request_delegates_capability_projection_to_gateway_catalog() {
    let request = authorize_tui_request(
        reqwest::Client::new().get("http://127.0.0.1:8642/healthz"),
        Some("  test-token  "),
        "tui:test-observer",
    )
    .build()
    .expect("decorated request should build");
    let headers = request.headers();

    assert_eq!(
        headers
            .get("x-cowd-surface-id")
            .and_then(|value| value.to_str().ok()),
        Some("tui")
    );
    assert_eq!(
        headers
            .get("x-cowd-observer-id")
            .and_then(|value| value.to_str().ok()),
        Some("tui:test-observer")
    );
    assert_eq!(
        headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer test-token")
    );
    assert!(headers.get("x-cowd-requested-capabilities").is_none());
}

#[test]
fn gateway_sse_json_maps_core_cowd_events() {
    assert!(matches!(
        gateway_sse_json_to_cowd_event_for_session(
            &serde_json::json!({
                "type": "UserMessageCommitted",
                "message_id": "tui:message-1",
                "sequence": 7,
                "execution_id": "execution-1",
                "turn_id": "turn-1",
                "content": "hello",
                "created_at_ms": 42
            }),
            Some("session-1")
        ),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::UserMessageCommitted {
                correlation: GatewayEventCorrelation {
                    session_id,
                    message_id: Some(message_id),
                    execution_id: Some(execution_id),
                    turn_id: Some(turn_id),
                    ..
                },
                sequence: 7,
                ..
            }
        }) if session_id == "session-1"
            && message_id == "tui:message-1"
            && execution_id == "execution-1"
            && turn_id == "turn-1"
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "TextDelta",
            "text": "hello",
            "start_bytes": 0,
            "end_bytes": 5,
            "stream_revision": 5
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TextDelta { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "ReasoningSummaryDelta",
            "summary": "checking"
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ReasoningSummaryDelta { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "ToolStart",
            "id": "tool-1",
            "name": "read"
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ToolStart { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "TerminalCommitted",
            "terminal_id": "terminal-1",
            "response": "done"
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalCommitted { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "TurnComplete",
            "assistant_text": "draft"
        })),
        None
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "ExecutionPhase",
            "status": "CallingModel",
            "detail": "requesting model"
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ExecutionPhase { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "SessionInputProjection",
            "projection": {
                "session_id": "session-1",
                "pending_count": 0,
                "inputs": []
            }
        })),
        Some(CowdEvent::SessionInputProjection { projection })
            if projection["pending_count"] == 0
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "SessionInputDispositionChanged",
            "receipt": {
                "disposition_id": "disposition-1",
                "state": "applied",
                "action": "add_required_task"
            }
        })),
        Some(CowdEvent::SessionInputDispositionChanged { receipt })
            if receipt["disposition_id"] == "disposition-1"
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "TurnInboxUpdated",
            "inbox": {
                "session_id": "session-1",
                "pending_count": 0,
                "items": []
            }
        })),
        Some(CowdEvent::SessionInputProjection { projection })
            if projection["inputs"].as_array().is_some_and(Vec::is_empty)
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "ContextEnvelope",
            "envelope": {
                "id": "ctx-v31",
                "selected": []
            }
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ContextEnvelope { .. }
        })
    ));
    assert!(matches!(
        gateway_sse_json_to_cowd_event(&serde_json::json!({
            "type": "TokenUsage",
            "input": 1,
            "output": 2,
            "cache_create": 3,
            "cache_read": 4
        })),
        Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TokenUsage { .. }
        })
    ));
}

#[test]
fn gateway_sse_frame_reads_durable_commit_cursor_from_event_id() {
    assert_eq!(
        gateway_sse_frame_commit_cursor("id: 73\ndata: {\"type\":\"TerminalCommitted\"}"),
        Some(73)
    );
    assert_eq!(
        gateway_sse_frame_commit_cursor("data: {\"type\":\"TextDelta\"}"),
        None
    );
}

#[test]
fn gateway_sse_frame_parses_canonical_projection_delta_only_from_named_event() {
    let delta = serde_json::json!({
        "schema_version": 2,
        "reducer_version": harness_contract::projection::EXECUTION_PROJECTION_REDUCER_VERSION,
        "execution_id": "graph-1",
        "from_revision": 1,
        "target_revision": 1,
        "base_cursor": 4,
        "target_cursor": 5,
        "detail_scope": "summary",
        "authorization_revision": 1,
        "redaction_revision": "sha256:test",
        "source_health": "fresh",
        "operations": [
            {
                "op": "advance_cursor",
                "cursor": 5
            }
        ],
        "resync_reason": null
    });
    let frame = format!("id: 5\nevent: projection_delta\ndata: {delta}");
    let parsed = gateway_sse_frame_projection_delta(&frame).expect("projection delta");
    assert_eq!(parsed.execution_id, "graph-1");
    assert_eq!(parsed.target_cursor, 5);
    assert!(gateway_sse_frame_projection_delta(&format!("data: {delta}")).is_none());
}

#[tokio::test]
async fn e10_projection_revocation_and_unknown_event_fail_closed_before_any_delta() {
    assert!(gateway_sse_frame_projection_authorization_revoked(
        "event: projection_authorization_revoked\ndata: {\"reason\":\"credential epoch changed\"}"
    ));
    assert!(!gateway_sse_frame_projection_authorization_revoked(
        "event: projection_delta\ndata: {}"
    ));

    let client = GatewayApiClient::new("http://127.0.0.1:1".to_string(), None).expect("client");
    let (tx, _rx) = crate::cowd_event_channel();
    let revoked = client
            .apply_execution_projection_sse_frame(
                "event: projection_authorization_revoked\ndata: {\"reason\":\"credential epoch changed\"}",
                "execution-e10",
                true,
                7,
                0,
                &tx,
            )
            .await
            .expect_err("revocation must terminate the projection stream");
    assert!(matches!(
        revoked,
        GatewayApiError::Status(reqwest::StatusCode::FORBIDDEN, message)
            if message.contains("revoked")
    ));

    let unknown = client
        .apply_execution_projection_sse_frame(
            "event: future_unregistered_projection\ndata: {}",
            "execution-e10",
            true,
            7,
            0,
            &tx,
        )
        .await
        .expect_err("unknown projection events must not mutate local state");
    assert!(matches!(
        unknown,
        GatewayApiError::Contract(message)
            if message.contains("unknown event `future_unregistered_projection`")
    ));
    assert!(matches!(
        validate_execution_projection_identity("execution-e10", "foreign-execution"),
        Err(GatewayApiError::Contract(message))
            if message.contains("foreign execution")
    ));
}

#[test]
fn session_sse_rejects_explicit_foreign_identity_for_all_ui_event_classes() {
    for event_type in ["Warning", "RuntimePolicyDecision", "ExecutionGraphSummary"] {
        let frame = format!(
                "event: message\ndata: {{\"type\":\"{event_type}\",\"session_id\":\"foreign-session\",\"message\":\"foreign\"}}\n\n"
            );
        let error = strict_gateway_sse_frame_to_cowd_event_for_session(&frame, "session-current")
            .expect_err("an explicit foreign session must fail closed before parsing");
        assert!(
            error.contains("does not match subscribed session"),
            "{event_type}: {error}"
        );
    }
    assert!(matches!(
        validate_session_authorization_revoke_identity(
            "event: message\ndata: {\"type\":\"SessionAuthorizationRevoked\",\"session_id\":\"foreign-session\",\"reason\":\"foreign revoke\"}\n\n",
            "session-current",
        ),
        Err(GatewayApiError::Contract(message))
            if message.contains("does not match subscribed session")
    ));
}

#[tokio::test]
async fn session_http_contracts_reject_foreign_projection_history_input_and_index() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let responses = [
            (
                "/api/sessions/session-current/stats",
                serde_json::json!({"session_id":"foreign-session","tokens":{"total":0}}),
            ),
            (
                "/api/sessions/session-current/input-projection",
                serde_json::json!({
                    "session_id":"foreign-session",
                    "total":0,
                    "pending_count":0,
                    "queued_next_count":0,
                    "consumed_count":0,
                    "inputs":[],
                    "updated_at":"2026-07-24T00:00:00Z"
                }),
            ),
            (
                "/api/sessions/session-current/messages?offset=0&limit=1",
                serde_json::json!({
                    "session_id":"foreign-session",
                    "messages":[],
                    "total":0,
                    "offset":0,
                    "next_seq":0,
                    "limit":1,
                    "has_more":false
                }),
            ),
            (
                "/api/sessions/session-current/execution",
                serde_json::json!({
                    "session_id":"foreign-session",
                    "active_execution_ids":[]
                }),
            ),
        ];
        for (path, body) in responses {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = vec![0; 4096];
            let size = socket.read(&mut request).await.expect("read");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with(&format!("GET {path} HTTP/1.1")));
            let body = body.to_string();
            socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write");
        }
    });
    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");

    assert!(matches!(
        client.session_stats("session-current").await,
        Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
    ));
    assert!(matches!(
        client.session_input_projection("session-current").await,
        Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
    ));
    assert!(matches!(
        client
            .session_messages_offset("session-current", 0, 1)
            .await,
        Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
    ));
    assert!(matches!(
        client.session_execution_index("session-current").await,
        Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
    ));
    server.await.expect("server joins");
}

#[tokio::test]
async fn ensure_session_http_receipt_rejects_a_foreign_session_identity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut request = vec![0; 4096];
        let size = socket.read(&mut request).await.expect("read");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
            request.starts_with("POST /api/sessions/session-current/ensure HTTP/1.1"),
            "{request}"
        );
        let body = serde_json::json!({"ok":true,"session_id":"foreign-session"}).to_string();
        socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write");
    });
    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");

    assert!(matches!(
        client.ensure_session("session-current", "model").await,
        Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
    ));
    server.await.expect("server joins");
}

#[test]
fn every_session_operation_receipt_requires_the_requested_identity() {
    assert!(matches!(
        require_gateway_session_operation_ok(
            serde_json::json!({"ok":true,"session_id":"foreign-session"}),
            "ensure session",
            "session-current",
        ),
        Err(GatewayApiError::Contract(message))
            if message.contains("foreign-session")
    ));
    assert!(matches!(
        require_gateway_session_operation_ok(
            serde_json::json!({"ok":true}),
            "attach session",
            "session-current",
        ),
        Err(GatewayApiError::Contract(message))
            if message.contains("missing")
    ));
    assert!(validate_session_json_identity_at(
        "session-current",
        &serde_json::json!({
            "resource": {"session_id":"session-current"}
        }),
        "resource upload receipt",
        &["/resource/session_id"],
    )
    .is_ok());
    assert!(matches!(
        validate_session_json_identity_at(
            "session-current",
            &serde_json::json!({
                "resource": {"session_id":"foreign-session"}
            }),
            "resource upload receipt",
            &["/resource/session_id"],
        ),
        Err(GatewayApiError::Contract(message))
            if message.contains("foreign-session")
    ));
}

#[test]
fn gateway_sse_frame_maps_data_json() {
    let event = gateway_sse_frame_to_cowd_event(concat!(
        "event: message\n",
        "data: {",
        "\"type\":\"TextDelta\",",
        "\"session_id\":\"session-1\",",
        "\"execution_id\":\"execution-1\",",
        "\"turn_id\":\"turn-1\",",
        "\"model_step_id\":\"step-1\",",
        "\"item_id\":\"text-1\",",
        "\"segment_id\":\"text-1:text:0\",",
        "\"part_id\":\"execution-1:assistant-output\",",
        "\"causal_sequence\":1,",
        "\"delta_sequence\":1,",
        "\"text\":\"hi\",",
        "\"start_bytes\":0,",
        "\"end_bytes\":2,",
        "\"stream_revision\":2",
        "}\n\n"
    ))
    .expect("typed causal SSE event");
    assert!(matches!(
        event,
        CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TextDelta {
                correlation: GatewayEventCorrelation {
                    model_step_id: Some(model_step_id),
                    item_id: Some(item_id),
                    segment_id: Some(segment_id),
                    ..
                },
                ..
            }
        } if model_step_id == "step-1"
            && item_id == "text-1"
            && segment_id == "text-1:text:0"
    ));
    assert!(gateway_sse_frame_to_cowd_event("data: [DONE]\n\n").is_none());
    assert_eq!(
        gateway_sse_frame_resync_reason(
            "data: {\"type\":\"session_stream_resync\",\"reason\":\"transport_lag\"}\n\n"
        )
        .as_deref(),
        Some("transport_lag")
    );
    assert_eq!(
        gateway_sse_frame_resync_reason(
            "data: {\"type\":\"RuntimeStreamLagged\",\"skipped\":7}\n\n"
        )
        .as_deref(),
        Some("runtime relay lag (7 events skipped)")
    );
}

#[test]
fn permission_revision_event_is_typed_instead_of_becoming_an_unsupported_warning() {
    let value = serde_json::json!({
        "type": "PermissionRevisionChanged",
        "permission_mode": "workspace_write",
        "revision": 12,
        "applies_to_active_turn": true
    });

    let event = gateway_sse_json_to_cowd_event_for_session(&value, Some("session-a"))
        .expect("permission revision should remain a typed Runtime event");
    assert!(matches!(
        event,
        CowdEvent::PermissionRevisionChanged {
            permission_mode,
            revision: 12,
            applies_to_active_turn: true,
        } if permission_mode == "workspace_write"
    ));
}

#[tokio::test]
async fn typed_evolution_and_managed_agent_controls_use_gateway_owned_routes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let checks = [
            (
                "GET /api/evolution/evaluation-policy HTTP/1.1",
                Vec::<&str>::new(),
            ),
            (
                "GET /api/evolution/evaluation-policy/reviews HTTP/1.1",
                Vec::<&str>::new(),
            ),
            (
                "POST /api/evolution/reviews/release-1/decision HTTP/1.1",
                vec![
                    "\"decision\":\"approve\"",
                    "\"reason\":\"operator checked\"",
                ],
            ),
            (
                "POST /api/evolution/evaluation-policy/reviews/policy-1/decision HTTP/1.1",
                vec!["\"decision\":\"reject\"", "\"reason\":\"operator checked\""],
            ),
            (
                "GET /api/runtime/managed-agents HTTP/1.1",
                Vec::<&str>::new(),
            ),
            (
                "POST /api/runtime/managed-agents/dispatch HTTP/1.1",
                vec!["\"dispatcher_id\":\"tui-operator\"", "\"limit\":16"],
            ),
            (
                "POST /api/runtime/managed-agents/agent-1/health/reset HTTP/1.1",
                Vec::<&str>::new(),
            ),
        ];
        for (expected_start, expected_fragments) in checks {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read request");
            let request = String::from_utf8_lossy(&buf[..n]);
            assert!(request.starts_with(expected_start), "request was {request}");
            for fragment in expected_fragments {
                assert!(request.contains(fragment), "request was {request}");
            }
            assert!(
                !request.contains("actor_principal"),
                "TUI must not supply an approval actor: {request}"
            );
            socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                    )
                    .await
                    .expect("write response");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
        .expect("client");
    assert_eq!(
        client.evolution_evaluation_policy().await.expect("policy")["ok"],
        true
    );
    assert_eq!(
        client
            .evolution_evaluation_policy_reviews()
            .await
            .expect("policy reviews")["ok"],
        true
    );
    assert_eq!(
        client
            .evolution_review_decision("release-1", "approve", "operator checked")
            .await
            .expect("release decision")["ok"],
        true
    );
    assert_eq!(
        client
            .evolution_evaluation_policy_review_decision("policy-1", "reject", "operator checked",)
            .await
            .expect("policy decision")["ok"],
        true
    );
    assert_eq!(client.managed_agents().await.expect("agents")["ok"], true);
    assert_eq!(
        client
            .dispatch_managed_agents("tui-operator", 16)
            .await
            .expect("dispatch")["ok"],
        true
    );
    assert_eq!(
        client
            .reset_managed_agent_health("agent-1")
            .await
            .expect("health reset")["ok"],
        true
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn session_history_index_is_typed_bounded_and_session_scoped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with(
            "GET /api/sessions/session-1/history-index?metadata_limit=128&card_limit=64 HTTP/1.1"
        ));
        assert!(req.contains("authorization: Bearer test-token"));
        let body = serde_json::json!({
            "schema_version": 1,
            "session_id": "session-1",
            "projection_generation": 9,
            "durable_cursor": 42,
            "event_cursor": 41,
            "history_revision": 7,
            "total_messages": 100000,
            "total_bytes": 8000000,
            "latest_checkpoint_sequence": 90000,
            "latest_checkpoint_event_id": "checkpoint-1",
            "index_generation": 4,
            "indexed_through_sequence": 99999,
            "index_card_count": 250,
            "index_complete": true,
            "recovery_state": "ready",
            "recent_metadata": [],
            "cards": []
        })
        .to_string();
        socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
        .expect("client");
    let projection = client
        .session_history_index("session-1")
        .await
        .expect("history index");
    assert_eq!(projection.session_id, "session-1");
    assert_eq!(projection.total_messages, 100_000);
    assert_eq!(projection.projection_generation, 9);
    assert!(projection.recent_metadata.is_empty());
    server.await.expect("server task");
}

#[tokio::test]
async fn runtime_control_plane_gets_json_with_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 2048];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /api/runtime/control-plane HTTP/1.1"));
        assert!(req.contains("authorization: Bearer test-token"));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
        .expect("client");
    let json = client.runtime_control_plane().await.expect("json");
    assert_eq!(json["ok"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn session_stream_holds_live_delta_until_revision_barrier_then_hydrates_history() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut subscription_socket, _) = listener.accept().await.expect("accept subscription");
        let mut request = vec![0; 4096];
        let size = subscription_socket
            .read(&mut request)
            .await
            .expect("read subscription request");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
            request.starts_with("POST /api/runtime/live-subscriptions HTTP/1.1"),
            "{request}"
        );
        let subscription = serde_json::json!({
            "schema_version": 1,
            "id": "live-test",
            "surface_instance": "tui:test",
            "revision": 1,
            "selector": {"sources": [{
                "kind": "session",
                "id": "session-1",
                "cursor": 0,
                "detail_scope": "summary"
            }]},
            "selector_hash": "selector-1",
            "expires_at_ms": u64::MAX,
            "stream_url": "/api/runtime/live/live-test"
        })
        .to_string();
        subscription_socket
                .write_all(
                    format!(
                        "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{subscription}",
                        subscription.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write subscription response");
        drop(subscription_socket);

        let (mut stream_socket, _) = listener.accept().await.expect("accept stream");
        let mut request = vec![0; 4096];
        let size = stream_socket
            .read(&mut request)
            .await
            .expect("read stream request");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
            request.starts_with("GET /api/runtime/live/live-test HTTP/1.1"),
            "{request}"
        );
        let ready = serde_json::json!({
            "schema_version": 1,
            "subscription_id": "live-test",
            "subscription_revision": 1,
            "source_kind": "subscription",
            "source_id": "live-test",
            "detail_scope": "summary",
            "delivery_class": "snapshot_reconstructable",
            "source_health": "baseline",
            "event": "subscription.ready",
            "payload": {"revision": 1}
        });
        let terminal = serde_json::json!({
            "schema_version": 1,
            "subscription_id": "live-test",
            "subscription_revision": 1,
            "source_kind": "session",
            "source_id": "session-1",
            "detail_scope": "summary",
            "source_cursor": 9,
            "delivery_class": "durable",
            "source_health": "live",
            "event": "TerminalCommitted",
            "payload": {
                "type": "TerminalCommitted",
                "session_id": "session-1",
                "execution_id": "execution-1",
                "turn_id": "turn-1",
                "part_id": "item-text-1:text:0",
                "message_id": "assistant-2",
                "terminal_id": "terminal-1",
                "response": "live answer"
            }
        });
        let resync = serde_json::json!({
            "schema_version": 1,
            "subscription_id": "live-test",
            "subscription_revision": 1,
            "source_kind": "session",
            "source_id": "session-1",
            "detail_scope": "summary",
            "source_cursor": 9,
            "delivery_class": "snapshot_reconstructable",
            "source_health": "resync_required",
            "event": "source.resync_required",
            "payload": {"reason": "fixture complete"}
        });
        stream_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: keep-alive\r\n\r\nevent: live\r\ndata: {terminal}\r\n\r\nevent: live\r\ndata: {ready}\r\n\r\nevent: live\r\ndata: {resync}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write stream response");

        let (mut probe_socket, _) = listener.accept().await.expect("accept history index");
        let mut request = vec![0; 4096];
        let size = probe_socket
            .read(&mut request)
            .await
            .expect("read history index");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
                request.starts_with(
                    "GET /api/sessions/session-1/history-index?metadata_limit=128&card_limit=64 HTTP/1.1"
                ),
                "{request}"
            );
        let history_index = serde_json::json!({
            "schema_version": 1,
            "session_id": "session-1",
            "projection_generation": 1,
            "durable_cursor": 1,
            "event_cursor": 1,
            "history_revision": 1,
            "total_messages": 1,
            "total_bytes": 64,
            "latest_checkpoint_sequence": null,
            "latest_checkpoint_event_id": null,
            "index_generation": 1,
            "indexed_through_sequence": 0,
            "index_card_count": 1,
            "index_complete": true,
            "recovery_state": "ready",
            "recent_metadata": [],
            "cards": []
        })
        .to_string();
        probe_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{history_index}",
                        history_index.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write history index");
        let history = serde_json::json!({
            "session_id": "session-1",
            "messages": [{
                "id": "user-1",
                "session_id": "session-1",
                "sequence": 0,
                "role": "user",
                "blocks": [{"type": "text", "text": "historical question"}],
                "created_at_ms": 1
            }],
            "total": 1,
            "offset": 0,
            "from_seq": 0,
            "next_seq": 1,
            "limit": 500,
            "has_more": false
        })
        .to_string();
        let (mut history_socket, _) = listener.accept().await.expect("accept history page");
        let mut request = vec![0; 4096];
        let size = history_socket
            .read(&mut request)
            .await
            .expect("read history page");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
            request.starts_with("GET /api/sessions/session-1/messages?offset=0&limit=500 HTTP/1.1"),
            "{request}"
        );
        history_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{history}",
                        history.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write history page");
        drop(stream_socket);
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let (tx, mut rx) = crate::cowd_event_channel();
    let progress = client
        .consume_session_live_source(
            "session-1",
            tx.clone(),
            None,
            Arc::new(AtomicUsize::new(0)),
            1,
        )
        .await
        .expect("subscribe");
    assert_eq!(progress.commit_cursor, Some(9));
    assert_eq!(progress.next_message_sequence, 1);
    drop(tx);

    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        CowdEvent::SessionScoped { session_id, event, .. }
            if session_id == "session-1"
                && matches!(event.as_ref(), CowdEvent::SessionHistoryPage {
                    page: SessionMessagesPage {
                        session_id,
                        has_more: false,
                        ..
                    }
                } if session_id == "session-1")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        CowdEvent::SessionScoped { session_id, event, .. }
            if session_id == "session-1"
                && matches!(event.as_ref(), CowdEvent::SessionStreamConnection {
                    session_id,
                    state: SessionStreamConnectionState::Connected
                } if session_id == "session-1")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        CowdEvent::SessionScoped {
            session_id, event, ..
        }
            if session_id == "session-1"
                && matches!(event.as_ref(), CowdEvent::GatewaySession {
                    event: GatewaySessionEvent::TerminalCommitted {
                        correlation: GatewayEventCorrelation {
                            message_id: Some(message_id),
                            terminal_id: Some(terminal_id),
                            ..
                        },
                        assistant_text,
                        ..
                    }
                } if message_id == "assistant-2"
                    && terminal_id == "terminal-1"
                    && assistant_text == "live answer")
    )));
    server.await.expect("server task");
}

#[tokio::test]
async fn e10_session_history_failure_is_a_typed_visible_recovery_event() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut subscription_socket, _) = listener.accept().await.expect("accept subscription");
        let mut request = vec![0; 4096];
        let _ = subscription_socket
            .read(&mut request)
            .await
            .expect("read subscription request");
        let subscription = serde_json::json!({
            "schema_version": 1,
            "id": "live-test",
            "surface_instance": "tui:test",
            "revision": 1,
            "selector": {"sources": [{
                "kind": "session",
                "id": "session-1",
                "cursor": 0,
                "detail_scope": "summary"
            }]},
            "selector_hash": "selector-1",
            "expires_at_ms": u64::MAX,
            "stream_url": "/api/runtime/live/live-test"
        })
        .to_string();
        subscription_socket
                .write_all(
                    format!(
                        "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{subscription}",
                        subscription.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write subscription response");
        drop(subscription_socket);

        let (mut stream_socket, _) = listener.accept().await.expect("accept stream");
        let mut request = vec![0; 4096];
        let _ = stream_socket
            .read(&mut request)
            .await
            .expect("read stream request");
        let resync = serde_json::json!({
            "schema_version": 1,
            "subscription_id": "live-test",
            "subscription_revision": 1,
            "source_kind": "session",
            "source_id": "session-1",
            "detail_scope": "summary",
            "source_cursor": 0,
            "delivery_class": "snapshot_reconstructable",
            "source_health": "resync_required",
            "event": "source.resync_required",
            "payload": {"reason": "hydrate using durable history"}
        });
        stream_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nevent: live\r\ndata: {resync}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write stream response");

        for _ in 0..4 {
            let (mut socket, _) = listener.accept().await.expect("accept follow-up request");
            let mut request = vec![0; 4096];
            let size = socket
                .read(&mut request)
                .await
                .expect("read follow-up request");
            let request = String::from_utf8_lossy(&request[..size]);
            if request.starts_with("GET /api/sessions/session-1/history-index") {
                socket
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\ncontent-length: 31\r\nconnection: close\r\n\r\n{\"error\":\"history unavailable\"}",
                        )
                        .await
                        .expect("write history response");
                break;
            }
            if request.starts_with("DELETE /api/runtime/live-subscriptions/live-test") {
                socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"ok\":true}",
                        )
                        .await
                        .expect("write delete response");
                continue;
            }
            panic!("unexpected follow-up request: {request}");
        }
        drop(stream_socket);
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let (tx, mut rx) = crate::cowd_event_channel();
    let progress = client
        .consume_session_live_source(
            "session-1",
            tx.clone(),
            None,
            Arc::new(AtomicUsize::new(0)),
            1,
        )
        .await
        .expect("live stream remains usable while history retries");
    assert_eq!(progress.next_message_sequence, 0);
    let hydration = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .hydrate_session_history("session-1", tx, Arc::new(AtomicUsize::new(0)), 1)
                .await;
        }
    });
    let failure = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let Ok(event) = rx.try_recv() else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            if matches!(
                event,
                CowdEvent::SessionScoped { ref session_id, ref event, .. }
                    if session_id == "session-1"
                        && matches!(event.as_ref(), CowdEvent::SessionHistoryHydrationFailed {
                            session_id,
                            error
                        } if session_id == "session-1" && error.contains("history unavailable"))
            ) {
                break event;
            }
        }
    })
    .await
    .expect("typed hydration failure should be visible without timing assumptions");
    assert!(matches!(
        failure,
        CowdEvent::SessionScoped { session_id, .. } if session_id == "session-1"
    ));
    hydration.abort();
    server.await.expect("server task");
}

#[tokio::test]
async fn tui_message_identity_is_reused_for_durable_message_and_idempotency() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept message");
        let mut request = vec![0; 8192];
        let size = socket.read(&mut request).await.expect("read message");
        let request = String::from_utf8_lossy(&request[..size]);
        assert!(
            request.starts_with("POST /api/sessions/session-1/messages HTTP/1.1"),
            "{request}"
        );
        assert!(
            request.contains("\"client_message_id\":\"tui:message-1\""),
            "{request}"
        );
        assert!(
            request.contains("\"idempotency_key\":\"tui:message-1\""),
            "{request}"
        );
        assert!(!request.contains("tui:tui:"), "{request}");
        let body = r#"{"session_id":"session-1","status":"accepted"}"#;
        let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let response = client
        .send_message_with_resources("session-1", "hello", &[], Some("tui:message-1"))
        .await
        .expect("send");
    assert_eq!(response["session_id"], "session-1");
    assert_eq!(response["status"], "accepted");
    server.await.expect("server task");
}

#[tokio::test]
async fn cowd_projection_gets_surface_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept projection");
        let mut buf = vec![0; 2048];
        let n = socket.read(&mut buf).await.expect("read projection");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /api/cowd/projection?surface=tui HTTP/1.1"));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"surface\":\"tui\",\"capability_count\":1,\"capabilities\":[]}",
                )
                .await
                .expect("write projection");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let json = client.cowd_projection("tui").await.expect("json");
    assert_eq!(json["surface"], "tui");
    assert_eq!(json["capability_count"], 1);
    server.await.expect("server task");
}

#[tokio::test]
async fn gateway_contract_endpoints_get_json_with_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let routes = [
            (
                "/api/gateway/capability-contract",
                r#"{"kind":"gateway.capability_contract","capability_count":1,"capabilities":[]}"#,
            ),
            (
                "/api/gateway/openai-tools",
                r#"{"kind":"gateway.openai_tools","tool_count":1,"tools":[]}"#,
            ),
        ];
        for (path, body) in routes {
            let (mut socket, _) = listener.accept().await.expect("accept contract");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read contract");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with(&format!("GET {path} HTTP/1.1")), "{req}");
            assert!(req.contains("authorization: Bearer test-token"));
            socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write contract");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
        .expect("client");
    let contract = client
        .gateway_capability_contract()
        .await
        .expect("contract");
    let tools = client.gateway_openai_tools().await.expect("tools");
    assert_eq!(contract["kind"], "gateway.capability_contract");
    assert_eq!(tools["kind"], "gateway.openai_tools");
    server.await.expect("server task");
}

#[tokio::test]
async fn session_stats_gets_canonical_session_totals() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept stats");
        let mut buf = vec![0; 2048];
        let n = socket.read(&mut buf).await.expect("read stats");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /api/sessions/session%20v31/stats HTTP/1.1"));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"session_id\":\"session v31\",\"tokens\":{\"input\":10,\"output\":2,\"total\":12}}",
                )
                .await
                .expect("write stats");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let json = client.session_stats("session v31").await.expect("json");
    assert_eq!(json["tokens"]["total"], 12);
    server.await.expect("server task");
}

#[tokio::test]
async fn session_execution_index_uses_the_gateway_contract_route() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept execution index");
        let mut buf = vec![0; 2048];
        let n = socket.read(&mut buf).await.expect("read execution index");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /api/sessions/session%20v31/execution HTTP/1.1"));
        let body = r#"{"session_id":"session v31","active_execution_ids":[]}"#;
        socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write execution index");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let index = client
        .session_execution_index("session v31")
        .await
        .expect("execution index");
    assert_eq!(index.session_id, "session v31");
    assert!(index.active_execution_ids.is_empty());
    server.await.expect("server task");
}

#[tokio::test]
async fn structured_projection_gets_all_list_contracts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let routes = [
            (
                "/api/cowd/structured/facts",
                r#"{"kind":"cowd.structured.facts","count":1,"items":[]}"#,
            ),
            (
                "/api/cowd/structured/evidence",
                r#"{"kind":"cowd.structured.evidence","count":1,"items":[]}"#,
            ),
            (
                "/api/cowd/structured/watermarks",
                r#"{"kind":"cowd.structured.watermarks","count":1,"items":[]}"#,
            ),
        ];
        for (path, body) in routes {
            let (mut socket, _) = listener.accept().await.expect("accept structured");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read structured");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with(&format!("GET {path} HTTP/1.1")),
                "unexpected request: {req}"
            );
            socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write structured");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    assert_eq!(
        client.structured_facts().await.expect("facts")["kind"],
        "cowd.structured.facts"
    );
    assert_eq!(
        client.structured_evidence().await.expect("evidence")["kind"],
        "cowd.structured.evidence"
    );
    assert_eq!(
        client.structured_watermarks().await.expect("watermarks")["kind"],
        "cowd.structured.watermarks"
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn reality_projection_gets_status_flow_and_boundaries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let routes = [
            (
                "/api/reality/status",
                r#"{"kind":"reality.status","status":"ready","engines":{}}"#,
            ),
            (
                "/api/reality/flow?session_id=session-tui",
                r#"{"kind":"reality.fact_flow","source":"growth.promotions","session_id":"session-tui","stages":[],"events":[],"promotions":[]}"#,
            ),
            (
                "/api/reality/boundaries",
                r#"{"kind":"reality.boundaries","boundaries":[]}"#,
            ),
        ];
        for (path, body) in routes {
            let (mut socket, _) = listener.accept().await.expect("accept reality");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read reality");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with(&format!("GET {path} HTTP/1.1")),
                "unexpected request: {req}"
            );
            socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write reality");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    assert_eq!(
        client.reality_status().await.expect("status")["kind"],
        "reality.status"
    );
    assert_eq!(
        client
            .reality_flow(Some("session-tui"))
            .await
            .expect("flow")["source"],
        "growth.promotions"
    );
    assert_eq!(
        client.reality_boundaries().await.expect("boundaries")["kind"],
        "reality.boundaries"
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn runtime_session_lease_control_uses_http_routes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept acquire");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read acquire");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/runtime/session-leases/acquire HTTP/1.1"));
        assert!(req.contains("\"session_id\":\"session-1\""));
        assert!(req.contains("\"mode\":\"collaborative\""));
        assert!(req.contains("x-cowd-observer-id:"));
        assert!(!req.contains("\"observer_id\":"));
        assert!(!req.contains("\"owner\":"));
        let body = r#"{"ok":true,"session_id":"session-1","mode":"collaborative"}"#;
        let response = format!(
                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write acquire");

        let (mut socket, _) = listener.accept().await.expect("accept release");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read release");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/runtime/session-leases/release HTTP/1.1"));
        assert!(req.contains("\"session_id\":\"session-1\""));
        assert!(req.contains("x-cowd-observer-id:"));
        assert!(!req.contains("\"observer_id\":"));
        assert!(!req.contains("\"owner\":"));
        let body = r#"{"ok":true,"session_id":"session-1","released":true}"#;
        let response = format!(
                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write release");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let acquired = client
        .acquire_runtime_session_lease("session-1", "collaborative")
        .await
        .expect("acquire");
    assert_eq!(acquired["ok"], true);
    let released = client
        .release_runtime_session_lease("session-1")
        .await
        .expect("release");
    assert_eq!(released["released"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn preflight_cross_plane_action_posts_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/cross-plane/action/preflight HTTP/1.1"));
        assert!(req.contains("authorization: Bearer test-token"));
        assert!(req.contains("\"operation\":\"send_text\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 14\r\n\r\n{\"ready\":true}",
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
        .expect("client");
    let json = client
        .preflight_cross_plane_action(serde_json::json!({
            "operation": "send_text",
            "capability": "surface.webui.send",
        }))
        .await
        .expect("json");
    assert_eq!(json["ready"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn surface_send_posts_gateway_surface_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/surfaces/webui/send HTTP/1.1"));
        assert!(req.contains("\"recipient\":\"user:demo\""));
        assert!(req.contains("\"text\":\"hello\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 39\r\n\r\n{\"kind\":\"surface.result\",\"status\":\"ok\"}",
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let json = client
        .surface_send(
            "webui",
            "user:demo",
            None,
            "hello",
            serde_json::json!({"source": "test"}),
        )
        .await
        .expect("json");
    assert_eq!(json["status"], "ok");
    server.await.expect("server task");
}

#[tokio::test]
async fn respond_approval_posts_decision() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/approval/respond HTTP/1.1"));
        assert!(req.contains("\"id\":\"approval-1\""));
        assert!(req.contains("\"approved\":true"));
        assert!(req.contains("\"scope\":\"session\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\n\r\n{\"resolved\":true}",
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let json = client
        .respond_approval("approval-1", true, Some("session"), None)
        .await
        .expect("json");
    assert_eq!(json["resolved"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn connector_resources_gets_search_page() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0; 2048];
        let n = socket.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with(
            "GET /api/connectors/resources?limit=20&offset=40&q=Ready%20Doc HTTP/1.1"
        ));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16\r\n\r\n{\"resources\":[]}",
                )
                .await
                .expect("write");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let json = client
        .connector_resources(Some("Ready Doc"), 20, 40)
        .await
        .expect("json");
    assert!(json["resources"].as_array().unwrap().is_empty());
    server.await.expect("server task");
}

#[tokio::test]
async fn message_plane_endpoints_use_gateway_routes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let routes = [
            (
                "GET",
                "/api/message-connectors",
                r#"{"kind":"message.connector.registry","connectors":[]}"#,
            ),
            (
                "GET",
                "/api/message-connectors/feishu/status",
                r#"{"kind":"message.connector.status","connector":"feishu"}"#,
            ),
            (
                "POST",
                "/api/message-connectors/feishu/repair",
                r#"{"kind":"message.connector.repair","connector":"feishu"}"#,
            ),
            (
                "GET",
                "/api/message-endpoints",
                r#"{"kind":"message.endpoint.directory","endpoints":[]}"#,
            ),
            (
                "GET",
                "/api/message-routes",
                r#"{"kind":"message.delivery.routes","routes":[]}"#,
            ),
            (
                "GET",
                "/api/message-bindings",
                r#"{"kind":"message.conversation.bindings","bindings":[]}"#,
            ),
        ];
        for (method, path, body) in routes {
            let (mut socket, _) = listener.accept().await.expect("accept message plane");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read message plane");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with(&format!("{method} {path} HTTP/1.1")),
                "unexpected request: {req}"
            );
            socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write message plane");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    assert_eq!(
        client.message_connectors().await.expect("connectors")["kind"],
        "message.connector.registry"
    );
    assert_eq!(
        client
            .message_connector_status("feishu")
            .await
            .expect("status")["kind"],
        "message.connector.status"
    );
    assert_eq!(
        client
            .message_connector_repair("feishu")
            .await
            .expect("repair")["kind"],
        "message.connector.repair"
    );
    assert_eq!(
        client.message_endpoints().await.expect("endpoints")["kind"],
        "message.endpoint.directory"
    );
    assert_eq!(
        client.message_routes().await.expect("routes")["kind"],
        "message.delivery.routes"
    );
    assert_eq!(
        client.message_bindings().await.expect("bindings")["kind"],
        "message.conversation.bindings"
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn connector_service_tools_and_execute_use_management_routes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept tools");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read tools");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /api/connectors/services/local.docs/tools HTTP/1.1"));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 12\r\n\r\n{\"tools\":[]}",
                )
                .await
                .expect("write tools");

        let (mut socket, _) = listener.accept().await.expect("accept execute");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read execute");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/connectors/services/local.docs/execute HTTP/1.1"));
        assert!(req.contains("\"tool_id\":\"service.local.docs.read\""));
        assert!(req.contains("\"mode\":\"dry_run\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write execute");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let tools = client
        .connector_service_tools("local.docs")
        .await
        .expect("tools");
    assert!(tools["tools"].as_array().unwrap().is_empty());
    let result = client
        .execute_connector_service(
            "local.docs",
            serde_json::json!({
                "actor_principal": "tui:operator",
                "tool_id": "service.local.docs.read",
                "resource_id": "tui-doc",
                "title": "TUI Doc",
                "mode": "dry_run",
            }),
        )
        .await
        .expect("execute");
    assert_eq!(result["ok"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn tool_operations_routes_use_management_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let checks: Vec<(&str, &str, Vec<&str>)> = vec![
            ("GET", "/api/tools", vec![]),
            (
                "POST",
                "/api/tools/execute",
                vec!["\"name\":\"tool_cache_stats\"", "\"mode\":\"read_only\""],
            ),
            ("GET", "/api/tools/cache", vec![]),
            (
                "POST",
                "/api/tools/batch-readonly",
                vec!["\"max_concurrency\":3", "\"name\":\"tool_cache_stats\""],
            ),
            (
                "POST",
                "/api/tools/mutations/preview",
                vec!["\"path\":\"README.md\""],
            ),
            (
                "POST",
                "/api/tools/mutations/apply",
                vec!["\"expected_hashes\"", "\"README.md\":\"hash-1\""],
            ),
            ("GET", "/api/tools/checkpoints", vec![]),
            (
                "POST",
                "/api/tools/checkpoints",
                vec!["\"label\":\"before edit\""],
            ),
            ("GET", "/api/tools/checkpoints/cp-1/diff", vec![]),
            ("POST", "/api/tools/checkpoints/cp-1/restore", vec![]),
            (
                "POST",
                "/api/tools/intent-plan",
                vec!["\"prompt\":\"inspect\"", "\"selected_tools\""],
            ),
            (
                "POST",
                "/api/tools/context-fanout/plan",
                vec!["\"prompt\":\"fanout\""],
            ),
            (
                "GET",
                "/api/runtime/timeline?session_id=session%20a%2Fb&limit=25",
                vec![],
            ),
            (
                "POST",
                "/api/cross-plane/policy/simulate",
                vec!["\"requested_capability\":\"service.read\""],
            ),
        ];

        for (method, path, needles) in checks {
            let (mut socket, _) = listener.accept().await.expect("accept tool ops");
            let mut buf = vec![0; 8192];
            let n = socket.read(&mut buf).await.expect("read tool ops");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with(&format!("{method} {path} HTTP/1.1")),
                "unexpected request for {method} {path}: {req}"
            );
            for needle in needles {
                assert!(req.contains(needle), "missing `{needle}` in request: {req}");
            }
            socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                    )
                    .await
                    .expect("write tool ops");
        }
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    assert_eq!(client.tool_registry().await.expect("registry")["ok"], true);
    assert_eq!(
        client
            .tool_execute("tool_cache_stats", serde_json::json!({}), "read_only")
            .await
            .expect("execute")["ok"],
        true
    );
    assert_eq!(client.tool_cache_stats().await.expect("cache")["ok"], true);
    assert_eq!(
        client
            .tool_batch_readonly(
                vec![serde_json::json!({ "name": "tool_cache_stats", "input": {} })],
                3,
            )
            .await
            .expect("batch")["ok"],
        true
    );
    let edits = vec![serde_json::json!({
        "path": "README.md",
        "old_string": "A",
        "new_string": "B"
    })];
    assert_eq!(
        client
            .tool_mutation_preview(edits.clone())
            .await
            .expect("preview")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_mutation_apply(edits, serde_json::json!({ "README.md": "hash-1" }))
            .await
            .expect("apply")["ok"],
        true
    );
    assert_eq!(
        client.tool_checkpoints().await.expect("checkpoints")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_checkpoint_create("before edit")
            .await
            .expect("checkpoint create")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_checkpoint_diff("cp-1")
            .await
            .expect("checkpoint diff")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_checkpoint_restore("cp-1")
            .await
            .expect("checkpoint restore")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_intent_plan("inspect", vec!["tool_cache_stats".to_string()])
            .await
            .expect("intent")["ok"],
        true
    );
    assert_eq!(
        client
            .tool_context_fanout_plan("fanout")
            .await
            .expect("fanout")["ok"],
        true
    );
    assert_eq!(
        client
            .runtime_timeline("session a/b", 25)
            .await
            .expect("timeline")["ok"],
        true
    );
    assert_eq!(
        client
            .cross_plane_policy_simulate(serde_json::json!({
                "requested_capability": "service.read"
            }))
            .await
            .expect("policy simulate")["ok"],
        true
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn connector_resource_lifecycle_routes_use_management_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept revalidate");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read revalidate");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/connectors/resources/revalidate HTTP/1.1"));
        assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
        assert!(req.contains("\"state\":\"stale\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write revalidate");

        let (mut socket, _) = listener.accept().await.expect("accept promote");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read promote");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("POST /api/connectors/resources/promote-memory HTTP/1.1"));
        assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
        assert!(req.contains("\"session_id\":\"session-tui\""));
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write promote");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let revalidated = client
        .revalidate_connector_resource("service://local.docs/document/tui-doc", "stale")
        .await
        .expect("revalidate");
    assert_eq!(revalidated["ok"], true);
    let promoted = client
        .promote_connector_resource_to_memory(
            "service://local.docs/document/tui-doc",
            Some("session-tui"),
        )
        .await
        .expect("promote");
    assert_eq!(promoted["ok"], true);
    server.await.expect("server task");
}

#[tokio::test]
async fn create_session_forwards_execution_policy_preset_and_omits_when_absent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept preset create");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read preset create");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(
            req.starts_with("POST /api/sessions HTTP/1.1"),
            "unexpected request: {req}"
        );
        let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
        let parsed: serde_json::Value = serde_json::from_str(body).expect("session body");
        assert_eq!(parsed["model"], "deepseek-v4");
        assert_eq!(parsed["execution_policy_preset"], "yolo");
        let response_body = r#"{"id":"session-preset","title":"preset","model":"deepseek-v4"}"#;
        socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write preset create");

        let (mut socket, _) = listener.accept().await.expect("accept plain create");
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read plain create");
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(
            req.starts_with("POST /api/sessions HTTP/1.1"),
            "unexpected request: {req}"
        );
        let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
        let parsed: serde_json::Value = serde_json::from_str(body).expect("session body");
        assert_eq!(parsed["model"], "deepseek-v4");
        assert!(parsed.get("execution_policy_preset").is_none());
        let response_body = r#"{"id":"session-plain","title":"plain","model":"deepseek-v4"}"#;
        socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write plain create");
    });

    let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
    let preset = client
        .create_session(Some("deepseek-v4"), Some("yolo"))
        .await
        .expect("create with preset");
    assert_eq!(preset["id"], "session-preset");
    let plain = client
        .create_session(Some("deepseek-v4"), None)
        .await
        .expect("create without preset");
    assert_eq!(plain["id"], "session-plain");
    server.await.expect("server task");
}
