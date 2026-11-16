//! Drive a real Host/Graph terminal through the public Session receipt routes.
use super::*;

#[test]
fn rc_real_host_graph_public_session_ack_and_revocation() {
    public_receipt_case_on_a_large_stack(false);
}

#[test]
fn rc_real_host_graph_replay_then_public_session_ack() {
    public_receipt_case_on_a_large_stack(true);
}

/// The case drives a real Host/Graph turn through a deep future. Its debug
/// state machine does not fit the 2 MiB stack libtest gives a test thread, so
/// a plain `#[tokio::test]` aborts before the product can answer. Run the same
/// body on a thread sized for the case: this is a harness property, not a
/// product budget change.
fn public_receipt_case_on_a_large_stack(restart_before_outbox_ack: bool) {
    std::thread::Builder::new()
        .name("rc-public-receipt".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime")
                .block_on(public_receipt_case(restart_before_outbox_ack))
        })
        .expect("large-stack thread")
        .join()
        .expect("public receipt case panicked");
}

async fn public_receipt_case(restart_before_outbox_ack: bool) {
    let (root, mut provider, mut producer, mut reviewer, providers, mut services) =
        Box::pin(recovery_delivery_services()).await;
    // Keep the original isolated failure namespace for owner-side diagnosis
    // even if the later terminal observer times out outside durable_turn_until.
    eprintln!("rc_terminal_failure_namespace_retained={}",
        services.topology.preserve_failed_test_namespace().is_some());
    let principal = crate::api_routes::test_human_principal();
    let owner = principal.claims().principal_id.clone();
    let mut turn = Box::pin(ReviewedTurn::produce_with_optional_for_owner(
        &services,
        &mut provider,
        &mut producer,
        &mut reviewer,
        false,
        false,
        None,
        false,
        true,
        &owner,
    ))
    .await;
    let mut before_restart_digest = None;
    let terminal = if restart_before_outbox_ack {
        let (original, mut fault) = Box::pin(turn
            .delivery_barrier(&services, &mut provider, Point::BeforeAck))
            .await;
        let messages = session_messages(&services.session, &turn.session_id).await;
        let original_message = messages
            .iter()
            .find(|message| message.stable_message_id == original.message_id)
            .expect("fault reached after original Session commit");
        before_restart_digest = Some(
            session::public_terminal_payload_v1(&original_message.content_json)
                .unwrap()
                .content_digest,
        );
        eprintln!("rc_fault_hit=BeforeAck restart_scope=same_process_service_rebuild original_session=true");
        services.runtime.stop_accepting_for_recovery();
        services.supervisor.stop_accepting();
        fault.release(Release::ResponseLost).unwrap();
        turn.observer = observe(turn.request_identity()).unwrap();
        services = Box::pin(restart_services(root.path(), services, providers)).await;
        let (reclaimed, mut replay) = Box::pin(turn
            .delivery_barrier(&services, &mut provider, Point::BeforeAck))
            .await;
        assert_ne!(reclaimed.claim_owner, original.claim_owner);
        assert!(reclaimed.revision > original.revision);
        assert!(
            services
                .topology
                .runtime_event_store
                .ack_session_terminal(
                    &original.terminal_id,
                    original.claim_owner.as_deref().unwrap(),
                    original.revision,
                    recovery_now_ms()
                )
                .is_err(),
            "stale outbox claimant accepted"
        );
        assert_eq!(reclaimed.commit_cursor, original.commit_cursor);
        assert_eq!(reclaimed.message_id, original.message_id);
        replay.release(Release::Continue).unwrap();
        Box::pin(turn.assert_materialized(&services, &original)).await;
        original
    } else {
        let (terminal, _) = Box::pin(drive_final_until(&mut provider, &mut turn.observer.observed))
            .await
            .expect("original Host must commit a terminal");
        turn.observer.bind().unwrap();
        Box::pin(turn.assert_materialized(&services, &terminal)).await;
        terminal
    };
    assert_eq!(
        std::fs::read(root.path().join("workspace/recovery-effect.txt")).unwrap(),
        b"recovery-effect-once\n"
    );

    // Test router uses its real authentication middleware and the same Session
    // and Runtime services which produced this terminal; it does not create a
    // second terminal or manufacture an ACK.
    let mut state = crate::api_routes::tests::test_state();
    let app_state = Arc::get_mut(&mut state).unwrap();
    app_state.config_home = services.config_home.clone();
    app_state.workspace_root = root.path().join("workspace");
    let gateway_services = Arc::make_mut(&mut app_state.services);
    gateway_services.session = Arc::clone(&services.session);
    gateway_services.runtime = Some(Arc::clone(&services.runtime));
    gateway_services.selected_storage = None;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, crate::api_routes::api_router(state))
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}/api/sessions/{}", turn.session_id);
    let observer = "rc-integration";
    let attach = client
        .post(format!("{base}/attach"))
        .header("x-cowd-observer-id", observer)
        .json(&json!({"surface":"test", "role":"reader", "receipt_protocol":"session-public-v1"}))
        .send()
        .await
        .unwrap();
    assert!(
        attach.status().is_success(),
        "real reader attachment failed: {}",
        attach.status()
    );
    let attach: Value = attach.json().await.unwrap();
    let client_id = attach["receipt_attachment"]["client_id"].as_str().unwrap();
    let message_url = format!("{base}/messages/{}", terminal.message_id);
    let context = client
        .get(format!("{message_url}/receipt-context"))
        .header("x-cowd-observer-id", observer)
        .header("x-cowd-client-id", client_id)
        .send()
        .await
        .unwrap();
    assert!(context.status().is_success());
    let before: harness_contract::live::SessionMessageReceiptContext =
        context.json().await.unwrap();
    let (cursor, digest) = match before {
        harness_contract::live::SessionMessageReceiptContext::Available {
            session_id,
            message_id,
            client_id: actual_client,
            runtime_commit_cursor,
            content_digest,
            payload_json,
            session_source,
            outbox_observation,
            receipt,
            ..
        } => {
            use sha2::Digest;
            assert_eq!(
                content_digest,
                format!(
                    "session-public-v1:sha256:{:x}",
                    sha2::Sha256::digest(payload_json.as_bytes())
                )
            );
            assert_eq!(session_source.request_id, turn.request_id);
            assert_eq!(session_source.turn_id, turn.turn_id);
            assert_eq!(
                session_source.session_generation,
                turn.input.session_generation.to_string()
            );
            assert_eq!(
                session_source.input_sequence,
                turn.input.sequence.to_string()
            );
            if let harness_contract::live::SessionReceiptOutboxObservation::RuntimeOutbox {
                terminal_id,
                runtime_commit_cursor: observed_cursor,
                execution_id,
                turn_id,
                request_id,
                ..
            } = outbox_observation
            {
                assert_eq!(terminal_id, turn.terminal_id);
                assert_eq!(observed_cursor, runtime_commit_cursor);
                assert!(execution_id.as_deref().is_none_or(|id| id == turn.graph_id));
                assert_eq!(turn_id, turn.turn_id);
                assert_eq!(request_id, turn.request_id);
            }
            assert_eq!(session_id, turn.session_id);
            assert_eq!(message_id, terminal.message_id);
            assert_eq!(actual_client, client_id);
            assert!(receipt.is_none());
            (runtime_commit_cursor.value().unwrap(), content_digest)
        }
        other => panic!("real terminal context unavailable: {other:?}"),
    };
    assert_eq!(cursor, terminal.commit_cursor);
    if let Some(before_restart_digest) = before_restart_digest {
        assert_eq!(
            digest, before_restart_digest,
            "replay changed the public payload digest"
        );
    }
    let ack_url = format!("{message_url}/ack");
    for (identity, body) in [
        (
            client_id.to_owned(),
            json!({"runtime_commit_cursor":cursor.saturating_add(1).to_string(), "content_digest":digest}),
        ),
        (
            uuid::Uuid::new_v4().to_string(),
            json!({"runtime_commit_cursor":cursor.to_string(), "content_digest":digest}),
        ),
        (
            client_id.to_owned(),
            json!({"runtime_commit_cursor":cursor.to_string(), "content_digest":format!("session-public-v1:sha256:{}", "0".repeat(64))}),
        ),
    ] {
        let response = client
            .post(&ack_url)
            .header("x-cowd-observer-id", observer)
            .header("x-cowd-client-id", identity)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            !response.status().is_success(),
            "invalid public ACK accepted"
        );
    }
    let body = json!({"runtime_commit_cursor":cursor.to_string(), "content_digest":digest});
    let response = client
        .post(&ack_url)
        .header("x-cowd-observer-id", observer)
        .header("x-cowd-client-id", client_id)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "public ACK failed: {}",
        response.status()
    );
    let ack: harness_contract::live::SessionMessageAck = response.json().await.unwrap();
    assert_eq!(ack.session_id, turn.session_id);
    assert_eq!(ack.message_id, terminal.message_id);
    assert_eq!(ack.client_id, client_id);
    assert_eq!(ack.runtime_commit_cursor.value().unwrap(), cursor);
    assert_eq!(ack.content_digest, digest);
    let current = client
        .get(format!("{message_url}/receipt-context"))
        .header("x-cowd-observer-id", observer)
        .header("x-cowd-client-id", client_id)
        .send()
        .await
        .unwrap();
    assert!(current.status().is_success());
    let current: harness_contract::live::SessionMessageReceiptContext =
        current.json().await.unwrap();
    match current {
        harness_contract::live::SessionMessageReceiptContext::Available {
            receipt: Some(receipt),
            ..
        } => assert_eq!(receipt, ack),
        other => panic!("ACK missing on public reread: {other:?}"),
    }
    let actor = format!("principal:{owner}:surface:{observer}");
    services
        .session
        .revoke_receipt_actor(&turn.session_id, &actor)
        .await
        .unwrap();
    let revoked = client
        .post(&ack_url)
        .header("x-cowd-observer-id", observer)
        .header("x-cowd-client-id", client_id)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(!revoked.status().is_success(), "revoked actor accepted");
    stop_tx.send(()).unwrap();
    server.await.unwrap();
    let _lease = services.stop().await;
}
