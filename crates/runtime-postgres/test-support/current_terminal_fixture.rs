use std::{path::Path, sync::Arc};

pub(crate) fn current_payload(
    terminal_id: &str,
    ingress_message_id: &str,
    consumed_input_sequence: usize,
    text: &str,
) -> serde_json::Value {
    serde_json::json!({
        "schema_version": runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,
        "text": text,
        "goal_completion": "satisfied",
        "ingress_message_id": ingress_message_id,
        "consumed_input_sequence": consumed_input_sequence,
        "collaboration_evidence": null,
        "token_usage": {
            "input_tokens": 0, "output_tokens": 0,
            "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0
        },
        "transcript": [{"role": "assistant", "blocks": [{"type": "text", "text": text}]}],
        "terminal_presentation": {
            "presentation_id": format!("presentation:{terminal_id}"),
            "attempt_id": format!("attempt:{terminal_id}"),
            "envelope_id": format!("envelope:{terminal_id}"),
            "envelope_revision": 1,
            "state": "committed", "answer_origin": "terminal_narrator",
            "models_attempted": [], "validation": {"status": "valid", "findings": []},
            "generated_at_ms": 10, "committed_at_ms": 11
        }
    })
}

pub(crate) async fn publish(
    events: &runtime::RuntimeEventStore,
    artifacts: &Arc<runtime::ArtifactStore>,
    terminal: &mut runtime::SessionTerminalInput,
    payload: &serde_json::Value,
) -> runtime::ArtifactRef {
    events.bind_artifact_store(Arc::clone(artifacts)).unwrap();
    let bytes = serde_json::to_vec(payload).unwrap();
    let artifact = artifacts
        .write_bytes(
            runtime::ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.session-terminal+json".into(),
                visibility_scope: format!("session:{}", terminal.session_id),
                expected_bytes: Some(bytes.len() as u64),
                original_name: Some(format!("{}.json", terminal.terminal_id)),
            },
            &bytes,
        )
        .await
        .unwrap();
    artifacts
        .pin(
            &artifact,
            &terminal.terminal_id,
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .unwrap();
    terminal.payload_ref = runtime::encode_session_terminal_artifact_ref(&artifact).unwrap();
    assert_eq!(artifacts.resolve(&artifact.selector).unwrap(), artifact);
    assert_eq!(
        artifacts
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .unwrap(),
        bytes
    );
    artifact
}

pub(crate) fn publish_file(
    events: &runtime::RuntimeEventStore,
    artifacts: &Arc<runtime::ArtifactStore>,
    source: &Path,
    terminal: &mut runtime::SessionTerminalInput,
    payload: &serde_json::Value,
) -> runtime::ArtifactRef {
    events.bind_artifact_store(Arc::clone(artifacts)).unwrap();
    let bytes = serde_json::to_vec(payload).unwrap();
    std::fs::write(source, &bytes).unwrap();
    let artifact = artifacts
        .write_path_blocking(
            runtime::ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.session-terminal+json".into(),
                visibility_scope: format!("session:{}", terminal.session_id),
                expected_bytes: Some(bytes.len() as u64),
                original_name: Some(format!("{}.json", terminal.terminal_id)),
            },
            source,
        )
        .unwrap();
    artifacts
        .pin(
            &artifact,
            &terminal.terminal_id,
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .unwrap();
    terminal.payload_ref = runtime::encode_session_terminal_artifact_ref(&artifact).unwrap();
    assert_eq!(artifacts.resolve(&artifact.selector).unwrap(), artifact);
    assert_eq!(
        artifacts
            .read_blocking(&artifact, &artifact.visibility_scope, None)
            .unwrap(),
        bytes
    );
    artifact
}

pub(crate) fn inline_graph_event(
    event: runtime::execution_core::graph::ExecutionGraphEvent,
    stream_id: &str,
    event_key: &str,
    binding: harness_contract::projection::RuntimeActivityBinding,
    refs: Vec<runtime::RuntimeEventRef>,
) -> runtime::RuntimeTransactionEventInput {
    use sha2::{Digest, Sha256};
    assert!(!event_key.trim().is_empty());
    let kind = event.kind();
    let value = serde_json::to_value(&event).unwrap();
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(bytes.len() <= 16 * 1024);
    runtime::RuntimeTransactionEventInput {
        event: runtime::RuntimeEventInput {
            stream_id: stream_id.into(),
            scope: runtime::RuntimeEventScope::ExecutionGraph,
            kind: kind.into(),
            status: Some("planned".into()),
            actor: Some("current-terminal-persistence-fixture".into()),
            refs,
            payload: serde_json::json!({
                "content_schema": 2, "sha256": format!("sha256:{:x}", Sha256::digest(&bytes)),
                "bytes": bytes.len(), "body": {"kind": "inline", "value": value}
            }),
        }
        .with_activity_binding(binding)
        .unwrap(),
        idempotency_key: Some(event_key.into()),
        schema_version: 2,
    }
}

pub(crate) fn poison_terminal_ref(
    executor: &storage::PostgresExecutor,
    terminal_id: &str,
    expected_ref: &str,
    bad_ref: &str,
) {
    let mut connection = executor.checkout_critical().unwrap();
    let before: serde_json::Value = connection
        .query_one(
            "SELECT to_jsonb(outbox) FROM runtime_session_outbox outbox WHERE terminal_id=$1",
            &[&terminal_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(before["status"], "pending");
    assert_eq!(before["payload_ref"], expected_ref);
    assert_ne!(expected_ref, bad_ref);
    let affected = connection.execute(
        "UPDATE runtime_session_outbox SET payload_ref=$1 WHERE terminal_id=$2 AND payload_ref=$3 AND status='pending'",
        &[&bad_ref, &terminal_id, &expected_ref],
    ).unwrap();
    assert_eq!(affected, 1);
    let after: serde_json::Value = connection
        .query_one(
            "SELECT to_jsonb(outbox) FROM runtime_session_outbox outbox WHERE terminal_id=$1",
            &[&terminal_id],
        )
        .unwrap()
        .get(0);
    let mut expected = before;
    expected["payload_ref"] = serde_json::json!(bad_ref);
    assert_eq!(after, expected);
}
