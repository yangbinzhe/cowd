use super::*;

fn index() -> crate::DurableRuntimeEvent {
    crate::DurableRuntimeEvent {
        event_id: "791-index".into(),
        stream_id: "execution-agent-receipts:owner:node:1".into(),
        sequence: 1,
        scope: RuntimeEventScope::ExecutionNode,
        kind: "execution.agent_tool.intent".into(),
        status: None,
        actor: Some("governed_tool".into()),
        refs: vec![],
        payload: json!({"idempotency_key":"unrelated", "effect_kind":"write"}),
        created_at_ms: 1,
        commit_cursor: 1,
        transaction_id: "791-original".into(),
        transaction_index: 0,
        schema_version: 1,
        idempotency_key: Some("agent-tool-intent:unrelated".into()),
    }
}

#[test]
fn own791_unrelated_structural_corruption_is_never_a_disjoint_certificate() {
    let original = index();
    let source = original.stream_id.as_str();
    validate_receipt_index_record(&original, source, 1, 1).unwrap();
    for fault in 0..10 {
        let mut event = original.clone();
        match fault {
            0 => event.stream_id.push_str("-foreign"),
            1 => event.sequence = 2,
            2 => event.schema_version = 2,
            3 => event.scope = RuntimeEventScope::Session,
            4 => event.actor = Some("foreign".into()),
            5 => event.payload["idempotency_key"] = json!(""),
            6 => event.payload["idempotency_key"] = serde_json::Value::Null,
            7 => event.kind = "execution.agent_tool.unclassified".into(),
            8 => event.idempotency_key = None,
            9 => event.idempotency_key = Some("agent-tool-intent:borrowed".into()),
            _ => unreachable!(),
        }
        assert!(
            validate_receipt_index_record(&event, source, 1, 1).is_err(),
            "{fault}"
        );
    }
    assert!(validate_receipt_index_record(&original, source, 0, 1).is_err());
    assert!(validate_receipt_index_record(&original, source, 1, 0).is_err());
}

#[test]
fn own791_structural_integrity_does_not_certify_physical_settlement() {
    let mut event = index();
    validate_receipt_index_record(&event, &event.stream_id, 1, 1).unwrap();
    event.kind = "execution.agent_tool.receipt".into();
    event.idempotency_key = Some("agent-tool-receipt:unrelated".into());
    event.payload["outcome"] = json!({"status":"unknown"});
    event.payload["sequence"] = json!(1);
    validate_receipt_index_record(&event, &event.stream_id, 1, 1).unwrap();
}
