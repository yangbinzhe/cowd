use super::*;
use crate::{
    AppendTransactionRequest, RuntimeEventInput, RuntimeEventScope, RuntimeTransactionEventInput,
};
use std::collections::BTreeMap;

fn append_index(store: &RuntimeEventStore, source: &str, position: u64, schema: u32) {
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: format!("791-proof:{position}"),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: source.into(),
                expected_revision: position - 1,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: source.into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.agent_tool.receipt".into(),
                    status: None,
                    actor: Some("governed_tool".into()),
                    refs: vec![],
                    payload: serde_json::json!({
                        "idempotency_key":format!("unrelated-{position}"),
                        "effect_kind":"read","sequence":position,
                        "outcome":{"status":"unknown"},
                    }),
                },
                idempotency_key: Some(format!("agent-tool-receipt:unrelated-{position}")),
                schema_version: schema,
            }],
        })
        .unwrap();
}

#[test]
fn own791_proof_validates_old_read_headers_before_cursor_and_read_projection() {
    let store = RuntimeEventStore::for_test();
    let source = "execution-agent-receipts:791-proof:node:1";
    append_index(&store, source, 1, 2);
    for position in 2..=130 {
        append_index(&store, source, position, 1);
    }
    let mut cuts = BTreeMap::new();
    assert!(pinned_events(&store, source, &mut cuts, u64::MAX)
        .unwrap()
        .is_empty());
    let error = pinned_stream_events(
        &store,
        source,
        &mut cuts,
        u64::MAX,
        PinnedEventProjection::ReceiptIndexEffects,
    )
    .unwrap_err();
    assert!(error.contains("receipt-index structural"), "{error}");
}

#[test]
fn own791_proof_unknown_read_remains_projected_but_its_cut_fences_same_transaction() {
    let store = RuntimeEventStore::for_test();
    let source = "execution-agent-receipts:791-proof:node:1";
    let mut cuts = BTreeMap::new();
    assert!(pinned_stream_events(
        &store,
        source,
        &mut cuts,
        0,
        PinnedEventProjection::ReceiptIndexEffects,
    )
    .unwrap()
    .is_empty());
    assert_eq!(cuts.get(source), Some(&0));
    append_index(&store, source, 1, 1);
    assert!(pinned_stream_events(
        &store,
        source,
        &mut cuts,
        0,
        PinnedEventProjection::ReceiptIndexEffects,
    )
    .is_err());
    cuts.clear();
    assert!(pinned_stream_events(
        &store,
        source,
        &mut cuts,
        0,
        PinnedEventProjection::ReceiptIndexEffects,
    )
    .unwrap()
    .is_empty());
    assert_eq!(cuts.get(source), Some(&1));
    append_index(&store, source, 2, 1);
    let target = "791-proof-negative-terminal";
    let error = store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "791-raced-terminal".into(),
            expected_streams: cuts
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .chain(std::iter::once(ExpectedStreamRevision {
                    stream_id: target.into(),
                    expected_revision: 0,
                }))
                .collect(),
            events: vec![RuntimeEventInput {
                stream_id: target.into(),
                scope: RuntimeEventScope::Session,
                kind: "791.negative-only-terminal".into(),
                status: None,
                actor: Some("negative-test-not-authority".into()),
                refs: vec![],
                payload: serde_json::json!({}),
            }
            .into()],
        })
        .unwrap_err();
    assert!(
        matches!(error, crate::RuntimeEventStoreError::StaleRevision { ref stream_id, .. }
        if stream_id == source),
        "{error}"
    );
    assert_eq!(store.stream_revision(target).unwrap(), 0);
}



#[test]
fn own864_receipt_index_effects_accepts_schema2_and_rejects_false_source_contract() {
    for fault in 0..5 {
        let store = RuntimeEventStore::for_test();
        let source = "execution-agent-receipts:own864-reader:node:1";
        let mut payload = serde_json::json!({"idempotency_key":"own864-reader",
            "effect_kind":"read","outcome":{"status":"unknown"},"sequence":1,
            "sequence_base":0,"index_revision_before":0,"sequence_owner":"original_receipt_index",
            "source_observation_wave_sequence":1});
        let mut schema = 2;
        match fault {
            0 => {},
            1 => schema = 99,
            2 => payload["source_observation_wave_sequence"] = serde_json::json!("foreign"),
            3 => payload["sequence_base"] = serde_json::json!(2),
            4 => payload["index_revision_before"] = serde_json::json!(1),
            _ => unreachable!(),
        }
        store.append_transaction(AppendTransactionRequest {
            transaction_id:"own864-reader".into(),
            expected_streams:vec![ExpectedStreamRevision {stream_id:source.into(),expected_revision:0}],
            events:vec![RuntimeTransactionEventInput {
                event:RuntimeEventInput {stream_id:source.into(),scope:RuntimeEventScope::ExecutionNode,
                    kind:"execution.agent_tool.receipt".into(),status:None,
                    actor:Some("governed_tool".into()),refs:vec![],payload},
                idempotency_key:Some("agent-tool-receipt:own864-reader".into()),schema_version:schema,
            }],
        }).unwrap();
        let mut cuts = BTreeMap::new();
        let observed = pinned_stream_events(&store, source, &mut cuts, u64::MAX,
            PinnedEventProjection::ReceiptIndexEffects);
        assert_eq!(observed.is_ok(), fault == 0, "{fault}: {observed:?}");
        if fault == 0 { assert_eq!(cuts.get(source), Some(&1)); }
    }
}
