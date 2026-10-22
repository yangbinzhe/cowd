//! Fixture oracle for the original delegated receipt owner. The root activity
//! query is deliberately not a descendant-receipt discovery API.
use super::*;

pub(super) fn stream(
    parent: &harness_contract::execution_graph::ExecutionParentBinding,
    attempt: u32,
) -> String {
    format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        parent.execution_id, parent.node_id
    )
}
pub(super) fn original_receipts(
    store: &runtime::RuntimeEventStore,
    call: &PhysicalCall,
) -> Result<Vec<runtime::RuntimeEventRecord>, String> {
    let parent = call
        .parent_execution
        .as_ref()
        .ok_or("physical call has no original parent")?;
    let attempt = call
        .attempt
        .filter(|attempt| *attempt > 0)
        .ok_or("physical call has no original attempt")?;
    let events = store.list_stream(&stream(parent, attempt))?;
    let receipts = events
        .into_iter()
        .filter(|event| {
            event.kind == "execution.agent_tool.receipt"
                && event.payload["idempotency_key"] == call.idempotency_key
        })
        .collect::<Vec<_>>();
    for receipt in &receipts {
        let primary = store
            .event_by_idempotency_key(
                &format!("execution-effect:{}", call.idempotency_key),
                &format!("{}:receipt", call.idempotency_key),
            )
            .map_err(|error| error.to_string())?
            .ok_or("index receipt has no original primary")?;
        let identity_matches = |event: &runtime::RuntimeEventRecord| {
            event.actor.as_deref() == Some("governed_tool")
                && event.payload["idempotency_key"] == call.idempotency_key
                && [
                    ("execution_graph", parent.execution_id.as_str()),
                    ("execution_node", parent.node_id.as_str()),
                    ("agent_attempt", &attempt.to_string()),
                    ("tool_invocation", call.tool_use_id.as_str()),
                ]
                .into_iter()
                .all(|(kind, id)| {
                    event.refs.iter().filter(|r| r.kind == kind).count() == 1
                        && event.refs.iter().any(|r| r.kind == kind && r.id == id)
                })
        };
        if receipt.schema_version != 2
            || primary.schema_version != 1
            || !identity_matches(receipt)
            || !identity_matches(&primary)
            || receipt.idempotency_key.as_deref()
                != Some(format!("agent-tool-receipt:{}", call.idempotency_key).as_str())
            || primary.kind != "execution.effect.receipt"
            || primary.payload["replay"]["call_id"] != call.tool_use_id
            || primary.payload["replay"]["parent_execution"]
                != json!({"execution_id":parent.execution_id,"node_id":parent.node_id})
            || receipt.payload["outcome"]["tool_name"] != call.tool_name
            || receipt.payload["outcome"] != primary.payload["outcome"]
            || receipt.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
            || receipt.transaction_id != primary.transaction_id
            || receipt.commit_cursor != primary.commit_cursor
        {
            return Err(
                "delegated receipt differs from its original call/attempt/primary transaction"
                    .into(),
            );
        }
    }
    Ok(receipts)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn call() -> PhysicalCall {
        PhysicalCall {
            tool_use_id: "independent-write".into(),
            tool_name: "edit".into(),
            input: "{}".into(),
            idempotency_key: "literal-original-effect".into(),
            attempt: Some(1),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "original-child-graph".into(),
                node_id: "original-child-node".into(),
            }),
        }
    }
    fn records(foreign: bool, primary: bool, index_schema: u32) -> runtime::RuntimeEventStore {
        let store = runtime::RuntimeEventStore::for_test();
        let mut refs = vec![
            runtime::RuntimeEventRef {
                kind: "execution_graph".into(),
                id: "original-child-graph".into(),
            },
            runtime::RuntimeEventRef {
                kind: "execution_node".into(),
                id: "original-child-node".into(),
            },
            runtime::RuntimeEventRef {
                kind: "agent_attempt".into(),
                id: if foreign { "2" } else { "1" }.into(),
            },
            runtime::RuntimeEventRef {
                kind: "tool_invocation".into(),
                id: "independent-write".into(),
            },
        ];
        let common = json!({"idempotency_key":"literal-original-effect", "outcome":{"tool_name":"edit","status":"executed"},
            "resolved_effect_scope":{"independently_authored_scope":"original file"},
            "replay":{"call_id":"independent-write","parent_execution":{"execution_id":"original-child-graph","node_id":"original-child-node"}}});
        let event = |stream: &str, kind: &str, key: &str, refs: Vec<runtime::RuntimeEventRef>| {
            runtime::RuntimeTransactionEventInput {
                event: runtime::RuntimeEventInput {
                    stream_id: stream.into(),
                    scope: runtime::RuntimeEventScope::ExecutionNode,
                    kind: kind.into(),
                    status: None,
                    actor: Some("governed_tool".into()),
                    refs,
                    payload: common.clone(),
                },
                schema_version: if kind == "execution.agent_tool.receipt" {
                    index_schema
                } else {
                    1
                },
                idempotency_key: Some(key.into()),
            }
        };
        let mut events = vec![event(
            "execution-agent-receipts:original-child-graph:original-child-node:1",
            "execution.agent_tool.receipt",
            "agent-tool-receipt:literal-original-effect",
            refs.clone(),
        )];
        if primary {
            events.push(event(
                "execution-effect:literal-original-effect",
                "execution.effect.receipt",
                "literal-original-effect:receipt",
                std::mem::take(&mut refs),
            ));
        }
        store
            .append_transaction(runtime::AppendTransactionRequest {
                transaction_id: "independent-atomic-receipt-pair".into(),
                expected_streams: vec![
                    runtime::ExpectedStreamRevision {
                        stream_id:
                            "execution-agent-receipts:original-child-graph:original-child-node:1"
                                .into(),
                        expected_revision: 0,
                    },
                    runtime::ExpectedStreamRevision {
                        stream_id: "execution-effect:literal-original-effect".into(),
                        expected_revision: 0,
                    },
                ],
                events,
            })
            .unwrap();
        store
    }
    #[test]
    fn delegated_write_receipt_is_found_without_a_root_activity_index() {
        let store = records(false, true, 2);
        assert!(store
            .events_for_root_execution("original-root", None, 8)
            .unwrap()
            .is_empty());
        let receipts = original_receipts(&store, &call()).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].payload["outcome"]["status"], "executed");
        let mut foreign = call();
        foreign.attempt = Some(2);
        assert!(original_receipts(&store, &foreign).unwrap().is_empty());
    }
    #[test]
    fn delegated_index_uses_its_own_v2_schema_not_the_primary_v1_schema() {
        assert!(original_receipts(&records(false, true, 1), &call()).is_err());
        assert!(original_receipts(&records(false, true, 3), &call()).is_err());
        assert_eq!(
            original_receipts(&records(false, true, 2), &call())
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn missing_primary_and_foreign_attempt_refs_do_not_prove_a_write() {
        assert!(original_receipts(&records(false, false, 2), &call()).is_err());
        assert!(original_receipts(&records(true, true, 2), &call()).is_err());
    }
}
