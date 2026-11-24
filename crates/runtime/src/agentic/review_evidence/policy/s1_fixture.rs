//! 033 S1' kernel fixture: one delegated child read + one review actual-read
//! receipt.
//!
//! The live B-family death chain (see `hub-inbox033/e1-s1-attribution.md`):
//!   hop 1  `conversation/tool_plane.rs:3579` - the delegated child's receipt
//!          never settles ("original child receipt remains unsettled")
//!   hop 2  `conversation/tool_plane.rs:1171` - the unknown effect is promoted
//!          ("original tool effect remains unknown")
//!   hop 3  `agentic/review_evidence/policy.rs:3521` - the review's actual
//!          input is refused ("proof_actual_input_extent_unsupported: actual
//!          read lacks an original physical extent")
//!   hop 3' `agentic/review_evidence/policy.rs:3264` - the composite shape
//!          ("original composite child acquisitions incomplete")
//!
//! This fixture pins hop 3 in the shape the review path really has: a
//! delegated child read (its receipt carries the parent execution) and one
//! review actual-read receipt.
//!
//! Red control (today's behaviour, and the shape S3 has to change): the
//! child's read named a workspace path and its receipt carries no
//! acquisition seal, so the review's actual input is refused - the review
//! turn can never reach a terminal answer and the attempt is charged to the
//! model instead of to the structural fault.
//!
//! Green shape (the target): the same delegated child read that named a
//! durable record settles from its immutable receipt, so the review's actual
//! input is acquired and the review can reach a terminal answer.

use super::ActualInputSnapshot;
use crate::agentic::program::AgenticProgramProjection;
use crate::{ExpectedStreamRevision, RuntimeEventScope, RuntimeEventStore};
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind};
use std::sync::Arc;

fn readers() -> super::super::ResultContentReaders<'static> {
    super::super::ResultContentReaders {
        artifacts: None,
        history: None,
        process_inputs: None,
    }
}

fn empty_actual() -> ActualInputSnapshot {
    ActualInputSnapshot {
        file_dependencies: Vec::new(),
        immutable_inputs: Vec::new(),
        sources: std::collections::BTreeMap::new(),
        material_events: Vec::new(),
        canonical_facts: Vec::new(),
    }
}

/// A delegated child read whose receipt settled into the durable journal.
///
/// The receipt is the child's own read: it names the parent execution it ran
/// under, and the index event is the observation twin of the same
/// transaction.
fn delegated_child_read(
    store: &Arc<RuntimeEventStore>,
    tool_name: &str,
    call_id: &str,
    key: &str,
    session: &str,
    graph_id: &str,
    node_id: &str,
    extra_primary: serde_json::Value,
) -> (crate::DurableRuntimeEvent, crate::DurableRuntimeEvent) {
    let effect = ToolEffectDescriptor {
        tool_id: tool_name.into(),
        descriptor_hash: format!("{tool_name}-descriptor"),
        effect_kind: ToolEffectKind::Read,
        idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
        scopes: vec![harness_contract::policy::PermissionScope {
            resource: harness_contract::policy::PermissionResource::File,
            operation: harness_contract::policy::PermissionOperation::Read,
            target: Some(".".into()),
        }],
        required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
        approval_class: harness_contract::tool::ToolApprovalClass::None,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: Default::default(),
    };
    let outcome = serde_json::json!({
        "status": "executed",
        "output": "original body",
        "tool_name": tool_name,
        "tool_use_id": call_id,
    });
    let replay = serde_json::json!({
        "origin": {
            "origin_step_id": "s1-step",
            "call_id": call_id,
            "catalog_digest": "s1-catalog",
            "source": {"kind": "provider", "attempt_id": "s1-attempt"},
        },
        "session_id": session,
        "call_id": call_id,
        "parent_execution": {"execution_id": graph_id, "node_id": node_id},
        "invocation": {"effect": effect},
    });
    let mut primary_payload = serde_json::json!({
        "idempotency_key": key,
        "tool_use_id": call_id,
        "tool_name": tool_name,
        "input_sha256": "s1-input-digest",
        "outcome": outcome,
        "outcome_truncated": false,
        "replay": replay,
    });
    if let (Some(target), Some(source)) = (
        primary_payload.as_object_mut(),
        extra_primary.as_object(),
    ) {
        for (name, value) in source {
            target.insert(name.clone(), value.clone());
        }
    }
    let index_payload = serde_json::json!({
        "idempotency_key": key,
        "effect_kind": "read",
        "outcome": outcome,
        "authorized_scopes": [],
        "effect_scope": effect.scopes[0],
        "sequence": 1,
    });
    let index_stream = format!("execution-agent-receipts:{graph_id}:{node_id}:1");
    store
        .append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("s1-receipt:{key}"),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: format!("execution-effect:{key}"),
                    expected_revision: 0,
                },
                ExpectedStreamRevision {
                    stream_id: index_stream.clone(),
                    expected_revision: 0,
                },
            ],
            events: vec![
                crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("execution-effect:{key}"),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "execution.read.receipt".into(),
                        status: Some("completed".into()),
                        actor: Some("governed_tool".into()),
                        refs: vec![],
                        payload: primary_payload,
                    },
                    idempotency_key: Some(format!("{key}:read-receipt")),
                    schema_version: 1,
                },
                crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: index_stream.clone(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "execution.agent_tool.receipt".into(),
                        status: Some("completed".into()),
                        actor: Some("governed_tool".into()),
                        refs: vec![],
                        payload: index_payload,
                    },
                    idempotency_key: Some(format!("agent-tool-receipt:{key}")),
                    schema_version: 1,
                },
            ],
        })
        .expect("the delegated child read receipt settles in one transaction");
    let primary = store
        .list_stream(&format!("execution-effect:{key}"))
        .expect("primary read receipt")
        .into_iter()
        .find(|event| event.kind == "execution.read.receipt")
        .expect("primary read receipt");
    let index = store
        .list_stream(&index_stream)
        .expect("index read receipt")
        .into_iter()
        .find(|event| event.kind == "execution.agent_tool.receipt")
        .expect("index read receipt");
    (primary, index)
}

fn review_boundary(
    store: &Arc<RuntimeEventStore>,
    session: &str,
    key: &str,
    graph_id: &str,
    node_id: &str,
    call_id: &str,
) -> super::super::results::ResultInputBoundary {
    let consumer = store
        .append(crate::RuntimeEventInput {
            stream_id: format!("session:{session}"),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.effect.intent".into(),
            status: Some("completed".into()),
            actor: Some("governed_tool".into()),
            refs: vec![],
            payload: serde_json::json!({
                "idempotency_key": key,
                "replay": {
                    "origin": {"origin_step_id": "s1-step", "call_id": call_id},
                    "session_id": session,
                    "call_id": call_id,
                    "parent_execution": {"execution_id": graph_id, "node_id": node_id},
                },
            }),
        })
        .expect("the review's read-back consumer event");
    super::super::results::ResultInputBoundary {
        producer: "agent-run:run:s1-graph".into(),
        packet: None,
        step: None,
        cursor: consumer.commit_cursor,
        call_key: key.into(),
        source: harness_contract::goal::ProofInputEvent {
            stream_id: consumer.stream_id.clone(),
            sequence: consumer.sequence,
            event_id: consumer.event_id.clone(),
            payload_digest: super::super::proof_value_digest(&consumer.payload),
        },
    }
}

fn program(session: &str) -> AgenticProgramProjection {
    let mut program = AgenticProgramProjection::empty("s1-program", "s1-objective");
    program.session_id = session.into();
    program
}

/// 033 S1' red control: the delegated child read that named a workspace path
/// leaves no acquisition seal, so the review's actual input is refused and
/// the review turn has no terminal answer. The composite shape is the same
/// fault in its orchestrated form.
#[test]
fn s1_delegated_child_read_refuses_the_review_actual_input() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let session = "s1-session";
    let graph_id = "s1-graph";
    let node_id = "s1-node";
    let call_id = "s1-call";
    let key = "s1-effect-key";

    let (primary, index) = delegated_child_read(
        &store,
        "read_ref",
        call_id,
        key,
        session,
        graph_id,
        node_id,
        serde_json::json!({}),
    );
    // The receipt is a delegated child's read: it names the execution it ran
    // under, and the index event is the observation twin of the same
    // transaction.
    assert_eq!(
        primary.payload["replay"]["parent_execution"]["execution_id"],
        graph_id
    );
    assert_eq!(primary.transaction_id, index.transaction_id);
    assert!(index.transaction_index > primary.transaction_index);

    let boundary = review_boundary(
        &store, session, key, graph_id, node_id, call_id,
    );

    // RED (hop 3): the child's read named a workspace path, so the actual
    // read has no original physical extent and the review's actual input is
    // refused.
    let mut refused = empty_actual();
    let mut refused_windows = std::collections::BTreeMap::new();
    let error = super::collect_actual_acquisition(
        &store,
        &program(session),
        &primary,
        &index,
        Some(&serde_json::json!({"reference": "file://workspace/report.txt"})),
        &[],
        &boundary,
        readers(),
        &mut refused,
        &mut refused_windows,
    )
    .expect_err("a workspace read without an acquisition seal cannot authenticate an actual input");
    // 034 GB-1 (batch 1 acceptance): the fragmented gate's original wording
    // stays verbatim inside the gate's envelope, and the envelope carries the
    // code plus what each of the three proof strategies was missing - one
    // refusal, a complete diagnosis.
    let original = "proof_actual_input_extent_unsupported: actual read lacks an original physical extent";
    assert!(
        error.starts_with(original),
        "the original refusal wording stays verbatim at the head: {error}"
    );
    assert!(error.contains("code=fact_unproven"), "{error}");
    for strategy in ["nested", "discovery", "immutable"] {
        assert!(
            error.contains(&format!("{strategy}(missing=")),
            "the diagnosis names every strategy: {error}"
        );
    }
    assert!(
        refused.canonical_facts.is_empty(),
        "a refused actual input settles nothing: {:?}",
        refused.canonical_facts
    );

    // RED (hop 3', composite): the orchestrated child read whose child
    // acquisitions never completed.
    // Its own child execution: the delegated receipts live in one stream per
    // (graph, node, attempt), so the orchestrated read runs under its own
    // node instead of colliding with the read above.
    let orchestrated_node = "s1-node-orchestrated";
    let (orchestrated, orchestrated_index) = delegated_child_read(
        &store,
        "tool_orchestrate",
        "s1-orchestrated-call",
        "s1-orchestrated-key",
        session,
        graph_id,
        orchestrated_node,
        serde_json::json!({"child_read_acquisitions_v1": []}),
    );
    let orchestrated_boundary = review_boundary(
        &store,
        session,
        "s1-orchestrated-key",
        graph_id,
        orchestrated_node,
        "s1-orchestrated-call",
    );
    let mut composite_refused = empty_actual();
    let mut composite_windows = std::collections::BTreeMap::new();
    let error = super::collect_actual_acquisition(
        &store,
        &program(session),
        &orchestrated,
        &orchestrated_index,
        Some(&serde_json::json!({"steps": []})),
        &[],
        &orchestrated_boundary,
        readers(),
        &mut composite_refused,
        &mut composite_windows,
    )
    .expect("review contract v2: an incomplete composite child acquisition is reported, not refused");
    assert_eq!(composite_refused.canonical_facts.len(), 1, "{facts:?}", facts = composite_refused.canonical_facts);
    assert_eq!(composite_refused.canonical_facts[0]["grade"], "acquired_reported");
    assert_eq!(
        composite_refused.canonical_facts[0]["note"],
        "original composite child acquisitions incomplete"
    );
    assert!(composite_windows.is_empty());
}

/// 033 S1' green shape: the same delegated child read, naming a durable
/// record, settles from its immutable receipt - the review's actual input is
/// acquired and the review can reach a terminal answer.
#[test]
fn s1_delegated_child_read_of_a_durable_record_settles_the_review_actual_input() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let session = "s1-session";
    let graph_id = "s1-graph";
    let node_id = "s1-node";
    let call_id = "s1-call";
    let key = "s1-effect-key";

    let (primary, index) = delegated_child_read(
        &store,
        "read_ref",
        call_id,
        key,
        session,
        graph_id,
        node_id,
        serde_json::json!({}),
    );
    let boundary = review_boundary(
        &store, session, key, graph_id, node_id, call_id,
    );

    let mut actual = empty_actual();
    let mut windows = std::collections::BTreeMap::new();
    super::collect_actual_acquisition(
        &store,
        &program(session),
        &primary,
        &index,
        Some(&serde_json::json!({"reference": "artifact://art-s1"})),
        &[],
        &boundary,
        readers(),
        &mut actual,
        &mut windows,
    )
    .expect("a delegated child read of a durable record settles the review's actual input");
    assert!(
        actual
            .canonical_facts
            .iter()
            .any(|fact| fact["grade"] == "acquired_immutable_receipt"),
        "the actual input is acquired from the child's immutable receipt: {:?}",
        actual.canonical_facts
    );
}

/// The refusal has to survive the turn.
///
/// The review actual input is refused because the read carries no original
/// physical extent, and the attempt is charged to the model instead of to the
/// structural fault. The durable record is the only place that says which
/// read lacked the extent, so the refusal is written on its own stream - never
/// the semantic lane's, and nothing is re-armed by it.
#[test]
fn a_refused_review_acquisition_is_persisted_on_its_own_stream() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let session = "s1-session";
    let graph_id = "s1-graph";
    let node_id = "s1-node";
    let call_id = "s1-call";
    let key = "s1-effect-key";
    let (primary, index) = delegated_child_read(
        &store,
        "read_ref",
        call_id,
        key,
        session,
        graph_id,
        node_id,
        serde_json::json!({}),
    );
    let boundary = review_boundary(&store, session, key, graph_id, node_id, call_id);
    let mut refused = empty_actual();
    let mut refused_windows = std::collections::BTreeMap::new();
    let error = super::collect_actual_acquisition(
        &store,
        &program(session),
        &primary,
        &index,
        Some(&serde_json::json!({"reference": "file://workspace/report.txt"})),
        &[],
        &boundary,
        readers(),
        &mut refused,
        &mut refused_windows,
    )
    .expect_err("a workspace read without an acquisition seal cannot authenticate an actual input");
    assert!(
        error.to_string().contains("extent"),
        "the refusal still names the missing extent: {error}"
    );
    let stream_id = "agentic-review-acquisition-rejected:s1-program";
    let events = store
        .list_stream(stream_id)
        .expect("the refusal stream is readable");
    assert_eq!(
        events.len(),
        1,
        "the refusal is persisted exactly once on its own stream: {events:?}"
    );
    let event = &events[0];
    assert_eq!(event.kind, "agentic.review_acquisition_rejected");
    assert_eq!(
        event.payload["disposition"],
        "incomplete_snapshot_not_published"
    );
    assert!(
        event.payload["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("extent"),
        "the record names the missing extent: {}",
        event.payload
    );
    assert!(
        !event.stream_id.contains("agentic-program:"),
        "the record observes on its own stream and never the semantic lane: {}",
        event.stream_id
    );
}
