use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Duration;

use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventReactor,
    RuntimeEventScope, RuntimeEventStore, RuntimeProjectionDescriptor, RuntimeProjectionEventInterest,
    RuntimeProjectionInterest, RuntimeProjectionLane, RuntimeProjectionPass, SessionTerminalInput,
};
use runtime_postgres::PostgresRuntimeEventStore;

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn conclusion() -> AppendTransactionRequest {
    AppendTransactionRequest {
        transaction_id: "program-conclusion:kernel:1".into(),
        expected_streams: vec![
            ExpectedStreamRevision { stream_id: "goal:kernel".into(), expected_revision: 0 },
            ExpectedStreamRevision { stream_id: "agentic-program:kernel".into(), expected_revision: 0 },
        ],
        events: vec![
            RuntimeEventInput { stream_id: "goal:kernel".into(), scope: RuntimeEventScope::Goal,
                kind: "goal.completed".into(), status: Some("satisfied".into()),
                actor: Some("kernel-persistence-fixture".into()), refs: vec![],
                payload: serde_json::json!({"fixture":"persistence-boundary-only"}), }.into(),
            RuntimeEventInput { stream_id: "agentic-program:kernel".into(), scope: RuntimeEventScope::Program,
                kind: "agentic.objective_verdict_bound".into(), status: Some("verified".into()),
                actor: Some("kernel-persistence-fixture".into()), refs: vec![],
                payload: serde_json::json!({"fixture":"persistence-boundary-only"}), }.into(),
        ],
    }
}

fn graph_terminal(verdict_event_id: &str) -> (AppendTransactionRequest, SessionTerminalInput) {
    let terminal = SessionTerminalInput {
        terminal_id: "terminal:kernel".into(), message_id: "message:kernel".into(),
        session_id: "session:kernel".into(), execution_id: Some("root:kernel".into()),
        turn_id: Some("turn:kernel".into()), request_id: Some("request:kernel".into()),
        session_generation: Some(1), input_sequence: Some(0),
        input_claim_owner: Some("owner:kernel".into()), input_claim_token: Some("token:kernel".into()),
        input_claim_revision: Some(1), controlled_recovery_claim_fingerprints: vec![],
        payload_ref: "artifact:kernel-terminal".into(),
    };
    let transaction = AppendTransactionRequest {
        transaction_id: "graph-terminal:kernel".into(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "execution-graph:kernel".into(), expected_revision: 0,
        }],
        events: vec![
            RuntimeEventInput {
                stream_id: "execution-graph:kernel".into(), scope: RuntimeEventScope::ExecutionGraph,
                kind: "execution.graph.completed".into(), status: Some("completed".into()),
                actor: Some("kernel-persistence-fixture".into()),
                refs: vec![runtime::RuntimeEventRef { kind: "objective_verdict".into(), id: verdict_event_id.into() }],
                payload: serde_json::json!({"fixture":"persistence-boundary-only"}),
            }.into(),
            RuntimeEventInput {
                stream_id: "execution-graph:kernel".into(), scope: RuntimeEventScope::ExecutionGraph,
                kind: "runtime.session.terminal_requested".into(), status: Some("pending".into()),
                actor: Some("kernel-persistence-fixture".into()), refs: vec![],
                payload: serde_json::to_value(&terminal).unwrap(),
            }.into(),
        ],
    };
    (transaction, terminal)
}

fn connect(fixture: &PostgresTestScope) -> RuntimeEventStore {
    PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store()
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_conclusion_rolls_back_all_streams_when_the_second_event_insert_fails() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresRuntimeEventStore::new(executor.clone()).unwrap().into_runtime_event_store();
    executor.checkout_critical().unwrap().batch_execute(
        "ALTER TABLE runtime_events ADD CONSTRAINT kernel_reject_verdict CHECK (kind <> 'agentic.objective_verdict_bound')"
    ).unwrap();
    assert!(store.append_transaction(conclusion()).is_err());
    for stream in ["goal:kernel", "agentic-program:kernel"] {
        assert_eq!(store.stream_revision(stream).unwrap(), 0);
        assert!(store.list_stream(stream).unwrap().is_empty());
    }
    assert!(store.events_after_cursor(0, 10).unwrap().is_empty());
    executor.checkout_critical().unwrap().batch_execute(
        "ALTER TABLE runtime_events DROP CONSTRAINT kernel_reject_verdict"
    ).unwrap();
    let receipt = store.append_transaction(conclusion()).unwrap();
    assert_eq!(receipt.event_ids.len(), 2);
    assert!(!receipt.duplicate);
    drop(store);
    let reopened = connect(&fixture);
    let duplicate = reopened.append_transaction(conclusion()).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.commit_cursor, receipt.commit_cursor);
    assert_eq!(duplicate.event_ids, receipt.event_ids);
    assert_eq!(reopened.events_after_cursor(0, 10).unwrap().len(), 1);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_graph_failure_keeps_the_committed_verdict_and_rolls_back_its_outbox() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresRuntimeEventStore::new(executor.clone()).unwrap().into_runtime_event_store();
    let verdict = store.append_transaction(conclusion()).unwrap();
    let (transaction, terminal) = graph_terminal(&verdict.event_ids[1]);
    executor.checkout_critical().unwrap().batch_execute(
        "ALTER TABLE runtime_events ADD CONSTRAINT kernel_reject_terminal CHECK (kind <> 'runtime.session.terminal_requested')"
    ).unwrap();
    assert!(store.append_transaction_with_terminal(transaction.clone(), terminal.clone()).is_err());
    assert_eq!(store.stream_revision("goal:kernel").unwrap(), 1);
    assert_eq!(store.stream_revision("agentic-program:kernel").unwrap(), 1);
    assert_eq!(store.stream_revision("execution-graph:kernel").unwrap(), 0);
    assert!(store.session_terminal(&terminal.terminal_id).unwrap().is_none());
    executor.checkout_critical().unwrap().batch_execute(
        "ALTER TABLE runtime_events DROP CONSTRAINT kernel_reject_terminal"
    ).unwrap();
    drop(store);
    let reopened = connect(&fixture);
    let graph = reopened.append_transaction_with_terminal(transaction.clone(), terminal.clone()).unwrap();
    let duplicate = reopened.append_transaction_with_terminal(transaction, terminal.clone()).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.event_ids, graph.event_ids);
    assert_eq!(duplicate.commit_cursor, graph.commit_cursor);
    assert_eq!(reopened.stream_revision("goal:kernel").unwrap(), 1);
    let outbox = reopened.session_terminal(&terminal.terminal_id).unwrap().unwrap();
    assert_eq!(outbox.commit_cursor, graph.commit_cursor);
    assert_eq!(outbox.message_id, terminal.message_id);
    assert_eq!(outbox.status, "pending");
    assert_eq!(reopened.events_after_cursor(0, 10).unwrap().len(), 2);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_outbox_restart_reclaims_the_same_message_and_rejects_the_old_worker_ack() {
    let fixture = PostgresTestScope::new();
    let store = connect(&fixture);
    let verdict = store.append_transaction(conclusion()).unwrap();
    let (transaction, terminal) = graph_terminal(&verdict.event_ids[1]);
    store.append_transaction_with_terminal(transaction, terminal.clone()).unwrap();
    let first = store.claim_session_terminals("crashed-worker", 100, 10, 1).unwrap().remove(0);
    assert_eq!(first.message_id, terminal.message_id);
    drop(store);
    let reopened = connect(&fixture);
    assert!(reopened.claim_session_terminals("new-worker", 109, 10, 1).unwrap().is_empty());
    let second = reopened.claim_session_terminals("new-worker", 111, 10, 1).unwrap().remove(0);
    assert_eq!(second.message_id, first.message_id);
    assert_eq!(second.commit_cursor, first.commit_cursor);
    assert_eq!(second.attempts, 2);
    assert!(reopened.ack_session_terminal(&first.terminal_id, "crashed-worker", first.revision, 112).is_err());
    let ack = reopened.ack_session_terminal(&second.terminal_id, "new-worker", second.revision, 112).unwrap();
    assert_eq!(ack.status, "materialized");
    assert_eq!(ack.message_id, terminal.message_id);
    drop(reopened);
    let final_store = connect(&fixture);
    assert!(final_store.claim_session_terminals("third-worker", 200, 10, 1).unwrap().is_empty());
    assert_eq!(final_store.session_terminal(&terminal.terminal_id).unwrap().unwrap().status, "materialized");
    assert_eq!(final_store.stream_revision("goal:kernel").unwrap(), 1);
    assert_eq!(final_store.stream_revision("execution-graph:kernel").unwrap(), 2);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_external_commit_without_a_local_doorbell_is_recovered_by_durable_scan() {
    let fixture = PostgresTestScope::new();
    let reader = Arc::new(connect(&fixture));
    let writer = connect(&fixture);
    let local_signal = reader.subscribe_commits();
    let cursor = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let recovered = Arc::new(tokio::sync::Notify::new());
    let interest = RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
        RuntimeEventScope::Program, "agentic.objective_verdict_bound",
    )]);
    let descriptor = RuntimeProjectionDescriptor::new(
        "kernel:external-doorbell-loss", interest.clone(), 8, Duration::from_millis(50),
    ).unwrap();
    let lane_reader = reader.clone();
    let lane_cursor = cursor.clone();
    let lane_entered = entered.clone();
    let lane_recovered = recovered.clone();
    let lane = RuntimeProjectionLane::blocking(descriptor, move |batch_size| {
        let page = lane_reader.projection_scan_page(
            lane_cursor.load(Ordering::SeqCst), &interest, batch_size, 100, 1024 * 1024,
        ).map_err(|error| error.to_string())?;
        lane_cursor.store(page.scanned_through_cursor, Ordering::SeqCst);
        lane_entered.notify_one();
        if page.matched_events > 0 { lane_recovered.notify_one(); }
        Ok(RuntimeProjectionPass::scanned(page.scanned_commits, batch_size).with_matches(page.matched_events))
    });
    let reactor = Arc::new(RuntimeEventReactor::sealed(reader.clone(), [lane]).unwrap());
    reactor.start().unwrap();
    let initialized = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    let receipt = writer.append_transaction(conclusion()).unwrap();
    assert!(!local_signal.has_changed().unwrap());
    let result = tokio::time::timeout(Duration::from_secs(3), recovered.notified()).await;
    let shutdown = reactor.shutdown().await;
    assert!(initialized.is_ok());
    assert!(result.is_ok());
    assert!(shutdown.timed_out_lanes.is_empty());
    assert_eq!(cursor.load(Ordering::SeqCst), receipt.commit_cursor);
}
