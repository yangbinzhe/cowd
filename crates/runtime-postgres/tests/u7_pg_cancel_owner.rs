//! Narrow PG transaction cancellation seam, not the U7 resource-owner gate.
#![cfg(feature = "test-fixtures")]
use std::sync::{Arc, mpsc};
use std::time::Duration;
use runtime::{AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope};
use runtime_postgres::{PgOwnedCommitControl, PgOwnedCommitState, PostgresRuntimeEventStore};
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;

fn request(id: &str) -> AppendTransactionRequest {
    AppendTransactionRequest {
        transaction_id: format!("u7-pg-cancel:{id}"),
        expected_streams: vec![ExpectedStreamRevision { stream_id: format!("u7-pg-cancel:{id}"), expected_revision: 0 }],
        events: vec![RuntimeEventInput { stream_id: format!("u7-pg-cancel:{id}"),
            scope: RuntimeEventScope::Recovery, kind: "u7.pg.cancel_probe".into(), status: Some("prepared".into()),
            actor: Some("u7-pg-cancel-test".into()), refs: vec![], payload: serde_json::json!({"version":1}) }.into()],
    }
}

#[test]
#[ignore = "requires an owned isolated PostgreSQL runtime backend"]
fn real_pg_precommit_cancel_rolls_back_and_independent_reader_sees_no_event() {
    let fixture = postgres_scope::PostgresTestScope::new();
    let writer = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
    let reader = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
    assert!(reader.fixture_backend_is::<PostgresRuntimeEventStore>());
    assert!(!runtime::RuntimeEventStore::for_test().fixture_backend_is::<PostgresRuntimeEventStore>());
    let control = Arc::new(PgOwnedCommitControl::default());
    let (started, waiting) = mpsc::sync_channel(0);
    let (release, blocked) = mpsc::sync_channel(0);
    control.set_precommit_probe(move || { started.send(()).unwrap(); blocked.recv().unwrap(); });
    let owner = {
        let control = control.clone();
        std::thread::spawn(move || writer.append_transaction_cancellable(request("negative"), control.as_ref()))
    };
    waiting.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(control.state(), PgOwnedCommitState::Running);
    assert!(control.request_cancel());
    assert_eq!(control.state(), PgOwnedCommitState::CancelRequested);
    assert!(reader.list_stream("u7-pg-cancel:negative").unwrap().is_empty());
    release.send(()).unwrap();
    assert!(owner.join().unwrap().unwrap().is_none());
    assert_eq!(control.wait_terminal(Duration::from_secs(5)), PgOwnedCommitState::RolledBack);
    let reopened = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
    assert!(reopened.list_stream("u7-pg-cancel:negative").unwrap().is_empty(), "no durable write after acknowledged cancellation");
    assert!(!control.request_cancel());
    let positive = Arc::new(PgOwnedCommitControl::default());
    let writer = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
    let receipt = writer.append_transaction_cancellable(request("positive"), positive.as_ref()).unwrap().unwrap();
    assert_eq!(positive.state(), PgOwnedCommitState::Committed);
    assert!(!positive.request_cancel(), "committed transaction cannot be reported cancelled");
    let events = reopened.list_stream("u7-pg-cancel:positive").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, receipt.event_ids[0]);
    // Preserve the schema for independent review; the PG owner retains volume.
    std::mem::forget(fixture);
}

#[test]
#[ignore = "requires an owned isolated PostgreSQL runtime backend"]
fn real_pg_cancel_commit_race_has_one_reconciled_outcome() {
    let fixture = postgres_scope::PostgresTestScope::new();
    let reader = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
    assert!(reader.fixture_backend_is::<PostgresRuntimeEventStore>());
    for i in 0..4 {
        let id = format!("race-{i}");
        let writer = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
        let control = Arc::new(PgOwnedCommitControl::default());
        let gate = Arc::new(std::sync::Barrier::new(2));
        let probe_gate = gate.clone();
        control.set_precommit_probe(move || { probe_gate.wait(); });
        let task_control = control.clone();
        let req = request(&id);
        let owner = std::thread::spawn(move || writer.append_transaction_cancellable(req, task_control.as_ref()));
        gate.wait();
        let shared: &dyn runtime::runtime_event_store::RuntimeTransactionCommitControl = control.as_ref();
        let cancel_won = shared.request_cancel();
        let outcome = owner.join().unwrap().unwrap();
        let fresh = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
        let events = fresh.list_stream(&format!("u7-pg-cancel:{id}")).unwrap();
        if cancel_won {
            assert!(outcome.is_none());
            assert_eq!(control.wait_terminal(Duration::from_secs(5)), PgOwnedCommitState::RolledBack);
            assert!(events.is_empty(), "winning cancellation must leave no committed event");
        } else {
            let receipt = outcome.expect("commit decision won, but not committed");
            assert_eq!(control.state(), PgOwnedCommitState::Committed);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_id, receipt.event_ids[0]);
        }
    }
    std::mem::forget(fixture);
}
