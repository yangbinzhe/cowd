//! PostgreSQL-only persistence conformance for the Runtime event ledger.

use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
};
use runtime_postgres::PostgresRuntimeEventStore;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_runtime_events_survive_adapter_reconstruction() {
    let fixture = PostgresTestScope::new();
    let scoped = fixture.reconnect();
    let first = PostgresRuntimeEventStore::new(scoped.clone())
        .expect("Runtime PostgreSQL store")
        .into_runtime_event_store();
    first
        .append_transaction(AppendTransactionRequest {
            transaction_id: "runtime-pg-conformance-v1".to_string(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "conformance:runtime".to_string(),
                expected_revision: 0,
            }],
            events: vec![RuntimeEventInput {
                stream_id: "conformance:runtime".to_string(),
                scope: RuntimeEventScope::Recovery,
                kind: "runtime.pg_conformance".to_string(),
                status: Some("committed".to_string()),
                actor: Some("backend-conformance".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"schema": 1}),
            }
            .into()],
        })
        .expect("append transaction");
    drop(first);

    let reopened = PostgresRuntimeEventStore::new(scoped)
        .expect("reopen Runtime PostgreSQL store")
        .into_runtime_event_store();
    let events = reopened
        .replay_scope_stream_prefix(RuntimeEventScope::Recovery, "conformance:runtime")
        .expect("replay persisted stream");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "runtime.pg_conformance");
    drop(reopened);
}
