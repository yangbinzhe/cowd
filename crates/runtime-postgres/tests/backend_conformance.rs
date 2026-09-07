//! PostgreSQL-only persistence conformance for the Runtime event ledger.

use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
};
use runtime_postgres::PostgresRuntimeEventStore;
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

fn isolated_executor() -> (PostgresExecutor, PostgresExecutor, String) {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("runtime.conformance.pg".to_string(), url)]);
    let base = PostgresExecutor::connect(
        PostgresConnectionConfig::new(
            "runtime-backend-conformance",
            "runtime.conformance.pg",
            "cowd-runtime-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL executor");
    let schema = format!("cowdruntime_{}", uuid::Uuid::new_v4().simple());
    base.checkout_critical()
        .expect("connection")
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .expect("create schema");
    let scoped = base.scoped_namespace(&schema).expect("scoped executor");
    (base, scoped, schema)
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_runtime_events_survive_adapter_reconstruction() {
    let (base, scoped, schema) = isolated_executor();
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
    base.checkout_critical()
        .expect("cleanup connection")
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .expect("drop schema");
}
