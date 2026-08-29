//! Real dual-backend digest conformance for the Runtime event ledger.

use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
    RuntimeEventStore,
};
use runtime_postgres::{copy_quiesced_runtime_event_store, PostgresRuntimeEventStore};
use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn sqlite_and_postgres_runtime_snapshots_have_equal_digests() {
    let sqlite = RuntimeEventStore::try_open_in_memory().expect("SQLite Runtime store");
    sqlite
        .append_transaction(AppendTransactionRequest {
            transaction_id: "runtime-backend-conformance-v1".to_string(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "conformance:runtime".to_string(),
                expected_revision: 0,
            }],
            events: vec![RuntimeEventInput {
                stream_id: "conformance:runtime".to_string(),
                scope: RuntimeEventScope::Recovery,
                kind: "runtime.backend_conformance".to_string(),
                status: Some("committed".to_string()),
                actor: Some("backend-conformance".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"schema": 1, "ordered": true}),
            }
            .into()],
        })
        .expect("seed SQLite Runtime transaction");

    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("runtime.conformance.pg".to_string(), url)]);
    let postgres = PostgresRuntimeEventStore::connect(
        PostgresConnectionConfig::new(
            "runtime-backend-conformance",
            "runtime.conformance.pg",
            "cowd-runtime-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL Runtime store")
    .into_runtime_event_store();

    let root = tempfile::tempdir().expect("manifest directory");
    let manifest = copy_quiesced_runtime_event_store(
        &sqlite,
        &postgres,
        root.path().join("runtime-cutover.json"),
    )
    .expect("quiesced Runtime copy remains digest exact");
    assert_eq!(manifest.source_digest, manifest.target_digest);
    assert_eq!(manifest.commit_count, 1);
    assert_eq!(manifest.event_count, 1);
}
