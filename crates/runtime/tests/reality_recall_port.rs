#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use matrix_core::{MatrixFact, MatrixFactInput, MatrixSourceKind, MatrixSourceSnapshotInput};
use matrix_repository::open_matrix_sqlite_repository_handle;
use runtime::{AgentBindingRequest, ContextSourceKind, RealityRecallPort, RuntimeServices};
use storage::{SqliteConnectionFactory, StorageRegistry};

#[test]
fn reality_recall_port_injects_only_fact_and_matrix_evidence_granted_by_the_binding() {
    let home = tempfile::tempdir().unwrap();
    let registry = StorageRegistry::default_for_config_home(home.path());

    let matrix_handle = registry
        .sqlite_handle("matrix")
        .expect("matrix storage handle");
    std::fs::create_dir_all(matrix_handle.path.parent().expect("matrix parent"))
        .expect("matrix dir");
    let repository =
        open_matrix_sqlite_repository_handle(&matrix_handle).expect("matrix repository");
    let snapshot = repository
        .create_source_snapshot(MatrixSourceSnapshotInput {
            snapshot_id: Some("recall-port-snapshot".to_string()),
            source_pack_id: None,
            source_system: "fixture".to_string(),
            source_kind: MatrixSourceKind::Manual,
            resource_ref: Some("fixture://matrix".to_string()),
            business_period: None,
            captured_at: Some(Utc::now()),
            schema_version: Some("v1".to_string()),
            row_count: Some(1),
            checksum: None,
            confidence: Some(0.96),
            metadata: serde_json::Value::Null,
        })
        .expect("persist source snapshot");
    repository
        .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("matrix-recall-fact".to_string()),
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            fact_type: "supply.shortage".to_string(),
            entity_refs: vec!["matrix:entity:supplier-east".to_string()],
            metric_key: Some("shortage_risk".to_string()),
            dimensions: serde_json::json!({"region": "east"}),
            measures: serde_json::json!({"shortage_days": 12}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some(snapshot.reference()),
            confidence: Some(0.96),
            raw_hash: None,
        }))
        .expect("ingest Matrix fact");

    let fact_handle = registry.sqlite_handle("fact").expect("fact storage handle");
    std::fs::create_dir_all(fact_handle.path.parent().expect("fact parent")).expect("fact dir");
    let connection = SqliteConnectionFactory::default()
        .open_handle(fact_handle)
        .expect("fact db");
    connection
        .execute_batch(
            "CREATE TABLE fact_records (
                fact_id TEXT PRIMARY KEY,
                fact_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )
        .expect("fact table");
    let mut fact = fact_kernel::FactRecord::new(
        "supply.policy",
        "east region requires an expedited allocation",
    );
    fact.id = fact_kernel::FactId::from_string("fact-recall-policy");
    fact.confidence = fact_kernel::Confidence::from_basis_points(9_400);
    connection
        .execute(
            "INSERT INTO fact_records (fact_id, fact_type, status, payload_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                fact.id.as_str(),
                &fact.fact_type,
                &fact.status,
                serde_json::to_string(&fact).expect("fact json"),
                fact.updated_at.to_rfc3339(),
            ],
        )
        .expect("persist Fact");

    let services = RuntimeServices::in_memory().expect("runtime");
    let mut request = AgentBindingRequest::new(
        AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct").unwrap(),
        RevisionSelector::LatestApprovedStable,
        "instance:recall-boundary",
        "session:recall-boundary",
        "task:recall-boundary",
    );
    request.granted_capabilities = vec![AgentCapability::Read];
    request.fact_refs = vec!["fact:fact-recall-policy".to_string()];
    request.matrix_snapshot_refs = vec![snapshot.reference()];
    let mut binding = services
        .compile_agent_binding(request)
        .expect("binding")
        .snapshot;

    let port = RealityRecallPort::for_config_home(home.path());
    let report = port.recall_for_binding(&binding, "east shortage allocation", 12);
    assert!(report
        .items
        .iter()
        .any(|item| item.source == ContextSourceKind::Fact));
    assert!(report
        .items
        .iter()
        .any(|item| item.source == ContextSourceKind::Matrix));
    assert!(report
        .sources
        .iter()
        .all(|source| source.status == "enabled_and_wired"));

    binding.data_lease.fact_refs.clear();
    binding.data_lease.matrix_snapshot_refs.clear();
    let denied = port.recall_for_binding(&binding, "east shortage allocation", 12);
    assert!(
        denied.items.is_empty(),
        "no lease must not fall back to global recall"
    );
    assert!(denied
        .sources
        .iter()
        .all(|source| source.status == "disabled_by_binding"));
}
