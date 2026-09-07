#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use fact_kernel::FactLedger;
use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use matrix_core::{MatrixFact, MatrixFactInput, MatrixSourceKind, MatrixSourceSnapshotInput};
use matrix_repository::{MatrixStore, PostgresMatrixRepository};
use runtime::{AgentBindingRequest, ContextSourceKind, RealityRecallPort, RuntimeServices};
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

struct MatrixFixture {
    repository: std::sync::Arc<PostgresMatrixRepository>,
    executor: PostgresExecutor,
    schema: String,
}

impl MatrixFixture {
    fn isolated() -> Self {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let resolver = StaticSecretRefResolver::new([("runtime.reality.recall".to_string(), url)]);
        let executor = PostgresExecutor::connect(
            PostgresConnectionConfig::new(
                "runtime-reality-recall-test",
                "runtime.reality.recall",
                "runtime-reality-recall-test",
            ),
            &resolver,
        )
        .expect("PostgreSQL executor");
        let schema = format!("runtime_recall_{}", uuid::Uuid::new_v4().simple());
        executor
            .checkout_critical()
            .expect("PostgreSQL connection")
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .expect("isolated schema");
        let repository = std::sync::Arc::new(
            PostgresMatrixRepository::new(
                executor.scoped_namespace(&schema).expect("scoped executor"),
            )
            .expect("Matrix repository"),
        );
        Self {
            repository,
            executor,
            schema,
        }
    }
}

impl Drop for MatrixFixture {
    fn drop(&mut self) {
        if let Ok(mut connection) = self.executor.checkout_critical() {
            let _ = connection.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.schema
            ));
        }
    }
}

#[test]
#[ignore = "requires COWD_TEST_POSTGRES_URL"]
fn reality_recall_port_injects_only_fact_and_matrix_evidence_granted_by_the_binding() {
    let home = tempfile::tempdir().unwrap();
    let fixture = MatrixFixture::isolated();
    let repository = std::sync::Arc::clone(&fixture.repository);
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

    let fact_ledger = fact_kernel::EphemeralFactLedger::new();
    let mut fact = fact_kernel::FactRecord::new(
        "supply.policy",
        "east region requires an expedited allocation",
    );
    fact.id = fact_kernel::FactId::from_string("fact-recall-policy");
    fact.confidence = fact_kernel::Confidence::from_basis_points(9_400);
    fact_ledger.upsert_fact(fact).expect("persist Fact");

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

    let port = RealityRecallPort::with_fact_and_matrix_store(
        home.path(),
        std::sync::Arc::new(fact_ledger),
        repository,
    );
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

    binding.data_lease.fact_refs = vec!["not-a-fact-reference".to_string()];
    let invalid = port.recall_for_binding(&binding, "east shortage allocation", 12);
    assert!(invalid.items.is_empty());
    assert!(invalid
        .sources
        .iter()
        .all(|source| source.status == "degraded"));
}
