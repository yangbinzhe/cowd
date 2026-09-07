#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use matrix_core::{
    MatrixScenarioOutputContract, MatrixScenarioResult, MatrixScenarioSpec, MatrixSnapshotRef,
    MatrixSourceKind, MatrixSourceSnapshotInput,
};
use matrix_repository::{MatrixStore, PostgresMatrixRepository};
use runtime::{
    AgentBindingRequest, MatrixScenarioStartRequest, RealityRecallPort, RuntimeServices,
};
use serde_json::json;
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
        let resolver = StaticSecretRefResolver::new([("runtime.matrix.scenario".to_string(), url)]);
        let executor = PostgresExecutor::connect(
            PostgresConnectionConfig::new(
                "runtime-matrix-scenario-test",
                "runtime.matrix.scenario",
                "runtime-matrix-scenario-test",
            ),
            &resolver,
        )
        .expect("PostgreSQL executor");
        let schema = format!("runtime_matrix_{}", uuid::Uuid::new_v4().simple());
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
fn matrix_scenario_port_requires_the_binding_snapshot_lease_and_emits_candidate_only_results() {
    let home = tempfile::tempdir().unwrap();
    let services = RuntimeServices::in_memory().expect("runtime");
    let fixture = MatrixFixture::isolated();
    let repository = std::sync::Arc::clone(&fixture.repository);
    let source = repository
        .create_source_snapshot(MatrixSourceSnapshotInput {
            snapshot_id: Some("orders-v7".to_string()),
            source_pack_id: None,
            source_system: "fixture-orders".to_string(),
            source_kind: MatrixSourceKind::Manual,
            resource_ref: Some("fixture://orders-v7".to_string()),
            business_period: None,
            captured_at: None,
            schema_version: Some("orders/v7".to_string()),
            row_count: Some(12),
            checksum: Some("fixture-orders-v7".to_string()),
            confidence: Some(1.0),
            metadata: json!({"fixture": true}),
        })
        .expect("persist immutable source snapshot");
    let snapshot = MatrixSnapshotRef::from_source_snapshot(&source);
    let spec = MatrixScenarioSpec::new(
        snapshot.clone(),
        json!({"supplier_outage_hours": 48}),
        "model:inventory-delay",
        MatrixScenarioOutputContract {
            required_outputs: vec!["shortage_risk".to_string()],
            evidence_required: true,
        },
    );
    let mut allowed = AgentBindingRequest::new(
        AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct").unwrap(),
        RevisionSelector::LatestApprovedStable,
        "instance:scenario",
        "session:scenario",
        "task:scenario",
    );
    allowed.granted_capabilities = vec![AgentCapability::Read];
    allowed.matrix_snapshot_refs = vec![snapshot.snapshot_ref.clone()];
    let allowed_binding = services.compile_agent_binding(allowed).unwrap().snapshot;
    let port = RealityRecallPort::with_fact_and_matrix_store(
        home.path(),
        std::sync::Arc::new(fact_kernel::EphemeralFactLedger::new()),
        repository,
    )
    .matrix_scenarios();
    let run = port
        .start(
            &allowed_binding,
            MatrixScenarioStartRequest {
                spec: spec.clone(),
                parameters: json!({"days": 2}),
            },
        )
        .expect("leased scenario start");
    let result = MatrixScenarioResult::simulated(
        &run,
        json!({"shortage_risk": "high"}),
        vec!["evidence:simulation".to_string()],
    );
    let completed = port
        .complete(&allowed_binding, result)
        .expect("leased completion");
    let candidate = port
        .fact_candidate(
            &allowed_binding,
            &completed,
            "Simulated shortage risk is high.",
        )
        .expect("candidate only");
    assert_eq!(
        candidate.reality,
        fact_kernel::hypothesis::RealityBoundary::Simulated
    );
    assert!(candidate.source.id.starts_with("matrix:scenario_result:"));

    let mut denied = AgentBindingRequest::new(
        AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct").unwrap(),
        RevisionSelector::LatestApprovedStable,
        "instance:denied",
        "session:denied",
        "task:denied",
    );
    denied.granted_capabilities = vec![AgentCapability::Read];
    let denied_binding = services.compile_agent_binding(denied).unwrap().snapshot;
    assert!(port
        .start(
            &denied_binding,
            MatrixScenarioStartRequest {
                spec,
                parameters: json!({})
            }
        )
        .is_err());
}
