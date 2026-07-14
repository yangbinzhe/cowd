#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use matrix_core::{
    MatrixScenarioOutputContract, MatrixScenarioResult, MatrixScenarioSpec, MatrixSnapshotRef,
    MatrixSourceKind, MatrixSourceSnapshotInput,
};
use matrix_repository::open_matrix_sqlite_repository_handle;
use runtime::{
    AgentBindingRequest, MatrixScenarioStartRequest, RealityRecallPort, RuntimeServices,
};
use serde_json::json;
use storage::StorageRegistry;

#[test]
fn matrix_scenario_port_requires_the_binding_snapshot_lease_and_emits_candidate_only_results() {
    let home = tempfile::tempdir().unwrap();
    let services = RuntimeServices::in_memory().expect("runtime");
    let registry = StorageRegistry::default_for_config_home(home.path());
    let handle = registry
        .sqlite_handle("matrix")
        .expect("matrix storage handle");
    std::fs::create_dir_all(handle.path.parent().expect("matrix parent")).expect("matrix dir");
    let repository = open_matrix_sqlite_repository_handle(&handle).expect("matrix repository");
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
    let port = RealityRecallPort::for_config_home(home.path()).matrix_scenarios();
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
        fact_kernel::hypothesis::FactReality::Simulated
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
