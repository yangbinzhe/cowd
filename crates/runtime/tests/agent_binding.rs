#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, AgentTaskIntent, DefinitionScope, RevisionSelector,
};
use harness_contract::context::ChildExecutionBudgetReservation;
use runtime::{AgentBindingRequest, RuntimeServices};

#[test]
fn binding_compiler_intersects_capabilities_and_freezes_data_leases_into_a_snapshot() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let mut request = AgentBindingRequest::new(
        AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore").expect("builtin id"),
        RevisionSelector::LatestApprovedStable,
        "instance:binding-test",
        "session:binding-test",
        "task:binding-test",
    );
    request.role_slot_id = Some("researcher:1".to_string());
    request.team_id = Some("team:binding-test".to_string());
    request.granted_capabilities = vec![AgentCapability::Read, AgentCapability::Search];
    request.fact_boundaries = vec!["observed".to_string()];
    request.fact_refs = vec!["fact:shipment-delay".to_string()];
    request.matrix_snapshot_refs = vec!["matrix:source_snapshot:orders-v7".to_string()];
    request.team_working_state_visible = true;

    let compiled = services
        .compile_agent_binding(request)
        .expect("compile immutable binding");
    let snapshot = compiled.snapshot;
    assert_eq!(
        snapshot.definition_ref.definition_id.as_str(),
        "builtin/cowd/explore"
    );
    assert_eq!(snapshot.instance.instance_id, "instance:binding-test");
    assert_eq!(
        snapshot.instance.role_slot_id.as_deref(),
        Some("researcher:1")
    );
    assert_eq!(snapshot.data_lease.fact_refs, vec!["fact:shipment-delay"]);
    assert_eq!(
        snapshot.data_lease.matrix_snapshot_refs,
        vec!["matrix:source_snapshot:orders-v7"]
    );
    assert!(snapshot
        .effective_capabilities
        .contains(&AgentCapability::Read));
    assert!(snapshot
        .effective_capabilities
        .contains(&AgentCapability::Search));
    snapshot.validate().expect("persistable binding snapshot");
    let packet = snapshot
        .compile_task_packet(
            AgentTaskIntent {
                selected_agent_id: None,
                definition_ref: Some(snapshot.definition_ref.clone()),
                granted_capabilities: Vec::new(),
                principal_id: "test.principal".to_string(),
                source_turn_id: "turn:binding-test".to_string(),
                run_id: "run:binding-test".to_string(),
                task_id: "task:binding-test".to_string(),
                root_task_id: "task:root:binding-test".to_string(),
                parent_task_id: Some("task:root:binding-test".to_string()),
                session_id: "session:binding-test".to_string(),
                mission_id: "mission:binding-test".to_string(),
                team_id: Some("team:binding-test".to_string()),
                graph_id: "graph:binding-test".to_string(),
                node_id: "node:binding-test".to_string(),
                attempt: 1,
                expected_graph_revision: 0,
                objective: "Inspect only leased facts and Matrix snapshots.".to_string(),
                required_acceptance: harness_contract::context::RequiredAcceptance {
                    criteria: vec!["evidence".to_string()],
                    evidence_obligations: Vec::new(),
                },
                acceptance: vec!["evidence".to_string()],
                constraints: Vec::new(),
                context_refs: Vec::new(),
                evidence_refs: Vec::new(),
                resource_scopes: Vec::new(),
                allowed_tools: Vec::new(),
                allowed_skills: Vec::new(),
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                model_lease: "default".to_string(),
                budget_lease: ChildExecutionBudgetReservation::single(
                    "lease:binding-test",
                    "run:binding-test",
                    "agent_task",
                    4096,
                    307_200,
                    u64::MAX,
                    1,
                ),
                deadline_at_ms: u64::MAX,
                managed_invocation: None,
                idempotency_key: "binding-test:1".to_string(),
            },
            {
                let graph_identity =
                    harness_contract::execution::ExecutionIdentity::for_task_graph(
                        "test.principal",
                        "test-workspace",
                        "mission:binding-test",
                        "task:binding-test",
                        "session:binding-test",
                        "turn:binding-test",
                        "graph:binding-test",
                    )
                    .expect("task graph identity");
                let team_identity = harness_contract::execution::ExecutionIdentity::for_team_node(
                    &graph_identity,
                    "team:binding-test",
                    "node:binding-test",
                )
                .expect("team identity");
                harness_contract::execution::ExecutionIdentity::for_agent_node(
                    &team_identity,
                    "run:binding-test",
                    "node:binding-test",
                )
                .expect("agent identity")
            },
        )
        .expect("only a compiled binding may produce an executable packet");
    assert_eq!(packet.agent_id(), "instance:binding-test");
    assert_eq!(
        packet.binding.expect("binding packet").binding_digest,
        snapshot.binding_digest
    );
}
