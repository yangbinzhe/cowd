#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::DefinitionScope;
use harness_contract::team::{
    TeamInstantiationRequest, TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
};
#[path = "support/canonical_agent_fixture.rs"]
mod canonical_agent_fixture;

fn request(mission_id: &str) -> TeamInstantiationRequest {
    TeamInstantiationRequest {
        request_id: "working-state-commit".to_string(),
        team_id: "team:working-state-commit".to_string(),
        mission_id: mission_id.to_string(),
        lineage: harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session:working-state-commit".to_string(),
            turn_id: "turn:working-state-commit".to_string(),
            root_task_id: "task:root:working-state-commit".to_string(),
            task_id: "task:root:working-state-commit".to_string(),
            generation: 1,
        },
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
        strategy_binding: None,
        template_selector: TeamTemplateSelector::LatestStable {
            template_id: TeamTemplateDefinitionId::new(
                DefinitionScope::Builtin,
                "cowd/direct-executor",
            )
            .unwrap(),
        },
        objective: "Produce an evidence-backed bounded conclusion.".to_string(),
        acceptance: vec!["summary".to_string(), "evidence".to_string()],
        risk: None,
        role_binding_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "deepseek-v4-flash".to_string(),
        execution_budget: harness_contract::context::ParentExecutionBudget::new(
            "working-state-team-budget",
            65_536,
            4_915_200,
            u64::MAX,
            32,
            1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        resource_scopes: vec![
            "read:crates/runtime".to_string(),
            "session:working-state-commit".to_string(),
        ],
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
    }
}

#[tokio::test]
async fn terminal_graph_transition_commits_exactly_one_replayable_team_working_state_entry() {
    let (services, _provider) =
        canonical_agent_fixture::services_with_canonical_agent("session:working-state-commit")
            .await;
    let projection = services
        .team_runtime()
        .instantiate(request(services.mission_runtime().default_mission_id()))
        .await
        .expect("team completion");
    assert_eq!(projection.status, "completed");

    let state = services
        .team_runtime()
        .working_state("team:working-state-commit")
        .expect("working state");
    assert_eq!(state.graph_id, projection.graph_id);
    assert_eq!(state.entries.len(), 1);
    assert!(state.entries[0].summary.contains("completed with evidence"));
    assert!(!state.entries[0].producer_instance_id.is_empty());
    assert!(state.entries[0]
        .boundary
        .contains("no raw chain-of-thought"));
    let expected_refs = [
        ("principal", "runtime.team"),
        ("mission", services.mission_runtime().default_mission_id()),
        ("task", "team:working-state-commit:task:executor:1"),
        ("session", "session:working-state-commit"),
        ("turn", "turn:working-state-commit"),
        ("team_run", "team:working-state-commit"),
    ];
    for stream_id in [
        projection.graph_id.as_str(),
        "team-working-state:team:working-state-commit",
    ] {
        let event = services
            .event_reader()
            .list_stream(stream_id)
            .expect("lineage event stream")
            .into_iter()
            .last()
            .expect("lineage event");
        for (kind, id) in expected_refs {
            assert!(
                event
                    .refs
                    .iter()
                    .any(|reference| reference.kind == kind && reference.id == id),
                "{stream_id} must retain {kind}:{id}"
            );
        }
    }
    assert_eq!(
        services
            .team_runtime()
            .working_state("team:working-state-commit")
            .unwrap(),
        state,
        "event replay is idempotent"
    );
}
