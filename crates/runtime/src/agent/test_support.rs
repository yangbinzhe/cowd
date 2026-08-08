use harness_contract::{
    agent::{AgentAssignment, AgentDefinitionId, AgentDefinitionRevisionRef, DefinitionScope},
    execution::ExecutionIdentity,
    execution_graph::{ExecutionGraph, ExecutionGraphLineage},
};

pub(crate) fn execution_graph_lineage(graph_id: &str) -> ExecutionGraphLineage {
    let task_id = format!("test-task:{graph_id}");
    ExecutionGraphLineage {
        session_id: "test-session".to_string(),
        turn_id: format!("test-turn:{graph_id}"),
        root_task_id: task_id.clone(),
        task_id,
        generation: 1,
    }
}

pub(crate) fn attach_execution_graph_lineage(graph: &mut ExecutionGraph) {
    graph.lineage = Some(execution_graph_lineage(&graph.id));
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn agent_assignment(
    definition_ref: Option<AgentDefinitionRevisionRef>,
    instance_id: &str,
    run_id: &str,
    task_id: &str,
    session_id: &str,
    mission_id: &str,
    team_run_id: Option<&str>,
    graph_id: &str,
    node_id: &str,
) -> AgentAssignment {
    let graph_identity = ExecutionIdentity::for_task_graph(
        "test.principal",
        "test-workspace",
        mission_id,
        task_id,
        session_id,
        "test-turn",
        graph_id,
    )
    .expect("valid test graph identity");
    let parent_identity = team_run_id.map_or(graph_identity.clone(), |team_run_id| {
        ExecutionIdentity::for_team_node(&graph_identity, team_run_id, node_id)
            .expect("valid test Team identity")
    });
    let execution_identity = ExecutionIdentity::for_agent_node(&parent_identity, run_id, node_id)
        .expect("valid test Agent identity");
    AgentAssignment {
        execution_identity,
        definition_ref: definition_ref.unwrap_or_else(|| {
            AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/test-agent")
                    .expect("valid test definition id"),
                1,
            )
            .expect("valid test definition revision")
        }),
        instance_id: instance_id.to_string(),
        run_id: run_id.to_string(),
        role_id: "test-agent".to_string(),
        task_id: task_id.to_string(),
        root_task_id: task_id.to_string(),
        session_id: session_id.to_string(),
        mission_id: mission_id.to_string(),
        team_run_id: team_run_id.map(ToString::to_string),
        graph_id: graph_id.to_string(),
        node_id: node_id.to_string(),
        scope_refs: Vec::new(),
        capability_policy: Vec::new(),
    }
}
