use harness_contract::{
    agent::{AgentAssignment, AgentDefinitionId, AgentDefinitionRevisionRef, DefinitionScope},
    execution::ExecutionIdentity,
    execution_graph::{ExecutionGraph, ExecutionGraphLineage},
};
use std::sync::{Arc, OnceLock};

use session::UnifiedSessionStore;
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

fn postgres_executor() -> PostgresExecutor {
    static EXECUTOR: OnceLock<Result<PostgresExecutor, String>> = OnceLock::new();
    EXECUTOR
        .get_or_init(|| {
            let url = std::env::var("COWD_TEST_POSTGRES_URL")
                .map_err(|_| "COWD_TEST_POSTGRES_URL is required".to_string())?;
            let resolver = StaticSecretRefResolver::new([("runtime.test.pg".to_string(), url)]);
            let mut config = PostgresConnectionConfig::new(
                "runtime-test",
                "runtime.test.pg",
                "cowd-runtime-test",
            );
            config.max_connections = 8;
            config.min_idle_connections = None;
            PostgresExecutor::connect(config, &resolver).map_err(|error| error.to_string())
        })
        .clone()
        .unwrap_or_else(|error| panic!("isolated PostgreSQL test executor: {error}"))
}

/// Build a complete Session contract over an isolated PostgreSQL schema.
/// Test runners remove `cowdrt_*` schemas after the process exits.
pub(crate) fn session_store() -> UnifiedSessionStore {
    let executor = postgres_executor();
    let schema = format!("cowdrt_{}", uuid::Uuid::new_v4().simple());
    executor
        .checkout_critical()
        .expect("PostgreSQL test connection")
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .expect("create isolated Runtime test schema");
    let scoped = executor
        .scoped_namespace(&schema)
        .expect("scope Runtime test schema");
    let backend = session_postgres::PostgresSessionStore::new(scoped)
        .expect("initialize Session PostgreSQL test adapter");
    UnifiedSessionStore::from_backend(Arc::new(backend))
}

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
