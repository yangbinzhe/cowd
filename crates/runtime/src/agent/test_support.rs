use harness_contract::{
    agent::{AgentAssignment, AgentDefinitionId, AgentDefinitionRevisionRef, DefinitionScope},
    execution::ExecutionIdentity,
    execution_graph::{ExecutionGraph, ExecutionGraphLineage},
};
use std::sync::{Arc, OnceLock};

use session::UnifiedSessionStore;
use storage::{PostgresConnectionConfig, PostgresExecutor};

type TestPostgresExecutorKey = (
    Result<[u8; 32], String>,
    Result<PostgresConnectionConfig, String>,
);

struct TestPostgresExecutorCache {
    key: TestPostgresExecutorKey,
    result: Result<PostgresExecutor, String>,
}

impl TestPostgresExecutorCache {
    fn get(&self, key: &TestPostgresExecutorKey) -> Result<PostgresExecutor, String> {
        if &self.key != key {
            return Err(
                "Runtime PostgreSQL test executor configuration changed; start a new process"
                    .into(),
            );
        }
        self.result.clone()
    }
}

fn postgres_executor() -> PostgresExecutor {
    static EXECUTOR: OnceLock<TestPostgresExecutorCache> = OnceLock::new();
    let config = storage::test_fixture_pool_budget(1, None).map(|budget| {
        let mut config =
            PostgresConnectionConfig::new("runtime-test", "runtime.test.pg", "cowd-runtime-test");
        config.max_connections = budget;
        config.min_idle_connections = None;
        config.checkout_timeout_ms = storage::test_fixture_checkout_timeout_ms();
        config
    });
    let snapshot = std::env::var("COWD_TEST_POSTGRES_URL")
        .map_err(|_| "COWD_TEST_POSTGRES_URL is required".to_string())
        .map(|url| {
            storage::test_fixture_postgres_url_snapshot(
                "runtime.test.pg",
                &storage::ResolvedPostgresUrl::new(url),
            )
        });
    let key = (
        snapshot
            .as_ref()
            .map(|(token, _)| *token)
            .map_err(Clone::clone),
        config.clone(),
    );
    EXECUTOR
        .get_or_init(|| {
            let result = config.and_then(|config| {
                snapshot.and_then(|(_, resolver)| {
                    PostgresExecutor::connect(config, &resolver).map_err(|error| error.to_string())
                })
            });
            TestPostgresExecutorCache {
                key: key.clone(),
                result,
            }
        })
        .get(&key)
        .unwrap_or_else(|error| panic!("isolated PostgreSQL test executor: {error}"))
}

#[cfg(test)]
mod fixture_cache_tests {
    use super::*;

    #[test]
    fn fixture_cache_rejects_changed_url_role_and_budget_and_keeps_failure() {
        let config = PostgresConnectionConfig::new("runtime-test", "same-reference", "test");
        let snapshot = |value| {
            storage::test_fixture_postgres_url_snapshot(
                "same-reference",
                &storage::ResolvedPostgresUrl::new(value),
            )
            .0
        };
        let token = snapshot("postgres://role_a:secret@localhost/fixture");
        let key = (Ok(token), Ok(config.clone()));
        let cache = std::sync::OnceLock::new();
        let attempts = std::cell::Cell::new(0);
        let read = |key: &TestPostgresExecutorKey| {
            cache
                .get_or_init(|| {
                    attempts.set(attempts.get() + 1);
                    TestPostgresExecutorCache {
                        key: key.clone(),
                        result: Err("original failed attempt".into()),
                    }
                })
                .get(key)
        };
        assert_eq!(read(&key).unwrap_err(), "original failed attempt");
        for url in [
            "postgres://role_a:secret@localhost/other",
            "postgres://role_b:secret@localhost/fixture",
            "postgres://role_a:changed@localhost/fixture",
        ] {
            let error = read(&(Ok(snapshot(url)), Ok(config.clone()))).unwrap_err();
            assert!(error.contains("configuration changed"));
            assert!(!error.contains(url));
        }
        let mut changed = config.clone();
        changed.max_connections += 1;
        assert!(read(&(Ok(token), Ok(changed)))
            .unwrap_err()
            .contains("configuration changed"));
        assert!(read(&(Err("URL unavailable".into()), Ok(config)))
            .unwrap_err()
            .contains("configuration changed"));
        assert_eq!(read(&key).unwrap_err(), "original failed attempt");
        assert_eq!(attempts.get(), 1, "changed keys must not initialize a pool");
    }


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

pub(crate) fn register_running_root(
    store: Arc<crate::RuntimeEventStore>,
    context: &crate::CowdExecutionContext,
) {
    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };
    let commits = crate::ExecutionCommitService::new(store);
    let mut graph = ExecutionGraph::new("test Root execution");
    graph.id = context.execution_id.clone();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: context.session_id.clone(),
        turn_id: context.turn_id.clone(),
        root_task_id: format!("task:{}", graph.id),
        task_id: format!("task:{}", graph.id),
        generation: 1,
    });
    let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "test Root");
    let node_id = node.id.clone();
    graph.nodes.push(node);
    graph = commits.register_graph(graph).unwrap().graph;
    graph = commits
        .transition_node(&graph, &node_id, ExecutionNodeStatus::Ready, None, vec![])
        .unwrap()
        .graph;
    let node = graph.nodes.iter().find(|node| node.id == node_id).unwrap();
    let attempt =
        crate::execution_core::graph::ExecutionNodeBinding::next_attempt(&graph, &node_id).unwrap();
    let ticket = crate::execution_core::graph::NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node_id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt,
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    crate::execution_core::graph::ExecutionNodeBinding::validate_ticket(
        &ticket, &graph, node, attempt,
    )
    .unwrap();
    commits
        .bind_and_start_node(
            &graph,
            &node_id,
            crate::execution_core::graph::ExecutionNodeBinding {
                executor_kind: ticket.executor_kind,
                ticket_idempotency_key: ticket.idempotency_key,
                attempt,
                resource_lease_refs: vec![],
                scope_lease_ref: None,
                worktree_lease_ref: None,
            },
        )
        .unwrap();
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
