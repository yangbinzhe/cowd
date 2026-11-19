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

/// Resolve the PostgreSQL instance the runtime fixtures use.
///
/// Configuration stays in `COWD_TEST_POSTGRES_URL`, but a local server is
/// detected instead of required: several trees run their tests side by side and
/// a hard-coded single instance turned every other tree into a failure. Standard
/// `PG*` variables describe the local server when they are present.
fn detected_postgres_url() -> Option<String> {
    static URL: OnceLock<Option<String>> = OnceLock::new();
    URL.get_or_init(|| {
        if let Ok(url) = std::env::var("COWD_TEST_POSTGRES_URL") {
            if !url.trim().is_empty() {
                return Some(url);
            }
        }
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let database = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string());
        let passwords: Vec<String> = match std::env::var("PGPASSWORD") {
            Ok(password) => vec![password],
            Err(_) => vec!["postgres".to_string(), String::new()],
        };
        for password in passwords {
            let url = format!("postgres://{user}:{password}@{host}:{port}/{database}");
            if postgres_accepts(&url) {
                return Some(url);
            }
        }
        None
    })
    .clone()
}

/// A listening socket is not a usable server: the local default can be another
/// project's instance that rejects this fixture's credentials. Detection has to
/// authenticate, so a wrong guess skips the test instead of failing it.
fn postgres_accepts(url: &str) -> bool {
    let mut config =
        PostgresConnectionConfig::new("runtime-test-probe", "runtime.test.pg", "cowd-runtime-test");
    config.max_connections = 1;
    config.min_idle_connections = None;
    config.checkout_timeout_ms = 5_000;
    let resolver = storage::StaticSecretRefResolver::new([(
        "runtime.test.pg".to_string(),
        url.to_string(),
    )]);
    PostgresExecutor::connect(config, &resolver).is_ok()
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
    let snapshot = detected_postgres_url()
        .ok_or_else(|| {
            "no PostgreSQL server: set COWD_TEST_POSTGRES_URL or PGHOST/PGPORT/PGUSER/PGPASSWORD"
                .to_string()
        })
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

/// Test hygiene: turn an absent private environment into a clean skip.
///
/// The isolated PostgreSQL instance (`COWD_TEST_POSTGRES_URL`) and the private
/// fixture roots (`COWD_R1_06_FIXTURE_ROOT`, `COWD_TEST_FIXTURE_ROOT`) are
/// optional environments. A test that needs one of them cannot run when it is
/// absent, but that is an absent environment and not a product verdict, so it
/// must not be reported as a failure.
///
/// The caller returns early when this returns `true`. The gate is inert when
/// the environment *is* present, so any product judgement inside the same test
/// is still exercised and still fails when it is wrong.
#[track_caller]
pub(crate) fn skip_if_env_absent(var: &str) -> bool {
    if std::env::var_os(var).is_some() {
        return false;
    }
    if var == "COWD_TEST_POSTGRES_URL" && detected_postgres_url().is_some() {
        return false;
    }
    eprintln!("COWD_TEST_SKIPPED: {var} is absent; this test needs a private environment");
    record_absent_environment_skip(var);
    true
}

/// Serialize the skip log so parallel tests cannot interleave half-written
/// lines. One process, one log, one writer at a time.
fn skip_log_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// Record one absent-environment skip so a run can prove how many tests
/// skipped and for which variable. The record is written only when the runner
/// asks for it through `COWD_TEST_SKIP_LOG`; otherwise the marker stays on
/// stderr only.
///
/// The whole record is assembled into one string and written with a single
/// `write_all` under a process-level lock: a formatted write issues several
/// writes, and under parallel tests those pieces interleave into torn lines.
#[track_caller]
fn record_absent_environment_skip(var: &str) {
    let Ok(path) = std::env::var("COWD_TEST_SKIP_LOG") else {
        return;
    };
    use std::io::Write;
    let thread = std::thread::current();
    let test = match thread.name() {
        Some(name) => name.to_string(),
        None => {
            let caller = std::panic::Location::caller();
            format!("{}:{}", caller.file(), caller.line())
        }
    };
    let line = format!("{test}\t{var}\n");
    let guard = match skip_log_lock().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = file.write_all(line.as_bytes());
    }
    drop(guard);
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

pub(crate) fn bind_running_graph_node(
    commits: &crate::ExecutionCommitService,
    graph: &ExecutionGraph,
    node_id: &str,
) -> (
    ExecutionGraph,
    harness_contract::agent_action::OriginalAgentExecution,
) {
    use harness_contract::execution_graph::ExecutionNodeStatus;

    let mut graph = graph.clone();
    if graph.node_statuses.get(node_id).is_none() {
        graph
            .node_statuses
            .insert(node_id.to_string(), ExecutionNodeStatus::Planned);
    }
    let graph = if graph.node_statuses.get(node_id) == Some(&ExecutionNodeStatus::Planned) {
        commits
            .transition_node(&graph, node_id, ExecutionNodeStatus::Ready, None, Vec::new())
            .expect("agent action fixture node becomes ready")
            .graph
    } else {
        graph
    };
    let attempt =
        crate::execution_core::graph::ExecutionNodeBinding::next_attempt(&graph, node_id)
            .expect("agent action fixture attempt");
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .expect("agent action fixture node");
    let binding = crate::execution_core::graph::ExecutionNodeBinding {
        executor_kind: node.executor_kind.clone(),
        ticket_idempotency_key: node.idempotency_key.clone(),
        attempt,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let ticket = crate::execution_core::graph::NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node_id.to_string(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt,
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            std::collections::BTreeMap::from([(node_id.to_string(), binding)]),
            vec![ticket],
        )
        .expect("agent action fixture binds and starts its physical node")
        .graph;
    assert_eq!(
        graph.node_statuses.get(node_id),
        Some(&ExecutionNodeStatus::Running)
    );
    let original = harness_contract::agent_action::OriginalAgentExecution {
        graph_id: graph.id.clone(),
        node_id: node_id.to_string(),
        attempt,
    };
    debug_assert_ne!(original.attempt, 0);
    (graph, original)
}

pub(crate) fn register_running_agent_action_graph(
    store: Arc<crate::RuntimeEventStore>,
    graph_id: &str,
    node_id: &str,
    session_id: &str,
    turn_id: &str,
) -> harness_contract::agent_action::OriginalAgentExecution {
    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
    let graphs = crate::ExecutionGraphStateStore::new(store);
    if let Ok(graph) = graphs.load(graph_id) {
        if graph.node_statuses.get(node_id)
            == Some(&ExecutionNodeStatus::Running)
        {
            return graph
                .recovery_cursor
                .node_attempts
                .get(node_id)
                .filter(|attempt| **attempt != 0)
                .map(|attempt| harness_contract::agent_action::OriginalAgentExecution {
                    graph_id: graph_id.to_string(),
                    node_id: node_id.to_string(),
                    attempt: *attempt,
                })
                .expect("running fixture node has an original attempt");
        }
        return bind_running_graph_node(&commits, &graph, node_id).1;
    }
    let mut graph = ExecutionGraph::new("agent action physical owner");
    graph.id = graph_id.to_string();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_task_id: format!("fixture-task:{graph_id}"),
        task_id: format!("fixture-task:{graph_id}"),
        generation: 1,
    });
    let node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "fixture", "fixture-payload");
    let mut node = node;
    node.id = node_id.to_string();
    node.idempotency_key = format!("{node_id}:attempt");
    graph
        .node_statuses
        .insert(node_id.to_string(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    let graph = commits
        .register_graph(graph)
        .expect("agent action fixture graph registers")
        .graph;
    bind_running_graph_node(&commits, &graph, node_id).1
}

struct OriginalDispatchSessionPort {
    record: std::sync::Mutex<crate::RuntimeSessionInputRecord>,
}

#[async_trait::async_trait]
impl crate::SessionRuntimeQueryPort for OriginalDispatchSessionPort {
    async fn cancellation_authorization_by_id(
        &self,
        _request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
    ) -> Result<Option<session::SessionDomainEvent>, session::SessionError> {
        Ok(None)
    }

    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        None
    }

    async fn session_record(
        &self,
        _session_id: &str,
    ) -> Result<Option<crate::RuntimeSessionRecord>, session::SessionError> {
        Ok(None)
    }

    async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputRecord>, session::SessionError> {
        let record = self
            .record
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok((record.request_id == request_id).then_some(record))
    }

    async fn runtime_input_by_input_id(
        &self,
        _input_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputRecord>, session::SessionError> {
        Ok(None)
    }

    async fn pending_session_input_applications(
        &self,
        _session_id: &str,
        _session_generation: u64,
        _after: Option<(usize, String)>,
        _limit: usize,
    ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
        Ok(Vec::new())
    }

    async fn input_admission(
        &self,
        _session_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputAdmission>, session::SessionError> {
        Ok(None)
    }

    async fn evidence_access(
        &self,
        _session_id: &str,
        _evidence_id: &str,
    ) -> Result<Option<harness_contract::context::EvidenceAccessRef>, session::SessionError> {
        Ok(None)
    }

    async fn session_evidence_receipt_ids(
        &self,
        _session_id: &str,
        _limit: usize,
    ) -> Result<Vec<String>, session::SessionError> {
        Ok(Vec::new())
    }
}

pub(crate) fn install_original_dispatch_session(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    request_id: &str,
    task_id: &str,
) -> (
    String,
    crate::execution_core::services::ActiveExecutionBusLease,
) {
    let ingress = crate::TurnIngressRef {
        request_id: request_id.to_string(),
        turn_id: turn_id.to_string(),
        message_id: format!("message:{request_id}"),
        session_id: session_id.to_string(),
        primary_task_id: task_id.to_string(),
        root_task_id: task_id.to_string(),
        session_generation: 1,
        input_sequence: 1,
        claim_owner: format!("owner:{session_id}"),
        claim_token: format!("claim:{request_id}"),
        claim_revision: 1,
        handoff_id: None,
    };
    let execution_id =
        crate::session_ingress_graph_id(session_id, request_id, turn_id);
    let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
    if graphs.load(&execution_id).is_err() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        };

        let mut graph = ExecutionGraph::new("original dispatch root");
        graph.id = execution_id.clone();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            root_task_id: task_id.to_string(),
            task_id: task_id.to_string(),
            generation: 1,
        });
        let mut node =
            ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "fixture");
        node.id = format!("{execution_id}:dispatch-root");
        node.idempotency_key = format!("{execution_id}:dispatch-root:attempt");
        graph.nodes.push(node);
        crate::ExecutionCommitService::new(Arc::clone(services.event_store()))
            .register_graph(graph)
            .expect("original dispatch root graph");
    }
    let control = services
        .create_turn_cancellation_control(&execution_id, &ingress)
        .expect("original dispatch turn control");
    services.record_live_execution(
        session_id,
        execution_id.clone(),
        turn_id.to_string(),
    );
    let lease = services
        .bind_active_execution_bus(execution_id.clone(), crate::CowdEventBus::new())
        .expect("original dispatch execution bus");
    let now = chrono::Utc::now().timestamp_millis() as u64;
    let record = crate::RuntimeSessionInputRecord {
        input_id: format!("input:{request_id}"),
        request_id: request_id.to_string(),
        turn_id: turn_id.to_string(),
        message_id: ingress.message_id.clone(),
        session_id: session_id.to_string(),
        sequence: ingress.input_sequence as usize,
        session_generation: ingress.session_generation,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: Some(turn_id.to_string()),
        classification_json: None,
        task_route_hint: None,
        status: crate::RuntimeSessionInputStatus::Running,
        runtime_commit_cursor: None,
        attempts: 1,
        next_attempt_at_ms: 0,
        claim_owner: Some(ingress.claim_owner.clone()),
        claim_token: Some(ingress.claim_token.clone()),
        claim_fence_epoch: Some(ingress.claim_revision),
        claim_expires_at_ms: Some(now + 600_000),
        failure_class: None,
        last_error: None,
        revision: ingress.claim_revision,
        created_at_ms: now,
        updated_at_ms: now,
        terminal_at_ms: None,
        runtime_options_json: None,
        application_receipt: None,
    };
    let port = Arc::new(OriginalDispatchSessionPort {
        record: std::sync::Mutex::new(record),
    });
    let fence = crate::SessionExecutionFence::from_claim(
        port as Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress.request_id.clone(),
        ingress.session_id.clone(),
        ingress.session_generation,
        ingress.input_sequence as usize,
        ingress.claim_owner.clone(),
        ingress.claim_token.clone(),
    )
    .expect("original dispatch Session fence");
    let input = crate::SessionInputStream::new(session_id.to_string());
    input.set_active_turn(Some(harness_contract::turn::TurnId::from_string(
        turn_id,
    )));
    // The owner must be the very control the Session published. A fresh
    // `PermissionPolicy::new` owns a different Arc, and the supervisor's
    // checked source then reports the policy owner as replaced.
    let original_policy = services
        .session_execution_policy_control(session_id)
        .map_or_else(
            || {
                crate::permissions::PermissionPolicy::new(
                    crate::permissions::PermissionMode::ReadOnly,
                )
            },
            crate::permissions::PermissionPolicy::with_execution_policy_control,
        );
    lease
        .install_session_fence(
            &ingress,
            fence,
            control,
            input,
            Arc::new(crate::conversation::OriginalTurnInputControl::default()),
            original_policy,
        )
        .expect("original dispatch Session owner");
    (execution_id, lease)
}

/// Install the original dispatch Session owner **and** the root Task aggregate
/// its ingress claims, in the order the product requires.
///
/// The root Task is what the Session ingress names as both `primary_task_id`
/// and `root_task_id`; without it the Program projection has no root work to
/// attach a dispatched attempt to. Creating it here keeps every dispatch
/// fixture on the same shape instead of repeating the aggregate boilerplate.
pub(crate) fn install_original_dispatch_fixture(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    request_id: &str,
    task_id: &str,
) -> (
    String,
    crate::execution_core::services::ActiveExecutionBusLease,
) {
    let installed = install_original_dispatch_session(services, session_id, turn_id, request_id, task_id);
    if services
        .task_runtime_port()
        .get(task_id)
        .expect("root task read")
        .is_none()
    {
        let spec = services
            .task_runtime_port()
            .bind_task_spec(
                session_id,
                None,
                harness_contract::task::TaskSpec::new(format!("{task_id} root task")),
            )
            .expect("root task spec");
        services
            .task_aggregate_service()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: task_id.to_string(),
                mission_id: services.mission_runtime().default_mission_id().to_string(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: session_id.to_string(),
                origin_turn_id: turn_id.to_string(),
                root_task_id: task_id.to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "fixture".to_string(),
                spec,
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    format!("test://{request_id}/root"),
                )],
            })
            .expect("root task aggregate");
    }
    installed
}

/// Register the graph an Agent actor uses to prove it holds a live attempt
/// lease, and check that the registered graph really carries that proof.
///
/// The product accepts two proofs for an Agent actor: an original execution, or
/// an active attempt lease on an AgentTask node whose packet carries the
/// matching program/agent/team/membership. Fixtures that have no original
/// execution use this helper for the second proof.
pub(crate) fn register_agent_lease_graph(
    store: &Arc<crate::RuntimeEventStore>,
    projection: &crate::agentic::AgenticProgramProjection,
    actor: &harness_contract::agent_action::AgentActorBinding,
    graph_id: &str,
    task_ref: &str,
) {
    let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(store));
    let registered = graphs.load(graph_id).is_ok();
    if !registered {
        let graph = agent_lease_graph(projection, actor, graph_id, task_ref);
        crate::ExecutionCommitService::new(Arc::clone(store))
            .register_graph(graph)
            .expect("register agent lease graph");
    }
}

/// Build the graph an Agent actor uses to prove it holds a live attempt lease.
///
/// The product accepts two proofs for an Agent actor: an original execution, or
/// an active attempt lease on an AgentTask node whose packet carries the
/// matching program/agent/team/membership. Fixtures that have no original
/// execution register this graph so the lease matches.
pub(crate) fn agent_lease_graph(
    projection: &crate::agentic::AgenticProgramProjection,
    actor: &harness_contract::agent_action::AgentActorBinding,
    graph_id: &str,
    task_ref: &str,
) -> ExecutionGraph {
    use harness_contract::execution_graph::{
        ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
    };
    let mut graph = ExecutionGraph::new("Agent attempt lease");
    graph.id = graph_id.to_string();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: actor.session_id.clone(),
        turn_id: actor.turn_id.clone(),
        root_task_id: format!("task:{}", graph_id),
        task_id: format!("task:{}", task_ref),
        generation: 1,
    });
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
        "Agent attempt lease",
    );
    node.id = format!("{graph_id}:agent");
    node.idempotency_key = format!("{graph_id}:agent:1");
    node.payload_ref = serde_json::to_string(&agent_task_packet_for_actor(
        projection, actor, graph_id, &node.id, task_ref,
    ))
    .expect("AgentTask packet");
    graph.nodes.push(node);
    graph
}

/// Build the AgentTask packet a fixture graph node must carry so the Agent
/// actor that owns that node is recognised as the physical lease holder.
///
/// The product accepts two proofs for an Agent actor: an original execution, or
/// an active attempt lease on a running AgentTask node whose packet carries the
/// matching program/agent/team/membership. Fixtures use this helper for the
/// second proof.
pub(crate) fn agent_task_packet_for_actor(
    projection: &crate::agentic::AgenticProgramProjection,
    actor: &harness_contract::agent_action::AgentActorBinding,
    graph_id: &str,
    node_id: &str,
    task_ref: &str,
) -> harness_contract::agent::AgentTaskPacket {
    use harness_contract::agent::{AgenticExecutionBinding, AgenticExecutionFocus, AgentTaskPacket};
    let agent_id = actor.agent_id.clone().unwrap_or_default();
    let team_id = actor.team_id.clone().unwrap_or_default();
    let membership_id = projection
        .membership_for(&agent_id, &team_id)
        .map(|membership| membership.membership_id.clone())
        .unwrap_or_else(|| format!("membership:{agent_id}:{team_id}"));
    AgentTaskPacket {
        assignment: agent_assignment(
            None,
            &agent_id,
            "fixture-run",
            task_ref,
            actor.session_id.as_str(),
            "fixture-mission",
            Some(team_id.as_str()),
            graph_id,
            node_id,
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "review".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: Vec::new(),
        cohort_prompt_package: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget", "agent", "agent", 1, u64::MAX, 1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "fixture-key".into(),
        agentic_binding: Some(AgenticExecutionBinding {
            program_id: actor.program_id.clone(),
            agent_id: actor.agent_id.clone().unwrap_or_default(),
            membership_id,
            team_id: team_id.clone(),
            task_team_id: team_id,
            source_spec_revision: 0,
            task_purpose: None,
            focus: AgenticExecutionFocus::TaskExecute { task_ref: task_ref.to_string() },
        }),
    }
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

/// Register and start the physical Agent execution that owns a Program Task.
///
/// The A-family Program-wait fixtures must act under a real compiled AgentTask
/// binding: Runtime's own supervisor records the attempt dispatch, the packet is
/// compiled from a real intent, and the node is bound Running so the artifact
/// gate can authenticate the original execution.
pub(crate) fn register_physical_agent_child(
    services: &crate::RuntimeServices,
    root_execution_id: &str,
    parent_node_id: &str,
    program_id: &str,
    child_id: &str,
    task_ref: &str,
    actor: &harness_contract::agent_action::AgentActorBinding,
) -> harness_contract::agent_action::OriginalAgentExecution {
    use harness_contract::agent::{
        AgentCapability, AgentTaskIntent, AgenticExecutionBinding, AgenticExecutionFocus,
    };
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorKind, AgentAttemptMode,
        TaskAttemptDispatchInput,
    };
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
    };

    let actions = services.agent_action_service();
    let projection = actions.project(program_id).expect("program projection");
    let execution_task_id = projection
        .execution_task_id(task_ref)
        .expect("physical execution task id");
    let session_id = actor.session_id.clone();
    let turn_id = actor.turn_id.clone();
    services.publish_session_execution_policy(
        &session_id,
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let root_task_id = format!("task:{program_id}");
    let mission_id = services.mission_runtime().default_mission_id().to_string();
    if services
        .task_runtime_port()
        .get(&root_task_id)
        .expect("root task read")
        .is_none()
    {
        let spec = services
            .task_runtime_port()
            .bind_task_spec(
                &session_id,
                None,
                harness_contract::task::TaskSpec::new(format!("{program_id} root task")),
            )
            .expect("root task spec");
        services
            .task_aggregate_service()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: root_task_id.clone(),
                mission_id: mission_id.clone(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: session_id.clone(),
                origin_turn_id: turn_id.clone(),
                root_task_id: root_task_id.clone(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "fixture".to_string(),
                spec,
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    format!("test://{program_id}/root"),
                )],
            })
            .expect("root task aggregate");
    }
    let agent_id = actor.agent_id.clone().expect("agent id");
    let team_id = actor.team_id.clone().expect("team id");
    let membership_id = projection
        .membership_for(&agent_id, &team_id)
        .expect("active membership")
        .membership_id
        .clone();
    let catalog = services.agent_runtime().catalog();
    if catalog.get(&agent_id).is_none() {
        catalog.upsert(crate::AgentCatalogEntry {
            definition_ref: harness_contract::agent::AgentDefinitionRevisionRef::new(
                harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/direct",
                )
                .expect("builtin direct definition id"),
                1,
            )
            .expect("builtin direct definition revision"),
            agent_id: agent_id.clone(),
            name: "program wait fixture agent".to_string(),
            description: "program wait physical child fixture".to_string(),
            capabilities: vec!["read".to_string()],
            skill_refs: Vec::new(),
            scope: harness_contract::agent::DefinitionScope::Builtin,
            evaluation: harness_contract::evaluation::EvaluationContract::single_release_gate(
                "program-wait-fixture",
                "fixture",
            ),
        });
    }
    let definition_ref = catalog
        .get(&agent_id)
        .expect("invited agent catalog entry")
        .definition_ref
        .clone();
    let task_team_id = projection
        .tasks
        .get(task_ref)
        .expect("program task")
        .team_id
        .clone();
    let objective = projection
        .tasks
        .get(task_ref)
        .map(|task| task.objective.clone())
        .unwrap_or_else(|| "complete the program task".to_string());
    let node_id = format!("{child_id}:agent");
    let intent = AgentTaskIntent {
        selected_agent_id: Some(agent_id.clone()),
        definition_ref: Some(definition_ref),
        granted_capabilities: vec![AgentCapability::Read],
        principal_id: "runtime.agentic".to_string(),
        source_turn_id: turn_id.clone(),
        run_id: format!("{child_id}:run"),
        task_id: execution_task_id.clone(),
        root_task_id: root_task_id.clone(),
        parent_task_id: Some(root_task_id.clone()),
        session_id: session_id.clone(),
        mission_id: mission_id.clone(),
        team_id: None,
        graph_id: child_id.to_string(),
        node_id: node_id.clone(),
        attempt: 1,
        expected_graph_revision: 0,
        objective,
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: Vec::new(),
        constraints: Vec::new(),
        context_refs: vec![format!("program:{program_id}"), format!("task:{task_ref}")],
        evidence_refs: Vec::new(),
        resource_scopes: vec![format!("program:{program_id}")],
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "test".to_string(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget", "agent", "agent", 1, u64::MAX, 1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        idempotency_key: format!("{node_id}:attempt"),
        agentic_binding: Some(AgenticExecutionBinding {
            program_id: program_id.to_string(),
            agent_id: agent_id.clone(),
            membership_id: membership_id.clone(),
            team_id: team_id.clone(),
            task_team_id,
            source_spec_revision: projection.revision,
            task_purpose: None,
            focus: AgenticExecutionFocus::TaskExecute {
                task_ref: task_ref.to_string(),
            },
        }),
    };
    let mut graph = ExecutionGraph::new("Agentic child");
    graph.id = child_id.to_string();
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: session_id.clone(),
        turn_id: turn_id.clone(),
        root_task_id: root_task_id.clone(),
        task_id: execution_task_id,
        generation: 1,
    });
    graph.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: root_execution_id.to_string(),
        node_id: parent_node_id.to_string(),
    });
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
        serde_json::to_string(&intent).expect("AgentTask intent"),
    );
    node.id = node_id.clone();
    node.idempotency_key = intent.idempotency_key.clone();
    node.acceptance.criteria = intent.acceptance.clone();
    node.resource_scopes.push(format!("program:{program_id}"));
    graph.nodes.push(node);
    let mut graph = services
        .compile_graph_agent_intents(graph)
        .expect("compile AgentTask intent");
    {
        use harness_contract::agent::{AgentDisplayIdentity, AgentTaskPacket};
        let mut packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref)
            .expect("compiled AgentTask packet");
        if let Some(binding) = packet.binding.as_mut() {
            binding.display = Some(AgentDisplayIdentity {
                agent_id: agent_id.clone(),
                role_id: "agent".to_string(),
                role_display_name: None,
                label: "Program wait fixture".to_string(),
                role_label: "Agent".to_string(),
                focus_label: None,
                locale: "en".to_string(),
                provenance: format!("runtime.agentic:{program_id}"),
                digest: "fixture-display".to_string(),
            });
            binding.binding_digest = crate::agent::binding::recompute_binding_digest(binding)
                .expect("recompute binding digest");
        }
        graph.nodes[0].payload_ref = serde_json::to_string(&packet).expect("compiled packet");
    }
    services
        .commit_service()
        .register_graph(graph)
        .expect("register Agent child");
    let supervisor = harness_contract::agent_action::AgentActorBinding {
        original_execution: None,
        objective_id: projection.objective_id.clone(),
        program_id: projection.program_id.clone(),
        session_id: projection.session_id.clone(),
        turn_id: projection.turn_id.clone(),
        root_execution_id: projection.root_execution_id.clone(),
        required_team_count: projection.required_team_count,
        exact_team_count: projection.exact_team_count,
        objective_summary: projection.objective_summary.clone(),
        model_lease: projection.model_lease.clone(),
        permission_ceiling: Some(projection.permission_ceiling),
        resource_scopes: projection.resource_scopes.clone(),
        actor_id: "runtime.program-supervisor".to_string(),
        kind: AgentActorKind::Supervisor,
        execution_id: Some(child_id.to_string()),
        team_id: None,
        agent_id: None,
    };
    let dispatch = actions
        .apply_runtime_supervisor_internal_for_test(&AgentActionEnvelope {
            action_id: format!("{child_id}:dispatch"),
            actor: supervisor,
            expected_revision: None,
            action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: task_ref.to_string(),
                execution_id: child_id.to_string(),
                agent_ref: agent_id,
                membership_id,
                mode: AgentAttemptMode::Execute,
                generation: 0,
            }),
        })
        .expect("dispatch physical Agent attempt");
    assert_eq!(
        dispatch.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "physical Agent attempt dispatch was rejected: {:?}",
        dispatch.error
    );
    let graph = services
        .graph_state_store()
        .load(child_id)
        .expect("registered Agent child");
    bind_running_graph_node(services.commit_service(), &graph, &node_id).1
}
