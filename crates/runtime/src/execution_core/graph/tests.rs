use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage,
    ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use harness_contract::{context::EvidenceAccessRef, reality::EvidenceRef};

use super::*;
use crate::execution_core::RuntimeCompileTarget;
use crate::runtime_event_store::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};
use tokio::sync::Notify;

fn test_graph(objective: impl Into<String>) -> ExecutionGraph {
    let graph = ExecutionGraph::new(objective);
    let graph_id = graph.id.clone();
    let task_id = format!("test-task:{graph_id}");
    graph.with_lineage(ExecutionGraphLineage {
        session_id: "test-session".to_string(),
        turn_id: format!("test-turn:{graph_id}"),
        root_task_id: task_id.clone(),
        task_id,
        generation: 1,
    })
}

struct TestExecutor {
    fail_nodes: Vec<String>,
    delay: Duration,
    running: AtomicUsize,
    max_running: AtomicUsize,
    recoveries: AtomicUsize,
    calls: Mutex<Vec<String>>,
}

impl TestExecutor {
    fn new(fail_nodes: Vec<String>, delay: Duration) -> Self {
        Self {
            fail_nodes,
            delay,
            running: AtomicUsize::new(0),
            max_running: AtomicUsize::new(0),
            recoveries: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl NodeExecutor for TestExecutor {
    fn kind(&self) -> &str {
        "test"
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.payload_ref.is_empty() {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "empty payload".to_string(),
            });
        }
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let current = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_running.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ticket.node_id.clone());
        if self.fail_nodes.contains(&ticket.node_id) {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "injected failure".to_string(),
            });
        }
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn recover(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.recoveries.fetch_add(1, Ordering::SeqCst);
        Ok(ticket.clone())
    }
}

struct StartFailExecutor;

struct StartRaceExecutor {
    start_entered: Notify,
    release_start: Notify,
    poll_calls: AtomicUsize,
    cancel_calls: AtomicUsize,
    cancelled: AtomicBool,
}

struct ReplanRaceExecutor {
    poll_entered: Notify,
    release_poll: Notify,
}

struct CancelNestedRunnerExecutor {
    runner: OnceLock<ExecutionGraphRunner>,
    poll_entered: Notify,
    release_poll: Notify,
    cancel_entered: Notify,
    release_cancel: Notify,
    nested_completed: AtomicBool,
}

struct PostCommitExecutor {
    after_commits: AtomicUsize,
    after_aborts: AtomicUsize,
    invalid_domain_event: bool,
    poll_error: bool,
}

struct BlockingPostCommitExecutor {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct PermanentPendingExecutor {
    cancel_calls: AtomicUsize,
}

#[async_trait]
impl NodeExecutor for PermanentPendingExecutor {
    fn kind(&self) -> &str {
        "permanent_pending"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        std::future::pending().await
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Executes a nested graph from a parent node. Runtime orchestration uses the
/// same pattern through ToolBatch, so this protects the Runner from holding
/// its coordination gate across re-entrant executor work.
struct ReentrantRunnerExecutor {
    runner: OnceLock<ExecutionGraphRunner>,
}

#[async_trait]
impl NodeExecutor for ReentrantRunnerExecutor {
    fn kind(&self) -> &str {
        "reentrant_runner"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:reentrant", context.node.idempotency_key),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let runner = self
            .runner
            .get()
            .expect("runner is installed for reentrant executor");
        let mut nested = test_graph("nested execution from tool-like node");
        let mut nested_tool = node("nested-tool");
        nested_tool.resource_scopes = vec!["read:fixtures/shared.txt".to_string()];
        nested.nodes.push(nested_tool);
        runner
            .start(nested)
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
}

#[async_trait]
impl NodeExecutor for CancelNestedRunnerExecutor {
    fn kind(&self) -> &str {
        "cancel_nested_runner"
    }

    fn supports_resumable_pause(&self) -> bool {
        false
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:cancel-nested", context.node.idempotency_key),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_entered.notify_one();
        self.release_poll.notified().await;
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.runner
            .get()
            .expect("runner is installed for nested cancellation")
            .start(test_graph("nested graph during parent cancellation"))
            .await
            .map_err(|error| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        self.nested_completed.store(true, Ordering::SeqCst);
        self.cancel_entered.notify_one();
        self.release_cancel.notified().await;
        Ok(())
    }
}

struct PayloadScopedBackend {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl super::executors::ScopedNodeBackend for PayloadScopedBackend {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
}

struct PayloadScopedResolver {
    payload_ref: String,
    backend: Arc<PayloadScopedBackend>,
}

impl super::executors::ScopedNodeBackendResolver for PayloadScopedResolver {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        (ticket.payload_ref == self.payload_ref)
            .then(|| Arc::clone(&self.backend) as Arc<dyn ScopedNodeBackend>)
    }
}

#[async_trait]
impl NodeExecutor for PostCommitExecutor {
    fn kind(&self) -> &str {
        "post_commit"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if self.poll_error {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "preview stream failed before commit".to_string(),
            });
        }
        let mut outcome = NodeExecutionOutcome::new(completed_result(&ticket.node_id));
        if self.invalid_domain_event {
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("side-effect:{}", ticket.node_id),
                        scope: crate::RuntimeEventScope::AgentDefinition,
                        kind: "test.side_effect".to_string(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::Value::Null,
                    },
                    idempotency_key: Some(format!("protected:{}", ticket.idempotency_key)),
                    schema_version: 1,
                });
        }
        Ok(outcome)
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.after_commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn after_abort(
        &self,
        _ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        self.after_aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for BlockingPostCommitExecutor {
    fn kind(&self) -> &str {
        "blocking_post_commit"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let mut successor = node("successor");
        successor.executor_kind = "test".to_string();
        Ok(
            NodeExecutionOutcome::new(completed_result(&ticket.node_id)).with_replan(
                ExecutionGraphReplan {
                    nodes: vec![successor.clone()],
                    edges: vec![ExecutionEdge {
                        from: ticket.node_id.clone(),
                        to: successor.id,
                        kind: ExecutionEdgeKind::DependsOn,
                    }],
                    reason: "post-commit coordination regression".to_string(),
                },
            ),
        )
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for ReplanRaceExecutor {
    fn kind(&self) -> &str {
        "replan_race"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_entered.notify_one();
        self.release_poll.notified().await;
        Ok(
            NodeExecutionOutcome::new(completed_result(&ticket.node_id)).with_replan(
                ExecutionGraphReplan {
                    nodes: vec![node("late-tool")],
                    edges: vec![ExecutionEdge {
                        from: ticket.node_id.clone(),
                        to: "late-tool".to_string(),
                        kind: ExecutionEdgeKind::DependsOn,
                    }],
                    reason: "late dynamic tool".to_string(),
                },
            ),
        )
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.release_poll.notify_one();
        Ok(())
    }
}

impl StartRaceExecutor {
    fn new() -> Self {
        Self {
            start_entered: Notify::new(),
            release_start: Notify::new(),
            poll_calls: AtomicUsize::new(0),
            cancel_calls: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl NodeExecutor for StartRaceExecutor {
    fn kind(&self) -> &str {
        "start_race"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.start_entered.notify_one();
        self.release_start.notified().await;
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct EvidenceExecutor {
    evidence_id: Option<String>,
}

#[async_trait]
impl NodeExecutor for EvidenceExecutor {
    fn kind(&self) -> &str {
        "evidence_test"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let evidence_refs = self
            .evidence_id
            .as_ref()
            .map(|id| {
                vec![EvidenceAccessRef::durable(
                    EvidenceRef::observed("evidence", id),
                    format!("sha:{id}"),
                    1,
                    "text/plain",
                    format!("evidence://{id}"),
                    "workspace",
                )]
            })
            .unwrap_or_default();
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("source-result:{}", ticket.node_id)),
            summary: Some("source completed".to_string()),
            evidence_refs,
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }
}

struct TerminalBackend {
    calls: Arc<AtomicUsize>,
}

struct TerminalResolver {
    graph_id: String,
    backend: Arc<TerminalBackend>,
}

impl super::executors::SynthesizeBackendResolver for TerminalResolver {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn super::executors::SynthesizeBackend>> {
        (ticket.graph_id == self.graph_id)
            .then(|| Arc::clone(&self.backend) as Arc<dyn super::executors::SynthesizeBackend>)
    }
}

#[async_trait]
impl super::executors::SynthesizeBackend for TerminalBackend {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("terminal:{}", ticket.graph_id)),
            summary: Some("terminal completed".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for StartFailExecutor {
    fn kind(&self) -> &str {
        "start_fail"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Err(NodeExecutorError::Start {
            node_id: context.node.id,
            reason: "intentional start failure".to_string(),
        })
    }

    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        unreachable!("a failed start must never be polled")
    }
}

fn harness() -> (
    Arc<NodeExecutorRegistry>,
    ExecutionGraphStateStore,
    ExecutionCommitService,
) {
    let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    (
        Arc::new(NodeExecutorRegistry::new()),
        ExecutionGraphStateStore::new(Arc::clone(&event_store)),
        ExecutionCommitService::new(event_store),
    )
}

fn test_runner(
    registry: Arc<NodeExecutorRegistry>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
) -> ExecutionGraphRunner {
    let workspace_id = format!("test-{}", uuid::Uuid::new_v4());
    let leases = WorktreeLeaseManager::open(
        std::env::temp_dir()
            .join("cowd-execution-graph-tests")
            .join(&workspace_id)
            .join("leases.json"),
    )
    .expect("worktree leases");
    ExecutionGraphRunner::new(
        registry,
        state,
        commits,
        Arc::new(ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Provider,
                ResourceQuota::new(1, 4, 8).expect("provider quota"),
            ),
            (
                ExecutionResourceKind::Agent,
                ResourceQuota::new(1, 4, 8).expect("agent quota"),
            ),
            (
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 4, 8).expect("tool quota"),
            ),
        ])),
        Arc::new(ScopeLockManager::new()),
        Arc::new(leases),
        workspace_id,
        std::env::temp_dir(),
    )
}

fn node(id: &str) -> ExecutionNodeSpec {
    ExecutionNodeSpec {
        id: id.to_string(),
        kind: ExecutionNodeKind::ToolBatch,
        payload_ref: format!("payload:{id}"),
        executor_kind: "test".to_string(),
        idempotency_key: format!("idempotency:{id}"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
        work: None,
    }
}

fn completed_result(id: &str) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some(format!("result:{id}")),
        summary: Some(format!("result {id}")),
        evidence_refs: Vec::new(),
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    }
}

#[tokio::test]
async fn supervisor_runs_one_hundred_graphs_with_bounded_cross_key_parallelism() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(10)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        128,
        8,
        Duration::from_secs(2),
    ));

    let mut graph_ids = Vec::new();
    for index in 0..100 {
        let mut graph = test_graph(format!("parallel graph {index}"));
        graph.id = format!("parallel-graph-{index}");
        graph.nodes.push(node(&format!("node-{index}")));
        let receipt = supervisor
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        graph_ids.push(receipt.graph_id);
    }

    let mut waiters = tokio::task::JoinSet::new();
    for graph_id in graph_ids {
        let supervisor = Arc::clone(&supervisor);
        waiters.spawn(async move {
            supervisor
                .wait_for_quiescence(&graph_id)
                .await
                .expect("graph reaches quiescence");
        });
    }
    while let Some(result) = waiters.join_next().await {
        result.unwrap();
    }

    let max_running = executor.max_running.load(Ordering::SeqCst);
    assert!(
        max_running > 1,
        "different graph keys must run concurrently"
    );
    assert!(
        max_running <= 8,
        "supervisor concurrency must respect its configured upper bound"
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 100);
    assert_eq!(supervisor.health().tracked_keys, 0);
    let shutdown = supervisor.shutdown().await;
    assert_eq!(shutdown.remaining_keys, 0);
    assert_eq!(shutdown.forced_aborts, 0);
}

#[tokio::test]
async fn two_root_teams_overlap_through_real_supervisor_and_agent_resource_quota() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(75)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        2,
        Duration::from_secs(2),
    ));

    for index in 0..2 {
        let mut graph = test_graph(format!("root Team {index}"));
        graph.id = format!("root-team-overlap-{index}");
        let mut agent = node(&format!("root-team-agent-{index}"));
        agent.kind = ExecutionNodeKind::AgentTask;
        agent.payload_ref = serde_json::json!({ "deadline_at_ms": u64::MAX }).to_string();
        graph.nodes.push(agent);
        supervisor
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("root Team admitted");
    }
    for index in 0..2 {
        supervisor
            .wait_for_quiescence(&format!("root-team-overlap-{index}"))
            .await
            .expect("root Team settles");
    }
    assert_eq!(
        executor.max_running.load(Ordering::SeqCst),
        2,
        "two independent root Teams must overlap when graph and Agent capacity allow it"
    );
    supervisor.shutdown().await;
}

#[tokio::test]
async fn durable_agent_deadline_cancels_permanent_branch_and_unblocks_finally() {
    let (registry, state, commits) = harness();
    let pending = Arc::new(PermanentPendingExecutor {
        cancel_calls: AtomicUsize::new(0),
    });
    registry.register(pending.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("deadline keeps finally reachable");
    let graph_id = graph.id.clone();
    let mut stuck = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        pending.kind(),
        serde_json::json!({
            "deadline_at_ms": crate::tool_invocation::now_ms().saturating_add(50)
        })
        .to_string(),
    );
    stuck.id = "stuck-agent".to_string();
    stuck.idempotency_key = "stuck-agent-attempt".to_string();
    let sibling = node("healthy-sibling");
    let mut finally = node("finally");
    finally.work = Some(harness_contract::execution_graph::ExecutionWorkContract {
        role: harness_contract::execution_graph::ExecutionWorkRole::Synthesize,
        dependency: harness_contract::execution_graph::ExecutionDependencyPolicy::Finally,
        ..harness_contract::execution_graph::ExecutionWorkContract::new(
            harness_contract::execution_graph::ExecutionWorkRole::Synthesize,
        )
    });
    graph.edges = vec![
        ExecutionEdge {
            from: stuck.id.clone(),
            to: finally.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: sibling.id.clone(),
            to: finally.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    graph.nodes = vec![stuck, sibling, finally];

    tokio::time::timeout(Duration::from_secs(1), runner.start(graph))
        .await
        .expect("durable deadline prevents a permanent branch hang")
        .expect("graph reaches quiescence");
    let graph = state.load(&graph_id).unwrap();
    assert_eq!(
        graph.node_statuses["stuck-agent"],
        ExecutionNodeStatus::Cancelled
    );
    assert_eq!(
        graph.node_results["stuck-agent"]
            .failure
            .as_ref()
            .map(|failure| failure.kind.as_str()),
        Some("execution_deadline_exceeded")
    );
    assert_eq!(
        graph.node_statuses["healthy-sibling"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        graph.node_statuses["finally"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(pending.cancel_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn supervisor_coalesces_same_key_wakes_and_reclaims_the_slot() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(50)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        64,
        8,
        Duration::from_secs(2),
    ));
    let mut graph = test_graph("same key");
    graph.id = "same-key-graph".to_string();
    graph.nodes.push(node("same-key-node"));
    supervisor
        .submit_graph(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();

    let mut waiters = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let supervisor = Arc::clone(&supervisor);
        waiters.spawn(async move {
            supervisor
                .wait_for_quiescence("same-key-graph")
                .await
                .expect("same key waiter");
        });
    }
    while let Some(result) = waiters.join_next().await {
        result.unwrap();
    }

    assert_eq!(executor.max_running.load(Ordering::SeqCst), 1);
    assert_eq!(
        executor.calls.lock().unwrap().as_slice(),
        &["same-key-node".to_string()]
    );
    assert_eq!(supervisor.health().tracked_keys, 0);
    supervisor.shutdown().await;
}

#[tokio::test]
async fn graph_host_returns_after_admission_before_slow_execution_finishes() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(200)));
    registry.register(executor).unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        1,
        Duration::from_secs(2),
    );
    let mut graph = test_graph("admission only");
    graph.id = "admission-only-graph".to_string();
    graph.nodes.push(node("slow-node"));
    let receipt = supervisor
        .submit_graph(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(receipt.graph_id, "admission-only-graph");
    let projection = supervisor.projection(&receipt.graph_id).await.unwrap();
    assert!(
        projection
            .nodes
            .iter()
            .any(|node| !node.status.is_terminal()),
        "admission must not wait for the slow node to finish"
    );
    supervisor
        .wait_for_quiescence(&receipt.graph_id)
        .await
        .unwrap();
    supervisor.shutdown().await;
}

#[tokio::test]
async fn supervisor_shutdown_cancels_owned_work_and_zeroes_owner_health() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        1,
        Duration::from_secs(2),
    );
    let started = Arc::new(Notify::new());
    let started_work = Arc::clone(&started);
    supervisor
        .spawn_owned(
            "shutdown-test",
            Box::pin(async move {
                started_work.notify_one();
                std::future::pending::<()>().await;
            }),
        )
        .await
        .unwrap();
    started.notified().await;

    let report = supervisor.shutdown().await;
    let owner = report.owners.get("shutdown-test").unwrap();
    assert_eq!(owner.active, 0);
    assert_eq!(owner.aborted, 1);
    assert_eq!(report.forced_aborts, 0);
    assert_eq!(report.remaining_keys, 0);
}

#[tokio::test]
async fn supervisor_reaps_aborted_join_handles_through_its_owned_reaper() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        1,
        1,
        Duration::from_secs(2),
    );
    let worker = tokio::spawn(std::future::pending::<()>());

    supervisor.reap_join_handle("join-handle-reaper-test", worker);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor
                .health()
                .owners
                .get("join-handle-reaper-test")
                .is_some_and(|owner| owner.completed == 1 && owner.active == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted join handle is reaped");

    let report = supervisor.shutdown().await;
    let owner = report.owners.get("join-handle-reaper-test").unwrap();
    assert_eq!(owner.submitted, 1);
    assert_eq!(owner.completed, 1);
    assert_eq!(owner.active, 0);
    assert_eq!(report.forced_aborts, 0);
}

#[test]
fn compiler_exposes_the_three_canonical_bootstrap_targets() {
    let compiler = ExecutionGraphCompiler;
    for target in [
        RuntimeCompileTarget::InlineModel,
        RuntimeCompileTarget::EvidenceGraph,
        RuntimeCompileTarget::ExecutionGraph,
    ] {
        let graph = compiler
            .compile(ExecutionCompileRequest {
                objective: "solve".to_string(),
                payload_ref: "payload:turn".to_string(),
                target,
                resource_scopes: Vec::new(),
            })
            .expect("available target");
        assert!(!graph.nodes.is_empty());
    }
}

#[test]
fn conversation_compile_targets_have_distinct_initial_dags_and_constraints() {
    let compiler = ExecutionGraphCompiler;
    let compile = |target| {
        compiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: "turn".to_string(),
                payload_ref: "durable-turn-payload".to_string(),
                target,
                resource_scopes: vec!["write:src/lib.rs".to_string()],
            })
            .unwrap()
    };
    let inline = compile(RuntimeCompileTarget::InlineModel);
    let evidence = compile(RuntimeCompileTarget::EvidenceGraph);
    let execution = compile(RuntimeCompileTarget::ExecutionGraph);

    assert_eq!(inline.nodes.len(), 1);
    assert_eq!(evidence.nodes.len(), 2);
    assert_eq!(execution.nodes.len(), 3);
    assert!(evidence.nodes[0]
        .acceptance
        .criteria
        .contains(&"evidence_read_before_synthesis".to_string()));
    assert!(execution.nodes.iter().any(|node| {
        node.resource_scopes == ["write:src/lib.rs"]
            && node
                .acceptance
                .criteria
                .contains(&"mutation_resources_must_be_leased".to_string())
    }));
}

#[test]
fn compiler_applies_resource_scopes_only_to_side_effect_nodes() {
    let graph = ExecutionGraphCompiler
        .compile(ExecutionCompileRequest {
            objective: "update one file".to_string(),
            payload_ref: "payload:turn".to_string(),
            target: RuntimeCompileTarget::ExecutionGraph,
            resource_scopes: vec!["write:src/lib.rs".to_string(), "worktree:.".to_string()],
        })
        .expect("execution graph");

    for node in graph.nodes {
        if node.kind == ExecutionNodeKind::ToolBatch {
            assert_eq!(node.resource_scopes, ["write:src/lib.rs", "worktree:."]);
        } else {
            assert!(node.resource_scopes.is_empty());
        }
    }
}

#[test]
fn registry_rejects_ambiguous_duplicate_executor_binding() {
    let registry = NodeExecutorRegistry::new();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    assert!(matches!(
        registry.register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        ))),
        Err(NodeExecutorError::DuplicateExecutor(kind)) if kind == "test"
    ));
}

#[tokio::test]
async fn executor_start_failure_never_persists_running_or_binding() {
    let (registry, state, commits) = harness();
    registry.register(Arc::new(StartFailExecutor)).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("start failure");
    let mut failing = node("start-failure");
    failing.executor_kind = "start_fail".to_string();
    graph.nodes.push(failing);
    let graph_id = graph.id.clone();

    let report = runner
        .start(graph)
        .await
        .expect("start failure is isolated to the failing node");
    assert_eq!(report.failed, 1);
    let persisted = state.load(&graph_id).expect("persisted graph");
    assert_eq!(
        persisted.node_statuses["start-failure"],
        ExecutionNodeStatus::Failed
    );
}

#[tokio::test]
async fn cancel_queued_during_start_commits_before_poll_and_prevents_side_effects() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(StartRaceExecutor::new());
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("cancel startup race");
    let mut race_node = node("race");
    race_node.executor_kind = "start_race".to_string();
    graph.nodes.push(race_node);
    let graph_id = graph.id.clone();
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "race", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let run = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move { runner.run_until_quiescent(&graph_id).await })
    };
    executor.start_entered.notified().await;
    let command = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: graph.revision + 1,
                        reason: "startup race".to_string(),
                    },
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    executor.release_start.notify_one();

    let cancelled = command.await.unwrap().unwrap();
    run.await.unwrap().unwrap();
    assert_eq!(
        cancelled.node_statuses["race"],
        ExecutionNodeStatus::Cancelled
    );
    assert!(executor.cancelled.load(Ordering::SeqCst));
    assert_eq!(executor.cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["race"],
        ExecutionNodeStatus::Cancelled
    );
}

#[tokio::test]
async fn aborted_cancel_releases_intent_and_nested_child_graph_can_progress() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(CancelNestedRunnerExecutor {
        runner: OnceLock::new(),
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        cancel_entered: Notify::new(),
        release_cancel: Notify::new(),
        nested_completed: AtomicBool::new(false),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    executor
        .runner
        .set(runner.clone())
        .unwrap_or_else(|_| panic!("runner is installed once"));

    let mut graph = test_graph("abort cancellation intent");
    let mut candidate = node("cancel-probe");
    candidate.executor_kind = executor.kind().to_string();
    graph.nodes.push(candidate);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();

    let cancel = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: running.revision,
                        reason: "abort command future".to_string(),
                    },
                )
                .await
        })
    };
    executor.cancel_entered.notified().await;
    assert!(
        executor.nested_completed.load(Ordering::SeqCst),
        "executor.cancel must advance a nested graph without the parent coordination lock"
    );

    // Make the parent result wait on the in-flight command intent, then abort
    // that command. CommandIntentOwner::drop must wake the old waiter.
    executor.release_poll.notify_one();
    tokio::task::yield_now().await;
    cancel.abort();
    let _ = cancel.await;
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("old intent waiter is released")
        .expect("run task joins")
        .expect("parent graph can finish after command abort");

    let completed = state.load(&graph_id).unwrap();
    assert_eq!(
        completed.node_statuses["cancel-probe"],
        ExecutionNodeStatus::Completed
    );
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        runner.command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: completed.revision,
                reason: "subsequent command".to_string(),
            },
        ),
    )
    .await
    .expect("subsequent command is not stuck")
    .expect("subsequent command is accepted");
    assert_eq!(
        cancelled.node_statuses["cancel-probe"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn active_non_resumable_executor_rejects_pause_without_cancelling() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(CancelNestedRunnerExecutor {
        runner: OnceLock::new(),
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        cancel_entered: Notify::new(),
        release_cancel: Notify::new(),
        nested_completed: AtomicBool::new(false),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    executor
        .runner
        .set(runner.clone())
        .unwrap_or_else(|_| panic!("runner is installed once"));

    let mut graph = test_graph("non-resumable active pause");
    let mut candidate = node("agent-like");
    candidate.executor_kind = executor.kind().to_string();
    graph.nodes.push(candidate);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();
    assert!(matches!(
        runner
            .command(
                &graph_id,
                ExecutionGraphCommand::Pause {
                    expected_revision: running.revision,
                    reason: "unsupported active pause".to_string(),
                },
            )
            .await,
        Err(ExecutionRunnerError::Commit(
            ExecutionCommitError::InvalidCommand(_)
        ))
    ));
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["agent-like"],
        ExecutionNodeStatus::Running
    );
    executor.release_poll.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn pause_queued_during_start_commits_before_poll_and_prevents_side_effects() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(StartRaceExecutor::new());
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("pause startup race");
    let mut race_node = node("race");
    race_node.executor_kind = "start_race".to_string();
    graph.nodes.push(race_node);
    let graph_id = graph.id.clone();
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "race", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let run = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move { runner.run_until_quiescent(&graph_id).await })
    };
    executor.start_entered.notified().await;
    let command = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Pause {
                        expected_revision: graph.revision + 1,
                        reason: "startup race".to_string(),
                    },
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    executor.release_start.notify_one();

    let paused = command.await.unwrap().unwrap();
    run.await.unwrap().unwrap();
    assert_eq!(paused.node_statuses["race"], ExecutionNodeStatus::Paused);
    assert!(executor.cancelled.load(Ordering::SeqCst));
    assert_eq!(executor.cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["race"],
        ExecutionNodeStatus::Paused
    );
}

#[test]
fn worktree_scope_rejects_absolute_parent_and_symlink_escape() {
    let workspace = tempfile::tempdir().unwrap();
    let child = workspace.path().join("child");
    std::fs::create_dir(&child).unwrap();
    assert_eq!(
        validate_worktree_path(workspace.path(), "child").unwrap(),
        child.canonicalize().unwrap()
    );
    assert!(validate_worktree_path(workspace.path(), "/tmp").is_err());
    assert!(validate_worktree_path(workspace.path(), "../outside").is_err());

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/tmp", workspace.path().join("escape")).unwrap();
        assert!(validate_worktree_path(workspace.path(), "escape").is_err());
    }
}

#[test]
fn terminal_replan_is_one_transaction_and_survives_projection_restart() {
    let database = tempfile::NamedTempFile::new().unwrap();
    let event_store = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("atomic terminal replan");
    graph.nodes.push(node("model"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "model",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "model",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let before_revision = graph.revision;
    let added = node("tool");
    let receipt = commits
        .transition_node_with_replan(
            &graph,
            "model",
            completed_result("model"),
            Vec::new(),
            vec![added],
            vec![ExecutionEdge {
                from: "model".to_string(),
                to: "tool".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            }],
            "model requested tool".to_string(),
        )
        .unwrap();

    assert_eq!(receipt.graph.revision, before_revision + 1);
    assert_eq!(receipt.transaction.event_ids.len(), 2);
    drop(commits);
    drop(event_store);
    let reopened = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let projected = ExecutionGraphStateStore::new(reopened)
        .load(&graph.id)
        .unwrap();
    assert_eq!(projected.revision, before_revision + 1);
    assert_eq!(
        projected.node_statuses["model"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        projected.node_statuses["tool"],
        ExecutionNodeStatus::Planned
    );
    assert!(projected
        .nodes
        .iter()
        .any(|candidate| candidate.id == "tool"));
}

#[test]
fn graph_admission_rejects_missing_canonical_business_lineage_without_panicking() {
    let event_store = Arc::new(RuntimeEventStore::open_in_memory().unwrap());
    let commits = ExecutionCommitService::new(event_store);
    let error = match commits.register_graph(ExecutionGraph::new("missing lineage")) {
        Ok(_) => panic!("unscoped graph must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(error, ExecutionCommitError::InvalidCommand(_)));
    assert!(error.to_string().contains("canonical business lineage"));
}

#[tokio::test]
async fn cancel_wins_over_inflight_dynamic_replan_without_partial_graph_mutation() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(ReplanRaceExecutor {
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("command versus replan");
    let mut model = node("model");
    model.executor_kind = "replan_race".to_string();
    graph.nodes.push(model);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();
    assert_eq!(running.node_statuses["model"], ExecutionNodeStatus::Running);
    let cancelled = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "operator superseded replan".to_string(),
            },
        )
        .await
        .unwrap();
    run.await.unwrap().unwrap();

    assert_eq!(
        cancelled.node_statuses["model"],
        ExecutionNodeStatus::Cancelled
    );
    let projected = state.load(&graph_id).unwrap();
    assert_eq!(
        projected.node_statuses["model"],
        ExecutionNodeStatus::Cancelled
    );
    assert!(!projected
        .nodes
        .iter()
        .any(|candidate| candidate.id == "late-tool"));
}

#[tokio::test]
async fn execution_graph_host_admits_and_supervisor_drives_the_same_graph() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor =
        crate::RuntimeExecutionSupervisor::new(Arc::new(test_runner(registry, state, commits)));
    let mut graph = test_graph("host submission");
    graph.nodes.push(node("host-node"));
    let graph_id = graph.id.clone();

    let (receipt, report) = supervisor
        .submit_and_wait(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("host submission");

    assert_eq!(receipt.graph_id, graph_id);
    let projection = supervisor.projection(&graph_id).await.unwrap();
    assert_eq!(projection.nodes[0].status, ExecutionNodeStatus::Completed);
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn nested_graph_submission_from_executor_never_deadlocks_runner_coordination() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(ReentrantRunnerExecutor {
        runner: OnceLock::new(),
    });
    registry.register(executor.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    assert!(executor.runner.set(runner.clone()).is_ok());
    let mut graph = test_graph("parent submits nested graph");
    let mut node = node("orchestrate");
    node.kind = ExecutionNodeKind::AgentTask;
    node.executor_kind = "reentrant_runner".to_string();
    node.resource_scopes = vec!["write:fixtures/shared.txt".to_string()];
    graph.nodes.push(node);
    let graph_id = graph.id.clone();

    let report = tokio::time::timeout(Duration::from_secs(1), runner.start(graph))
        .await
        .expect("nested graph submission must not wait on the parent coordination lock")
        .expect("parent graph completes");

    assert_eq!(report.completed, 1);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["orchestrate"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn rejects_missing_executor_before_persisting_graph() {
    let (registry, state, commits) = harness();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("missing executor");
    graph.nodes.push(node("missing"));
    let graph_id = graph.id.clone();

    assert!(matches!(
        runner.start(graph).await,
        Err(ExecutionRunnerError::Executor(
            NodeExecutorError::Unavailable { .. }
        ))
    ));
    assert!(matches!(
        state.load(&graph_id),
        Err(ExecutionStateStoreError::NotFound(_))
    ));
}

#[tokio::test]
async fn executes_independent_ready_nodes_concurrently_then_dependency() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(30)));
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("parallel wave");
    graph.nodes = vec![node("a"), node("b"), node("join")]
        .into_iter()
        .map(|mut node| {
            let mut work = harness_contract::execution_graph::ExecutionWorkContract::new(
                harness_contract::execution_graph::ExecutionWorkRole::Tool,
            );
            work.expected_duration_ms = 30;
            node.work = Some(work);
            node
        })
        .collect();
    graph.edges = vec![
        ExecutionEdge {
            from: "a".to_string(),
            to: "join".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: "b".to_string(),
            to: "join".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.expect("run graph");
    assert_eq!(report.completed, 3);
    assert!(executor.max_running.load(Ordering::SeqCst) >= 2);
    let calls = executor
        .calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let join_index = calls.iter().position(|node| node == "join").unwrap();
    assert!(calls.iter().position(|node| node == "a").unwrap() < join_index);
    assert!(calls.iter().position(|node| node == "b").unwrap() < join_index);
    let final_graph = state.load(&graph_id).unwrap();
    assert!(final_graph.recovery_cursor.commit_cursor > 0);
    let work = harness_contract::execution_graph::project_work_graph(&final_graph)
        .expect("work graph metrics");
    assert_eq!(work.width, 2);
    assert_eq!(work.depth, 2);
    assert!(work.actual_serial_ms >= 90);
    assert!(work.actual_critical_path_ms < work.actual_serial_ms);
    assert!(work
        .actual_speedup_basis_points
        .is_some_and(|value| value > 10_000));
}

#[tokio::test]
async fn failure_blocks_only_dependent_branch() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            vec!["fail".to_string()],
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("failure propagation");
    graph.nodes = vec![node("fail"), node("dependent"), node("independent")];
    graph.edges = vec![ExecutionEdge {
        from: "fail".to_string(),
        to: "dependent".to_string(),
        kind: ExecutionEdgeKind::DependsOn,
    }];
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.expect("run graph");
    assert_eq!(report.failed, 1);
    assert_eq!(report.blocked, 1);
    assert_eq!(report.completed, 1);
    let final_graph = state.load(&graph_id).unwrap();
    assert_eq!(
        final_graph.node_statuses,
        BTreeMap::from([
            ("dependent".to_string(), ExecutionNodeStatus::Blocked),
            ("fail".to_string(), ExecutionNodeStatus::Failed),
            ("independent".to_string(), ExecutionNodeStatus::Completed),
        ])
    );
}

#[tokio::test]
async fn pause_resume_and_cancel_are_revision_checked() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("commands");
    graph.nodes.push(node("a"));
    let graph_id = graph.id.clone();
    let mut graph = commits.register_graph(graph).unwrap().graph;
    graph = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let paused = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Pause {
                expected_revision: graph.revision,
                reason: "operator".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(paused.node_statuses["a"], ExecutionNodeStatus::Paused);
    assert!(matches!(
        runner
            .command(
                &graph_id,
                ExecutionGraphCommand::Cancel {
                    expected_revision: graph.revision,
                    reason: "stale".to_string(),
                },
            )
            .await,
        Err(ExecutionRunnerError::Commit(
            ExecutionCommitError::StaleRevision { .. }
        ))
    ));
    let resumed = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Resume {
                expected_revision: paused.revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.node_statuses["a"], ExecutionNodeStatus::Ready);
    runner.run_until_quiescent(&graph_id).await.unwrap();
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["a"],
        ExecutionNodeStatus::Completed
    );
}

#[test]
fn duplicate_transition_returns_original_transaction_without_duplicate_events() {
    let (_registry, state, commits) = harness();
    let mut graph = test_graph("idempotent commit");
    graph.nodes.push(node("a"));
    let graph = commits.register_graph(graph).unwrap().graph;

    let first = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();
    let duplicate = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();

    assert!(!first.transaction.duplicate);
    assert!(duplicate.transaction.duplicate);
    assert_eq!(
        first.transaction.commit_cursor,
        duplicate.transaction.commit_cursor
    );
    assert_eq!(state.load(&graph.id).unwrap().revision, 2);
}

#[tokio::test]
async fn recovery_requeues_retryable_running_node_and_preserves_waiting_approval() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(1)));
    registry.register(executor.clone()).unwrap();
    let mut graph = test_graph("recover");
    let mut retryable = node("retryable");
    retryable.retry_policy.max_attempts = 2;
    graph.nodes = vec![retryable, node("approval")];
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "retryable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "retryable",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "approval",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "approval",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "approval",
            ExecutionNodeStatus::WaitingApproval,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;

    let recovery = ExecutionGraphRecovery::new(&state, &commits, &registry);
    let recovered = recovery.recover(&graph.id).await.unwrap();
    assert_eq!(
        recovered.node_statuses["retryable"],
        ExecutionNodeStatus::Ready
    );
    assert_eq!(
        recovered.node_statuses["approval"],
        ExecutionNodeStatus::WaitingApproval
    );
    assert_eq!(executor.recoveries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_rebuilds_running_node_from_persistent_graph_payload() {
    let database = tempfile::NamedTempFile::new().unwrap();
    let event_store = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("persistent restart");
    graph.nodes.push(node("durable"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph_id = graph.id.clone();
    drop(commits);
    drop(event_store);

    let reopened = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let state = ExecutionGraphStateStore::new(Arc::clone(&reopened));
    let commits = ExecutionCommitService::new(reopened);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(1)));
    registry.register(executor.clone()).unwrap();
    let recovered = ExecutionGraphRecovery::new(&state, &commits, &registry)
        .recover(&graph_id)
        .await
        .unwrap();
    assert_eq!(recovered.nodes[0].payload_ref, "payload:durable");
    assert_eq!(
        recovered.node_statuses["durable"],
        ExecutionNodeStatus::Ready
    );

    test_runner(registry, state.clone(), commits)
        .run_until_quiescent(&graph_id)
        .await
        .unwrap();
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["durable"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(executor.recoveries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_installs_fresh_scoped_resolver_and_executes_from_persistent_payload() {
    let database = tempfile::NamedTempFile::new().unwrap();
    let event_store = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("resolver restart");
    let mut durable = node("durable");
    durable.executor_kind = "durable_scoped".to_string();
    graph.nodes.push(durable);
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph_id = graph.id.clone();
    drop(commits);
    drop(event_store);

    let reopened = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let state = ExecutionGraphStateStore::new(Arc::clone(&reopened));
    let commits = ExecutionCommitService::new(reopened);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(super::executors::ScopedNodeExecutor::new("durable_scoped"));
    let calls = Arc::new(AtomicUsize::new(0));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: "payload:durable".to_string(),
        backend: Arc::new(PayloadScopedBackend {
            calls: Arc::clone(&calls),
        }),
    }));
    registry
        .register(executor as Arc<dyn NodeExecutor>)
        .unwrap();
    ExecutionGraphRecovery::new(&state, &commits, &registry)
        .recover(&graph_id)
        .await
        .unwrap();
    test_runner(registry, state.clone(), commits)
        .run_until_quiescent(&graph_id)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["durable"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn restart_replays_completed_effect_receipt_without_provider_or_tool_reexecution() {
    let database = tempfile::NamedTempFile::new().unwrap();
    let event_store = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("effect receipt restart");
    graph.nodes.push(node("durable"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: "durable".to_string(),
        executor_kind: "test".to_string(),
        service_class: graph.service_class,
        attempt: 2,
        idempotency_key: "idempotency:durable:2".to_string(),
        payload_ref: "payload:durable".to_string(),
    };
    assert_eq!(
        commits.begin_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Fresh
    );
    commits
        .commit_execution_effect(
            &ticket,
            &NodeExecutionOutcome::new(completed_result("durable")),
        )
        .unwrap();
    let graph_id = graph.id.clone();
    drop(commits);
    drop(event_store);

    let reopened = Arc::new(RuntimeEventStore::try_open(database.path()).unwrap());
    let state = ExecutionGraphStateStore::new(Arc::clone(&reopened));
    let commits = ExecutionCommitService::new(reopened);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(1)));
    registry.register(executor.clone()).unwrap();
    ExecutionGraphRecovery::new(&state, &commits, &registry)
        .recover(&graph_id)
        .await
        .unwrap();
    test_runner(registry, state.clone(), commits)
        .run_until_quiescent(&graph_id)
        .await
        .unwrap();

    assert!(executor
        .calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty());
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["durable"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn restart_blocks_inflight_effect_without_receipt_as_typed_uncertain() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(1)));
    registry.register(executor.clone()).unwrap();
    let mut graph = test_graph("uncertain effect restart");
    graph.nodes.push(node("durable"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    commits
        .begin_execution_effect(&NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: "durable".to_string(),
            executor_kind: "test".to_string(),
            service_class: graph.service_class,
            attempt: 2,
            idempotency_key: "idempotency:durable:2".to_string(),
            payload_ref: "payload:durable".to_string(),
        })
        .unwrap();
    ExecutionGraphRecovery::new(&state, &commits, &registry)
        .recover(&graph.id)
        .await
        .unwrap();
    test_runner(registry, state.clone(), commits)
        .run_until_quiescent(&graph.id)
        .await
        .unwrap();
    let recovered = state.load(&graph.id).unwrap();
    assert_eq!(
        recovered.node_statuses["durable"],
        ExecutionNodeStatus::Blocked
    );
    assert_eq!(
        recovered.node_results["durable"]
            .failure
            .as_ref()
            .unwrap()
            .kind,
        "effect_completion_uncertain"
    );
    assert!(executor
        .calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty());
}

#[tokio::test]
async fn process_side_effect_hook_runs_only_after_graph_commit() {
    for (
        invalid_domain_event,
        poll_error,
        expected_run_ok,
        expected_after_commits,
        expected_after_aborts,
    ) in [
        (false, false, true, 1, 0),
        (true, false, false, 0, 1),
        // Poll failure is a failed-node report rather than Runner corruption,
        // but any already-streamed preview still has to be aborted.
        (false, true, true, 0, 1),
    ] {
        let (registry, state, commits) = harness();
        let executor = Arc::new(PostCommitExecutor {
            after_commits: AtomicUsize::new(0),
            after_aborts: AtomicUsize::new(0),
            invalid_domain_event,
            poll_error,
        });
        registry.register(executor.clone()).unwrap();
        let runner = test_runner(registry, state, commits);
        let mut graph = test_graph("post commit boundary");
        let mut terminal = node("terminal");
        terminal.executor_kind = "post_commit".to_string();
        graph.nodes.push(terminal);
        let result = runner.start(graph).await;
        assert_eq!(result.is_ok(), expected_run_ok, "{result:?}");
        assert_eq!(
            executor.after_commits.load(Ordering::SeqCst),
            expected_after_commits
        );
        assert_eq!(
            executor.after_aborts.load(Ordering::SeqCst),
            expected_after_aborts,
            "a preview-producing outcome that cannot commit must be explicitly aborted"
        );
    }
}

#[tokio::test]
async fn post_commit_callback_never_holds_graph_coordination_lock() {
    let (registry, state, commits) = harness();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    registry
        .register(Arc::new(BlockingPostCommitExecutor {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }))
        .unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = Arc::new(test_runner(registry, state.clone(), commits));
    let mut graph = test_graph("post commit coordination");
    let mut root = node("root");
    root.executor_kind = "blocking_post_commit".to_string();
    graph.nodes.push(root);
    let graph_id = graph.id.clone();
    let run = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move { runner.start(graph).await })
    };

    entered.notified().await;
    let revision = state.load(&graph_id).unwrap().revision;
    let command = runner.command(
        &graph_id,
        ExecutionGraphCommand::Pause {
            expected_revision: revision,
            reason: "exercise coordination while callback is pending".to_string(),
        },
    );
    let paused = tokio::time::timeout(Duration::from_millis(100), command)
        .await
        .expect("post-commit callback must not block graph command coordination")
        .expect("pause should commit while callback remains pending");
    assert!(paused.revision > revision);

    release.notify_one();
    let report = run.await.unwrap().unwrap();
    assert!(report.completed >= 1);
    assert!(state
        .load(&graph_id)
        .unwrap()
        .node_statuses
        .contains_key("successor"));
}

#[tokio::test]
async fn workspace_root_scope_is_a_valid_hierarchical_lock_scope() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state, commits);
    let mut graph = test_graph("workspace root scope");
    let mut root = node("root");
    root.resource_scopes = vec!["read:.".to_string()];
    graph.nodes.push(root);

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
}

#[tokio::test]
async fn workspace_absolute_scope_is_normalized_before_locking() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state, commits);
    let mut graph = test_graph("workspace absolute scope");
    let mut root = node("root");
    root.resource_scopes = vec![format!(
        "read:{}",
        std::env::temp_dir().join("Cargo.toml").display()
    )];
    graph.nodes.push(root);

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(report.blocked, 0);
}

#[tokio::test]
async fn invalid_resource_scope_becomes_a_durable_node_blocker() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("invalid resource scope");
    let mut root = node("root");
    root.resource_scopes = vec!["read:/outside-workspace".to_string()];
    graph.nodes.push(root);
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.blocked, 1);
    let graph = state.load(&graph_id).unwrap();
    assert_eq!(graph.node_statuses["root"], ExecutionNodeStatus::Blocked);
    assert_eq!(
        graph.node_results["root"].failure.as_ref().unwrap().kind,
        "resource_acquisition_failed"
    );
}

async fn run_verify_graph(evidence_id: Option<&str>, required: &str) -> (ExecutionGraph, usize) {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(EvidenceExecutor {
            evidence_id: evidence_id.map(str::to_string),
        }))
        .unwrap();
    registry
        .register(Arc::new(super::executors::VerifyNodeExecutor::new(
            state.clone(),
        )))
        .unwrap();
    let synthesize = Arc::new(super::executors::SynthesizeNodeExecutor::new());
    registry
        .register(Arc::clone(&synthesize) as Arc<dyn NodeExecutor>)
        .unwrap();

    let mut graph = test_graph("verify gate");
    let source = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "evidence_test", "source");
    let mut verify = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        super::executors::VerifyNodeExecutor::KIND,
        "verify",
    );
    verify.acceptance.required_evidence = vec![required.to_string()];
    let synth = ExecutionNodeSpec::new(
        ExecutionNodeKind::Synthesize,
        super::executors::SynthesizeNodeExecutor::KIND,
        "synthesize",
    );
    graph.edges = vec![
        ExecutionEdge {
            from: source.id.clone(),
            to: verify.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: verify.id.clone(),
            to: synth.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    graph.nodes = vec![source, verify, synth];
    let graph_id = graph.id.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    synthesize.install_resolver(Arc::new(TerminalResolver {
        graph_id: graph_id.clone(),
        backend: Arc::new(TerminalBackend {
            calls: Arc::clone(&calls),
        }),
    }));
    let runner = test_runner(registry, state.clone(), commits);
    runner.start(graph).await.expect("verify graph runs");
    (state.load(&graph_id).unwrap(), calls.load(Ordering::SeqCst))
}

#[tokio::test]
async fn verify_missing_evidence_blocks_synthesize() {
    let (graph, synthesize_calls) = run_verify_graph(None, "required-proof").await;
    let verify = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Verify)
        .unwrap();
    let synth = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Synthesize)
        .unwrap();
    assert_eq!(
        graph.node_statuses[&verify.id],
        ExecutionNodeStatus::Blocked
    );
    assert_eq!(
        graph.node_results[&verify.id]
            .failure
            .as_ref()
            .map(|failure| failure.kind.as_str()),
        Some("missing_evidence")
    );
    assert_eq!(graph.node_statuses[&synth.id], ExecutionNodeStatus::Blocked);
    assert_eq!(synthesize_calls, 0);
    assert!(
        harness_contract::execution_graph::project_execution_graph(&graph)
            .terminal_result_ref
            .is_none()
    );
}

#[tokio::test]
async fn verify_satisfied_evidence_allows_exactly_one_terminal_synthesis() {
    let (graph, synthesize_calls) =
        run_verify_graph(Some("required-proof"), "required-proof").await;
    let verify = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Verify)
        .unwrap();
    let synth = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Synthesize)
        .unwrap();
    assert_eq!(
        graph.node_statuses[&verify.id],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        graph.node_statuses[&synth.id],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(synthesize_calls, 1);
    assert_eq!(
        harness_contract::execution_graph::project_execution_graph(&graph)
            .terminal_result_ref
            .as_deref(),
        Some(format!("terminal:{}", graph.id).as_str())
    );
}

#[test]
fn graph_enumeration_excludes_legacy_non_graph_execution_scope_streams() {
    let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("canonical graph");
    graph.nodes.push(node("canonical"));
    let graph = commits
        .register_graph(graph)
        .expect("graph registers")
        .graph;
    event_store
        .append(RuntimeEventInput {
            stream_id: "session:legacy-strategy".to_string(),
            scope: RuntimeEventScope::ExecutionGraph,
            kind: "runtime.strategy.selected".to_string(),
            status: Some("running".to_string()),
            actor: Some("legacy".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"decision_id": "legacy"}),
        })
        .expect("legacy event records");

    let state = ExecutionGraphStateStore::new(event_store);
    assert_eq!(
        state.graph_ids().expect("graph ids"),
        vec![graph.id.clone()]
    );
    assert_eq!(
        state
            .nonterminal_graph_ids()
            .expect("nonterminal graph ids"),
        vec![graph.id]
    );
}
