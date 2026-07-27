use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Duration;

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphCommand,
    ExecutionGraphValidationError, ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{futures::OwnedNotified, Mutex, Notify, OwnedMutexGuard};
use tokio::task::JoinSet;

use super::commit_service::{ExecutionCommitError, ExecutionCommitService, ExecutionEffectState};
use super::events::ExecutionNodeBinding;
use super::recovery::{ExecutionGraphRecovery, ExecutionRecoveryError};
use super::registry::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError, NodeExecutorRegistry,
};
use super::resources::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager, ScopeLockLease,
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource, WorktreeLease,
    WorktreeLeaseManager, WorktreeLeaseRequest, WorktreeOwnership,
};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

#[derive(Debug, Error)]
pub enum ExecutionRunnerError {
    #[error(transparent)]
    InvalidGraph(#[from] ExecutionGraphValidationError),
    #[error(transparent)]
    Executor(#[from] NodeExecutorError),
    #[error(transparent)]
    Commit(#[from] ExecutionCommitError),
    #[error(transparent)]
    State(#[from] ExecutionStateStoreError),
    #[error("execution node task failed to join: {0}")]
    Join(String),
    #[error("executor returned illegal outcome `{status:?}` for node `{node_id}`")]
    IllegalOutcome {
        node_id: String,
        status: ExecutionNodeStatus,
    },
    #[error("execution graph projection is missing node `{0}`")]
    NodeMissing(String),
    #[error("new execution graphs must be submitted with the Start command")]
    InvalidStartCommand,
    #[error("execution resource acquisition failed for node `{node_id}`: {reason}")]
    Resource { node_id: String, reason: String },
    #[error("execution mutation is blocked: {0}")]
    MutationBlocked(String),
    #[error("execution node `{node_id}` was superseded by a graph command")]
    CommandSuperseded { node_id: String },
    #[error("runtime execution supervisor is unavailable: {0}")]
    SupervisorUnavailable(String),
    #[error("runtime execution driver failed: {0}")]
    Driver(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRunReport {
    pub graph_id: String,
    pub revision: u64,
    pub completed: usize,
    pub failed: usize,
    pub blocked: usize,
    pub cancelled: usize,
    pub waiting: usize,
}

struct ActiveNode {
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    _resources: NodeResourceGuards,
}

struct NodeResourceGuards {
    resource: Option<ExecutionResourceLease>,
    scope: Option<ScopeLockLease>,
    worktree: Option<WorktreeLease>,
}

impl NodeResourceGuards {
    fn binding(&self, ticket: &NodeExecutionTicket) -> ExecutionNodeBinding {
        ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: ticket.attempt,
            resource_lease_refs: self
                .resource
                .as_ref()
                .map(|resource| vec![resource.id().to_string()])
                .unwrap_or_default(),
            scope_lease_ref: self.scope.as_ref().map(|lease| lease.id().to_string()),
            worktree_lease_ref: self
                .worktree
                .as_ref()
                .map(|lease| lease.record().lease_id.to_string()),
        }
    }

    fn release_worktree(&mut self) {
        if let Some(lease) = self.worktree.take() {
            let _ = lease.release();
        }
    }
}

impl Drop for NodeResourceGuards {
    fn drop(&mut self) {
        self.release_worktree();
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionGraphRunner {
    registry: Arc<NodeExecutorRegistry>,
    state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    resource_manager: Arc<ExecutionResourceManager>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    workspace_id: String,
    workspace_root: PathBuf,
    active: Arc<Mutex<BTreeMap<(String, String), ActiveNode>>>,
    coordination: Arc<StdMutex<BTreeMap<String, Weak<Mutex<()>>>>>,
    command_intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
    mutation_gate: Arc<RwLock<Option<MutationGate>>>,
}

type MutationGate = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

struct CommandIntentOwner {
    graph_id: String,
    intent: Arc<Notify>,
    intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
}

type ActiveCancellation = (String, Arc<dyn NodeExecutor>, NodeExecutionTicket);

struct CancellationFinalizationOwner {
    active: Vec<ActiveCancellation>,
}

impl CancellationFinalizationOwner {
    fn new(active: Vec<ActiveCancellation>) -> Self {
        Self { active }
    }

    fn active(&self) -> &[ActiveCancellation] {
        &self.active
    }
}

impl Drop for CancellationFinalizationOwner {
    fn drop(&mut self) {
        for (_, executor, ticket) in &self.active {
            executor.cancellation_finalized(ticket);
        }
    }
}

impl CommandIntentOwner {
    fn install(
        graph_id: &str,
        intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
    ) -> Result<Self, ExecutionRunnerError> {
        let intent = Arc::new(Notify::new());
        let mut current = intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.contains_key(graph_id) {
            return Err(ExecutionRunnerError::MutationBlocked(format!(
                "graph `{graph_id}` already has a command in flight"
            )));
        }
        current.insert(graph_id.to_string(), Arc::clone(&intent));
        drop(current);
        Ok(Self {
            graph_id: graph_id.to_string(),
            intent,
            intents,
        })
    }
}

impl Drop for CommandIntentOwner {
    fn drop(&mut self) {
        let removed = {
            let mut intents = self
                .intents
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if intents
                .get(&self.graph_id)
                .is_some_and(|current| Arc::ptr_eq(current, &self.intent))
            {
                intents.remove(&self.graph_id);
                true
            } else {
                false
            }
        };
        if removed {
            self.intent.notify_waiters();
        }
    }
}

impl ExecutionGraphRunner {
    #[must_use]
    pub(crate) fn new(
        registry: Arc<NodeExecutorRegistry>,
        state_store: ExecutionGraphStateStore,
        commit_service: ExecutionCommitService,
        resource_manager: Arc<ExecutionResourceManager>,
        scope_locks: Arc<ScopeLockManager>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            registry,
            state_store,
            commit_service,
            resource_manager,
            scope_locks,
            worktree_leases,
            workspace_id: workspace_id.into(),
            workspace_root: workspace_root.into(),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            coordination: Arc::new(StdMutex::new(BTreeMap::new())),
            command_intents: Arc::new(StdMutex::new(BTreeMap::new())),
            mutation_gate: Arc::new(RwLock::new(None)),
        }
    }

    fn command_intent_waiter(&self, graph_id: &str) -> Option<OwnedNotified> {
        self.command_intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(graph_id)
            .cloned()
            .map(Notify::notified_owned)
    }

    pub(crate) fn state_store(&self) -> &ExecutionGraphStateStore {
        &self.state_store
    }

    pub(crate) async fn recover_graph(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        ExecutionGraphRecovery::new(&self.state_store, &self.commit_service, &self.registry)
            .recover(graph_id)
            .await
    }

    async fn graph_coordination_without_command(&self, graph_id: &str) -> OwnedMutexGuard<()> {
        loop {
            if let Some(waiter) = self.command_intent_waiter(graph_id) {
                waiter.await;
                continue;
            }
            let coordination = self.graph_coordination(graph_id).await;
            if let Some(waiter) = self.command_intent_waiter(graph_id) {
                drop(coordination);
                waiter.await;
                continue;
            }
            return coordination;
        }
    }

    async fn graph_coordination(&self, graph_id: &str) -> OwnedMutexGuard<()> {
        {
            let mut registry = self
                .coordination
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.retain(|_, lock| lock.strong_count() > 0);
            registry
                .get(graph_id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let lock = Arc::new(Mutex::new(()));
                    registry.insert(graph_id.to_string(), Arc::downgrade(&lock));
                    lock
                })
        }
        .lock_owned()
        .await
    }

    pub(crate) fn install_mutation_gate(
        &self,
        gate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        *self
            .mutation_gate
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::new(gate));
    }

    fn ensure_mutation_allowed(&self) -> Result<(), ExecutionRunnerError> {
        let gate = self
            .mutation_gate
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        gate.map_or(Ok(()), |gate| {
            gate().map_err(ExecutionRunnerError::MutationBlocked)
        })
    }

    #[cfg(test)]
    pub(crate) async fn start(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let registered = self.register(graph).await?;
        self.run_until_quiescent(&registered.id).await
    }

    /// Persist a valid graph without executing its first node. Callers that
    /// need live surface attachment use this boundary to publish the durable
    /// graph identity before any provider or tool work begins.
    pub(crate) async fn register(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        validate_execution_graph(&graph)?;
        self.registry.validate_graph(&graph)?;
        Ok(self.commit_service.register_graph_async(graph).await?.graph)
    }

    pub(crate) async fn run_until_quiescent(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        loop {
            let mut graph = self.state_store.load_async(graph_id).await?;
            graph = self.advance_dependencies(graph).await?;
            let ready = graph
                .nodes
                .iter()
                .filter(|node| graph.node_statuses[&node.id] == ExecutionNodeStatus::Ready)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            if ready.is_empty() {
                return Ok(report(&graph));
            }

            let mut wave = JoinSet::new();
            for node_id in ready {
                let node = graph
                    .nodes
                    .iter()
                    .find(|node| node.id == node_id)
                    .ok_or_else(|| ExecutionRunnerError::NodeMissing(node_id.clone()))?
                    .clone();
                let runner = self.clone();
                let graph_id = graph_id.to_string();
                wave.spawn(async move { runner.start_and_execute_node(&graph_id, node).await });
            }

            while let Some(joined) = wave.join_next().await {
                let result =
                    joined.map_err(|error| ExecutionRunnerError::Join(error.to_string()))?;
                let (node_id, outcome) = match result {
                    Err(ExecutionRunnerError::CommandSuperseded { .. }) => continue,
                    Err(ExecutionRunnerError::Resource { node_id, reason }) => {
                        self.block_unstarted_resource_node(graph_id, &node_id, reason)
                            .await?;
                        continue;
                    }
                    Ok(value) => value,
                    Err(ExecutionRunnerError::Executor(error)) => {
                        // `start` failed before a durable Running transition
                        // or effect intent exists. Keeping the node Ready lets
                        // an explicit caller decide whether to retry; treating
                        // it as a completed wave would immediately schedule it
                        // again and spin forever.
                        if matches!(error, NodeExecutorError::Start { .. }) {
                            return Err(ExecutionRunnerError::Executor(error));
                        }
                        let node_id = executor_error_node_id(&error).to_string();
                        if let Some(waiter) = self.command_intent_waiter(graph_id) {
                            waiter.await;
                        }
                        self.active
                            .lock()
                            .await
                            .remove(&(graph_id.to_string(), node_id.clone()));
                        let result = failed_result(&error);
                        let terminal_status = result.status;
                        let _coordination = self.graph_coordination_without_command(graph_id).await;
                        let current = self.state_store.load_async(graph_id).await?;
                        if current.node_statuses.get(&node_id)
                            == Some(&ExecutionNodeStatus::Running)
                        {
                            self.commit_service
                                .transition_node_async(
                                    current,
                                    node_id,
                                    terminal_status,
                                    Some(result),
                                    Vec::new(),
                                )
                                .await?;
                            continue;
                        }
                        // A Pause/Cancel command may have superseded the
                        // executor while it was returning its terminal
                        // result. The command's durable graph state wins.
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                validate_outcome(&node_id, &outcome)?;
                if let Some(waiter) = self.command_intent_waiter(graph_id) {
                    waiter.await;
                }
                // Durable graph state must be serialized, but executor callbacks are
                // process-local follow-up work. Holding the graph coordination lock
                // while an executor publishes its transcript can deadlock unrelated
                // graph progress and leave a durable successor permanently Ready.
                let committed_executor = {
                    let _coordination = self.graph_coordination_without_command(graph_id).await;
                    let current = self.state_store.load_async(graph_id).await?;
                    if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running) {
                        self.active
                            .lock()
                            .await
                            .remove(&(graph_id.to_string(), node_id.clone()));
                        continue;
                    }
                    if let Some(replan) = outcome.replan {
                        self.registry.validate_nodes(&replan.nodes)?;
                        self.commit_service
                            .transition_node_with_replan_async(
                                current,
                                node_id.clone(),
                                outcome.result,
                                outcome.domain_events,
                                replan.nodes,
                                replan.edges,
                                replan.reason,
                            )
                            .await?;
                    } else {
                        self.commit_service
                            .transition_node_async(
                                current,
                                node_id.clone(),
                                outcome.result.status,
                                Some(outcome.result),
                                outcome.domain_events,
                            )
                            .await?;
                    }
                    self.active
                        .lock()
                        .await
                        .get(&(graph_id.to_string(), node_id.clone()))
                        .map(|active| (Arc::clone(&active.executor), active.ticket.clone()))
                };
                if let Some((executor, ticket)) = committed_executor {
                    let after_commit = executor.after_commit(&ticket).await;
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id.clone()));
                    after_commit?;
                } else {
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id.clone()));
                }
            }
        }
    }

    async fn start_and_execute_node(
        &self,
        graph_id: &str,
        node: harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<(String, NodeExecutionOutcome), ExecutionRunnerError> {
        let leaf_effect_owner = node.kind
            == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            && node.executor_kind == "tool_batch";
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        }
        let resources = self
            .acquire_node_resources(&self.state_store.load_async(graph_id).await?, &node)
            .await?;
        let resource_kind = resources
            .resource
            .as_ref()
            .map(|resource| resource.kind().clone());
        let resource_queue_wait = resources
            .resource
            .as_ref()
            .map_or(Duration::ZERO, ExecutionResourceLease::queue_wait);
        let resource_started = std::time::Instant::now();
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let graph = self.state_store.load_async(graph_id).await?;
        if graph.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Ready) {
            self.record_resource_terminal(
                resource_kind.as_ref(),
                resource_queue_wait,
                resource_started,
                crate::execution_core::graph::ResourceResultClass::Cancelled,
            );
            return Err(ExecutionRunnerError::NodeMissing(node.id));
        }
        let executor = match self.registry.get(&node.executor_kind) {
            Some(executor) => executor,
            None => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                return Err(NodeExecutorError::Unavailable {
                    executor_kind: node.executor_kind.clone(),
                    node_id: node.id.clone(),
                }
                .into());
            }
        };
        let attempt = graph
            .recovery_cursor
            .node_attempts
            .get(&node.id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let ticket = match executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node,
                attempt,
            })
            .await
        {
            Ok(ticket) => ticket,
            Err(error) => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                return Err(error.into());
            }
        };
        let binding = resources.binding(&ticket);
        self.active.lock().await.insert(
            (ticket.graph_id.clone(), ticket.node_id.clone()),
            ActiveNode {
                executor: Arc::clone(&executor),
                ticket: ticket.clone(),
                _resources: resources,
            },
        );
        if let Err(error) = self
            .commit_service
            .bind_and_start_node_async(graph, ticket.node_id.clone(), binding)
            .await
        {
            self.active
                .lock()
                .await
                .remove(&(ticket.graph_id.clone(), ticket.node_id.clone()));
            let _ = executor.cancel(&ticket).await;
            self.record_resource_terminal(
                resource_kind.as_ref(),
                resource_queue_wait,
                resource_started,
                crate::execution_core::graph::ResourceResultClass::Failed,
            );
            return Err(error.into());
        }
        drop(_coordination);
        // The coordination gate protects the state check and durable effect
        // intent. It must not cover executor work: a ToolBatch may submit a
        // child execution graph (for example `runtime_orchestrate`), which
        // correctly needs the same Runner to make progress. Holding the gate
        // across that await creates a parent/child re-entrancy deadlock.
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
        }
        let effect_state = {
            let _poll_gate = self.graph_coordination_without_command(graph_id).await;
            let current = self.state_store.load_async(graph_id).await?;
            if current.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running) {
                self.active
                    .lock()
                    .await
                    .remove(&(ticket.graph_id.clone(), ticket.node_id.clone()));
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Ok((
                    ticket.node_id.clone(),
                    NodeExecutionOutcome::new(ExecutionNodeResult {
                        // This outcome is only an internal wake-up value. The command's
                        // already-committed graph status remains authoritative and the
                        // caller discards this result because the node is no longer running.
                        status: ExecutionNodeStatus::Cancelled,
                        result_ref: None,
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: None,
                        usage: Default::default(),
                        finished_at_ms: now_ms(),
                    }),
                ));
            }
            if leaf_effect_owner {
                ExecutionEffectState::Fresh
            } else {
                self.commit_service.begin_execution_effect(&ticket)?
            }
        };
        match effect_state {
            ExecutionEffectState::Completed(outcome) => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Ok((ticket.node_id.clone(), outcome));
            }
            ExecutionEffectState::Uncertain => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "effect intent `{}` has no durable receipt",
                        ticket.idempotency_key
                    ),
                }
                .into());
            }
            ExecutionEffectState::Fresh => {}
        }
        let outcome = executor.poll_or_await(&ticket).await;
        if let Some(resource_kind) = resource_kind {
            let result_class = match &outcome {
                Ok(outcome)
                    if outcome.result.status
                        == harness_contract::execution_graph::ExecutionNodeStatus::Cancelled =>
                {
                    crate::execution_core::graph::ResourceResultClass::Cancelled
                }
                Ok(outcome)
                    if outcome.result.status
                        == harness_contract::execution_graph::ExecutionNodeStatus::Completed =>
                {
                    crate::execution_core::graph::ResourceResultClass::Completed
                }
                Ok(_) | Err(_) => crate::execution_core::graph::ResourceResultClass::Failed,
            };
            let _ = self.resource_manager.record_observation(
                &resource_kind,
                crate::execution_core::graph::ResourceObservation::terminal(
                    resource_queue_wait,
                    resource_started.elapsed(),
                    result_class,
                ),
            );
        }
        let outcome = outcome?;
        if !leaf_effect_owner {
            self.commit_service
                .commit_execution_effect(&ticket, &outcome)?;
        }
        Ok((ticket.node_id.clone(), outcome))
    }

    fn record_resource_terminal(
        &self,
        kind: Option<&ExecutionResourceKind>,
        queue_wait: Duration,
        started: std::time::Instant,
        result_class: crate::execution_core::graph::ResourceResultClass,
    ) {
        let Some(kind) = kind else {
            return;
        };
        let _ = self.resource_manager.record_observation(
            kind,
            crate::execution_core::graph::ResourceObservation::terminal(
                queue_wait,
                started.elapsed(),
                result_class,
            ),
        );
    }

    async fn acquire_node_resources(
        &self,
        graph: &ExecutionGraph,
        node: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<NodeResourceGuards, ExecutionRunnerError> {
        let resource_kind = match node.kind {
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask => {
                Some(ExecutionResourceKind::Agent)
            }
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch => {
                // ToolBatch is a container. Each leaf invocation is admitted
                // by ToolExecutionPlane; taking a second Tool lease here can
                // deadlock a one-slot quota.
                // The container contract must still be validated so malformed
                // paths become durable blockers before any leaf can execute.
                for scope in &node.resource_scopes {
                    if let Some(path) = scope
                        .strip_prefix("read:")
                        .or_else(|| scope.strip_prefix("write:"))
                    {
                        let _ = self.scoped_resource_for_path(&node.id, path)?;
                    } else if let Some(path) = scope.strip_prefix("worktree:") {
                        validate_worktree_path(&self.workspace_root, path).map_err(|reason| {
                            ExecutionRunnerError::Resource {
                                node_id: node.id.clone(),
                                reason,
                            }
                        })?;
                    }
                }
                return Ok(NodeResourceGuards {
                    resource: None,
                    scope: None,
                    worktree: None,
                });
            }
            // InlineModel owns Provider admission in ConversationRuntime.
            // ToolBatch leaves own Tool admission in ToolExecutionPlane.
            // Deterministic control nodes do not consume either family.
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel
            | harness_contract::execution_graph::ExecutionNodeKind::Verify
            | harness_contract::execution_graph::ExecutionNodeKind::Synthesize
            | harness_contract::execution_graph::ExecutionNodeKind::Approval
            | harness_contract::execution_graph::ExecutionNodeKind::SessionDispatch
            | harness_contract::execution_graph::ExecutionNodeKind::Timer => None,
        };
        let resource = if let Some(resource_kind) = resource_kind {
            Some(
                self.resource_manager
                    .acquire(resource_kind, Some(std::time::Duration::from_secs(30)))
                    .await
                    .map_err(|error| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: error.to_string(),
                    })?,
            )
        } else {
            None
        };
        let mut scope_requests = Vec::new();
        let mut worktree_path = None;
        for scope in &node.resource_scopes {
            if let Some(path) = scope.strip_prefix("read:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path)?,
                        mode: ScopeLockMode::Read,
                    });
                }
            } else if let Some(path) = scope.strip_prefix("write:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path)?,
                        mode: ScopeLockMode::Write,
                    });
                }
            } else if let Some(path) = scope.strip_prefix("worktree:") {
                worktree_path = Some(validate_worktree_path(&self.workspace_root, path).map_err(
                    |reason| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason,
                    },
                )?);
            }
        }
        // AgentTask 是委派/授权边界，不是文件副作用边界。其 in-process
        // 子代理会在同一个 Runner 中创建真正执行 read/write 的 ToolBatch。
        // 如果父 AgentTask 在等待子图期间持有文件锁，子 ToolBatch 对同一路径
        // 的合法读写会永久等待父节点结束，形成父等子、子等父的自死锁。
        // 文件互斥仍由叶子效果节点获取，AgentTask 继续持有 Agent 配额和
        // 可选 worktree 租约，授权范围也仍保留在节点合同中。
        let scope = if scope_requests.is_empty() {
            None
        } else {
            Some(
                self.scope_locks
                    .acquire(scope_requests, None)
                    .await
                    .map_err(|error| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: error.to_string(),
                    })?,
            )
        };
        let worktree = worktree_path
            .map(|path| {
                self.worktree_leases.acquire(WorktreeLeaseRequest {
                    workspace_id: self.workspace_id.clone(),
                    task_id: graph.id.clone(),
                    owner_id: node.id.clone(),
                    path,
                    ownership: WorktreeOwnership::UserManaged,
                    ttl: Duration::from_secs(300),
                })
            })
            .transpose()
            .map_err(|error| ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: error.to_string(),
            })?;
        Ok(NodeResourceGuards {
            resource,
            scope,
            worktree,
        })
    }

    fn scoped_resource_for_path(
        &self,
        node_id: &str,
        path: &str,
    ) -> Result<ScopedResource, ExecutionRunnerError> {
        let requested = Path::new(path.trim());
        let relative = if requested.is_absolute() {
            requested.strip_prefix(&self.workspace_root).map_err(|_| {
                ExecutionRunnerError::Resource {
                    node_id: node_id.to_string(),
                    reason: format!(
                        "absolute resource scope `{}` is outside workspace `{}`",
                        requested.display(),
                        self.workspace_root.display()
                    ),
                }
            })?
        } else {
            requested
        };
        if relative.as_os_str().is_empty() || relative == Path::new(".") {
            return ScopedResource::workspace(&self.workspace_id).map_err(|error| {
                ExecutionRunnerError::Resource {
                    node_id: node_id.to_string(),
                    reason: error.to_string(),
                }
            });
        }
        ScopedResource::file(&self.workspace_id, relative).map_err(|error| {
            ExecutionRunnerError::Resource {
                node_id: node_id.to_string(),
                reason: error.to_string(),
            }
        })
    }

    async fn block_unstarted_resource_node(
        &self,
        graph_id: &str,
        node_id: &str,
        reason: String,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if current.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Ready) {
            return Ok(());
        }
        self.commit_service
            .transition_node_async(
                current,
                node_id.to_string(),
                ExecutionNodeStatus::Blocked,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Blocked,
                    result_ref: None,
                    summary: None,
                    evidence_refs: Vec::new(),
                    failure: Some(harness_contract::execution_graph::ExecutionFailure {
                        kind: "resource_acquisition_failed".to_string(),
                        message: reason,
                        retryable: false,
                        evidence_refs: Vec::new(),
                    }),
                    usage: Default::default(),
                    finished_at_ms: now_ms(),
                }),
                Vec::new(),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn command(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        if matches!(
            command,
            ExecutionGraphCommand::Pause { .. } | ExecutionGraphCommand::Cancel { .. }
        ) {
            let coordination = self.graph_coordination(graph_id).await;
            let graph = self.state_store.load_async(graph_id).await?;
            self.commit_service
                .validate_command_revision(&graph, &command)?;
            let intent = CommandIntentOwner::install(graph_id, Arc::clone(&self.command_intents))?;
            let active = self
                .active
                .lock()
                .await
                .iter()
                .filter(|((active_graph_id, _), _)| active_graph_id == graph_id)
                .map(|((_, node_id), node)| {
                    (
                        node_id.clone(),
                        Arc::clone(&node.executor),
                        node.ticket.clone(),
                    )
                })
                .collect::<Vec<_>>();
            if matches!(command, ExecutionGraphCommand::Pause { .. })
                && active
                    .iter()
                    .any(|(_, executor, _)| !executor.supports_resumable_pause())
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "an active execution node does not expose a resumable pause capability; cancel or wait for quiescence"
                        .to_string(),
                )
                .into());
            }
            let cancellation_finalizer = CancellationFinalizationOwner::new(active);
            drop(coordination);

            // Executor cancellation can wait for a nested child graph to
            // quiesce. It must never run while the global graph coordination
            // gate is held, because that child uses this same Runner.
            let mut cancellation_errors = Vec::new();
            if matches!(
                command,
                ExecutionGraphCommand::Pause { .. } | ExecutionGraphCommand::Cancel { .. }
            ) {
                for (node_id, executor, ticket) in cancellation_finalizer.active() {
                    if let Err(error) = executor.cancel(ticket).await {
                        cancellation_errors.push((node_id.clone(), error));
                    }
                }
            }

            let coordination = self.graph_coordination(graph_id).await;
            let current = self.state_store.load_async(graph_id).await?;
            if let Some((_, error)) = cancellation_errors.into_iter().find(|(node_id, _)| {
                current
                    .node_statuses
                    .get(node_id)
                    .is_some_and(|status| !status.is_terminal())
            }) {
                drop(coordination);
                return Err(ExecutionRunnerError::Executor(error));
            }
            // The intent gate prevents node transitions while cancellation is
            // in flight. Re-validating the original caller revision here is
            // the CAS that prevents a delayed command from overwriting a
            // concurrently committed graph mutation.
            if let Err(error) = self
                .commit_service
                .validate_command_revision(&current, &command)
            {
                drop(coordination);
                return Err(error.into());
            }
            let result = self
                .commit_service
                .apply_command_async(current, command.clone())
                .await;
            drop(intent);
            drop(coordination);
            drop(cancellation_finalizer);
            return result.map(|receipt| receipt.graph).map_err(Into::into);
        }

        let coordination = self.graph_coordination_without_command(graph_id).await;
        let graph = self.state_store.load_async(graph_id).await?;
        self.commit_service
            .validate_command_revision(&graph, &command)?;
        let graph = self
            .commit_service
            .apply_command_async(graph, command.clone())
            .await?
            .graph;
        drop(coordination);
        Ok(graph)
    }

    pub(crate) async fn projection(
        &self,
        graph_id: &str,
    ) -> Result<harness_contract::execution_graph::ExecutionGraphProjection, ExecutionRunnerError>
    {
        self.state_store
            .projection_async(graph_id)
            .await
            .map_err(ExecutionRunnerError::from)
    }

    async fn advance_dependencies(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        let graph_id = graph.id;
        let coordination = self.graph_coordination_without_command(&graph_id).await;
        let mut graph = self.state_store.load_async(&graph_id).await?;
        loop {
            let mut changed = false;
            let planned = graph
                .nodes
                .iter()
                .filter(|node| graph.node_statuses[&node.id] == ExecutionNodeStatus::Planned)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            for node_id in planned {
                let predecessors = graph
                    .edges
                    .iter()
                    .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn && edge.to == node_id)
                    .map(|edge| graph.node_statuses[&edge.from])
                    .collect::<Vec<_>>();
                let target = if predecessors
                    .iter()
                    .any(|status| status.is_terminal() && *status != ExecutionNodeStatus::Completed)
                {
                    Some(ExecutionNodeStatus::Blocked)
                } else if predecessors
                    .iter()
                    .all(|status| *status == ExecutionNodeStatus::Completed)
                {
                    Some(ExecutionNodeStatus::Ready)
                } else {
                    None
                };
                if let Some(target) = target {
                    graph = self
                        .commit_service
                        .transition_node_async(graph, node_id, target, None, Vec::new())
                        .await?
                        .graph;
                    changed = true;
                }
            }
            if !changed {
                drop(coordination);
                return Ok(graph);
            }
        }
    }
}

pub(crate) fn validate_worktree_path(
    workspace_root: &Path,
    requested: &str,
) -> Result<PathBuf, String> {
    let relative = Path::new(requested);
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err("worktree scope must be a non-empty relative path".to_string());
    }
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("worktree scope may not contain parent or root components".to_string());
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace root cannot be canonicalized: {error}"))?;
    let mut candidate = canonical_root.clone();
    for component in relative.components() {
        candidate.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|error| format!("worktree path cannot be inspected: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("worktree scope may not traverse symbolic links".to_string());
        }
    }
    let canonical_candidate = candidate
        .canonicalize()
        .map_err(|error| format!("worktree path cannot be canonicalized: {error}"))?;
    if canonical_candidate == canonical_root || !canonical_candidate.starts_with(&canonical_root) {
        return Err("worktree scope must resolve to a workspace descendant".to_string());
    }
    Ok(canonical_candidate)
}

fn validate_outcome(
    node_id: &str,
    outcome: &NodeExecutionOutcome,
) -> Result<(), ExecutionRunnerError> {
    if matches!(
        outcome.result.status,
        ExecutionNodeStatus::Completed
            | ExecutionNodeStatus::Failed
            | ExecutionNodeStatus::Blocked
            | ExecutionNodeStatus::Cancelled
            | ExecutionNodeStatus::WaitingInput
            | ExecutionNodeStatus::WaitingApproval
            | ExecutionNodeStatus::WaitingExternal
    ) {
        Ok(())
    } else {
        Err(ExecutionRunnerError::IllegalOutcome {
            node_id: node_id.to_string(),
            status: outcome.result.status,
        })
    }
}

fn executor_error_node_id(error: &NodeExecutorError) -> &str {
    match error {
        NodeExecutorError::DuplicateExecutor(kind) => kind,
        NodeExecutorError::Unavailable { node_id, .. }
        | NodeExecutorError::Invalid { node_id, .. }
        | NodeExecutorError::Start { node_id, .. }
        | NodeExecutorError::Poll { node_id, .. }
        | NodeExecutorError::Cancel { node_id, .. }
        | NodeExecutorError::Recover { node_id, .. }
        | NodeExecutorError::Uncertain { node_id, .. } => node_id,
    }
}

fn failed_result(error: &NodeExecutorError) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: if matches!(error, NodeExecutorError::Uncertain { .. }) {
            ExecutionNodeStatus::Blocked
        } else {
            ExecutionNodeStatus::Failed
        },
        result_ref: None,
        summary: None,
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: if matches!(error, NodeExecutorError::Uncertain { .. }) {
                "effect_completion_uncertain".to_string()
            } else {
                "executor_error".to_string()
            },
            message: error.to_string(),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: now_ms(),
    }
}

fn report(graph: &ExecutionGraph) -> ExecutionRunReport {
    let count = |status| {
        graph
            .node_statuses
            .values()
            .filter(|current| **current == status)
            .count()
    };
    ExecutionRunReport {
        graph_id: graph.id.clone(),
        revision: graph.revision,
        completed: count(ExecutionNodeStatus::Completed),
        failed: count(ExecutionNodeStatus::Failed),
        blocked: count(ExecutionNodeStatus::Blocked),
        cancelled: count(ExecutionNodeStatus::Cancelled),
        waiting: count(ExecutionNodeStatus::WaitingInput)
            + count(ExecutionNodeStatus::WaitingApproval)
            + count(ExecutionNodeStatus::WaitingExternal)
            + count(ExecutionNodeStatus::Paused),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
