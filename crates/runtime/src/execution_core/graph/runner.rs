use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Duration;

use harness_contract::acceptance::{AcceptanceEvaluation, AcceptanceVerdict, TerminalFactKind};
use harness_contract::context::{EvidenceAccessRef, EvidenceRef};
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionGraph, ExecutionGraphCommand, ExecutionGraphValidationError,
    ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{futures::OwnedNotified, oneshot, Mutex, Notify, OwnedMutexGuard};

use super::commit_service::{
    ExecutionCommitError, ExecutionCommitService, ExecutionEffectState, ExecutionTerminalCommit,
};
use super::events::ExecutionNodeBinding;
use super::recovery::{ExecutionGraphRecovery, ExecutionRecoveryError};
use super::registry::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError, NodeExecutorRegistry,
};
use super::resources::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager,
    ResourceAdmissionDecision, ResourceAdmissionRequest, ResourceWaitReason, ScopeLockError,
    ScopeLockLease, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
    WorktreeLease, WorktreeLeaseManager, WorktreeLeaseRequest, WorktreeOwnership,
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
    #[error("execution resource admission deferred for node `{node_id}`: {reason}")]
    ResourceDeferred { node_id: String, reason: String },
    #[error("execution node `{node_id}` exceeded its durable deadline `{deadline_at_ms}`")]
    DeadlineExceeded {
        node_id: String,
        deadline_at_ms: u64,
    },
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

#[derive(Debug)]
pub(crate) struct ExecutionGraphPumpSnapshot {
    pub(crate) ready: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
    pub(crate) report: ExecutionRunReport,
}

pub(crate) struct ExecutionPreparedPumpNode {
    pub(crate) node: harness_contract::execution_graph::ExecutionNodeSpec,
    startup_error: Option<ExecutionRunnerError>,
}

struct ActiveNode {
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    resource_kind: Option<ExecutionResourceKind>,
    resource_queue_wait: Duration,
    resource_started: std::time::Instant,
    effect_state: ExecutionEffectState,
    effect_receipt_required: bool,
    wave_prepared: bool,
    _resources: NodeResourceGuards,
}

struct PendingTerminalCommit {
    transition: ExecutionTerminalCommit,
    response: oneshot::Sender<TerminalCommitDisposition>,
}

#[derive(Default)]
struct TerminalBatchState {
    leader_active: bool,
    pending: Vec<PendingTerminalCommit>,
}

enum TerminalCommitDisposition {
    Committed,
    Superseded,
    Failed(String),
}

struct PreparedNodeStart {
    node: harness_contract::execution_graph::ExecutionNodeSpec,
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    resources: NodeResourceGuards,
    resource_kind: Option<ExecutionResourceKind>,
    resource_queue_wait: Duration,
    resource_started: std::time::Instant,
    effect_state: ExecutionEffectState,
    effect_receipt_required: bool,
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
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    active: Arc<Mutex<BTreeMap<(String, String), ActiveNode>>>,
    terminal_batches: Arc<Mutex<BTreeMap<String, TerminalBatchState>>>,
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
    #[cfg(test)]
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
        let workspace_root = workspace_root.into();
        let path_identity_resolver = Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(&workspace_root)
                .expect("ExecutionGraphRunner requires an existing workspace root"),
        );
        Self::new_with_path_identity_resolver(
            registry,
            state_store,
            commit_service,
            resource_manager,
            scope_locks,
            worktree_leases,
            workspace_id,
            workspace_root,
            path_identity_resolver,
        )
    }

    #[must_use]
    pub(crate) fn new_with_path_identity_resolver(
        registry: Arc<NodeExecutorRegistry>,
        state_store: ExecutionGraphStateStore,
        commit_service: ExecutionCommitService,
        resource_manager: Arc<ExecutionResourceManager>,
        scope_locks: Arc<ScopeLockManager>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
        path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
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
            path_identity_resolver,
            active: Arc::new(Mutex::new(BTreeMap::new())),
            terminal_batches: Arc::new(Mutex::new(BTreeMap::new())),
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

    pub(crate) fn load_delegated_agent_tool_receipts(
        &self,
        graph_id: &str,
        node_id: &str,
        attempt: u32,
    ) -> Result<Vec<super::commit_service::DurableAgentToolReceipt>, ExecutionCommitError> {
        self.commit_service
            .load_delegated_agent_tool_receipts(graph_id, node_id, attempt)
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
        match self.commit_service.register_graph_async(graph).await {
            Ok(receipt) => Ok(receipt.graph),
            Err(ExecutionCommitError::AlreadyAppliedSame { graph_id }) => self
                .state_store
                .load_async(graph_id)
                .await
                .map_err(ExecutionRunnerError::from),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_until_quiescent(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let durable_progress = Arc::new(Notify::new());
        let mut active_nodes = tokio::task::JoinSet::new();
        let mut in_flight = BTreeSet::new();
        loop {
            let snapshot = self.pump_snapshot(graph_id, &in_flight).await?;
            let ready = snapshot
                .ready
                .into_iter()
                .take(64usize.saturating_sub(active_nodes.len()))
                .collect::<Vec<_>>();
            let prepared = self.prepare_ready_wave(graph_id, ready).await?;
            for prepared in prepared {
                let node_id = prepared.node.id.clone();
                if !in_flight.insert(node_id.clone()) {
                    continue;
                }
                let runner = self.clone();
                let graph_id = graph_id.to_string();
                let progress = Arc::clone(&durable_progress);
                active_nodes.spawn(async move {
                    (
                        node_id,
                        runner
                            .execute_prepared_pump_node(&graph_id, prepared, progress)
                            .await,
                    )
                });
            }
            if active_nodes.is_empty() {
                return Ok(snapshot.report);
            }
            tokio::select! {
                () = durable_progress.notified() => {}
                joined = active_nodes.join_next() => {
                    let (node_id, result) = joined
                        .ok_or_else(|| ExecutionRunnerError::Join(
                            "test completion pump closed unexpectedly".to_string(),
                        ))?
                        .map_err(|error| ExecutionRunnerError::Join(error.to_string()))?;
                    in_flight.remove(&node_id);
                    result?;
                }
            }
        }
    }

    pub(crate) async fn pump_snapshot(
        &self,
        graph_id: &str,
        in_flight: &BTreeSet<String>,
    ) -> Result<ExecutionGraphPumpSnapshot, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        let graph = self.state_store.load_async(graph_id).await?;
        let mut graph = self.advance_dependencies(graph).await?;
        for node_id in quorum_tail_cancellations(&graph) {
            let current = self.state_store.load_async(graph_id).await?;
            if current
                .node_statuses
                .get(&node_id)
                .is_none_or(|status| status.is_terminal())
            {
                continue;
            }
            graph = self
                .command(
                    graph_id,
                    ExecutionGraphCommand::CancelNode {
                        expected_revision: current.revision,
                        node_id,
                        reason: "optional work cancelled after dependency quorum".to_string(),
                    },
                )
                .await?;
        }
        graph = self.advance_dependencies(graph).await?;
        let ready = graph
            .nodes
            .iter()
            .filter(|node| {
                graph.node_statuses[&node.id] == ExecutionNodeStatus::Ready
                    && !in_flight.contains(&node.id)
            })
            .cloned()
            .collect();
        Ok(ExecutionGraphPumpSnapshot {
            ready,
            report: report(&graph),
        })
    }

    pub(crate) async fn current_report(
        &self,
        graph_id: &str,
    ) -> Result<(ExecutionRunReport, bool), ExecutionRunnerError> {
        let graph = self.state_store.load_async(graph_id).await?;
        let has_actionable_node = graph.nodes.iter().any(|node| {
            let status = graph.node_statuses[&node.id];
            if matches!(
                status,
                ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running
            ) {
                return true;
            }
            if status != ExecutionNodeStatus::Planned {
                return false;
            }
            let predecessors = dependency_predecessors(&graph, node);
            let dependency = node
                .work
                .as_ref()
                .map(|work| work.dependency.clone())
                .unwrap_or_default();
            dependency_target(&dependency, &predecessors).is_some()
        });
        let quiescent = !has_actionable_node;
        Ok((report(&graph), quiescent))
    }

    pub(crate) fn subscribe_state_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.state_store.subscribe_commits()
    }

    /// Resource pressure is a soft wait, not a terminal graph outcome.  The
    /// completion pump owns the retry loop so a released lease wakes the same
    /// durable Ready node instead of silently ending its graph driver.
    pub(crate) async fn wait_for_resource_change(&self) {
        self.resource_manager.wait_for_change().await;
    }

    pub(crate) async fn terminal_report(
        &self,
        graph_id: &str,
    ) -> Result<Option<ExecutionRunReport>, ExecutionRunnerError> {
        let graph = self.state_store.load_async(graph_id).await?;
        Ok(graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
            .then(|| report(&graph)))
    }

    /// Resolve the exact parent join from durable child terminal truth.
    /// Duplicate observer/startup reconciliation passes are idempotent; a
    /// concurrent parent cancellation wins because the command is revision
    /// and WaitingExternal fenced.
    pub(crate) async fn resolve_parent_for_settled_child(
        &self,
        child_graph_id: &str,
    ) -> Result<Option<String>, ExecutionRunnerError> {
        let child = self.state_store.load_async(child_graph_id).await?;
        if child.nodes.is_empty()
            || child
                .node_statuses
                .values()
                .any(|status| !status.is_terminal())
        {
            return Ok(None);
        }
        let Some(parent) = child.parent_execution.clone() else {
            return Ok(None);
        };
        for _ in 0..3 {
            let current = self.state_store.load_async(&parent.execution_id).await?;
            if current.node_statuses.get(&parent.node_id)
                != Some(&ExecutionNodeStatus::WaitingExternal)
            {
                return Ok(None);
            }
            let parent_node = current
                .nodes
                .iter()
                .find(|node| node.id == parent.node_id)
                .ok_or_else(|| ExecutionRunnerError::Resource {
                    node_id: parent.node_id.clone(),
                    reason: "registered child parent node is absent".to_string(),
                })?;
            let request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &parent_node.payload_ref,
            )
            .map_err(|error| ExecutionRunnerError::Resource {
                node_id: parent.node_id.clone(),
                reason: format!("registered child parent payload is invalid: {error}"),
            })?;
            if parent_node.kind != harness_contract::execution_graph::ExecutionNodeKind::Subgraph
                || format!("team-graph:{}", request.team_id) != child.id
                || request.parent_execution.as_ref() != Some(&parent)
            {
                return Err(ExecutionRunnerError::Resource {
                    node_id: parent.node_id.clone(),
                    reason: "child execution does not match the durable parent join binding"
                        .to_string(),
                });
            }
            let parent_attempt = current
                .recovery_cursor
                .node_attempts
                .get(&parent.node_id)
                .copied()
                .unwrap_or_default();
            let command =
                harness_contract::execution_graph::ExecutionGraphCommand::ResolveChildExecution {
                    expected_revision: current.revision,
                    receipt: Box::new(
                        harness_contract::execution_graph::ChildExecutionTerminalReceipt {
                            parent_execution_id: parent.execution_id.clone(),
                            parent_node_id: parent.node_id.clone(),
                            child_execution_id: child.id.clone(),
                            child_revision: child.revision,
                            parent_attempt,
                            result: team_child_terminal_result(&child),
                            correlation_id: child_resolution_correlation(
                                &parent.execution_id,
                                &parent.node_id,
                                &child.id,
                                parent_attempt,
                                child.revision,
                            ),
                        },
                    ),
                };
            match self.command(&parent.execution_id, command).await {
                Ok(_) => return Ok(Some(parent.execution_id)),
                Err(ExecutionRunnerError::Commit(
                    super::commit_service::ExecutionCommitError::StaleRevision { .. },
                )) => continue,
                Err(error) => return Err(error),
            }
        }
        let latest = self.state_store.load_async(&parent.execution_id).await?;
        if latest.node_statuses.get(&parent.node_id) == Some(&ExecutionNodeStatus::WaitingExternal)
        {
            return Err(ExecutionRunnerError::Resource {
                node_id: parent.node_id,
                reason: "child terminal join remained stale after bounded CAS retries; durable resolver checkpoint must retry"
                    .to_string(),
            });
        }
        Ok(None)
    }

    /// Prepares every currently schedulable node and commits the admitted wave
    /// as one durable graph mutation. Executor work starts only after this
    /// returns, so short tasks can reach the configured concurrency instead of
    /// completing while later siblings are still paying one fsync each.
    pub(crate) async fn prepare_ready_wave(
        &self,
        graph_id: &str,
        nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
    ) -> Result<Vec<ExecutionPreparedPumpNode>, ExecutionRunnerError> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
            return Ok(nodes
                .into_iter()
                .map(|node| ExecutionPreparedPumpNode {
                    startup_error: Some(ExecutionRunnerError::CommandSuperseded {
                        node_id: node.id.clone(),
                    }),
                    node,
                })
                .collect());
        }
        // Win the graph mutation order before invoking executor.start. A
        // Pause/Cancel command that arrives afterward must observe this
        // wave's single Running revision, matching the established command
        // CAS contract, while all actual executor polling remains outside.
        let coordination = self.graph_coordination(graph_id).await;
        let graph = self.state_store.load_snapshot_async(graph_id).await?;
        let mut prepared = Vec::with_capacity(nodes.len());
        let mut output = Vec::with_capacity(nodes.len());
        for node in nodes {
            let deadline_at_ms = match execution_deadline_at_ms(&node) {
                Ok(value) => value,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        node,
                        startup_error: Some(error),
                    });
                    continue;
                }
            };
            if deadline_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
                output.push(ExecutionPreparedPumpNode {
                    startup_error: Some(ExecutionRunnerError::DeadlineExceeded {
                        node_id: node.id.clone(),
                        deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                    }),
                    node,
                });
                continue;
            }
            let resources = match self
                .acquire_node_resources(graph.as_ref(), &node, deadline_at_ms, Some(Duration::ZERO))
                .await
            {
                Ok(resources) => resources,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        node,
                        startup_error: Some(error),
                    });
                    continue;
                }
            };
            let resource_kind = resources
                .resource
                .as_ref()
                .map(|resource| resource.kind().clone());
            let resource_queue_wait = resources
                .resource
                .as_ref()
                .map_or(Duration::ZERO, ExecutionResourceLease::queue_wait);
            let resource_started = std::time::Instant::now();
            let Some(executor) = self.registry.get(&node.executor_kind) else {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                output.push(ExecutionPreparedPumpNode {
                    startup_error: Some(
                        NodeExecutorError::Unavailable {
                            executor_kind: node.executor_kind.clone(),
                            node_id: node.id.clone(),
                        }
                        .into(),
                    ),
                    node,
                });
                continue;
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
                    graph: Arc::clone(&graph),
                    node: node.clone(),
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
                    output.push(ExecutionPreparedPumpNode {
                        node,
                        startup_error: Some(error.into()),
                    });
                    continue;
                }
            };
            let leaf_effect_owner = node.kind
                == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.executor_kind == "tool_batch";
            let effect_state = if leaf_effect_owner {
                ExecutionEffectState::Fresh
            } else {
                self.commit_service.inspect_execution_effect(&ticket)?
            };
            let effect_receipt_required =
                !leaf_effect_owner && matches!(effect_state, ExecutionEffectState::Fresh);
            prepared.push(PreparedNodeStart {
                node,
                executor,
                ticket,
                resources,
                resource_kind,
                resource_queue_wait,
                resource_started,
                effect_state,
                effect_receipt_required,
            });
        }

        if prepared.is_empty() {
            return Ok(output);
        }

        let current = Arc::clone(&graph);
        let mut admitted = Vec::with_capacity(prepared.len());
        let mut superseded = Vec::new();
        for start in prepared {
            if current.node_statuses.get(&start.node.id) == Some(&ExecutionNodeStatus::Ready) {
                admitted.push(start);
            } else {
                superseded.push(start);
            }
        }
        if !admitted.is_empty() {
            let bindings = admitted
                .iter()
                .map(|start| {
                    (
                        start.node.id.clone(),
                        start.resources.binding(&start.ticket),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let effect_tickets = admitted
                .iter()
                .filter(|start| start.effect_receipt_required)
                .map(|start| start.ticket.clone())
                .collect::<Vec<_>>();
            {
                let mut active = self.active.lock().await;
                for start in admitted.drain(..) {
                    output.push(ExecutionPreparedPumpNode {
                        node: start.node.clone(),
                        startup_error: None,
                    });
                    active.insert(
                        (start.ticket.graph_id.clone(), start.ticket.node_id.clone()),
                        ActiveNode {
                            executor: start.executor,
                            ticket: start.ticket,
                            resource_kind: start.resource_kind,
                            resource_queue_wait: start.resource_queue_wait,
                            resource_started: start.resource_started,
                            effect_state: start.effect_state,
                            effect_receipt_required: start.effect_receipt_required,
                            wave_prepared: true,
                            _resources: start.resources,
                        },
                    );
                }
            }
            if let Err(error) = self
                .commit_service
                .bind_and_start_nodes_async(current.as_ref().clone(), bindings, effect_tickets)
                .await
            {
                let mut active = self.active.lock().await;
                let failed = output
                    .iter()
                    .filter(|item| item.startup_error.is_none())
                    .map(|item| item.node.id.clone())
                    .collect::<Vec<_>>();
                let mut aborted = Vec::with_capacity(failed.len());
                for node_id in failed {
                    if let Some(start) = active.remove(&(graph_id.to_string(), node_id)) {
                        aborted.push(start);
                    }
                }
                drop(active);
                drop(coordination);
                for start in aborted {
                    let _ = start.executor.cancel(&start.ticket).await;
                    self.record_resource_terminal(
                        start.resource_kind.as_ref(),
                        start.resource_queue_wait,
                        start.resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                }
                return Err(error.into());
            }
        }
        drop(coordination);
        for start in superseded {
            let _ = start.executor.cancel(&start.ticket).await;
            self.record_resource_terminal(
                start.resource_kind.as_ref(),
                start.resource_queue_wait,
                start.resource_started,
                crate::execution_core::graph::ResourceResultClass::Cancelled,
            );
            output.push(ExecutionPreparedPumpNode {
                startup_error: Some(ExecutionRunnerError::CommandSuperseded {
                    node_id: start.node.id.clone(),
                }),
                node: start.node,
            });
        }
        Ok(output)
    }

    pub(crate) async fn execute_prepared_pump_node(
        &self,
        graph_id: &str,
        prepared: ExecutionPreparedPumpNode,
        durable_progress: Arc<Notify>,
    ) -> Result<(), ExecutionRunnerError> {
        let result = match prepared.startup_error {
            Some(error) => Err(error),
            None => self.start_and_execute_node(graph_id, prepared.node).await,
        };
        self.finish_pump_node(graph_id, result, durable_progress)
            .await
    }

    async fn commit_terminal_wave(
        &self,
        graph_id: &str,
        transition: ExecutionTerminalCommit,
    ) -> Result<TerminalCommitDisposition, ExecutionRunnerError> {
        let (response, receiver) = oneshot::channel();
        let leader = {
            let mut batches = self.terminal_batches.lock().await;
            let batch = batches.entry(graph_id.to_string()).or_default();
            batch.pending.push(PendingTerminalCommit {
                transition,
                response,
            });
            if batch.leader_active {
                false
            } else {
                batch.leader_active = true;
                true
            }
        };
        if leader {
            // A tiny coalescing window gathers siblings that finished in the
            // same scheduler wave. It is bounded and does not delay a lone
            // dependency predecessor materially.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let pending = {
                let mut batches = self.terminal_batches.lock().await;
                batches
                    .remove(graph_id)
                    .map_or_else(Vec::new, |batch| batch.pending)
            };
            let coordination = self.graph_coordination_without_command(graph_id).await;
            match self.state_store.load_async(graph_id).await {
                Ok(current) => {
                    let mut admitted = Vec::new();
                    let mut admitted_responses = Vec::new();
                    let mut superseded_responses = Vec::new();
                    for pending in pending {
                        if current.node_statuses.get(&pending.transition.node_id)
                            == Some(&ExecutionNodeStatus::Running)
                        {
                            admitted.push(pending.transition);
                            admitted_responses.push(pending.response);
                        } else {
                            superseded_responses.push(pending.response);
                        }
                    }
                    let commit = if admitted.is_empty() {
                        Ok(())
                    } else {
                        self.commit_service
                            .transition_nodes_async(current, admitted)
                            .await
                            .map(|_| ())
                    };
                    for response in superseded_responses {
                        let _ = response.send(TerminalCommitDisposition::Superseded);
                    }
                    match commit {
                        Ok(()) => {
                            for response in admitted_responses {
                                let _ = response.send(TerminalCommitDisposition::Committed);
                            }
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            for response in admitted_responses {
                                let _ = response
                                    .send(TerminalCommitDisposition::Failed(reason.clone()));
                            }
                        }
                    }
                }
                Err(error) => {
                    let reason = error.to_string();
                    for pending in pending {
                        let _ = pending
                            .response
                            .send(TerminalCommitDisposition::Failed(reason.clone()));
                    }
                }
            }
            drop(coordination);
        }
        receiver.await.map_err(|_| {
            ExecutionRunnerError::Driver(
                "terminal wave coordinator closed before returning a disposition".to_string(),
            )
        })
    }

    async fn finish_pump_node(
        &self,
        graph_id: &str,
        result: Result<(String, NodeExecutionOutcome), ExecutionRunnerError>,
        durable_progress: Arc<Notify>,
    ) -> Result<(), ExecutionRunnerError> {
        let (node_id, outcome) = match result {
            Err(ExecutionRunnerError::CommandSuperseded { node_id }) => {
                if let Some(active) = self
                    .active
                    .lock()
                    .await
                    .remove(&(graph_id.to_string(), node_id))
                {
                    let _ = active.executor.cancel(&active.ticket).await;
                }
                return Ok(());
            }
            Err(error @ ExecutionRunnerError::ResourceDeferred { .. }) => return Err(error),
            Err(ExecutionRunnerError::DeadlineExceeded {
                node_id,
                deadline_at_ms,
            }) => {
                self.terminalize_deadline_node(graph_id, &node_id, deadline_at_ms)
                    .await?;
                durable_progress.notify_one();
                return Ok(());
            }
            Err(ExecutionRunnerError::Resource { node_id, reason }) => {
                self.block_unstarted_resource_node(graph_id, &node_id, reason)
                    .await?;
                durable_progress.notify_one();
                return Ok(());
            }
            Ok(value) => value,
            Err(ExecutionRunnerError::Executor(error)) => {
                if matches!(error, NodeExecutorError::Start { .. }) {
                    let node_id = executor_error_node_id(&error).to_string();
                    self.isolate_node_failure(graph_id, &node_id, error.to_string())
                        .await?;
                    durable_progress.notify_one();
                    return Ok(());
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
                if current.node_statuses.get(&node_id) == Some(&ExecutionNodeStatus::Running) {
                    self.commit_service
                        .transition_node_async(
                            current,
                            node_id,
                            terminal_status,
                            Some(result),
                            Vec::new(),
                        )
                        .await?;
                    durable_progress.notify_one();
                }
                // A Pause/Cancel command may have superseded the executor while
                // it returned. The command's durable graph state remains truth.
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let deadline_terminal = outcome
            .result
            .failure
            .as_ref()
            .is_some_and(|failure| failure.kind == "execution_deadline_exceeded");
        if let Err(error) = validate_outcome(&node_id, &outcome) {
            let aborted = self
                .active
                .lock()
                .await
                .get(&(graph_id.to_string(), node_id.clone()))
                .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
            if let Some((executor, ticket)) = aborted {
                let _ = executor
                    .after_abort(&ticket, "executor_outcome_validation_failed")
                    .await;
            }
            self.active
                .lock()
                .await
                .remove(&(graph_id.to_string(), node_id.clone()));
            return Err(error);
        }
        if let Some(replan) = outcome.replan.as_ref() {
            if let Err(error) = self.registry.validate_nodes(&replan.nodes) {
                let aborted = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = aborted {
                    let _ = executor
                        .after_abort(&ticket, "executor_replan_validation_failed")
                        .await;
                }
                self.active
                    .lock()
                    .await
                    .remove(&(graph_id.to_string(), node_id.clone()));
                return Err(error.into());
            }
        }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
        }
        let active_wave = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.clone()))
            .map(|active| {
                (
                    Arc::clone(&active.executor),
                    active.ticket.clone(),
                    active.wave_prepared,
                    active.effect_receipt_required,
                )
            });
        let batchable = active_wave.as_ref().is_some_and(|(_, _, wave, _)| *wave)
            && outcome.replan.is_none()
            && outcome.delivery_envelope.is_none()
            && outcome.terminal_presentation.is_none();
        if batchable {
            let (executor, ticket, _, effect_receipt_required) = active_wave
                .clone()
                .expect("batchable active wave has an execution binding");
            let disposition = self
                .commit_terminal_wave(
                    graph_id,
                    ExecutionTerminalCommit {
                        node_id: node_id.clone(),
                        outcome,
                        effect_ticket: effect_receipt_required.then_some(ticket.clone()),
                    },
                )
                .await?;
            match disposition {
                TerminalCommitDisposition::Committed => {
                    durable_progress.notify_one();
                    let after_commit = executor.after_commit(&ticket).await;
                    if deadline_terminal {
                        executor.cancellation_finalized(&ticket);
                    }
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id));
                    after_commit?;
                    return Ok(());
                }
                TerminalCommitDisposition::Superseded => {
                    executor
                        .after_abort(&ticket, "graph_command_superseded_before_terminal_wave")
                        .await?;
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id));
                    return Ok(());
                }
                TerminalCommitDisposition::Failed(reason) => {
                    let _ = executor
                        .after_abort(&ticket, "terminal_wave_commit_failed")
                        .await;
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id));
                    return Err(ExecutionRunnerError::Driver(reason));
                }
            }
        }
        if let Some((executor, ticket, true, true)) = active_wave {
            if let Err(error) = self
                .commit_service
                .commit_execution_effect(&ticket, &outcome)
            {
                let _ = executor
                    .after_abort(&ticket, "effect_receipt_commit_failed")
                    .await;
                return Err(error.into());
            }
        }
        // Commit terminal graph truth before waking the pump or publishing
        // process-local executor output.
        let committed_executor = {
            let _coordination = self.graph_coordination_without_command(graph_id).await;
            let mut current = self.state_store.load_async(graph_id).await?;
            if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running) {
                let aborted = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                drop(_coordination);
                if let Some((executor, ticket)) = aborted {
                    executor
                        .after_abort(&ticket, "graph_command_superseded_before_commit")
                        .await?;
                }
                self.active
                    .lock()
                    .await
                    .remove(&(graph_id.to_string(), node_id.clone()));
                return Ok(());
            }
            if let Some(envelope) = outcome.delivery_envelope.clone() {
                current.delivery_envelope = Some(envelope);
            }
            if let Some(presentation) = outcome.terminal_presentation.clone() {
                current.terminal_presentation = Some(presentation);
            }
            let transition = if let Some(replan) = outcome.replan {
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
                    .await
            } else {
                self.commit_service
                    .transition_node_async(
                        current,
                        node_id.clone(),
                        outcome.result.status,
                        Some(outcome.result),
                        outcome.domain_events,
                    )
                    .await
            };
            if let Err(error) = transition {
                let aborted = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                drop(_coordination);
                if let Some((executor, ticket)) = aborted {
                    let _ = executor
                        .after_abort(&ticket, "graph_transition_commit_failed")
                        .await;
                }
                self.active
                    .lock()
                    .await
                    .remove(&(graph_id.to_string(), node_id.clone()));
                return Err(error.into());
            }
            durable_progress.notify_one();
            self.active
                .lock()
                .await
                .get(&(graph_id.to_string(), node_id.clone()))
                .map(|active| (Arc::clone(&active.executor), active.ticket.clone()))
        };
        if let Some((executor, ticket)) = committed_executor {
            let after_commit = executor.after_commit(&ticket).await;
            if deadline_terminal {
                executor.cancellation_finalized(&ticket);
            }
            self.active
                .lock()
                .await
                .remove(&(graph_id.to_string(), node_id));
            after_commit?;
        } else {
            self.active
                .lock()
                .await
                .remove(&(graph_id.to_string(), node_id));
        }
        Ok(())
    }

    async fn start_and_execute_node(
        &self,
        graph_id: &str,
        node: harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<(String, NodeExecutionOutcome), ExecutionRunnerError> {
        let leaf_effect_owner = node.kind
            == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            && node.executor_kind == "tool_batch";
        let deadline_at_ms = execution_deadline_at_ms(&node)?;
        if deadline_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
            return Err(ExecutionRunnerError::DeadlineExceeded {
                node_id: node.id,
                deadline_at_ms: deadline_at_ms.unwrap_or_default(),
            });
        }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        }
        let already_prepared = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node.id.clone()))
            .map(|active| {
                (
                    Arc::clone(&active.executor),
                    active.ticket.clone(),
                    active.resource_kind.clone(),
                    active.resource_queue_wait,
                    active.resource_started,
                    active.effect_state.clone(),
                )
            });
        let wave_prepared = already_prepared.is_some();
        let (
            executor,
            ticket,
            resource_kind,
            resource_queue_wait,
            resource_started,
            prepared_effect_state,
        ) = if let Some(prepared) = already_prepared {
            prepared
        } else {
            let admission_graph = self.state_store.load_snapshot_async(graph_id).await?;
            let resources = self
                .acquire_node_resources(admission_graph.as_ref(), &node, deadline_at_ms, None)
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
            let coordination = self.graph_coordination_without_command(graph_id).await;
            let graph = self.state_store.load_snapshot_async(graph_id).await?;
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
                    graph: Arc::clone(&graph),
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
                    resource_kind: resource_kind.clone(),
                    resource_queue_wait,
                    resource_started,
                    effect_state: ExecutionEffectState::Fresh,
                    effect_receipt_required: false,
                    wave_prepared: false,
                    _resources: resources,
                },
            );
            if let Err(error) = self
                .commit_service
                .bind_and_start_node_async(graph.as_ref().clone(), ticket.node_id.clone(), binding)
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
            drop(coordination);
            (
                executor,
                ticket,
                resource_kind,
                resource_queue_wait,
                resource_started,
                ExecutionEffectState::Fresh,
            )
        };
        // The coordination gate protects the state check and durable effect
        // intent. It must not cover executor work: a ToolBatch may submit a
        // child execution graph (for example `runtime_orchestrate`), which
        // correctly needs the same Runner to make progress. Holding the gate
        // across that await creates a parent/child re-entrancy deadlock.
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
        }
        if wave_prepared
            && !self
                .active
                .lock()
                .await
                .contains_key(&(graph_id.to_string(), ticket.node_id.clone()))
        {
            return Err(ExecutionRunnerError::CommandSuperseded {
                node_id: ticket.node_id.clone(),
            });
        }
        let effect_state = if wave_prepared {
            // prepare_ready_wave committed both Running truth and, for
            // non-leaf executors, the effect intent in one transaction. A
            // second per-node graph read/lock here would recreate the very
            // dispatch serialization the wave boundary removes.
            prepared_effect_state
        } else {
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
        let outcome = if let Some(deadline_at_ms) = deadline_at_ms {
            let remaining = Duration::from_millis(deadline_at_ms.saturating_sub(now_ms()));
            tokio::select! {
                biased;
                outcome = executor.poll_or_await(&ticket) => outcome,
                () = tokio::time::sleep(remaining) => {
                    match tokio::time::timeout(Duration::from_secs(5), executor.cancel(&ticket)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => tracing::warn!(
                            graph_id = ticket.graph_id,
                            node_id = ticket.node_id,
                            %error,
                            "deadline terminalization could not confirm executor cancellation"
                        ),
                        Err(_) => tracing::warn!(
                            graph_id = ticket.graph_id,
                            node_id = ticket.node_id,
                            "deadline terminalization timed out while propagating executor cancellation"
                        ),
                    }
                    Ok(deadline_exceeded_outcome(&ticket.node_id, deadline_at_ms))
                }
            }
        } else {
            executor.poll_or_await(&ticket).await
        };
        let node_duration_ms = resource_started
            .elapsed()
            .saturating_add(resource_queue_wait)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
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
        let mut outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = executor
                    .after_abort(&ticket, "executor_poll_failed_after_preview")
                    .await;
                return Err(error.into());
            }
        };
        outcome.result.usage.duration_ms = outcome
            .result
            .usage
            .duration_ms
            .max(node_duration_ms.max(1));
        if !leaf_effect_owner && !wave_prepared {
            if let Err(error) = self
                .commit_service
                .commit_execution_effect(&ticket, &outcome)
            {
                let _ = executor
                    .after_abort(&ticket, "effect_receipt_commit_failed")
                    .await;
                return Err(error.into());
            }
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
        deadline_at_ms: Option<u64>,
        scope_wait: Option<Duration>,
    ) -> Result<NodeResourceGuards, ExecutionRunnerError> {
        let resource_kind = match node.kind {
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask => {
                Some(ExecutionResourceKind::Agent)
            }
            harness_contract::execution_graph::ExecutionNodeKind::Materialize => {
                Some(ExecutionResourceKind::Tool)
            }
            harness_contract::execution_graph::ExecutionNodeKind::Subgraph
            | harness_contract::execution_graph::ExecutionNodeKind::ToolBatch => {
                // Subgraph and ToolBatch are durable orchestration
                // containers. Their child Agent/tool leaves acquire the
                // authoritative resource and scope leases. Holding a path
                // lock here while synchronously awaiting a descendant that
                // needs the same path creates a parent-waits-child / child-
                // waits-parent deadlock. Keep validation at the container
                // boundary, but defer ownership to the actual effect leaf.
                for scope in &node.resource_scopes {
                    if let Some(path) = scope.strip_prefix("read:") {
                        let _ = self.scoped_resource_for_path(&node.id, path, false)?;
                    } else if let Some(path) = scope.strip_prefix("write:") {
                        let _ = self.scoped_resource_for_path(&node.id, path, true)?;
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
            let mut demands = vec![(resource_kind, 1)];
            let mut parent_execution_limit = None;
            if node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                let budget = serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(
                    &node.payload_ref,
                )
                .map(|packet| packet.budget_lease)
                .or_else(|packet_error| {
                    serde_json::from_str::<harness_contract::agent::AgentTaskIntent>(
                        &node.payload_ref,
                    )
                    .map(|intent| intent.budget_lease)
                    .map_err(|intent_error| {
                        format!(
                            "neither canonical AgentTaskPacket ({packet_error}) nor AgentTaskIntent ({intent_error}) carries a valid parent execution budget"
                        )
                    })
                })
                .map_err(|reason| ExecutionRunnerError::Resource {
                    node_id: node.id.clone(),
                    reason,
                })?;
                budget
                    .validate()
                    .map_err(|reason| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: reason.to_string(),
                    })?;
                let parent_kind =
                    ExecutionResourceKind::ParentExecution(budget.parent_budget_id.clone());
                demands.push((parent_kind.clone(), 1));
                parent_execution_limit = Some((parent_kind, budget.parent_budget.max_parallel));
            }
            let mut request = ResourceAdmissionRequest::new(graph.service_class, demands)
                .with_fairness_key(format!("graph:{}", graph.id));
            if let Some(work) = node
                .work
                .as_ref()
                .filter(|work| work.scheduling_priority > 0)
            {
                request = request.with_priority(work.scheduling_priority);
            }
            if let Some((kind, limit)) = parent_execution_limit {
                request = request.with_ephemeral_limit(kind, limit);
            }
            if let Some(deadline_at_ms) = deadline_at_ms {
                request = request.with_deadline_at_ms(deadline_at_ms);
            }
            let decision = self
                .resource_manager
                .admit(request)
                .await
                .map_err(|error| ExecutionRunnerError::Resource {
                    node_id: node.id.clone(),
                    reason: error.to_string(),
                })?;
            Some(match decision {
                ResourceAdmissionDecision::Granted { lease, .. } => lease,
                ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                    if wait_reason == ResourceWaitReason::DeadlineExpired {
                        return Err(ExecutionRunnerError::DeadlineExceeded {
                            node_id: node.id.clone(),
                            deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                        });
                    }
                    return Err(ExecutionRunnerError::ResourceDeferred {
                        node_id: node.id.clone(),
                        reason: format!("resource admission overloaded: {wait_reason:?}"),
                    });
                }
                ResourceAdmissionDecision::Deferred { wait_reason, .. } => {
                    if wait_reason == ResourceWaitReason::DeadlineExpired {
                        return Err(ExecutionRunnerError::DeadlineExceeded {
                            node_id: node.id.clone(),
                            deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                        });
                    }
                    return Err(ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: format!("resource admission refused: {wait_reason:?}"),
                    });
                }
            })
        } else {
            None
        };
        let mut scope_requests = Vec::new();
        let mut worktree_path = None;
        for scope in &node.resource_scopes {
            if let Some(path) = scope.strip_prefix("read:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path, false)?,
                        mode: ScopeLockMode::Read,
                    });
                }
            } else if let Some(path) = scope.strip_prefix("write:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path, true)?,
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
                match self.scope_locks.acquire(scope_requests, scope_wait).await {
                    Ok(lease) => lease,
                    Err(ScopeLockError::TimedOut { .. }) if scope_wait.is_some() => {
                        return Err(ExecutionRunnerError::ResourceDeferred {
                            node_id: node.id.clone(),
                            reason: "scope lock is currently owned by another effect leaf"
                                .to_string(),
                        });
                    }
                    Err(error) => {
                        return Err(ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        });
                    }
                },
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

    async fn terminalize_deadline_node(
        &self,
        graph_id: &str,
        node_id: &str,
        deadline_at_ms: u64,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if !matches!(
            current.node_statuses.get(node_id),
            Some(ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running)
        ) {
            return Ok(());
        }
        let outcome = deadline_exceeded_outcome(node_id, deadline_at_ms);
        self.commit_service
            .transition_node_async(
                current,
                node_id.to_string(),
                outcome.result.status,
                Some(outcome.result),
                Vec::new(),
            )
            .await?;
        Ok(())
    }

    fn scoped_resource_for_path(
        &self,
        node_id: &str,
        path: &str,
        write: bool,
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
            return ScopedResource::workspace(self.path_identity_resolver.workspace_id()).map_err(
                |error| ExecutionRunnerError::Resource {
                    node_id: node_id.to_string(),
                    reason: error.to_string(),
                },
            );
        }
        let rendered = relative.to_string_lossy();
        let identity = if write {
            self.path_identity_resolver.resolve_planned_file(&rendered)
        } else {
            self.path_identity_resolver.resolve_existing(&rendered)
        }
        .map_err(|error| ExecutionRunnerError::Resource {
            node_id: node_id.to_string(),
            reason: error.to_string(),
        })?;
        Ok(ScopedResource::workspace_object(identity))
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

    /// Record a node-level failure without terminating unrelated ready nodes.
    ///
    /// Start errors and panic recovery must not propagate to the completion
    /// pump as graph-level failures: the node is the only thing that failed.
    pub(crate) async fn isolate_node_failure(
        &self,
        graph_id: &str,
        node_id: &str,
        reason: String,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        let status = current.node_statuses.get(node_id).copied();
        if !matches!(
            status,
            Some(
                ExecutionNodeStatus::Ready
                    | ExecutionNodeStatus::Running
                    | ExecutionNodeStatus::Planned
            )
        ) {
            return Ok(());
        }
        // A failed start, executor panic, or node-local failure is a failure of
        // that node, not a safety boundary. Keep it retryable so recovery and
        // replan can decide the next move instead of permanently blocking it.
        let terminal_status = ExecutionNodeStatus::Failed;
        let result = ExecutionNodeResult {
            status: terminal_status,
            result_ref: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "node_execution_isolated_failure".to_string(),
                message: reason,
                retryable: true,
                evidence_refs: Vec::new(),
            }),
            usage: Default::default(),
            finished_at_ms: now_ms(),
        };
        self.commit_service
            .transition_node_async(
                current,
                node_id.to_string(),
                terminal_status,
                Some(result),
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
            ExecutionGraphCommand::Pause { .. }
                | ExecutionGraphCommand::Cancel { .. }
                | ExecutionGraphCommand::CancelNode { .. }
        ) {
            // Publish command intent before waiting for graph coordination so
            // a just-admitted execution wave cannot begin polling in the gap
            // between its Running commit and this command acquiring the lock.
            let intent = CommandIntentOwner::install(graph_id, Arc::clone(&self.command_intents))?;
            let coordination = self.graph_coordination(graph_id).await;
            let graph = self.state_store.load_async(graph_id).await?;
            self.commit_service
                .validate_command_revision(&graph, &command)?;
            let active = self
                .active
                .lock()
                .await
                .iter()
                .filter(|((active_graph_id, node_id), _)| {
                    active_graph_id == graph_id
                        && match &command {
                            ExecutionGraphCommand::CancelNode {
                                node_id: target, ..
                            } => node_id == target,
                            _ => true,
                        }
                })
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
                ExecutionGraphCommand::Pause { .. }
                    | ExecutionGraphCommand::Cancel { .. }
                    | ExecutionGraphCommand::CancelNode { .. }
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
            if result.is_ok() {
                let node_ids = cancellation_finalizer
                    .active()
                    .iter()
                    .map(|(node_id, _, _)| node_id.clone())
                    .collect::<Vec<_>>();
                let mut active = self.active.lock().await;
                for node_id in node_ids {
                    active.remove(&(graph_id.to_string(), node_id));
                }
                drop(active);
            }
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

    pub(crate) async fn revise_semantic_graph(
        &self,
        graph_id: &str,
        expected_revision: u64,
        nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
        edges: Vec<harness_contract::execution_graph::ExecutionEdge>,
        reason: String,
        mutation_id: String,
        completion: harness_contract::execution_graph::ExecutionCompletionContract,
        collaboration_program: Option<harness_contract::execution_graph::CollaborationProgram>,
        collaboration_escalation: Option<
            harness_contract::execution_graph::CollaborationEscalationReceipt,
        >,
        retired_instance_ids: Vec<String>,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        let coordination = self.graph_coordination_without_command(graph_id).await;
        let graph = self.state_store.load_async(graph_id).await?;
        if graph.revision != expected_revision {
            return Err(ExecutionCommitError::StaleRevision {
                graph_id: graph_id.to_string(),
                expected: expected_revision,
                actual: graph.revision,
            }
            .into());
        }
        self.registry.validate_nodes(&nodes)?;
        let graph = self
            .commit_service
            .replan_semantic_async(
                graph,
                nodes,
                edges,
                reason,
                mutation_id,
                completion,
                collaboration_program,
                collaboration_escalation,
                retired_instance_ids,
            )
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
                let node = graph.nodes.iter().find(|node| node.id == node_id);
                let predecessors = node
                    .map(|node| dependency_predecessors(&graph, node))
                    .unwrap_or_default();
                let dependency = node
                    .and_then(|node| node.work.as_ref())
                    .map(|work| work.dependency.clone())
                    .unwrap_or_default();
                let target = dependency_target(&dependency, &predecessors);
                let waits_for_autonomous_work = node
                    .is_some_and(|node| autonomous_work_blocks_terminal(&graph, node.kind, target));
                if waits_for_autonomous_work {
                    continue;
                }
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

fn autonomous_work_blocks_terminal(
    graph: &ExecutionGraph,
    node_kind: ExecutionNodeKind,
    target: Option<ExecutionNodeStatus>,
) -> bool {
    target == Some(ExecutionNodeStatus::Ready)
        && matches!(
            node_kind,
            ExecutionNodeKind::Synthesize
                | ExecutionNodeKind::Verify
                | ExecutionNodeKind::Materialize
        )
        && graph.autonomous_work.iter().any(|(work_id, work)| {
            work.required
                && graph.work_states.get(work_id).is_none_or(|state| {
                    state.status
                        != harness_contract::execution_graph::ExecutionWorkRuntimeStatus::Accepted
                })
        })
}

/// One predecessor lane carrying both its durable lifecycle status and its
/// committed terminal result. `EvidenceReady` consumes the result facts;
/// every other policy only needs the lifecycle status.
#[derive(Debug, Clone, Copy)]
struct DependencyPredecessor<'a> {
    status: ExecutionNodeStatus,
    result: Option<&'a ExecutionNodeResult>,
}

impl DependencyPredecessor<'_> {
    #[cfg(test)]
    fn status_only(status: ExecutionNodeStatus) -> Self {
        Self {
            status,
            result: None,
        }
    }
}

/// Build the exact `DependsOn` predecessor lanes for one node. The same
/// helper feeds `current_report`, `advance_dependencies` and every recovery
/// pump, so a lost notify is repaired by the next durable repump instead of
/// by a poll loop.
fn dependency_predecessors<'a>(
    graph: &'a ExecutionGraph,
    node: &'a harness_contract::execution_graph::ExecutionNodeSpec,
) -> Vec<DependencyPredecessor<'a>> {
    let required_evidence_refs = node
        .work
        .as_ref()
        .map(|work| work.required_evidence_refs.as_slice())
        .unwrap_or_default();
    graph
        .edges
        .iter()
        .filter(|edge| edge.kind.is_dependency() && edge.to == node.id)
        .map(|edge| {
            let status = verified_predecessor_status(graph, &edge.from, required_evidence_refs);
            DependencyPredecessor {
                status,
                result: graph.node_results.get(&edge.from),
            }
        })
        .collect()
}

fn dependency_target(
    policy: &harness_contract::execution_graph::ExecutionDependencyPolicy,
    predecessors: &[DependencyPredecessor<'_>],
) -> Option<ExecutionNodeStatus> {
    use harness_contract::execution_graph::ExecutionDependencyPolicy;

    if matches!(policy, ExecutionDependencyPolicy::Finally) {
        return predecessors
            .iter()
            .all(|predecessor| predecessor.status.is_terminal())
            .then_some(ExecutionNodeStatus::Ready);
    }

    if let ExecutionDependencyPolicy::EvidenceReady { predicate, .. } = policy {
        return evidence_ready_target(predicate, predecessors);
    }

    let completed = predecessors
        .iter()
        .filter(|predecessor| predecessor.status == ExecutionNodeStatus::Completed)
        .count();
    let possible = completed
        + predecessors
            .iter()
            .filter(|predecessor| !predecessor.status.is_terminal())
            .count();
    let required = match policy {
        ExecutionDependencyPolicy::All | ExecutionDependencyPolicy::Finally => predecessors.len(),
        ExecutionDependencyPolicy::Any { .. } => 1,
        ExecutionDependencyPolicy::Quorum { minimum, .. } => usize::from(*minimum),
        ExecutionDependencyPolicy::EvidenceReady { .. } => unreachable!(),
    };
    if completed >= required {
        Some(ExecutionNodeStatus::Ready)
    } else if possible < required {
        Some(ExecutionNodeStatus::Blocked)
    } else {
        None
    }
}

/// Ready, wait or permanently block one `EvidenceReady` edge.
///
/// The predicate consumes only Runtime-attested terminal facts: lifecycle
/// status, committed result facts and the acceptance verdict derived from
/// the persisted `required_acceptance`/`observed_acceptance` pair. It never
/// consults the filesystem, never re-runs an evaluator, and never rewrites a
/// predecessor's terminal status.
fn evidence_ready_target(
    predicate: &harness_contract::execution_graph::DependencyPredicate,
    predecessors: &[DependencyPredecessor<'_>],
) -> Option<ExecutionNodeStatus> {
    let harness_contract::execution_graph::DependencyPredicate::EvidenceReady {
        minimum,
        required_fact_kinds,
        accepted_execution_statuses,
        accepted_acceptance_verdicts,
        require_committed_effect,
    } = predicate;
    let required = usize::from(*minimum);
    if required == 0 {
        return Some(ExecutionNodeStatus::Ready);
    }
    let mut satisfied = 0usize;
    let mut waiting = 0usize;
    for predecessor in predecessors {
        match evidence_ready_satisfaction(
            predecessor,
            required_fact_kinds,
            accepted_execution_statuses,
            accepted_acceptance_verdicts,
            *require_committed_effect,
        ) {
            EvidenceReadyOutcome::Satisfied => satisfied += 1,
            EvidenceReadyOutcome::Waiting => waiting += 1,
            EvidenceReadyOutcome::Blocked => {}
        }
    }
    if satisfied >= required {
        Some(ExecutionNodeStatus::Ready)
    } else if satisfied + waiting >= required {
        None
    } else {
        Some(ExecutionNodeStatus::Blocked)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceReadyOutcome {
    Satisfied,
    Waiting,
    Blocked,
}

fn evidence_ready_satisfaction(
    predecessor: &DependencyPredecessor<'_>,
    required_fact_kinds: &[TerminalFactKind],
    accepted_execution_statuses: &[ExecutionNodeStatus],
    accepted_acceptance_verdicts: &[AcceptanceVerdict],
    require_committed_effect: bool,
) -> EvidenceReadyOutcome {
    let status = predecessor.status;
    if !status.is_terminal() {
        return EvidenceReadyOutcome::Waiting;
    }
    // A terminal predecessor without a committed result can never acquire
    // facts. It is a permanent blocker, never a waiting lane.
    let Some(result) = predecessor.result else {
        return EvidenceReadyOutcome::Blocked;
    };
    if !accepted_execution_statuses.contains(&status) {
        return EvidenceReadyOutcome::Blocked;
    }
    let verdict = derived_acceptance_verdict(result);
    if !accepted_acceptance_verdicts.contains(&verdict) {
        return EvidenceReadyOutcome::Blocked;
    }
    if require_committed_effect
        && !has_terminal_fact_kind(result, TerminalFactKind::CommittedEffect)
    {
        return EvidenceReadyOutcome::Blocked;
    }
    if !required_fact_kinds
        .iter()
        .all(|kind| has_terminal_fact_kind(result, *kind))
    {
        return EvidenceReadyOutcome::Blocked;
    }
    EvidenceReadyOutcome::Satisfied
}

/// Read the single terminal acceptance verdict from persisted node-result
/// facts.  The evaluator owns matching and digest construction at the
/// terminal producer; Runner only consumes that immutable result.
fn derived_acceptance_verdict(result: &ExecutionNodeResult) -> AcceptanceVerdict {
    // The Runtime contract-rejection terminal is the canonical
    // FrameworkInvalid marker: it preserves committed facts while reporting
    // that the Runtime acceptance/evaluation machinery could not validate
    // the result. Business failures never carry this marker.
    let framework_invalid = result.failure.as_ref().is_some_and(|failure| {
        failure.kind == "agent_backend"
            && failure
                .message
                .contains("Runtime rejected Agent terminal result")
    });
    if framework_invalid {
        return AcceptanceVerdict::FrameworkInvalid;
    }
    result
        .usage
        .acceptance_evaluation
        .as_ref()
        .filter(|evaluation| {
            evaluation.evaluator_revision
                == crate::acceptance_evaluator::AcceptanceEvaluator::REVISION
        })
        .map_or(AcceptanceVerdict::Unresolved, |evaluation| {
            evaluation.verdict
        })
}

fn has_terminal_fact_kind(result: &ExecutionNodeResult, kind: TerminalFactKind) -> bool {
    match kind {
        TerminalFactKind::CommittedEffect => {
            result
                .usage
                .observed_acceptance
                .observed_evidence
                .iter()
                .any(|evidence| evidence.workspace_prior_state.is_some())
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.evidence_ref.ref_type == "runtime_change")
        }
        TerminalFactKind::ObservedEvidence => {
            !result
                .usage
                .observed_acceptance
                .observed_evidence
                .is_empty()
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.is_durable())
        }
        TerminalFactKind::Artifact => result.evidence_refs.iter().any(|reference| {
            reference.evidence_ref.ref_type == "artifact" || reference.is_durable()
        }),
        TerminalFactKind::AcceptanceVerdict => {
            derived_acceptance_verdict(result) != AcceptanceVerdict::Unresolved
        }
    }
}

fn execution_deadline_at_ms(
    node: &harness_contract::execution_graph::ExecutionNodeSpec,
) -> Result<Option<u64>, ExecutionRunnerError> {
    if !matches!(
        node.kind,
        harness_contract::execution_graph::ExecutionNodeKind::AgentTask
            | harness_contract::execution_graph::ExecutionNodeKind::Subgraph
    ) {
        return Ok(None);
    }
    let payload =
        serde_json::from_str::<serde_json::Value>(&node.payload_ref).map_err(|error| {
            ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: format!("durable execution payload is invalid: {error}"),
            }
        })?;
    let deadline_at_ms = payload
        .get("deadline_at_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|deadline| *deadline != 0)
        .ok_or_else(|| ExecutionRunnerError::Resource {
            node_id: node.id.clone(),
            reason: "AgentTask/Subgraph has no Runtime-issued absolute deadline".to_string(),
        })?;
    Ok(Some(deadline_at_ms))
}

fn deadline_exceeded_outcome(node_id: &str, deadline_at_ms: u64) -> NodeExecutionOutcome {
    NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Cancelled,
        result_ref: Some(format!("execution-deadline:{node_id}:{deadline_at_ms}")),
        summary: Some("Execution branch stopped at its durable wall-clock deadline".to_string()),
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: "execution_deadline_exceeded".to_string(),
            message: format!(
                "execution node `{node_id}` exceeded absolute deadline `{deadline_at_ms}`"
            ),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: now_ms(),
    })
}

fn verified_predecessor_status(
    graph: &ExecutionGraph,
    predecessor_id: &str,
    required_evidence_refs: &[String],
) -> ExecutionNodeStatus {
    let status = graph.node_statuses[predecessor_id];
    if status != ExecutionNodeStatus::Completed || required_evidence_refs.is_empty() {
        return status;
    }
    let satisfied = graph
        .node_results
        .get(predecessor_id)
        .is_some_and(|result| {
            required_evidence_refs.iter().all(|required| {
                result.evidence_refs.iter().any(|reference| {
                    reference.evidence_ref.id == *required
                        || reference.retrieval_selector == *required
                        || format!(
                            "{}:{}",
                            reference.evidence_ref.ref_type, reference.evidence_ref.id
                        ) == *required
                })
            })
        });
    if satisfied {
        status
    } else {
        ExecutionNodeStatus::Failed
    }
}

fn quorum_tail_cancellations(graph: &ExecutionGraph) -> Vec<String> {
    let mut cancellations = BTreeSet::new();
    for consumer in &graph.nodes {
        if graph.node_statuses.get(&consumer.id) != Some(&ExecutionNodeStatus::Ready) {
            continue;
        }
        let Some(work) = consumer.work.as_ref() else {
            continue;
        };
        if !work.dependency.cancel_remaining() {
            continue;
        }
        let Some(group) = work.cancellation_group.as_deref() else {
            continue;
        };
        for predecessor_id in graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == consumer.id)
            .map(|edge| edge.from.as_str())
        {
            let Some(predecessor) = graph.nodes.iter().find(|node| node.id == predecessor_id)
            else {
                continue;
            };
            let cancellable = predecessor.work.as_ref().is_some_and(|predecessor_work| {
                !predecessor_work.required
                    && predecessor_work.cancellation_group.as_deref() == Some(group)
            });
            if cancellable
                && graph
                    .node_statuses
                    .get(predecessor_id)
                    .is_some_and(|status| !status.is_terminal())
            {
                cancellations.insert(predecessor_id.to_string());
            }
        }
    }
    cancellations.into_iter().collect()
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

/// Aggregate provider/tool usage exactly once from Team AgentTask leaves.
/// Verify/Synthesize are deterministic reducers over those leaves and must
/// never be counted a second time. Failed children without a synthesize result
/// therefore retain their actual cost.
pub(crate) fn aggregate_team_leaf_usage(
    graph: &ExecutionGraph,
) -> harness_contract::execution_graph::ExecutionUsage {
    let mut aggregate = harness_contract::execution_graph::ExecutionUsage::default();
    let mut models = BTreeSet::new();
    for node in graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
    {
        let Some(result) = graph.node_results.get(&node.id) else {
            continue;
        };
        let usage = &result.usage;
        if let Some(model) = usage
            .model
            .as_ref()
            .filter(|model| !model.trim().is_empty())
        {
            models.insert(model.clone());
        }
        aggregate.input_tokens = aggregate.input_tokens.saturating_add(usage.input_tokens);
        aggregate.output_tokens = aggregate.output_tokens.saturating_add(usage.output_tokens);
        aggregate.cached_tokens = aggregate.cached_tokens.saturating_add(usage.cached_tokens);
        aggregate.duration_ms = aggregate.duration_ms.max(usage.duration_ms);
        aggregate.tool_calls = aggregate.tool_calls.saturating_add(usage.tool_calls);
        aggregate.duplicate_tool_calls = aggregate
            .duplicate_tool_calls
            .saturating_add(usage.duplicate_tool_calls);
        aggregate.max_tool_concurrency_observed = aggregate
            .max_tool_concurrency_observed
            .max(usage.max_tool_concurrency_observed);
        aggregate.parallel_tool_batches = aggregate
            .parallel_tool_batches
            .saturating_add(usage.parallel_tool_batches);
        aggregate
            .runtime_write_attempt_paths
            .extend(usage.runtime_write_attempt_paths.iter().cloned());
        aggregate
            .observed_acceptance
            .merge_from(&usage.observed_acceptance);
    }
    aggregate.model = (models.len() == 1)
        .then(|| models.into_iter().next())
        .flatten();
    aggregate.runtime_write_attempt_paths.sort();
    aggregate.runtime_write_attempt_paths.dedup();
    aggregate
}

fn team_child_terminal_result(graph: &ExecutionGraph) -> ExecutionNodeResult {
    let synthesize = graph
        .nodes
        .iter()
        .find(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::Synthesize);
    let synthesize_result = synthesize.and_then(|node| graph.node_results.get(&node.id));
    let statuses = graph.node_statuses.values().copied().collect::<Vec<_>>();
    let status = if statuses
        .iter()
        .any(|status| *status == ExecutionNodeStatus::Failed)
    {
        ExecutionNodeStatus::Failed
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionNodeStatus::Blocked)
    {
        ExecutionNodeStatus::Blocked
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionNodeStatus::Cancelled)
    {
        ExecutionNodeStatus::Cancelled
    } else if synthesize_result
        .is_some_and(|result| result.status == ExecutionNodeStatus::Completed)
    {
        ExecutionNodeStatus::Completed
    } else {
        ExecutionNodeStatus::Blocked
    };
    let mut evidence_refs = graph
        .node_results
        .values()
        .flat_map(|result| result.evidence_refs.iter().cloned())
        .collect::<Vec<_>>();
    evidence_refs.sort_by(|left, right| {
        serde_json::to_string(left)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(right).unwrap_or_default())
    });
    evidence_refs.dedup();
    let failure = if status == ExecutionNodeStatus::Completed {
        None
    } else {
        graph
            .node_results
            .values()
            .find_map(|result| result.failure.clone())
            .or_else(|| {
                Some(harness_contract::execution_graph::ExecutionFailure {
                    kind: "child_graph_terminal_without_verified_result".to_string(),
                    message: format!(
                        "child execution `{}` settled as {}",
                        graph.id,
                        status_name_for_child(status)
                    ),
                    retryable: false,
                    evidence_refs: evidence_refs.clone(),
                })
            })
    };
    let result_ref = synthesize_result
        .and_then(|result| result.result_ref.clone())
        .or_else(|| Some(format!("execution-graph:{}", graph.id)));
    let mut usage = aggregate_team_leaf_usage(graph);
    if status == ExecutionNodeStatus::Completed {
        promote_completed_team_terminal_facts(
            &mut usage,
            &mut evidence_refs,
            &graph.id,
            graph.revision,
            result_ref.as_deref(),
            graph
                .lineage
                .as_ref()
                .map_or("unknown", |lineage| lineage.session_id.as_str()),
        );
    }
    ExecutionNodeResult {
        status,
        result_ref,
        summary: synthesize_result
            .and_then(|result| result.summary.clone())
            .or_else(|| {
                Some(format!(
                    "child execution `{}` settled as {}",
                    graph.id,
                    status_name_for_child(status)
                ))
            }),
        evidence_refs,
        failure,
        usage,
        finished_at_ms: now_ms(),
    }
}

/// Parent resolution bypasses `TeamSubgraphExecutor::poll_or_await`: the
/// durable supervisor joins an already-terminal child directly. Promote the
/// child terminal fact here so its parent node can satisfy a typed fan-in
/// dependency and cross-Team delivery contract after restart as well.
fn promote_completed_team_terminal_facts(
    usage: &mut harness_contract::execution_graph::ExecutionUsage,
    evidence_refs: &mut Vec<EvidenceAccessRef>,
    child_graph_id: &str,
    child_revision: u64,
    result_ref: Option<&str>,
    session_id: &str,
) {
    let terminal_id = format!(
        "{child_graph_id}:revision:{child_revision}:{}",
        result_ref.unwrap_or("terminal-result-unavailable")
    );
    if !evidence_refs
        .iter()
        .any(|reference| reference.evidence_ref.ref_type == "terminal_synthesis")
    {
        evidence_refs.push(EvidenceAccessRef::durable(
            EvidenceRef::observed("terminal_synthesis", terminal_id.clone()),
            format!("sha256:{:x}", Sha256::digest(terminal_id.as_bytes())),
            terminal_id.len() as u64,
            "application/vnd.cowd.team-terminal+json",
            format!("runtime-event:execution-graph:{child_graph_id}:terminal"),
            format!("session:{session_id}"),
        ));
    }
    if usage.acceptance_evaluation.is_none() {
        usage.acceptance_evaluation = Some(AcceptanceEvaluation {
            evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
            contract_digest: format!("team-child-terminal:{child_graph_id}:{child_revision}"),
            receipt_set_digest: format!("sha256:{:x}", Sha256::digest(terminal_id.as_bytes())),
            derived_obligations: vec![format!("team-child-terminal:{child_graph_id}")],
            verdict: AcceptanceVerdict::Satisfied,
        });
    }
}

const fn status_name_for_child(status: ExecutionNodeStatus) -> &'static str {
    match status {
        ExecutionNodeStatus::Completed => "completed",
        ExecutionNodeStatus::Blocked => "partial",
        ExecutionNodeStatus::Failed => "failed",
        ExecutionNodeStatus::Cancelled => "cancelled",
        _ => "non_terminal",
    }
}

pub(crate) fn child_resolution_correlation(
    parent_execution_id: &str,
    parent_node_id: &str,
    child_execution_id: &str,
    parent_attempt: u32,
    child_revision: u64,
) -> String {
    format!(
        "child:{parent_execution_id}:{parent_node_id}:{child_execution_id}:{parent_attempt}:{child_revision}"
    )
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

#[cfg(test)]
mod dependency_policy_tests {
    use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
    use harness_contract::context::{
        EvidenceAccessRef, EvidenceRef, ObservedAcceptance, RequiredAcceptance,
    };
    use harness_contract::execution_graph::{
        DependencyPredicate, ExecutionFailure, ExecutionUsage,
    };
    use harness_contract::execution_graph::{
        ExecutionDependencyPolicy, ExecutionNodeResult, ExecutionNodeStatus,
    };
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionWorkContract, ExecutionWorkRole, ExecutionWorkRuntimeState,
        ExecutionWorkRuntimeStatus,
    };

    use super::{
        aggregate_team_leaf_usage, autonomous_work_blocks_terminal, dependency_predecessors,
        dependency_target, quorum_tail_cancellations, team_child_terminal_result,
        verified_predecessor_status, DependencyPredecessor,
    };

    fn predecessor(status: ExecutionNodeStatus) -> DependencyPredecessor<'static> {
        DependencyPredecessor::status_only(status)
    }

    #[test]
    fn required_autonomous_work_blocks_terminal_reducer_until_peer_acceptance() {
        let mut graph = ExecutionGraph::new("autonomous work gate");
        let mut work = ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck);
        work.collaboration_work_id = Some("agent-work-a".to_string());
        work.objective = Some("independent cross-check".to_string());
        work.proposed_by = Some("agent-a".to_string());
        work.output_artifact_kinds = vec!["review".to_string()];
        graph
            .autonomous_work
            .insert("agent-work-a".to_string(), work);
        graph.work_states.insert(
            "agent-work-a".to_string(),
            ExecutionWorkRuntimeState {
                status: ExecutionWorkRuntimeStatus::Claimed,
                revision: 2,
                ..ExecutionWorkRuntimeState::default()
            },
        );
        assert!(autonomous_work_blocks_terminal(
            &graph,
            ExecutionNodeKind::Synthesize,
            Some(ExecutionNodeStatus::Ready),
        ));
        graph.work_states.get_mut("agent-work-a").unwrap().status =
            ExecutionWorkRuntimeStatus::Accepted;
        assert!(!autonomous_work_blocks_terminal(
            &graph,
            ExecutionNodeKind::Synthesize,
            Some(ExecutionNodeStatus::Ready),
        ));
    }

    #[test]
    fn any_and_quorum_become_ready_without_waiting_for_every_lane() {
        let running = [
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Running),
            predecessor(ExecutionNodeStatus::Planned),
        ];
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Any {
                    cancel_remaining: true,
                },
                &running,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: true,
                },
                &running,
            ),
            None
        );
        let quorum = [
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Running),
        ];
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: true,
                },
                &quorum,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn impossible_quorum_is_blocked() {
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: false,
                },
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Cancelled),
                ],
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn finally_waits_for_every_lane_terminal_but_never_requires_success() {
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Finally,
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Running),
                ],
            ),
            None
        );
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Finally,
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Cancelled),
                ],
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    fn durable_evidence(ref_type: &str, id: &str) -> EvidenceAccessRef {
        EvidenceAccessRef::durable(
            EvidenceRef::observed(ref_type, id),
            "sha",
            1,
            "text/plain",
            format!("evidence://{id}"),
            "workspace",
        )
    }

    fn framework_failed_result() -> ExecutionNodeResult {
        ExecutionNodeResult {
            status: ExecutionNodeStatus::Failed,
            result_ref: Some("agent-return:run".to_string()),
            summary: Some("partial".to_string()),
            evidence_refs: vec![
                durable_evidence("runtime_change", "receipt-1"),
                durable_evidence("evidence", "proof-1"),
            ],
            failure: Some(ExecutionFailure {
                kind: "agent_backend".to_string(),
                message: "Runtime rejected Agent terminal result: fixture".to_string(),
                retryable: false,
                evidence_refs: Vec::new(),
            }),
            usage: ExecutionUsage {
                required_acceptance: RequiredAcceptance {
                    criteria: vec!["ok".to_string()],
                    evidence_obligations: Vec::new(),
                },
                observed_acceptance: ObservedAcceptance {
                    satisfied_criteria: Vec::new(),
                    observed_evidence: Vec::new(),
                    unresolved_obligation_ids: vec!["ok".to_string()],
                },
                acceptance_evaluation: Some(harness_contract::acceptance::AcceptanceEvaluation {
                    evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
                    contract_digest: "fixture-contract".to_string(),
                    receipt_set_digest: "fixture-receipts".to_string(),
                    derived_obligations: Vec::new(),
                    verdict: AcceptanceVerdict::Unsatisfied,
                }),
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        }
    }

    fn evidence_ready_graph(
        source_status: ExecutionNodeStatus,
        source_result: Option<ExecutionNodeResult>,
        required_fact_kinds: Vec<TerminalFactKind>,
        accepted_execution_statuses: Vec<ExecutionNodeStatus>,
        accepted_acceptance_verdicts: Vec<AcceptanceVerdict>,
        require_committed_effect: bool,
    ) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new("evidence ready reviewer");
        let mut source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        source.id = "source".to_string();
        let mut reviewer =
            ExecutionNodeSpec::new(ExecutionNodeKind::Verify, "verify", "team:fixture");
        reviewer.id = "reviewer".to_string();
        reviewer.work = Some(ExecutionWorkContract {
            role: ExecutionWorkRole::CrossCheck,
            dependency: ExecutionDependencyPolicy::EvidenceReady {
                predicate: DependencyPredicate::EvidenceReady {
                    minimum: 1,
                    required_fact_kinds,
                    accepted_execution_statuses,
                    accepted_acceptance_verdicts,
                    require_committed_effect,
                },
                cancel_remaining: false,
            },
            ..ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck)
        });
        graph.nodes = vec![source, reviewer];
        graph.edges = vec![ExecutionEdge {
            from: "source".to_string(),
            to: "reviewer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        }];
        graph
            .node_statuses
            .insert("source".to_string(), source_status);
        if let Some(result) = source_result {
            graph.node_results.insert("source".to_string(), result);
        }
        graph
    }

    #[test]
    fn evidence_ready_accepts_failed_predecessor_with_durable_facts() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(framework_failed_result()),
            vec![
                TerminalFactKind::CommittedEffect,
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed, ExecutionNodeStatus::Completed],
            vec![
                AcceptanceVerdict::FrameworkInvalid,
                AcceptanceVerdict::Unsatisfied,
            ],
            true,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn evidence_ready_waits_for_non_terminal_predecessor() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
            vec![ExecutionNodeStatus::Completed],
            vec![AcceptanceVerdict::Satisfied],
            false,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            None
        );
    }

    #[test]
    fn evidence_ready_blocks_terminal_predecessor_without_committed_facts() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            None,
            Vec::new(),
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn evidence_ready_cancelled_partial_facts_follow_the_contract_opt_in() {
        let result = ExecutionNodeResult {
            status: ExecutionNodeStatus::Cancelled,
            result_ref: Some("agent-return:cancel".to_string()),
            summary: Some("partial evidence".to_string()),
            evidence_refs: vec![durable_evidence("evidence", "partial-1")],
            failure: None,
            usage: ExecutionUsage {
                required_acceptance: RequiredAcceptance {
                    criteria: vec!["ok".to_string()],
                    evidence_obligations: Vec::new(),
                },
                observed_acceptance: ObservedAcceptance {
                    satisfied_criteria: Vec::new(),
                    observed_evidence: Vec::new(),
                    unresolved_obligation_ids: vec!["ok".to_string()],
                },
                acceptance_evaluation: Some(harness_contract::acceptance::AcceptanceEvaluation {
                    evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
                    contract_digest: "fixture-contract".to_string(),
                    receipt_set_digest: "fixture-receipts".to_string(),
                    derived_obligations: Vec::new(),
                    verdict: AcceptanceVerdict::Unsatisfied,
                }),
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        };
        let opted_in = evidence_ready_graph(
            ExecutionNodeStatus::Cancelled,
            Some(result.clone()),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Cancelled],
            vec![AcceptanceVerdict::Unsatisfied],
            false,
        );
        let predecessors = dependency_predecessors(&opted_in, &opted_in.nodes[1]);
        assert_eq!(
            dependency_target(
                &opted_in.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );

        let excluded = evidence_ready_graph(
            ExecutionNodeStatus::Cancelled,
            Some(result),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Completed],
            vec![AcceptanceVerdict::Unsatisfied],
            false,
        );
        let predecessors = dependency_predecessors(&excluded, &excluded.nodes[1]);
        assert_eq!(
            dependency_target(
                &excluded.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn evidence_ready_committed_effect_is_required_only_when_declared() {
        let mut result = framework_failed_result();
        result
            .evidence_refs
            .retain(|reference| reference.evidence_ref.ref_type != "runtime_change");
        let required = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(result.clone()),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            true,
        );
        let predecessors = dependency_predecessors(&required, &required.nodes[1]);
        assert_eq!(
            dependency_target(
                &required.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );

        let optional = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(result),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let predecessors = dependency_predecessors(&optional, &optional.nodes[1]);
        assert_eq!(
            dependency_target(
                &optional.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn evidence_ready_minimum_keeps_waiting_while_any_lane_can_still_satisfy() {
        let mut graph = evidence_ready_graph(
            ExecutionNodeStatus::Running,
            None,
            vec![
                TerminalFactKind::CommittedEffect,
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let mut second_source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        second_source.id = "second".to_string();
        graph.nodes.insert(1, second_source);
        graph
            .node_statuses
            .insert("second".to_string(), ExecutionNodeStatus::Failed);
        graph
            .node_results
            .insert("second".to_string(), framework_failed_result());
        graph.edges.push(ExecutionEdge {
            from: "second".to_string(),
            to: "reviewer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        let dependency = &mut graph.nodes[2].work.as_mut().unwrap().dependency;
        if let ExecutionDependencyPolicy::EvidenceReady { predicate, .. } = dependency {
            let DependencyPredicate::EvidenceReady { minimum, .. } = predicate;
            *minimum = 2;
        }
        let predecessors = dependency_predecessors(&graph, &graph.nodes[2]);
        assert_eq!(
            dependency_target(
                &graph.nodes[2].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            None
        );
    }

    #[test]
    fn completed_predecessor_without_required_evidence_does_not_satisfy_quorum() {
        let mut graph = ExecutionGraph::new("verified quorum");
        let mut source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        source.id = "source".to_string();
        graph.nodes.push(source);
        graph
            .node_statuses
            .insert("source".to_string(), ExecutionNodeStatus::Completed);
        graph.node_results.insert(
            "source".to_string(),
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("answer".to_string()),
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        );

        assert_eq!(
            verified_predecessor_status(&graph, "source", &["proof".to_string()]),
            ExecutionNodeStatus::Failed
        );
    }

    #[test]
    fn ready_quorum_cancels_only_optional_tail_in_the_same_group() {
        let mut graph = ExecutionGraph::new("cancel redundant evidence lane");
        let mut completed =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "completed");
        completed.id = "completed".to_string();
        let mut completed_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        completed_work.required = false;
        completed_work.cancellation_group = Some("evidence".to_string());
        completed.work = Some(completed_work);
        let mut running = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "running");
        running.id = "running".to_string();
        let mut running_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        running_work.required = false;
        running_work.cancellation_group = Some("evidence".to_string());
        running.work = Some(running_work);
        let mut merge = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "merge");
        merge.id = "merge".to_string();
        let mut merge_work = ExecutionWorkContract::new(ExecutionWorkRole::Synthesize);
        merge_work.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: true,
        };
        merge_work.cancellation_group = Some("evidence".to_string());
        merge.work = Some(merge_work);
        graph.nodes = vec![completed, running, merge];
        graph.edges = vec![
            ExecutionEdge {
                from: "completed".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "running".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];
        graph
            .node_statuses
            .insert("completed".to_string(), ExecutionNodeStatus::Completed);
        graph
            .node_statuses
            .insert("running".to_string(), ExecutionNodeStatus::Running);
        graph
            .node_statuses
            .insert("merge".to_string(), ExecutionNodeStatus::Ready);
        assert_eq!(quorum_tail_cancellations(&graph), vec!["running"]);
    }

    #[test]
    fn child_terminal_preserves_failed_leaf_usage_without_counting_synthesis_twice() {
        let mut graph = ExecutionGraph::new("child terminal aggregation");
        graph.id = "team-graph:usage".to_string();
        let mut agent = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        agent.id = "agent".to_string();
        let mut synth = ExecutionNodeSpec::new(ExecutionNodeKind::Synthesize, "synth", "{}");
        synth.id = "synth".to_string();
        graph.nodes = vec![agent, synth];
        graph
            .node_statuses
            .insert("agent".to_string(), ExecutionNodeStatus::Failed);
        graph
            .node_statuses
            .insert("synth".to_string(), ExecutionNodeStatus::Cancelled);
        let mut leaf_usage = harness_contract::execution_graph::ExecutionUsage::default();
        leaf_usage.input_tokens = 13;
        leaf_usage.output_tokens = 8;
        leaf_usage.tool_calls = 2;
        let mut synth_usage = harness_contract::execution_graph::ExecutionUsage::default();
        synth_usage.input_tokens = 13;
        synth_usage.output_tokens = 8;
        graph.node_results.insert(
            "agent".to_string(),
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                result_ref: Some("artifact://partial".to_string()),
                summary: Some("partial evidence".to_string()),
                evidence_refs: Vec::new(),
                failure: Some(harness_contract::execution_graph::ExecutionFailure {
                    kind: "provider_failure".to_string(),
                    message: "fixture".to_string(),
                    retryable: false,
                    evidence_refs: Vec::new(),
                }),
                usage: leaf_usage,
                finished_at_ms: 1,
            },
        );
        graph.node_results.insert(
            "synth".to_string(),
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Cancelled,
                result_ref: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: synth_usage,
                finished_at_ms: 2,
            },
        );

        let aggregate = aggregate_team_leaf_usage(&graph);
        assert_eq!(aggregate.input_tokens, 13);
        assert_eq!(aggregate.output_tokens, 8);
        assert_eq!(aggregate.tool_calls, 2);
        let terminal = team_child_terminal_result(&graph);
        assert_eq!(terminal.status, ExecutionNodeStatus::Failed);
        assert_eq!(terminal.usage, aggregate);
        assert_eq!(terminal.failure.unwrap().kind, "provider_failure");
    }

    #[test]
    fn completed_child_terminal_promotes_typed_handoff_facts() {
        let mut graph = ExecutionGraph::new("completed child");
        graph.id = "team-graph:alpha".to_string();
        graph.revision = 7;
        let mut synth = ExecutionNodeSpec::new(ExecutionNodeKind::Synthesize, "synth", "{}");
        synth.id = "synth".to_string();
        graph.nodes = vec![synth];
        graph
            .node_statuses
            .insert("synth".to_string(), ExecutionNodeStatus::Completed);
        graph.node_results.insert(
            "synth".to_string(),
            ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("assistant_json:\"verified\"".to_string()),
                summary: Some("verified".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: ExecutionUsage::default(),
                finished_at_ms: 1,
            },
        );

        let terminal = team_child_terminal_result(&graph);

        assert_eq!(terminal.status, ExecutionNodeStatus::Completed);
        assert_eq!(
            terminal
                .usage
                .acceptance_evaluation
                .as_ref()
                .map(|evaluation| evaluation.verdict),
            Some(AcceptanceVerdict::Satisfied)
        );
        assert!(terminal.evidence_refs.iter().any(|reference| {
            reference.evidence_ref.ref_type == "terminal_synthesis" && reference.is_durable()
        }));
    }
}
