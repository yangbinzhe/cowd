use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Duration;

use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionGraph, ExecutionGraphCommand, ExecutionGraphValidationError,
    ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{futures::OwnedNotified, oneshot, Mutex, Notify, OwnedMutexGuard};

use super::commit_service::{
    CallbackDisposition, ExecutionCommitError, ExecutionCommitService, ExecutionEffectState,
    ExecutionTerminalCommit,
};
use super::events::{ExecutionBindingLeaseClosure, ExecutionNodeBinding};
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
use super::PHYSICAL_CLOSURE_BUDGET;

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
    #[error("unadmitted execution graph `{graph_id}` changed during resource preparation")]
    AdmissionRaced { graph_id: String },
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
    pub(crate) binding: Option<ExecutionNodeBinding>,
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
    _resources: NodeResourceGuards,
    callback_gate: Arc<Mutex<()>>,
    no_dispatch: Option<Arc<super::executors::agent_tool::NoDispatchObservation>>,
}

struct PendingTerminalCommit {
    transition: ExecutionTerminalCommit,
    expected_binding: Option<ExecutionNodeBinding>,
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
    original_binding: Option<ExecutionNodeBinding>,
    admission_pending: bool,
    release_blocked: bool,
    callback_completed: bool,
    drain_completed: bool,
    owner_fence: Option<super::resources::BindingOwnerFence>,
}

impl NodeResourceGuards {
    fn binding(&self, ticket: &NodeExecutionTicket) -> ExecutionNodeBinding {
        if let Some(binding) = &self.original_binding {
            return binding.clone();
        }
        ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: ticket.attempt,
            resource_lease_refs: self
                .resource
                .as_ref()
                .map(|resource| vec![resource.projection_reference()])
                .unwrap_or_default(),
            scope_lease_ref: self
                .scope
                .as_ref()
                .map(|lease| lease.projection_reference()),
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
        if !self.admission_pending {
            self.release_worktree();
        }
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

    pub(crate) async fn confirm_cancelled_bound_child_never_admitted(
        &self,
        expected: &ExecutionGraph,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(&expected.id).await;
        if self
            .active
            .lock()
            .await
            .keys()
            .any(|(graph_id, _)| graph_id == &expected.id)
        {
            return Err(ExecutionRunnerError::AdmissionRaced {
                graph_id: expected.id.clone(),
            });
        }
        let service = self.commit_service.clone();
        let expected = expected.clone();
        tokio::task::spawn_blocking(move || {
            service.confirm_cancelled_bound_child_never_admitted(&expected)
        })
        .await
        .map_err(|error| ExecutionRunnerError::Join(error.to_string()))??;
        Ok(())
    }

    pub(crate) async fn drain_physical_execution(&self) -> Vec<String> {
        self.registry.drain_physical_execution().await
    }

    pub(crate) async fn recover_graph(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        let retained = self
            .active
            .lock()
            .await
            .iter()
            .filter(|((active_graph, _), _)| active_graph == graph_id)
            .map(|((_, node_id), active)| {
                (node_id.clone(), active._resources.binding(&active.ticket))
            })
            .collect::<Vec<_>>();
        let graph = self.state_store.load_async(graph_id).await?;
        for (node_id, binding) in &retained {
            if self
                .commit_service
                .binding_callback_disposition(&graph, node_id, binding)?
                == CallbackDisposition::Pending
            {
                return Err(ExecutionRecoveryError::LeaseReconciliation(
                    "original in-process execution owner is live".into(),
                ));
            }
        }
        for (node_id, binding) in retained {
            self.take_bound_active_node(graph_id, &node_id, Some(&binding))
                .await
                .map_err(|error| ExecutionRecoveryError::LeaseReconciliation(error.to_string()))?;
        }
        if self
            .active
            .lock()
            .await
            .keys()
            .any(|(active_graph, _)| active_graph == graph_id)
        {
            return Err(ExecutionRecoveryError::LeaseReconciliation(
                "original in-process execution owner remains retained".into(),
            ));
        }
        ExecutionGraphRecovery::new(&self.state_store, &self.commit_service, &self.registry)
            .recover_with_leases(
                graph_id,
                &self.scope_locks,
                &self.worktree_leases,
                &self.workspace_id,
            )
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
        self.register_continuation(graph, None).await
    }

    pub(crate) async fn register_continuation(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        validate_execution_graph(&graph)?;
        self.registry.validate_graph(&graph)?;
        match self
            .commit_service
            .register_continuation_graph_async(graph, actor)
            .await
        {
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
            let prepared = match self.prepare_ready_wave(graph_id, ready).await {
                Ok(prepared) => prepared,
                Err(ExecutionRunnerError::ResourceDeferred { .. }) if active_nodes.is_empty() => {
                    self.wait_for_resource_change().await;
                    continue;
                }
                Err(ExecutionRunnerError::ResourceDeferred { .. }) => Vec::new(),
                Err(error) => return Err(error),
            };
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
        let mut ready = Vec::new();
        for node in &graph.nodes {
            if graph.node_statuses[&node.id] == ExecutionNodeStatus::Ready
                && !in_flight.contains(&node.id)
                && self.dependency_publications_ready(&graph, node)?
            {
                ready.push(node.clone());
            }
        }
        Ok(ExecutionGraphPumpSnapshot {
            ready,
            report: report(&graph),
        })
    }

    /// A terminal node commit is itself the durable wake-up boundary. Reduce
    /// dependencies before executor cleanup so a lost process-local notify or
    /// a slow `after_commit` hook cannot strand Verify/Synthesize behind an
    /// already-terminal Agent wave. This also commits typed orphan failure as
    /// soon as every Agent is terminal and required autonomous work remains.
    async fn converge_after_terminal_commit(
        &self,
        graph_id: &str,
    ) -> Result<(), ExecutionRunnerError> {
        let graph = self.state_store.load_async(graph_id).await?;
        self.advance_dependencies(graph).await?;
        Ok(())
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
        let graph = self.state_store.load_current_async(graph_id).await?;
        Ok(graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
            .then(|| report(&graph)))
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
        for _retry in 0..8 {
            match self.prepare_ready_wave_once(graph_id, nodes.clone()).await {
                Err(ExecutionRunnerError::AdmissionRaced { .. }) => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(ExecutionRunnerError::ResourceDeferred {
            node_id: graph_id.into(),
            reason: "fresh unadmitted graph changed during bounded preparation".into(),
        })
    }

    async fn prepare_ready_wave_once(
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
                    binding: None,
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
            if graph.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Ready)
                || !self.dependency_publications_ready(&graph, &node)?
            {
                continue;
            }
            if self
                .active
                .lock()
                .await
                .contains_key(&(graph_id.to_string(), node.id.clone()))
            {
                output.push(ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(ExecutionRunnerError::ResourceDeferred {
                        node_id: node.id.clone(),
                        reason: "original physical/lease owner still retained".into(),
                    }),
                    node,
                });
                continue;
            }
            let deadline_at_ms = match execution_deadline_at_ms(&node) {
                Ok(value) => value,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        binding: None,
                        node,
                        startup_error: Some(error),
                    });
                    continue;
                }
            };
            if deadline_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
                output.push(ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(ExecutionRunnerError::DeadlineExceeded {
                        node_id: node.id.clone(),
                        deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                    }),
                    node,
                });
                continue;
            }
            let mut resources = match self
                .acquire_node_resources(graph.as_ref(), &node, deadline_at_ms, Some(Duration::ZERO))
                .await
            {
                Ok(resources) => resources,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        binding: None,
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
                    binding: None,
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
            let attempt = match ExecutionNodeBinding::next_attempt(&graph, &node.id) {
                Ok(attempt) => attempt,
                Err(reason) => {
                    self.record_resource_terminal(
                        resource_kind.as_ref(),
                        resource_queue_wait,
                        resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                    self.discard_unpolled_wave(&prepared);
                    return Err(NodeExecutorError::Invalid {
                        node_id: node.id,
                        reason,
                    }
                    .into());
                }
            };
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
                    self.discard_unpolled_wave(&prepared);
                    return Err(error.into());
                }
            };
            if let Err(reason) =
                ExecutionNodeBinding::validate_ticket(&ticket, &graph, &node, attempt)
            {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                self.discard_unpolled_wave(&prepared);
                return Err(NodeExecutorError::Invalid {
                    node_id: node.id,
                    reason,
                }
                .into());
            }
            let leaf_effect_owner = node.kind
                == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.executor_kind == "tool_batch";
            let effect_state = match self
                .commit_service
                .validate_fresh_node_admission(&graph, &node, &ticket)
            {
                Ok(()) => ExecutionEffectState::Fresh,
                Err(error) => {
                    self.record_resource_terminal(
                        resource_kind.as_ref(),
                        resource_queue_wait,
                        resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                    self.discard_unpolled_wave(&prepared);
                    return Err(error.into());
                }
            };
            let effect_receipt_required =
                !leaf_effect_owner && matches!(effect_state, ExecutionEffectState::Fresh);
            let binding = resources.binding(&ticket);
            if ExecutionBindingLeaseClosure::required(&binding) {
                let key = ExecutionBindingLeaseClosure::fence_key(&graph, &node.id, &binding);
                resources.original_binding = Some(binding.clone());
                let mut admission_error = None;
                let owner = self.scope_locks.acquire_binding_owner(&key, || {
                    self.commit_service
                        .validate_unadmitted_binding_start(&graph, &node, &ticket)
                        .map_err(|error| {
                            let reason = error.to_string();
                            admission_error = Some(error);
                            reason
                        })
                });
                resources.owner_fence = Some(match owner {
                    Ok(owner) => owner,
                    Err(error) => {
                        self.discard_unpolled_wave(&prepared);
                        if matches!(
                            admission_error.as_ref(),
                            Some(ExecutionCommitError::EventStore(
                                crate::RuntimeEventStoreError::StaleRevision { .. }
                            ))
                        ) {
                            return Err(ExecutionRunnerError::AdmissionRaced {
                                graph_id: graph_id.into(),
                            });
                        }
                        if let Some(error) = admission_error {
                            return Err(error.into());
                        }
                        return Err(ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        });
                    }
                });
            }
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
            let mut observations = Vec::with_capacity(admitted.len());
            for start in &admitted {
                match self.registry.observe_builtin_binding(&start.ticket) {
                    Ok(observation) => observations.push(observation),
                    Err(error) => {
                        for (original, observation) in admitted.iter().zip(&observations) {
                            if let Some(observation) = observation {
                                self.registry.rollback_unadmitted_builtin_binding(
                                    &original.ticket,
                                    observation,
                                    &self.commit_service,
                                )?;
                            }
                        }
                        self.discard_unpolled_wave(&admitted);
                        return Err(error.into());
                    }
                }
            }
            {
                let mut active = self.active.lock().await;
                for (mut start, no_dispatch) in admitted.drain(..).zip(observations) {
                    start.resources.admission_pending = true;
                    output.push(ExecutionPreparedPumpNode {
                        binding: bindings.get(&start.node.id).cloned(),
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
                            _resources: start.resources,
                            callback_gate: Arc::new(Mutex::new(())),
                            no_dispatch,
                        },
                    );
                }
            }
            if let Err(error) = self
                .commit_service
                .bind_and_start_nodes_async(current.as_ref().clone(), bindings, effect_tickets)
                .await
            {
                let starts = self
                    .active
                    .lock()
                    .await
                    .iter()
                    .filter(|((id, node), _)| {
                        id == graph_id
                            && output
                                .iter()
                                .any(|item| item.startup_error.is_none() && &item.node.id == node)
                    })
                    .map(|(_, active)| {
                        (
                            active.ticket.clone(),
                            active._resources.binding(&active.ticket),
                        )
                    })
                    .collect::<Vec<_>>();
                let states = starts
                    .iter()
                    .map(|(ticket, binding)| {
                        self.commit_service
                            .binding_start_was_admitted(graph_id, ticket, binding)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if states.is_empty()
                    || states.iter().any(|value| *value) && states.iter().any(|value| !*value)
                {
                    return Err(ExecutionRunnerError::Driver("atomic start response has inconsistent canonical admission; original owners retained".into()));
                }
                if states.iter().all(|value| !*value) {
                    let mut active = self.active.lock().await;
                    for (ticket, binding) in &starts {
                        let original = active
                            .get(&(graph_id.to_string(), ticket.node_id.clone()))
                            .filter(|original| {
                                original.ticket == *ticket
                                    && original._resources.binding(ticket) == *binding
                            })
                            .ok_or_else(|| {
                                ExecutionRunnerError::Driver(
                                    "unadmitted rollback original owner changed".into(),
                                )
                            })?;
                        if let Some(observation) = original.no_dispatch.as_ref() {
                            self.registry.rollback_unadmitted_builtin_binding(
                                ticket,
                                observation,
                                &self.commit_service,
                            )?;
                        }
                    }
                    let aborted = starts
                        .iter()
                        .filter_map(|(ticket, _)| {
                            active.remove(&(graph_id.to_string(), ticket.node_id.clone()))
                        })
                        .collect::<Vec<_>>();
                    drop(active);
                    drop(coordination);
                    for mut start in aborted {
                        start._resources.admission_pending = false;
                        self.record_resource_terminal(
                            start.resource_kind.as_ref(),
                            start.resource_queue_wait,
                            start.resource_started,
                            crate::execution_core::graph::ResourceResultClass::Failed,
                        );
                    }
                    if matches!(&error, ExecutionCommitError::EventStore(
                        crate::RuntimeEventStoreError::StaleRevision { stream_id, .. }
                    ) if stream_id == graph_id)
                    {
                        return Err(ExecutionRunnerError::AdmissionRaced {
                            graph_id: graph_id.into(),
                        });
                    }
                    return Err(error.into());
                }
            }
        }
        drop(coordination);
        for start in superseded {
            let _ = start
                .executor
                .cancel_bounded(&start.ticket, PHYSICAL_CLOSURE_BUDGET)
                .await;
            self.record_resource_terminal(
                start.resource_kind.as_ref(),
                start.resource_queue_wait,
                start.resource_started,
                crate::execution_core::graph::ResourceResultClass::Cancelled,
            );
            output.push(ExecutionPreparedPumpNode {
                binding: Some(start.resources.binding(&start.ticket)),
                startup_error: Some(ExecutionRunnerError::CommandSuperseded {
                    node_id: start.node.id.clone(),
                }),
                node: start.node,
            });
        }
        Ok(output)
    }

    fn discard_unpolled_wave(&self, prepared: &[PreparedNodeStart]) {
        for start in prepared {
            self.record_resource_terminal(
                start.resource_kind.as_ref(),
                start.resource_queue_wait,
                start.resource_started,
                crate::execution_core::graph::ResourceResultClass::Failed,
            );
        }
    }

    pub(crate) async fn execute_prepared_pump_node(
        &self,
        graph_id: &str,
        prepared: ExecutionPreparedPumpNode,
        durable_progress: Arc<Notify>,
    ) -> Result<(), ExecutionRunnerError> {
        let expected_binding = prepared.binding.clone();
        let node_id = prepared.node.id.clone();
        let result = match prepared.startup_error {
            Some(error) => Err(error),
            None => {
                self.start_and_execute_node(graph_id, prepared.node, expected_binding.as_ref())
                    .await
            }
        };
        if !self
            .active_binding_matches(graph_id, &node_id, expected_binding.as_ref())
            .await
        {
            return Ok(());
        }
        self.finish_pump_node(graph_id, result, durable_progress, expected_binding)
            .await
    }

    #[cfg(test)]
    async fn drop_injected_empty_bound_owner(
        &self,
        graph_id: &str,
        node: &str,
        binding: Option<&ExecutionNodeBinding>,
        error: &ExecutionCommitError,
    ) -> Result<bool, ExecutionRunnerError> {
        if !matches!(error,ExecutionCommitError::InvalidCommand(reason) if reason=="450 original leaf completed before Graph result append fault")
        {
            return Ok(false);
        }
        let graph = self.state_store.load_async(graph_id).await?;
        if !super::executors::agent_tool::is_bound_agent_tool_graph(&graph)
            || graph.node_statuses.get(node) != Some(&ExecutionNodeStatus::Running)
        {
            return Err(ExecutionRunnerError::Driver(
                "test crash injection lost original bound Running identity".into(),
            ));
        }
        let mut active = self.active.lock().await;
        let key = (graph_id.to_string(), node.to_string());
        let Some(owner) = active.get(&key) else {
            return Ok(false);
        };
        let actual = owner._resources.binding(&owner.ticket);
        if Some(&actual) != binding
            || !actual.resource_lease_refs.is_empty()
            || actual.scope_lease_ref.is_some()
            || actual.worktree_lease_ref.is_some()
            || owner._resources.callback_completed
        {
            return Err(ExecutionRunnerError::Driver(
                "test crash injection cannot drop nonempty or closed owner".into(),
            ));
        }
        drop(active.remove(&key));
        Ok(true)
    }

    async fn active_binding_matches(
        &self,
        graph_id: &str,
        node_id: &str,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> bool {
        self.active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.to_string()))
            .map(|active| active._resources.binding(&active.ticket))
            .as_ref()
            == expected_binding
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn active_binding_for_ticket_for_test(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<ExecutionNodeBinding> {
        self.active
            .lock()
            .await
            .get(&(ticket.graph_id.clone(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| active._resources.binding(&active.ticket))
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn reject_stale_binding_for_test(
        &self,
        graph: &str,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<(), ExecutionRunnerError> {
        self.isolate_node_failure(
            graph,
            node,
            Some(binding),
            "late recovery owner probe".into(),
        )
        .await
    }

    async fn binding_callback_gate(
        &self,
        graph_id: &str,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<Mutex<()>>> {
        self.active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| active.callback_gate.clone())
    }

    async fn cancel_and_drain_locked(
        &self,
        graph_id: &str,
        executor: &Arc<dyn NodeExecutor>,
        ticket: &NodeExecutionTicket,
    ) -> Result<(), ExecutionRunnerError> {
        if self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .is_some_and(|active| active.ticket == *ticket && active._resources.drain_completed)
        {
            return Ok(());
        }
        if executor.cancellation_requires_quiescence(ticket) {
            executor
                .cancel_bounded(ticket, PHYSICAL_CLOSURE_BUDGET)
                .await?;
        } else {
            tokio::time::timeout(
                PHYSICAL_CLOSURE_BUDGET,
                executor.cancel_bounded(ticket, PHYSICAL_CLOSURE_BUDGET),
            )
            .await
            .map_err(|_| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: "original cancellation budget expired; drain unconfirmed".into(),
            })??;
        }
        if executor.cancellation_requires_quiescence(ticket) {
            use futures::FutureExt;
            match executor.cancel(ticket).now_or_never() {
                Some(Ok(())) => {}
                Some(Err(error)) => return Err(error.into()),
                None => {
                    return Err(NodeExecutorError::Cancel {
                        node_id: ticket.node_id.clone(),
                        reason: "bounded cancellation returned without physical quiescence".into(),
                    }
                    .into())
                }
            }
        }
        if let Some(active) = self
            .active
            .lock()
            .await
            .get_mut(&(graph_id.to_string(), ticket.node_id.clone()))
        {
            if active.ticket == *ticket {
                active._resources.drain_completed = true;
            }
        }
        Ok(())
    }

    async fn active_cancel_observed(
        &self,
        graph_id: &str,
        executor: &Arc<dyn NodeExecutor>,
        ticket: &NodeExecutionTicket,
    ) -> Result<(), ExecutionRunnerError> {
        let Some(gate) = self.binding_callback_gate(graph_id, ticket).await else {
            return Ok(());
        };
        let _owner = gate.lock().await;
        let graph = self.state_store.load_async(graph_id).await?;
        let binding = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| active._resources.binding(ticket));
        let Some(binding) = binding else {
            return Ok(());
        };
        if self
            .commit_service
            .binding_callback_disposition(&graph, &ticket.node_id, &binding)?
            == CallbackDisposition::Commit
        {
            return self
                .complete_callback_locked(graph_id, executor, ticket, None)
                .await;
        }
        self.cancel_and_drain_locked(graph_id, executor, ticket)
            .await
    }

    async fn complete_callback_locked(
        &self,
        graph_id: &str,
        executor: &Arc<dyn NodeExecutor>,
        ticket: &NodeExecutionTicket,
        requested_abort: Option<&str>,
    ) -> Result<(), ExecutionRunnerError> {
        let state = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| {
                (
                    active._resources.binding(ticket),
                    active._resources.callback_completed,
                    active.no_dispatch.clone(),
                )
            });
        let Some((binding, completed, observation)) = state else {
            return Ok(());
        };
        if completed {
            return Ok(());
        }
        let graph = self.state_store.load_async(graph_id).await?;
        let disposition =
            self.commit_service
                .binding_callback_disposition(&graph, &ticket.node_id, &binding)?;
        if disposition == CallbackDisposition::Pending {
            if requested_abort.is_some() {
                return Ok(());
            }
            return Err(ExecutionRunnerError::Driver(
                "callback has no canonical original outcome or retirement".into(),
            ));
        };
        let result = if disposition == CallbackDisposition::Abort {
            self.cancel_and_drain_locked(graph_id, executor, ticket)
                .await?;
            executor
                .after_abort(
                    ticket,
                    requested_abort.unwrap_or("canonical_binding_retirement"),
                )
                .await
        } else {
            run_after_commit_observed(executor, ticket).await
        };
        let result = match result {
            Ok(()) => {
                let mut changes = self.state_store.subscribe_commits();
                let observed = tokio::time::timeout(PHYSICAL_CLOSURE_BUDGET, async {
                    loop {
                        let current = self.state_store.load_async(graph_id).await?;
                        if self
                            .commit_service
                            .binding_physical_sources_with_witness(
                                &current,
                                &ticket.node_id,
                                &binding,
                                observation
                                    .as_ref()
                                    .and_then(|observation| observation.witness())
                                    .as_ref(),
                            )?
                            .is_some()
                        {
                            return Ok::<(), ExecutionRunnerError>(());
                        }
                        changes.changed().await.map_err(|_| {
                            ExecutionRunnerError::Driver(
                                "physical source stream closed before quiescence".into(),
                            )
                        })?;
                    }
                })
                .await
                .map_err(|_| {
                    // A physical closure that cannot be proven must name its own
                    // missing proof in production, not only inside the test
                    // harness: an unproven closure otherwise surfaces as one
                    // opaque terminal and every operator has to reconstruct the
                    // cause from the event store after the fact.
                    let diagnostic = self.commit_service.physical_closure_diagnostic(
                        &graph,
                        &ticket.node_id,
                        &binding,
                    );
                    #[cfg(test)]
                    eprintln!("OWN834_PHYSICAL_CLOSURE_UNPROVED {diagnostic}");
                    tracing::warn!(
                        OWN834_PHYSICAL_CLOSURE_UNPROVED = %diagnostic,
                        graph_id = %graph_id,
                        node_id = %ticket.node_id,
                        attempt = binding.attempt,
                        budget_seconds = PHYSICAL_CLOSURE_BUDGET.as_secs(),
                        "physical closure unconfirmed within original cancellation budget"
                    );
                    ExecutionRunnerError::Driver(
                        "physical closure unconfirmed within original cancellation budget".into(),
                    )
                })?;
                observed
            }
            Err(error) => Err(error.into()),
        };
        if let Some(active) = self
            .active
            .lock()
            .await
            .get_mut(&(graph_id.to_string(), ticket.node_id.clone()))
        {
            if active.ticket == *ticket {
                active._resources.callback_completed = result.is_ok();
                active._resources.release_blocked = result.is_err();
            }
        }
        result
    }

    async fn complete_callback_observed(
        &self,
        graph_id: &str,
        executor: &Arc<dyn NodeExecutor>,
        ticket: &NodeExecutionTicket,
        requested_abort: Option<&str>,
    ) -> Result<(), ExecutionRunnerError> {
        let Some(gate) = self.binding_callback_gate(graph_id, ticket).await else {
            return Ok(());
        };
        let _owner = gate.lock().await;
        self.complete_callback_locked(graph_id, executor, ticket, requested_abort)
            .await
    }

    fn close_active_binding_leases(
        &self,
        graph_id: &str,
        node_id: &str,
        finished: &mut ActiveNode,
    ) -> Result<(), ExecutionRunnerError> {
        if finished._resources.release_blocked || !finished._resources.callback_completed {
            return Err(ExecutionRunnerError::Driver(
                "original callback has not positively completed; binding release blocked".into(),
            ));
        }
        let binding = finished._resources.binding(&finished.ticket);
        if !ExecutionBindingLeaseClosure::required(&binding) {
            return Ok(());
        }
        for retry in 0..8 {
            let graph = self.state_store.load(graph_id)?;
            let position = self
                .state_store
                .binding_start_position(&graph, node_id, &binding)?
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver("original lease start missing".into())
                })?;
            let resources = &mut finished._resources;
            let owner = resources.owner_fence.as_mut().ok_or_else(|| {
                ExecutionRunnerError::Driver("original binding owner capability missing".into())
            })?;
            let released = owner
                .release_original(
                    &graph,
                    node_id,
                    &binding,
                    position,
                    &mut resources.resource,
                    &mut resources.scope,
                    &mut resources.worktree,
                    &self.worktree_leases,
                )
                .map_err(ExecutionRunnerError::Driver)?;
            if self
                .state_store
                .binding_lease_closure(&graph, node_id, &binding, position)?
            {
                self.registry.forget_builtin_binding(&finished.ticket);
                return Ok(());
            }
            match self
                .commit_service
                .commit_binding_lease_closure_with_witness(
                    &graph,
                    &released,
                    finished
                        .no_dispatch
                        .as_ref()
                        .and_then(|observation| observation.witness())
                        .as_ref(),
                ) {
                Ok(_) => {
                    self.registry.forget_builtin_binding(&finished.ticket);
                    return Ok(());
                }
                Err(ExecutionCommitError::EventStore(
                    crate::RuntimeEventStoreError::StaleRevision { .. },
                )) if retry < 7 => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(ExecutionRunnerError::Driver(
            "lease release commit retry exhausted".into(),
        ))
    }

    async fn take_bound_active_node(
        &self,
        graph_id: &str,
        node_id: &str,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<Option<ActiveNode>, ExecutionRunnerError> {
        let key = (graph_id.to_string(), node_id.to_string());
        let owner = self
            .active
            .lock()
            .await
            .get(&key)
            .filter(|active| {
                Some(active._resources.binding(&active.ticket)).as_ref() == expected_binding
            })
            .map(|active| {
                (
                    active.callback_gate.clone(),
                    active.executor.clone(),
                    active.ticket.clone(),
                )
            });
        let Some((gate, executor, ticket)) = owner else {
            return Ok(None);
        };
        let _owner = gate.lock().await;
        self.complete_callback_locked(graph_id, &executor, &ticket, None)
            .await?;
        let _coordination = self.graph_coordination(graph_id).await;
        let mut active = self.active.lock().await;
        if active
            .get(&key)
            .map(|active| active._resources.binding(&active.ticket))
            .as_ref()
            != expected_binding
        {
            return Ok(None);
        }
        let Some(finished) = active.get_mut(&key) else {
            return Ok(None);
        };
        self.close_active_binding_leases(graph_id, node_id, finished)?;
        drop(finished._resources.owner_fence.take());
        Ok(active.remove(&key))
    }

    async fn commit_terminal_wave(
        &self,
        graph_id: &str,
        transition: ExecutionTerminalCommit,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<TerminalCommitDisposition, ExecutionRunnerError> {
        let (response, receiver) = oneshot::channel();
        let leader = {
            let mut batches = self.terminal_batches.lock().await;
            let batch = batches.entry(graph_id.to_string()).or_default();
            batch.pending.push(PendingTerminalCommit {
                transition,
                expected_binding: expected_binding.cloned(),
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
                    let active = self.active.lock().await;
                    for pending in pending {
                        if current.node_statuses.get(&pending.transition.node_id)
                            == Some(&ExecutionNodeStatus::Running)
                            && pending.expected_binding.as_ref().is_some_and(|binding| {
                                current
                                    .recovery_cursor
                                    .node_attempts
                                    .get(&pending.transition.node_id)
                                    == Some(&binding.attempt)
                                    && active
                                        .get(&(
                                            graph_id.to_string(),
                                            pending.transition.node_id.clone(),
                                        ))
                                        .is_some_and(|active| {
                                            active._resources.binding(&active.ticket) == *binding
                                        })
                            })
                        {
                            admitted.push(pending.transition);
                            admitted_responses.push(pending.response);
                        } else {
                            superseded_responses.push(pending.response);
                        }
                    }
                    drop(active);
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
                    drop(coordination);
                    match commit {
                        Ok(()) => {
                            // One durable terminal wave has one convergence
                            // owner. Running this in every sibling waiter
                            // turns an O(1) atomic commit into O(width) graph
                            // reloads and serial dependency reductions.
                            let convergence = if admitted_responses.is_empty() {
                                Ok(())
                            } else {
                                self.converge_after_terminal_commit(graph_id).await
                            };
                            if let Err(error) = convergence {
                                let reason = error.to_string();
                                for response in admitted_responses {
                                    let _ = response
                                        .send(TerminalCommitDisposition::Failed(reason.clone()));
                                }
                                return receiver.await.map_err(|_| {
                                    ExecutionRunnerError::Driver(
                                        "terminal wave convergence failed before returning a disposition"
                                            .to_string(),
                                    )
                                });
                            }
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
                    drop(coordination);
                    let reason = error.to_string();
                    for pending in pending {
                        let _ = pending
                            .response
                            .send(TerminalCommitDisposition::Failed(reason.clone()));
                    }
                }
            }
        }
        receiver.await.map_err(|_| {
            ExecutionRunnerError::Driver(
                "terminal wave coordinator closed before returning a disposition".to_string(),
            )
        })
    }

    #[cfg(test)]
    pub(super) async fn pending_terminal_count(&self, graph_id: &str) -> usize {
        self.terminal_batches
            .lock()
            .await
            .get(graph_id)
            .map_or(0, |batch| batch.pending.len())
    }

    async fn finish_pump_node(
        &self,
        graph_id: &str,
        result: Result<(String, NodeExecutionOutcome), ExecutionRunnerError>,
        durable_progress: Arc<Notify>,
        expected_binding: Option<ExecutionNodeBinding>,
    ) -> Result<(), ExecutionRunnerError> {
        let Some((node_id, outcome)) = self
            .resolve_pump_result(
                graph_id,
                result,
                &durable_progress,
                expected_binding.as_ref(),
            )
            .await?
        else {
            return Ok(());
        };
        let deadline_terminal = outcome
            .result
            .failure
            .as_ref()
            .is_some_and(|failure| failure.kind == "execution_deadline_exceeded");
        if let Err(error) = validate_outcome(&node_id, &outcome) {
            self.isolate_node_failure(
                graph_id,
                &node_id,
                expected_binding.as_ref(),
                error.to_string(),
            )
            .await?;
            return Err(error);
        }
        if let Some(replan) = outcome.replan.as_ref() {
            if let Err(error) = self.registry.validate_nodes(&replan.nodes) {
                self.isolate_node_failure(
                    graph_id,
                    &node_id,
                    expected_binding.as_ref(),
                    error.to_string(),
                )
                .await?;
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
            .filter(|active| {
                Some(active._resources.binding(&active.ticket)).as_ref()
                    == expected_binding.as_ref()
            })
            .map(|active| {
                (
                    Arc::clone(&active.executor),
                    active.ticket.clone(),
                    active.effect_receipt_required,
                )
            });
        if active_wave.is_none() {
            return Ok(());
        }
        let batchable = outcome.replan.is_none()
            && outcome.delivery_envelope.is_none()
            && outcome.terminal_presentation.is_none();
        if batchable {
            let Some((executor, ticket, effect_receipt_required)) = active_wave.clone() else {
                return Err(ExecutionRunnerError::Driver(
                    "batchable active wave has no execution binding".to_string(),
                ));
            };
            let disposition = self
                .commit_terminal_wave(
                    graph_id,
                    ExecutionTerminalCommit {
                        node_id: node_id.clone(),
                        outcome,
                        effect_ticket: effect_receipt_required.then_some(ticket.clone()),
                    },
                    expected_binding.as_ref(),
                )
                .await?;
            match disposition {
                TerminalCommitDisposition::Committed => {
                    durable_progress.notify_one();
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        deadline_terminal.then_some("canonical_deadline_after_atomic_commit"),
                    )
                    .await?;
                    if deadline_terminal {
                        executor.cancellation_finalized(&ticket);
                    }
                    self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                        .await?;
                    return Ok(());
                }
                TerminalCommitDisposition::Superseded => {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_command_superseded_before_terminal_wave"),
                    )
                    .await?;
                    self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                        .await?;
                    return Ok(());
                }
                TerminalCommitDisposition::Failed(reason) => {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("terminal_wave_commit_failed"),
                    )
                    .await?;
                    self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                        .await?;
                    return Err(ExecutionRunnerError::Driver(reason));
                }
            }
        }

        // Commit terminal graph truth before waking the pump or publishing
        // process-local executor output.
        let committed_executor = {
            let _coordination = self.graph_coordination_without_command(graph_id).await;
            let mut current = self.state_store.load_async(graph_id).await?;
            if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                || !expected_binding.as_ref().is_some_and(|binding| {
                    current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt)
                })
                || !self
                    .active_binding_matches(graph_id, &node_id, expected_binding.as_ref())
                    .await
            {
                let aborted = active_wave
                    .as_ref()
                    .map(|(executor, ticket, _)| (Arc::clone(executor), ticket.clone()));
                drop(_coordination);
                if let Some((executor, ticket)) = aborted {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_command_superseded_before_commit"),
                    )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                    .await?;
                return Ok(());
            }
            let (_, ticket, _) = active_wave.as_ref().ok_or_else(|| {
                ExecutionRunnerError::Driver("atomic outcome binding missing".into())
            })?;
            let outcome_sources = self
                .commit_service
                .execution_outcome_sources(&current, ticket, &outcome)?;
            // A concurrent writer can advance this graph stream between the
            // load and the atomic outcome commit. Keep the captured outcome and
            // its original source fences while retrying instead of failing the whole
            // execution driver with a raw `revision mismatch`.
            const MAX_TRANSITION_ATTEMPTS: usize = 8;
            let mut attempt = 0;
            let transition = loop {
                attempt += 1;
                let result = self
                    .commit_service
                    .transition_execution_outcome_async(
                        current.clone(),
                        ticket.clone(),
                        outcome.clone(),
                        outcome_sources.clone(),
                    )
                    .await;
                match result {
                    Ok(receipt) => break Ok(receipt),
                    Err(ExecutionCommitError::EventStore(
                        crate::runtime_event_store::RuntimeEventStoreError::StaleRevision {
                            ..
                        },
                    )) if attempt < MAX_TRANSITION_ATTEMPTS => {
                        current = self.state_store.load_async(graph_id).await?;
                        if current.node_statuses.get(&node_id)
                            != Some(&ExecutionNodeStatus::Running)
                            || !expected_binding.as_ref().is_some_and(|binding| {
                                current.recovery_cursor.node_attempts.get(&node_id)
                                    == Some(&binding.attempt)
                            })
                            || !self
                                .active_binding_matches(
                                    graph_id,
                                    &node_id,
                                    expected_binding.as_ref(),
                                )
                                .await
                        {
                            break Err(ExecutionCommitError::InvalidCommand(
                                "node transition superseded by a concurrent writer".to_string(),
                            ));
                        }
                    }
                    Err(error) => break Err(error),
                }
            };
            if let Err(error) = transition {
                #[cfg(test)]
                if self
                    .drop_injected_empty_bound_owner(
                        graph_id,
                        &node_id,
                        expected_binding.as_ref(),
                        &error,
                    )
                    .await?
                {
                    return Err(error.into());
                }
                let aborted = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .filter(|active| {
                        Some(active._resources.binding(&active.ticket)).as_ref()
                            == expected_binding.as_ref()
                    })
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                drop(_coordination);
                if let Some((executor, ticket)) = aborted {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_transition_commit_failed"),
                    )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                    .await?;
                return Err(error.into());
            }
            self.active
                .lock()
                .await
                .get(&(graph_id.to_string(), node_id.clone()))
                .filter(|active| {
                    Some(active._resources.binding(&active.ticket)).as_ref()
                        == expected_binding.as_ref()
                })
                .map(|active| (Arc::clone(&active.executor), active.ticket.clone()))
        };
        self.converge_after_terminal_commit(graph_id).await?;
        durable_progress.notify_one();
        if let Some((executor, ticket)) = committed_executor {
            self.complete_callback_observed(
                graph_id,
                &executor,
                &ticket,
                deadline_terminal.then_some("canonical_deadline_after_atomic_commit"),
            )
            .await?;
            if deadline_terminal {
                executor.cancellation_finalized(&ticket);
            }
            self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                .await?;
        } else {
            self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                .await?;
        }
        Ok(())
    }

    async fn resolve_pump_result(
        &self,
        graph_id: &str,
        result: Result<(String, NodeExecutionOutcome), ExecutionRunnerError>,
        durable_progress: &Notify,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<Option<(String, NodeExecutionOutcome)>, ExecutionRunnerError> {
        match result {
            Err(ExecutionRunnerError::CommandSuperseded { node_id }) => {
                let running = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .filter(|active| {
                        Some(active._resources.binding(&active.ticket)).as_ref() == expected_binding
                    })
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = running {
                    self.active_cancel_observed(graph_id, &executor, &ticket)
                        .await?;
                    self.take_bound_active_node(graph_id, &node_id, expected_binding)
                        .await?;
                }
                Ok(None)
            }
            Err(error @ ExecutionRunnerError::ResourceDeferred { .. }) => Err(error),
            Err(ExecutionRunnerError::DeadlineExceeded {
                node_id,
                deadline_at_ms,
            }) => {
                self.terminalize_deadline_node(
                    graph_id,
                    &node_id,
                    deadline_at_ms,
                    expected_binding,
                )
                .await?;
                self.converge_after_terminal_commit(graph_id).await?;
                durable_progress.notify_one();
                Ok(None)
            }
            Err(ExecutionRunnerError::Resource { node_id, reason }) => {
                self.block_unstarted_resource_node(graph_id, &node_id, reason)
                    .await?;
                self.converge_after_terminal_commit(graph_id).await?;
                durable_progress.notify_one();
                Ok(None)
            }
            Ok(value) => Ok(Some(value)),
            Err(ExecutionRunnerError::Executor(error)) => {
                if matches!(error, NodeExecutorError::Start { .. }) {
                    let node_id = executor_error_node_id(&error).to_string();
                    match self
                        .isolate_node_failure(
                            graph_id,
                            &node_id,
                            expected_binding,
                            error.to_string(),
                        )
                        .await
                    {
                        Err(ExecutionRunnerError::CommandSuperseded { .. }) => return Ok(None),
                        result => result?,
                    }
                    self.converge_after_terminal_commit(graph_id).await?;
                    durable_progress.notify_one();
                    return Ok(None);
                }
                let node_id = executor_error_node_id(&error).to_string();
                if let Some(waiter) = self.command_intent_waiter(graph_id) {
                    waiter.await;
                }
                let result = failed_result(&error);
                let _coordination = self.graph_coordination_without_command(graph_id).await;
                let current = self.state_store.load_async(graph_id).await?;
                if !self
                    .active_binding_matches(graph_id, &node_id, expected_binding)
                    .await
                    || expected_binding.is_some_and(|binding| {
                        current.recovery_cursor.node_attempts.get(&node_id)
                            != Some(&binding.attempt)
                    })
                {
                    return Ok(None);
                }
                let transitioned =
                    current.node_statuses.get(&node_id) == Some(&ExecutionNodeStatus::Running);
                if transitioned {
                    let ticket = self
                        .active
                        .lock()
                        .await
                        .get(&(graph_id.to_string(), node_id.clone()))
                        .map(|active| active.ticket.clone())
                        .ok_or_else(|| {
                            ExecutionRunnerError::Driver(
                                "failed Running node lost original ticket".into(),
                            )
                        })?;
                    self.commit_service
                        .transition_execution_outcome_async(
                            current,
                            ticket,
                            NodeExecutionOutcome::new(result),
                            BTreeMap::new(),
                        )
                        .await?;
                }
                drop(_coordination);
                if transitioned {
                    self.converge_after_terminal_commit(graph_id).await?;
                    durable_progress.notify_one();
                }
                let pending = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = pending {
                    self.active_cancel_observed(graph_id, &executor, &ticket)
                        .await?;
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("executor_failure_before_resource_release"),
            )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding)
                    .await?;
                // A Pause/Cancel command may have superseded the executor while
                // it returned. The command's durable graph state remains truth.
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn start_and_execute_node(
        &self,
        graph_id: &str,
        node: harness_contract::execution_graph::ExecutionNodeSpec,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(String, NodeExecutionOutcome), ExecutionRunnerError> {
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
        let active = self.active.lock().await;
        let prepared = active.get(&(graph_id.to_string(), node.id.clone()));
        if prepared
            .map(|active| active._resources.binding(&active.ticket))
            .as_ref()
            != expected_binding
        {
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        }
        let Some(prepared) = prepared else {
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        };
        let (
            executor,
            ticket,
            resource_kind,
            resource_queue_wait,
            resource_started,
            effect_state,
            no_dispatch,
        ) = (
            Arc::clone(&prepared.executor),
            prepared.ticket.clone(),
            prepared.resource_kind.clone(),
            prepared.resource_queue_wait,
            prepared.resource_started,
            prepared.effect_state.clone(),
            prepared.no_dispatch.clone(),
        );
        drop(active);
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
        }
        if !self
            .active_binding_matches(graph_id, &ticket.node_id, expected_binding)
            .await
        {
            return Err(ExecutionRunnerError::CommandSuperseded {
                node_id: ticket.node_id.clone(),
            });
        }
        match effect_state {
            ExecutionEffectState::Completed(_) => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: "Completed effect must reconcile its original committed attempt, never enter fresh execution".into(),
                }.into());
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
                    if executor.cancellation_requires_quiescence(&ticket) {
                        self.active_cancel_observed(graph_id,&executor,&ticket).await?;
                    } else {
                    match tokio::time::timeout(PHYSICAL_CLOSURE_BUDGET, executor.cancel_bounded(&ticket, PHYSICAL_CLOSURE_BUDGET)).await {
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
                    }
                    Ok(deadline_exceeded_outcome(&ticket.node_id, deadline_at_ms))
                }
            }
        } else {
            execute_node_under_ceiling(&executor, &ticket).await
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
        let mut outcome = outcome.map_err(ExecutionRunnerError::from)?;
        if let Some(observation) = no_dispatch {
            observation
                .finalize_duration(&ticket, &mut outcome, node_duration_ms)
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        } else if outcome.no_dispatch.is_some() {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "NoDispatch duration has no original active observation".into(),
            }
            .into());
        } else {
            outcome.result.usage.duration_ms = outcome
                .result
                .usage
                .duration_ms
                .max(node_duration_ms.max(1));
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
        if node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            || (node.kind == harness_contract::execution_graph::ExecutionNodeKind::Verify
                && node.executor_kind == super::executors::CompileTargetGuardExecutor::KIND)
        {
            // ToolBatch is a durable orchestration container. Its tool
            // leaves acquire the
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
                original_binding: None,
                admission_pending: false,
                release_blocked: true,
                callback_completed: false,
                drain_completed: false,
                owner_fence: None,
            });
        }
        let resource_kind = match node.kind {
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask => {
                Some(ExecutionResourceKind::Agent)
            }
            harness_contract::execution_graph::ExecutionNodeKind::Materialize => {
                Some(ExecutionResourceKind::Tool)
            }
            // InlineModel owns Provider admission in ConversationRuntime.
            // ToolBatch leaves own Tool admission in ToolExecutionPlane.
            // Deterministic control nodes do not consume either family.
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel
            | harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
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
            original_binding: None,
            admission_pending: false,
            release_blocked: true,
            callback_completed: false,
            drain_completed: false,
            owner_fence: None,
        })
    }

    async fn terminalize_deadline_node(
        &self,
        graph_id: &str,
        node_id: &str,
        deadline_at_ms: u64,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(), ExecutionRunnerError> {
        let running = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.to_string()))
            .filter(|active| {
                Some(active._resources.binding(&active.ticket)).as_ref() == expected_binding
            })
            .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
        if let Some((executor, ticket)) = running.as_ref() {
            self.active_cancel_observed(graph_id, &executor, ticket)
                .await?;
        }
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if !self
            .active_binding_matches(graph_id, node_id, expected_binding)
            .await
            || expected_binding.is_some_and(|binding| {
                current.recovery_cursor.node_attempts.get(node_id) != Some(&binding.attempt)
            })
        {
            return Ok(());
        }
        if !matches!(
            current.node_statuses.get(node_id),
            Some(ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running)
        ) {
            return Ok(());
        }
        let outcome = deadline_exceeded_outcome(node_id, deadline_at_ms);
        if current.node_statuses.get(node_id) == Some(&ExecutionNodeStatus::Running) {
            let ticket = running
                .as_ref()
                .map(|(_, ticket)| ticket.clone())
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver(
                        "deadline Running node lost original ticket".into(),
                    )
                })?;
            self.commit_service
                .transition_execution_outcome_async(current, ticket, outcome, BTreeMap::new())
                .await?;
        } else {
            self.commit_service
                .transition_node_async(
                    current,
                    node_id.to_string(),
                    outcome.result.status,
                    Some(outcome.result),
                    Vec::new(),
                )
                .await?;
        }
        drop(_coordination);
        if let Some(finished) = self
            .take_bound_active_node(graph_id, node_id, expected_binding)
            .await?
        {
            finished.executor.cancellation_finalized(&finished.ticket);
        }
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
                    wait_class: None,
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
        expected_binding: Option<&ExecutionNodeBinding>,
        reason: String,
    ) -> Result<(), ExecutionRunnerError> {
        let key = (graph_id.to_string(), node_id.to_string());
        let running = self.active.lock().await.get(&key).map(|active| {
            (
                Arc::clone(&active.executor),
                active.ticket.clone(),
                active._resources.binding(&active.ticket),
            )
        });
        if running.as_ref().map(|(_, _, binding)| binding) != expected_binding {
            return Err(ExecutionRunnerError::CommandSuperseded {
                node_id: node_id.to_owned(),
            });
        }
        if let Some((executor, ticket, _)) = running.as_ref() {
            self.active_cancel_observed(graph_id, &executor, ticket)
                .await?;
            self.complete_callback_observed(
                graph_id,
                &executor,
                ticket,
                Some("node_execution_isolated_failure"),
            )
            .await?;
        }
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if let Some(binding) = expected_binding {
            if current.recovery_cursor.node_attempts.get(node_id) != Some(&binding.attempt)
                || !self
                    .active
                    .lock()
                    .await
                    .get(&key)
                    .is_some_and(|active| active._resources.binding(&active.ticket) == *binding)
            {
                return Err(ExecutionRunnerError::CommandSuperseded {
                    node_id: node_id.to_owned(),
                });
            }
        }
        let status = current.node_statuses.get(node_id).copied();
        if !matches!(
            status,
            Some(
                ExecutionNodeStatus::Ready
                    | ExecutionNodeStatus::Running
                    | ExecutionNodeStatus::Planned
            )
        ) {
            drop(_coordination);
            self.release_isolated_node(&key, expected_binding).await?;
            return Ok(());
        }
        // A failed start, executor panic, or node-local failure is a failure of
        // that node, not a safety boundary. It is terminal (`Failed` has no
        // outgoing transition), so it must not advertise retryability
        // (PLAN C3-P-g / I5). Recovery/replan act on the isolated node instead
        // of a `retryable` flag the graph state machine does not honour.
        let terminal_status = ExecutionNodeStatus::Failed;
        let result = ExecutionNodeResult {
            status: terminal_status,
            result_ref: None,
            wait_class: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "node_execution_isolated_failure".to_string(),
                message: reason,
                retryable: false,
                evidence_refs: Vec::new(),
            }),
            usage: Default::default(),
            finished_at_ms: now_ms(),
        };
        if status == Some(ExecutionNodeStatus::Running) {
            let ticket = running
                .as_ref()
                .map(|(_, ticket, _)| ticket.clone())
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver(
                        "isolated Running node lost original ticket".into(),
                    )
                })?;
            self.commit_service
                .transition_execution_outcome_async(
                    current,
                    ticket,
                    NodeExecutionOutcome::new(result),
                    BTreeMap::new(),
                )
                .await?;
        } else {
            self.commit_service
                .transition_node_async(
                    current,
                    node_id.to_string(),
                    terminal_status,
                    Some(result),
                    Vec::new(),
                )
                .await?;
        }
        drop(_coordination);
        self.release_isolated_node(&key, expected_binding).await?;
        Ok(())
    }

    async fn release_isolated_node(
        &self,
        key: &(String, String),
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(), ExecutionRunnerError> {
        if let Some(finished) = self
            .take_bound_active_node(&key.0, &key.1, expected_binding)
            .await?
        {
            self.record_resource_terminal(
                finished.resource_kind.as_ref(),
                finished.resource_queue_wait,
                finished.resource_started,
                crate::execution_core::graph::ResourceResultClass::Failed,
            );
            finished.executor.cancellation_finalized(&finished.ticket);
        }
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
                    if let Err(error) = self
                        .active_cancel_observed(graph_id, executor, ticket)
                        .await
                    {
                        cancellation_errors.push((node_id.clone(), error));
                    }
                }
            }

            let coordination = self.graph_coordination(graph_id).await;
            let current = self.state_store.load_async(graph_id).await?;
            if let Some((_, error)) = cancellation_errors.into_iter().next() {
                drop(coordination);
                return Err(error);
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
            drop(coordination);
            if result.is_ok() {
                let node_ids = cancellation_finalizer
                    .active()
                    .iter()
                    .map(|(node_id, _, _)| node_id.clone())
                    .collect::<Vec<_>>();
                for node_id in node_ids {
                    let binding = self
                        .active
                        .lock()
                        .await
                        .get(&(graph_id.to_string(), node_id.clone()))
                        .map(|active| active._resources.binding(&active.ticket));
                    self.take_bound_active_node(graph_id, &node_id, binding.as_ref())
                        .await?;
                }
            }
            drop(intent);
            drop(cancellation_finalizer);
            result?;
            return self
                .state_store
                .load_async(graph_id)
                .await
                .map_err(Into::into);
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

    fn published_dependency_predecessors<'graph>(
        &self,
        graph: &'graph ExecutionGraph,
        node: &'graph harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<Vec<DependencyPredecessor<'graph>>, ExecutionRunnerError> {
        let mut predecessors = dependency_predecessors(graph, node);
        let edges = graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == node.id);
        for (predecessor, edge) in predecessors.iter_mut().zip(edges) {
            let attempt = graph
                .recovery_cursor
                .node_attempts
                .get(&edge.from)
                .copied()
                .unwrap_or(0);
            if !predecessor.status.is_terminal() || attempt == 0 {
                continue;
            }
            let binding = self
                .state_store
                .current_binding(graph, &edge.from, attempt)?;
            if !ExecutionBindingLeaseClosure::required(&binding) {
                continue;
            }
            let start = self
                .state_store
                .binding_start_position(graph, &edge.from, &binding)?
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver("dependency original binding start missing".into())
                })?;
            if !self
                .state_store
                .binding_lease_closure(graph, &edge.from, &binding, start)?
            {
                predecessor.status = ExecutionNodeStatus::Running;
                predecessor.result = None;
            }
        }
        Ok(predecessors)
    }

    fn dependency_publications_ready(
        &self,
        graph: &ExecutionGraph,
        node: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<bool, ExecutionRunnerError> {
        let predecessors = self.published_dependency_predecessors(graph, node)?;
        let policy = node
            .work
            .as_ref()
            .map(|work| work.dependency.clone())
            .unwrap_or_default();
        Ok(dependency_target(&policy, &predecessors) == Some(ExecutionNodeStatus::Ready))
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
                let predecessors = match node {
                    Some(node) => self.published_dependency_predecessors(&graph, node)?,
                    None => Vec::new(),
                };
                let dependency = node
                    .and_then(|node| node.work.as_ref())
                    .map(|work| work.dependency.clone())
                    .unwrap_or_default();
                let target = dependency_target(&dependency, &predecessors);
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
        ExecutionDependencyPolicy::EvidenceReady { predicate, .. } => {
            return evidence_ready_target(predicate, predecessors);
        }
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

/// Bounded ceiling for a node that carries no durable deadline of its own.
///
/// Only `AgentTask` nodes read an absolute deadline from their payload, so every
/// other kind — `inline_model`, `tool_batch`, `synthesize`, `model` — used to be
/// awaited without any bound at all. That is not a theoretical gap: a paid run
/// lost ten minutes to one blocked `inline_model` node and ended in a forced
/// abort at shutdown, because nothing in the Runtime would ever stop it.
///
/// The ceiling is deliberately far above any real step (a root model step in that
/// same run took 23 seconds, a delegated Agent minutes), so it never truncates
/// work that is genuinely progressing. It only converts "hangs forever" into
/// "cancelled with a summary".
fn node_execution_ceiling() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("COWD_NODE_EXECUTION_CEILING_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_800),
    )
}

/// Await a node that carries no durable deadline of its own, under a bounded
/// ceiling. A node that is genuinely working is untouched; a node whose executor
/// stopped making progress is cancelled with an explicit summary instead of
/// hanging the turn forever.
async fn execute_node_under_ceiling(
    executor: &Arc<dyn NodeExecutor>,
    ticket: &NodeExecutionTicket,
) -> Result<NodeExecutionOutcome, NodeExecutorError> {
    let ceiling = node_execution_ceiling();
    let started = std::time::Instant::now();
    let ceiling_ms = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
    // Capture the deadline once. Re-reading an environment-backed ceiling after
    // expiry could report a deadline different from the one that actually
    // governed this attempt.
    let deadline_at_ms = now_ms().saturating_add(ceiling_ms);
    tracing::debug!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        ceiling_ms,
        "node execution entered bounded ceiling"
    );
    match tokio::time::timeout(ceiling, executor.poll_or_await(ticket)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::warn!(
                graph_id = ticket.graph_id,
                node_id = ticket.node_id,
                executor_kind = ticket.executor_kind,
                attempt = ticket.attempt,
                ceiling_ms,
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "node execution exceeded its bounded ceiling; cancelling with a summary"
            );
            if executor.cancellation_requires_quiescence(ticket) {
                executor
                    .cancel_bounded(ticket, PHYSICAL_CLOSURE_BUDGET)
                    .await?;
            } else {
                let _ = tokio::time::timeout(
                    PHYSICAL_CLOSURE_BUDGET,
                    executor.cancel_bounded(ticket, PHYSICAL_CLOSURE_BUDGET),
                )
                .await;
            }
            Ok(deadline_exceeded_outcome(&ticket.node_id, deadline_at_ms))
        }
    }
}

async fn run_after_commit_observed(
    executor: &Arc<dyn NodeExecutor>,
    ticket: &NodeExecutionTicket,
) -> Result<(), NodeExecutorError> {
    let started = std::time::Instant::now();
    tracing::info!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        phase = "after_commit_started",
        "node post-commit hook entered"
    );
    let result = executor.after_commit(ticket).await;
    tracing::info!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        phase = "after_commit_finished",
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        failed = result.is_err(),
        "node post-commit hook finished"
    );
    result
}

fn execution_deadline_at_ms(
    node: &harness_contract::execution_graph::ExecutionNodeSpec,
) -> Result<Option<u64>, ExecutionRunnerError> {
    if !matches!(
        node.kind,
        harness_contract::execution_graph::ExecutionNodeKind::AgentTask
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
            reason: "AgentTask has no Runtime-issued absolute deadline".to_string(),
        })?;
    // `u64::MAX` is the explicit no-wall-deadline sentinel used by durable
    // Agent packets. Do not convert it into an enormous Tokio timer: Runtime
    // still governs cancellation, resource admission and rolling claims.
    if deadline_at_ms == u64::MAX {
        return Ok(None);
    }
    Ok(Some(deadline_at_ms))
}

fn deadline_exceeded_outcome(node_id: &str, deadline_at_ms: u64) -> NodeExecutionOutcome {
    NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Cancelled,
        result_ref: Some(format!("execution-deadline:{node_id}:{deadline_at_ms}")),
        wait_class: None,
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

pub(super) fn validate_outcome(
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
        wait_class: None,
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
mod nodispatch_duration_617_tests {
    use super::super::executors::{
        ScopedNodeBackend, ScopedNodeBackendResolver, ScopedNodeExecutor,
    };
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RefusedBackend617 {
        owner: Weak<ScopedNodeExecutor>,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ScopedNodeBackend for RefusedBackend617 {
        async fn execute(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let owner = self.owner.upgrade().unwrap();
            owner
                .binding_observation(ticket)
                .unwrap()
                .refuse("denied-617", "original pre-effect scope refusal".into());
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("original refused batch".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    struct Resolver617(Arc<RefusedBackend617>);

    impl ScopedNodeBackendResolver for Resolver617 {
        fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            Some(self.0.clone())
        }
    }

    struct TamperedExecutor617 {
        inner: Arc<dyn NodeExecutor>,
        drop_witness: bool,
        observed: Option<Arc<std::sync::Mutex<Option<NodeExecutionOutcome>>>>,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for TamperedExecutor617 {
        fn kind(&self) -> &str {
            self.inner.kind()
        }

        fn supports_resumable_pause(&self) -> bool {
            self.inner.supports_resumable_pause()
        }

        fn validate(
            &self,
            node: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
            self.inner.validate(node)
        }

        async fn start(
            &self,
            context: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            self.inner.start(context).await
        }

        async fn poll_or_await(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            let mut outcome = self.inner.poll_or_await(ticket).await?;
            if let Some(observed) = &self.observed {
                *observed.lock().unwrap() = Some(outcome.clone());
                return Ok(outcome);
            }
            if self.drop_witness {
                outcome.no_dispatch = None;
            } else {
                outcome.result.summary = Some("tampered after original seal".into());
            }
            Ok(outcome)
        }

        async fn after_commit(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<(), NodeExecutorError> {
            self.inner.after_commit(ticket).await
        }

        async fn after_abort(
            &self,
            ticket: &NodeExecutionTicket,
            reason: &str,
        ) -> Result<(), NodeExecutorError> {
            self.inner.after_abort(ticket, reason).await
        }

        async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.inner.cancel(ticket).await
        }

        async fn cancel_bounded(
            &self,
            ticket: &NodeExecutionTicket,
            budget: std::time::Duration,
        ) -> Result<(), NodeExecutorError> {
            self.inner.cancel_bounded(ticket, budget).await
        }

        fn cancellation_requires_quiescence(&self, ticket: &NodeExecutionTicket) -> bool {
            self.inner.cancellation_requires_quiescence(ticket)
        }

        fn cancellation_finalized(&self, ticket: &NodeExecutionTicket) {
            self.inner.cancellation_finalized(ticket)
        }

        async fn recover(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            self.inner.recover(ticket).await
        }

        async fn drain_physical_execution(&self) -> Result<(), NodeExecutorError> {
            self.inner.drain_physical_execution().await
        }
    }

    #[derive(Default)]
    struct LifecycleProbe660 {
        budgets: std::sync::Mutex<Vec<std::time::Duration>>,
        finalized: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for LifecycleProbe660 {
        fn kind(&self) -> &str {
            "lifecycle-660"
        }

        fn supports_resumable_pause(&self) -> bool {
            false
        }

        fn validate(
            &self,
            _: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
            Ok(())
        }

        async fn start(
            &self,
            _: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            unreachable!()
        }

        async fn poll_or_await(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            unreachable!()
        }

        async fn cancel(&self, _: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            panic!("bounded cancellation must not fall back to unbounded cancellation")
        }

        async fn cancel_bounded(
            &self,
            _: &NodeExecutionTicket,
            budget: std::time::Duration,
        ) -> Result<(), NodeExecutorError> {
            self.budgets.lock().unwrap().push(budget);
            Ok(())
        }

        fn cancellation_requires_quiescence(&self, _: &NodeExecutionTicket) -> bool {
            true
        }

        fn cancellation_finalized(&self, _: &NodeExecutionTicket) {
            self.finalized.fetch_add(1, Ordering::SeqCst);
        }

        async fn recover(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            Err(NodeExecutorError::Recover {
                node_id: ticket.node_id.clone(),
                reason: "original recovery result".into(),
            })
        }
    }

    #[tokio::test]
    async fn r1_660_duration_observer_forwards_original_lifecycle_contract() {
        let inner = Arc::new(LifecycleProbe660::default());
        let wrapper = TamperedExecutor617 {
            inner: inner.clone(),
            drop_witness: false,
            observed: Some(Arc::new(std::sync::Mutex::new(None))),
        };
        let ticket = NodeExecutionTicket {
            graph_id: "graph-660".into(),
            node_id: "node-660".into(),
            executor_kind: "lifecycle-660".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "attempt-660".into(),
            payload_ref: "payload-660".into(),
        };
        assert_eq!(wrapper.kind(), inner.kind());
        assert!(!wrapper.supports_resumable_pause());
        assert!(wrapper.cancellation_requires_quiescence(&ticket));
        let budget = std::time::Duration::from_millis(37);
        wrapper.cancel_bounded(&ticket, budget).await.unwrap();
        assert_eq!(*inner.budgets.lock().unwrap(), vec![budget]);
        assert_eq!(inner.finalized.load(Ordering::SeqCst), 0);
        wrapper.cancellation_finalized(&ticket);
        assert_eq!(inner.finalized.load(Ordering::SeqCst), 1);
        assert_eq!(wrapper.recover(&ticket).await, inner.recover(&ticket).await);
    }

    async fn original_prepared617() -> (
        tempfile::TempDir,
        Arc<crate::RuntimeServices>,
        ExecutionGraphRunner,
        String,
        ExecutionPreparedPumpNode,
        Arc<RefusedBackend617>,
    ) {
        let workspace = tempfile::tempdir().unwrap();
        let services = crate::RuntimeServices::in_memory().unwrap();
        let backend = Arc::new(RefusedBackend617 {
            owner: Arc::downgrade(services.tool_batch_executor()),
            calls: AtomicUsize::new(0),
        });
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Resolver617(backend.clone())));
        let runner = ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            Arc::new(ExecutionResourceManager::new([])),
            Arc::new(ScopeLockManager::persistent(workspace.path().join("scopes")).unwrap()),
            Arc::new(WorktreeLeaseManager::open(workspace.path().join("leases.json")).unwrap()),
            "duration-617",
            workspace.path(),
        );
        let mut graph = ExecutionGraph::new("original refused batch duration");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-617".into(),
            turn_id: "turn-617".into(),
            root_task_id: "task-617".into(),
            task_id: "task-617".into(),
            generation: 1,
        });
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-617", "step_snapshots":{},
            "calls":[{"id":"denied-617","name":"read_file","input":"{}","depends_on":[]}]
        }))
        .unwrap();
        graph
            .nodes
            .push(harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
                "tool_batch",
                payload,
            ));
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut prepared = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        (
            workspace,
            services,
            runner,
            registered.id,
            prepared.remove(0),
            backend,
        )
    }

    #[tokio::test]
    async fn never_admitted_rejects_original_active_prepared_owner_without_clearing_it() {
        let (_workspace, _services, runner, graph_id, prepared, backend) =
            original_prepared617().await;
        let expected = runner.state_store.load_async(&graph_id).await.unwrap();
        let before = runner.active.lock().await.len();
        let error = runner
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .await
            .unwrap_err();
        assert!(
            matches!(error, ExecutionRunnerError::AdmissionRaced { .. }),
            "{error}"
        );
        assert_eq!(runner.active.lock().await.len(), before);
        assert!(runner
            .active
            .lock()
            .await
            .contains_key(&(graph_id, prepared.node.id)));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn r1_617_real_runner_duration_commits_original_refusal_and_rejects_old_seal() {
        let (_workspace, services, runner, graph_id, prepared, backend) =
            original_prepared617().await;
        let observed = Arc::new(std::sync::Mutex::new(None));
        let mut active = runner.active.lock().await;
        let original = active
            .get_mut(&(graph_id.clone(), prepared.node.id.clone()))
            .unwrap();
        let ticket = original.ticket.clone();
        let observation = original.no_dispatch.clone().unwrap();
        original.executor = Arc::new(TamperedExecutor617 {
            inner: original.executor.clone(),
            drop_witness: false,
            observed: Some(observed.clone()),
        });
        drop(active);
        runner
            .execute_prepared_pump_node(&graph_id, prepared, Arc::new(Notify::new()))
            .await
            .unwrap();
        let sealed = observed.lock().unwrap().take().unwrap();
        let old_witness = sealed.no_dispatch.clone().unwrap();
        old_witness
            .authenticate(&ticket, Some(&sealed.result))
            .unwrap();
        assert_eq!(sealed.result.usage.duration_ms, 0);
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let committed = &graph.node_results[&ticket.node_id];
        assert_eq!(committed.status, ExecutionNodeStatus::Completed);
        assert!(committed.usage.duration_ms >= 1);
        let mut expected = sealed.result;
        expected.usage.duration_ms = committed.usage.duration_ms;
        assert_eq!(*committed, expected);
        observation
            .witness()
            .unwrap()
            .authenticate(&ticket, Some(committed))
            .unwrap();
        assert!(old_witness.authenticate(&ticket, Some(committed)).is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(!runner
            .active
            .lock()
            .await
            .contains_key(&(graph_id, ticket.node_id)));
        assert!(runner.drain_physical_execution().await.is_empty());
    }

    #[tokio::test]
    async fn r1_617_real_runner_rejects_post_seal_tampering_or_lost_witness_before_commit() {
        for mode in ["summary", "lost-witness", "missing-observation"] {
            let (_workspace, services, runner, graph_id, prepared, backend) =
                original_prepared617().await;
            let mut active = runner.active.lock().await;
            let original = active
                .get_mut(&(graph_id.clone(), prepared.node.id.clone()))
                .unwrap();
            let ticket = original.ticket.clone();
            let observation = original.no_dispatch.clone().unwrap();
            if mode == "missing-observation" {
                original.no_dispatch = None;
            } else {
                original.executor = Arc::new(TamperedExecutor617 {
                    inner: original.executor.clone(),
                    drop_witness: mode == "lost-witness",
                    observed: None,
                });
            }
            drop(active);
            let before = services.event_store().durable_commit_cursor().unwrap();
            let error = runner
                .start_and_execute_node(&graph_id, prepared.node, prepared.binding.as_ref())
                .await
                .unwrap_err();
            if mode == "missing-observation" {
                assert!(matches!(error,
                    ExecutionRunnerError::Executor(NodeExecutorError::Invalid { reason, .. })
                        if reason == "NoDispatch duration has no original active observation"));
            } else {
                let expected = if mode == "lost-witness" {
                    "NoDispatch duration lost original witness"
                } else {
                    "NoDispatch sealed result mismatch"
                };
                assert!(matches!(error,
                    ExecutionRunnerError::Executor(NodeExecutorError::Poll { reason, .. })
                        if reason == expected));
            }
            assert_eq!(
                services.event_store().durable_commit_cursor().unwrap(),
                before
            );
            let graph = services.graph_state_store().load(&graph_id).unwrap();
            assert_eq!(
                graph.node_statuses[&ticket.node_id],
                ExecutionNodeStatus::Running
            );
            assert!(!graph.node_results.contains_key(&ticket.node_id));
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            let original = services
                .tool_batch_executor()
                .poll_or_await(&ticket)
                .await
                .unwrap();
            observation
                .witness()
                .unwrap()
                .authenticate(&ticket, Some(&original.result))
                .unwrap();
            assert!(runner.drain_physical_execution().await.is_empty());
        }
    }
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
        ExecutionWorkContract, ExecutionWorkRole,
    };

    use super::{
        dependency_predecessors, dependency_target, quorum_tail_cancellations,
        verified_predecessor_status, DependencyPredecessor,
    };

    fn predecessor(status: ExecutionNodeStatus) -> DependencyPredecessor<'static> {
        DependencyPredecessor::status_only(status)
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
            wait_class: None,
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
            wait_class: None,
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
                wait_class: None,
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
}
