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

// Keep this list aligned with the proof reader's explicitly retryable source
// races. Other validation errors remain failures, including forged evidence.
fn retryable_proof_source_change(error: &ExecutionCommitError) -> bool {
    let ExecutionCommitError::InvalidCommand(reason) = error else {
        return false;
    };
    matches!(reason.as_str(),
        "proof source changed while paging; retry current inputs"
        | "proof source changed at final page boundary; retry current inputs"
        | "proof Program source changed; retry current inputs"
        | "proof physical lineage changed during discovery; retry current inputs")
        || reason.starts_with("proof Program source changed; retry current inputs; advanced=")
        || reason.starts_with("proof source changed during validation; retry current inputs; source=")
}

// The semantic owner may rebuild post-tool observations and the next plan,
// but cannot rewrite the physical outcome or discard any other read fence.
fn accepts_goal_revalidation(
    original: &NodeExecutionOutcome,
    candidate: &NodeExecutionOutcome,
    original_sources: &BTreeMap<String, u64>,
    candidate_sources: &BTreeMap<String, u64>,
    stream: &str,
    expected: u64,
    actual: u64,
) -> bool {
    stream.starts_with("goal:")
        && actual > expected
        && original_sources.get(stream) == Some(&expected)
        && candidate_sources.get(stream) == Some(&actual)
        && original.result == candidate.result
        && original.delivery_envelope == candidate.delivery_envelope
        && original.terminal_presentation == candidate.terminal_presentation
        && original.no_dispatch == candidate.no_dispatch
        && original_sources.iter().all(|(key, value)| {
            key == stream || candidate_sources.get(key) == Some(value)
        })
}

// Opt-in diagnostic for the original synthetic completion call. The writer
// is deliberately separate: tracing must never perform persistent I/O while
// the graph or shared active-owner lock is held.
#[cfg(feature = "test-fixtures")]
pub(super) fn k05_completion_trace_enabled(ticket: &NodeExecutionTicket) -> bool {
    std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some()
        && crate::host::decode_tool_batch(&ticket.payload_ref).is_ok_and(|(calls, _)| {
            calls.iter().any(|call| {
                (call.id == "recovery-complete" && call.name == "objective_complete_request")
                    || (call.id == "d-objective-review" && call.name == "objective_review")
            })
        })
}

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) fn k05_trace_emit(line: String) {
    use std::sync::{mpsc, OnceLock};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SENDER: OnceLock<Option<mpsc::SyncSender<String>>> = OnceLock::new();
    static DROPPED: AtomicU64 = AtomicU64::new(0);
    let sender = SENDER.get_or_init(|| {
        let (sender, receiver) = mpsc::sync_channel::<String>(128);
        std::thread::Builder::new().name("k05-stage-observer".into()).spawn(move || {
            while let Ok(line) = receiver.recv() { eprintln!("{line}"); }
        }).ok().map(|_| sender)
    });
    let line = format!("{line} dropped={}", DROPPED.load(Ordering::Relaxed));
    if sender.as_ref().is_none_or(|sender| sender.try_send(line).is_err()) {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn k05_completion_trace(ticket: &NodeExecutionTicket, stage: &'static str, revision: Option<u64>, ok: Option<bool>) {
    use sha2::{Digest, Sha256};
    let call = if crate::host::decode_tool_batch(&ticket.payload_ref).is_ok_and(|(calls, _)|
        calls.iter().any(|call| call.id == "d-objective-review" && call.name == "objective_review")) {
        "d-objective-review"
    } else { "recovery-complete" };
    k05_trace_emit(format!(
        "k05_runner_stage graph_sha256={:x} node_sha256={:x} attempt={} call={} stage={} revision={:?} ok={:?}",
        Sha256::digest(ticket.graph_id.as_bytes()), Sha256::digest(ticket.node_id.as_bytes()),
        ticket.attempt, call, stage, revision, ok,
    ));
}

#[cfg(feature = "test-fixtures")]
fn k05_completion_cas_error(ticket: &NodeExecutionTicket, error: &ExecutionCommitError) {
    use sha2::{Digest, Sha256};
    let (category, stream, expected, actual) = match error {
        ExecutionCommitError::EventStore(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { stream_id, expected, actual }) =>
            ("event_store_stale_revision", Some(stream_id.as_str()), Some(*expected), Some(*actual)),
        ExecutionCommitError::StaleRevision { graph_id, expected, actual } =>
            ("graph_stale_revision", Some(graph_id.as_str()), Some(*expected), Some(*actual)),
        ExecutionCommitError::EventStore(_) => ("event_store_other", None, None, None),
        ExecutionCommitError::EffectAdmissionUnknown { .. } => ("effect_admission_unknown", None, None, None),
        ExecutionCommitError::Transition(_) => ("transition", None, None, None),
        ExecutionCommitError::Serialization(_) => ("serialization", None, None, None),
        ExecutionCommitError::InvalidCommand(_) => ("invalid_command", None, None, None),
        ExecutionCommitError::AlreadyAppliedSame { .. } => ("already_applied_same", None, None, None),
        ExecutionCommitError::GraphStreamCollision(_) => ("graph_stream_collision", None, None, None),
        ExecutionCommitError::MissingDomainIdempotency(_) => ("missing_domain_idempotency", None, None, None),
        ExecutionCommitError::ProtectedDomainScope(_) => ("protected_domain_scope", None, None, None),
        ExecutionCommitError::BlockingTask(_) => ("blocking_task", None, None, None),
        ExecutionCommitError::InvalidReplan(_) => ("invalid_replan", None, None, None),
    };
    let source_kind = stream.map(|id| {
        if id == ticket.graph_id.as_str() { "graph" }
        else if id.starts_with("execution-agent-receipts:") { "agent_receipts" }
        else if id.starts_with("execution-effect:") { "effect" }
        else if id.starts_with("agentic-program:") { "program" }
        else if id.starts_with("goal:") { "goal" }
        else if id.starts_with("session:") { "session" }
        else { "other" }
    }).unwrap_or("unknown");
    let source_sha = stream.map(|id| format!("{:x}", Sha256::digest(id.as_bytes())));
    k05_trace_emit(format!(
        "k05_runner_cas_error graph_sha256={:x} node_sha256={:x} attempt={} category={} source_kind={} source_sha256={:?} expected={:?} actual={:?} same_graph={}",
        Sha256::digest(ticket.graph_id.as_bytes()), Sha256::digest(ticket.node_id.as_bytes()),
        ticket.attempt, category, source_kind, source_sha, expected, actual,
        stream == Some(ticket.graph_id.as_str()),
    ));
}

// Local fixture diagnostic only; no payloads or authority are reconstructed.
#[cfg(feature = "test-fixtures")]
fn build16_callback_trace(graph: &str, node: &str, attempt: Option<u32>, phase: &str, detail: &str) {
    if std::env::var_os("COWD_BUILD16_CALLBACK_TRACE").as_deref() == Some(std::ffi::OsStr::new("1"))
        && graph.starts_with("agent-tool-batch:")
    {
        let detail: String = detail.chars().take(512).collect();
        eprintln!("BUILD16_CALLBACK graph={graph} node={node} attempt={attempt:?} phase={phase} detail={detail:?}");
    }
}

#[cfg(feature = "test-fixtures")]
fn build16_runner_error_kind(error: &ExecutionRunnerError) -> &'static str {
    match error {
        ExecutionRunnerError::Executor(_) => "executor",
        ExecutionRunnerError::Commit(_) => "commit",
        ExecutionRunnerError::State(_) => "state",
        ExecutionRunnerError::CommandSuperseded { .. } => "superseded",
        ExecutionRunnerError::TerminalWaveCleanup { .. } => "terminal_wave_cleanup",
        ExecutionRunnerError::Driver(_) => "driver",
        _ => "other",
    }
}

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
    #[error("terminal wave failed: {reason}; {phase} remains unresolved: {cleanup}")]
    TerminalWaveCleanup {
        reason: String,
        phase: &'static str,
        #[source]
        cleanup: Box<ExecutionRunnerError>,
    },
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

#[derive(Default)]
struct CancelOutcomeHandoff {
    state: StdMutex<CancelOutcomeState>,
    ready: Notify,
}
#[derive(Default)]
struct CancelOutcomeState {
    polled: bool,
    finished: bool,
    outcome: Option<NodeExecutionOutcome>,
}

struct ActiveNode {
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    resource_kind: Option<ExecutionResourceKind>,
    resource_queue_wait: Duration,
    resource_started: std::time::Instant,
    effect_state: ExecutionEffectState,
    effect_receipt_required: bool,
    cancel_outcome: Arc<CancelOutcomeHandoff>,
    // Identity and callback state remain visible without taking the owner's
    // resource lock. The table retains the Arc across a cancelled waiter.
    binding: ExecutionNodeBinding,
    resources: Arc<StdMutex<NodeResourceGuards>>,
    release_blocked: bool,
    callback_completed: bool,
    drain_completed: bool,
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
    #[cfg(test)]
    closure_probe: Arc<StdMutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>,
    #[cfg(test)]
    closure_finished_probe: Arc<StdMutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>,
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
            #[cfg(test)]
            closure_probe: Arc::new(StdMutex::new(None)),
            #[cfg(test)]
            closure_finished_probe: Arc::new(StdMutex::new(None)),
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
                (node_id.clone(), active.binding.clone())
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
        self.register_continuation(graph, None, None).await
    }

    pub(crate) async fn register_continuation(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
        source_goal_cut: Option<crate::ExpectedStreamRevision>,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        validate_execution_graph(&graph)?;
        self.registry.validate_graph(&graph)?;
        match self
            .commit_service
            .register_continuation_graph_async(graph, actor, source_goal_cut)
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
                            binding: start.resources.binding(&start.ticket),
                            ticket: start.ticket,
                            resource_kind: start.resource_kind,
                            resource_queue_wait: start.resource_queue_wait,
                            resource_started: start.resource_started,
                            effect_state: start.effect_state,
                            effect_receipt_required: start.effect_receipt_required,
                            cancel_outcome: Arc::new(CancelOutcomeHandoff::default()),
                            resources: Arc::new(StdMutex::new(start.resources)),
                            release_blocked: true,
                            callback_completed: false,
                            drain_completed: false,
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
                            active.binding.clone(),
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
                                    && original.binding.clone() == *binding
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
                        start.resources.lock().unwrap_or_else(std::sync::PoisonError::into_inner).admission_pending = false;
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
        #[cfg(feature = "test-fixtures")]
        let mut diagnostic_ticket = None;
        let handoff = self.active.lock().await.get(&(graph_id.to_string(), node_id.clone()))
            .filter(|active| Some(&active.binding) == expected_binding.as_ref())
            .map(|active| {
                #[cfg(feature = "test-fixtures")]
                { diagnostic_ticket = Some(active.ticket.clone()); }
                Arc::clone(&active.cancel_outcome)
            });
        #[cfg(feature = "test-fixtures")]
        let diagnostic_ticket = diagnostic_ticket.filter(k05_completion_trace_enabled);
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "start_and_execute_enter", None, None); }
        let result = match prepared.startup_error {
            Some(error) => Err(error),
            None => {
                self.start_and_execute_node(graph_id, prepared.node, expected_binding.as_ref())
                    .await
            }
        };
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "start_and_execute_return", None, Some(result.is_ok())); }
        #[cfg(feature = "test-fixtures")]
        {
            let detail = match &result {
                Ok((_, outcome)) => format!("result=ok status={:?}", outcome.result.status),
                Err(error) => format!("result=error kind={} original_detail=private", build16_runner_error_kind(error)),
            };
            build16_callback_trace(graph_id, &node_id, expected_binding.as_ref().map(|b| b.attempt), "drive_return", &detail);
        }
        if let Some(handoff) = handoff {
            let mut state = handoff.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.finished = true;
            state.outcome = result.as_ref().ok().map(|(_, outcome)| outcome.clone());
            drop(state);
            handoff.ready.notify_one();
        }
        let binding_matches = self
            .active_binding_matches(graph_id, &node_id, expected_binding.as_ref())
            .await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "binding_checked", None, Some(binding_matches)); }
        if !binding_matches {
            return Ok(());
        }
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "finish_pump_enter", None, None); }
        // Keep the large terminal-transition future out of its caller's
        // async frame. Its ownership, cancellation and fences are unchanged.
        let result = Box::pin(self.finish_pump_node(
            graph_id, result, durable_progress, expected_binding,
        )).await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "finish_pump_return", None, Some(result.is_ok())); }
        result
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
        let actual = owner.binding.clone();
        if Some(&actual) != binding
            || !actual.resource_lease_refs.is_empty()
            || actual.scope_lease_ref.is_some()
            || actual.worktree_lease_ref.is_some()
            || owner.callback_completed
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
            .map(|active| active.binding.clone())
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
            .map(|active| active.binding.clone())
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
            .is_some_and(|active| active.ticket == *ticket && active.drain_completed)
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
                active.drain_completed = true;
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
            .map(|active| active.binding.clone());
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
                    active.binding.clone(),
                    active.callback_completed,
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
        #[cfg(feature = "test-fixtures")]
        {
            let state_name = match disposition {
                CallbackDisposition::Commit => "committed_outcome",
                CallbackDisposition::Abort => "canonical_retirement",
                CallbackDisposition::Pending => "pending_no_outcome_or_retirement",
            };
            let snapshot = self.active.lock().await;
            let live = snapshot.get(&(graph_id.to_string(), ticket.node_id.clone()))
                .filter(|active| active.ticket == *ticket);
            let detail = match live {
                Some(active) => {
                    let driver = active.cancel_outcome.state.lock().ok().map(|state|
                        (state.polled, state.finished, state.outcome.is_some()));
                    format!("disposition={state_name} requested_abort={requested_abort:?} node_status={:?} graph_attempt={:?} callback_completed={} release_blocked={} drain_completed={} driver_polled_finished_has_outcome={driver:?}",
                        graph.node_statuses.get(&ticket.node_id), graph.recovery_cursor.node_attempts.get(&ticket.node_id),
                        active.callback_completed, active.release_blocked, active.drain_completed)
                },
                None => format!("disposition={state_name} requested_abort={requested_abort:?} active=unknown"),
            };
            build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt), "callback_disposition", &detail);
        }
        if disposition == CallbackDisposition::Pending {
            tracing::warn!(graph_id = %graph_id, node_id = %ticket.node_id,
                attempt = ticket.attempt, executor_kind = %ticket.executor_kind,
                requested_abort = ?requested_abort, phase = "callback_pending",
                "original callback has no committed outcome or retirement");
            if requested_abort.is_some() {
                return Ok(());
            }
            return Err(ExecutionRunnerError::Driver(
                "callback has no canonical original outcome or retirement".into(),
            ));
        };
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=callback_enter node={} executor={} abort={}", ticket.node_id, ticket.executor_kind, disposition == CallbackDisposition::Abort);
        }
        let result = if disposition == CallbackDisposition::Abort {
            self.cancel_and_drain_locked(graph_id, executor, ticket)
                .await.map_err(|error| {
                    #[cfg(feature = "test-fixtures")]
                    build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt),
                        "retirement_cancel_and_drain", &format!("result=error kind={} original_detail=private", build16_runner_error_kind(&error)));
                    error
                })?;
            #[cfg(feature = "test-fixtures")]
            build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt),
                "retirement_cancel_and_drain", "result=ok");
            executor
                .after_abort(
                    ticket,
                    requested_abort.unwrap_or("canonical_binding_retirement"),
                )
                .await
        } else {
            run_after_commit_observed(executor, ticket).await
        };
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=callback_return node={} executor={} ok={}", ticket.node_id, ticket.executor_kind, result.is_ok());
        }
        let result = match result {
            Ok(()) => {
                let mut changes = self.state_store.subscribe_commits();
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() {
                    eprintln!("r622_toolbatch phase=physical_wait_enter node={} executor={}", ticket.node_id, ticket.executor_kind);
                }
                let observed = tokio::time::timeout(PHYSICAL_CLOSURE_BUDGET, async {
                    loop {
                        let current = self.state_store.load_async(graph_id).await?;
                        match self
                            .commit_service
                            .binding_physical_sources_with_witness(
                                &current,
                                &ticket.node_id,
                                &binding,
                                observation
                                    .as_ref()
                                    .and_then(|observation| observation.witness())
                                    .as_ref(),
                            )? {
                            super::commit_service::PhysicalClosureProof::Proven(_) => {
                                return Ok::<(), ExecutionRunnerError>(());
                            }
                            super::commit_service::PhysicalClosureProof::Unprovable { ring, reason } => {
                                // A ring that can never hold must not spend the
                                // whole physical-closure budget: name it and
                                // fail closed now.
                                return Err(ExecutionRunnerError::Driver(format!(
                                    "physical closure unprovable: {ring}: {reason}"
                                )));
                            }
                            super::commit_service::PhysicalClosureProof::Pending => {}
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
                    #[cfg(feature = "test-fixtures")]
                    build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt),
                        "physical_sources", "result=timeout");
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
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() {
                    eprintln!("r622_toolbatch phase=physical_wait_return node={} executor={} ok={}", ticket.node_id, ticket.executor_kind, observed.is_ok());
                }
                observed
            }
            Err(error) => Err(error.into()),
        };
        #[cfg(feature = "test-fixtures")]
        build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt), "callback_and_physical_result",
            &match &result { Ok(()) => "result=ok physical_sources=confirmed".into(),
                Err(error) => format!("result=error physical_sources=unconfirmed kind={} original_detail=private", build16_runner_error_kind(error)) });
        let result = if result.is_ok() && disposition == CallbackDisposition::Commit && ticket.executor_kind == "synthesize" {
            let original_owner = {
                let active = self.active.lock().await;
                active.get(&(graph_id.to_string(), ticket.node_id.clone()))
                    .filter(|owner| owner.ticket == *ticket && owner.binding == binding)
                    .and_then(|owner| owner.cancel_outcome.state.lock().ok().and_then(|handoff| {
                        if !handoff.finished { return None; }
                        handoff.outcome.as_ref().and_then(|outcome| outcome.original_terminal_owner.as_ref().map(|owner| Arc::clone(&owner.0)))
                    }))
            };
            if let Some(owner) = original_owner {
                let ticket = ticket.clone();
                tokio::task::spawn_blocking(move || owner.release_after_original_physical_close(&ticket)).await
                    .map_err(|error| ExecutionRunnerError::Driver(format!("original terminal release task failed: {error}")))?
                    .map_err(ExecutionRunnerError::Driver)
            } else {
                // A non-terminal Synthesize replan has no terminal owner and
                // keeps its original callback behavior. For a terminal result,
                // missing the live handoff is Unknown and must retain custody.
                let document = self.commit_service.committed_host_transcript(ticket)?;
                if document.terminal_completion.is_some() {
                    Err(ExecutionRunnerError::Driver("original terminal handoff is unconfirmed".into()))
                } else { result }
            }
        } else { result };
        if let Some(active) = self
            .active
            .lock()
            .await
            .get_mut(&(graph_id.to_string(), ticket.node_id.clone()))
        {
            if active.ticket == *ticket {
                active.callback_completed = result.is_ok();
                active.release_blocked = result.is_err();
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
        #[cfg(feature = "test-fixtures")]
        if k05_completion_trace_enabled(ticket) { k05_completion_trace(ticket, "callback_gate_enter", None, None); }
        let _owner = gate.lock().await;
        #[cfg(feature = "test-fixtures")]
        if k05_completion_trace_enabled(ticket) { k05_completion_trace(ticket, "callback_gate_acquired", None, None); }
        self.complete_callback_locked(graph_id, executor, ticket, requested_abort)
            .await
    }

    fn close_active_binding_leases(
        &self,
        graph_id: &str,
        node_id: &str,
        ticket: &NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        resources: &mut NodeResourceGuards,
        no_dispatch: Option<&Arc<super::executors::agent_tool::NoDispatchObservation>>,
    ) -> Result<(), ExecutionRunnerError> {
        if !ExecutionBindingLeaseClosure::required(binding) {
            return Ok(());
        }
        #[cfg(test)]
        {
            let probe = self.closure_probe.lock().unwrap().clone();
            if let Some(probe) = probe {
                probe(graph_id);
            }
        }
        for retry in 0..8 {
            let graph = self.state_store.load(graph_id)?;
            let position = self
                .state_store
                .binding_start_position(&graph, node_id, binding)?
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver("original lease start missing".into())
                })?;
            let owner = resources.owner_fence.as_mut().ok_or_else(|| {
                ExecutionRunnerError::Driver("original binding owner capability missing".into())
            })?;
            let released = owner
                .release_original(
                    &graph,
                    node_id,
                    binding,
                    position,
                    &mut resources.resource,
                    &mut resources.scope,
                    &mut resources.worktree,
                    &self.worktree_leases,
                )
                .map_err(ExecutionRunnerError::Driver)?;
            if self
                .state_store
                .binding_lease_closure(&graph, node_id, binding, position)?
            {
                self.registry.forget_builtin_binding(ticket);
                return Ok(());
            }
            match self
                .commit_service
                .commit_binding_lease_closure_with_witness(
                    &graph,
                    &released,
                    no_dispatch
                        .and_then(|observation| observation.witness())
                        .as_ref(),
                ) {
                Ok(_) => {
                    self.registry.forget_builtin_binding(ticket);
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
            .filter(|active| Some(active.binding.clone()).as_ref() == expected_binding)
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
        let (resources, binding, no_dispatch) = {
            let active = self.active.lock().await;
            let Some(finished) = active.get(&key).filter(|finished| {
                Some(&finished.binding) == expected_binding && finished.ticket == ticket
            }) else {
                return Ok(None);
            };
            if finished.release_blocked || !finished.callback_completed {
                return Err(ExecutionRunnerError::Driver(
                    "original callback has not positively completed; binding release blocked"
                        .into(),
                ));
            }
            (
                Arc::clone(&finished.resources),
                finished.binding.clone(),
                finished.no_dispatch.clone(),
            )
        };
        // The table retains this exact owner and its fence while the lock is
        // released. A cancelled waiter therefore cannot drop the leases.
        {
            let mut original = resources.lock().map_err(|_| {
                ExecutionRunnerError::Driver(
                    "original binding resource owner poisoned; release blocked".into(),
                )
            })?;
            self.close_active_binding_leases(
                graph_id,
                node_id,
                &ticket,
                &binding,
                &mut original,
                no_dispatch.as_ref(),
            )?;
        }
        #[cfg(test)]
        {
            let probe = self.closure_finished_probe.lock().unwrap().clone();
            if let Some(probe) = probe {
                probe(graph_id);
            }
        }
        let mut active = self.active.lock().await;
        if !active.get(&key).is_some_and(|finished| {
            finished.ticket == ticket
                && finished.binding == binding
                && Arc::ptr_eq(&finished.resources, &resources)
        }) {
            return Err(ExecutionRunnerError::Driver(
                "original binding owner changed during closure".into(),
            ));
        }
        let mut original = resources.lock().map_err(|_| {
            ExecutionRunnerError::Driver(
                "original binding resource owner poisoned after closure".into(),
            )
        })?;
        let fence = original.owner_fence.take();
        let finished = active.remove(&key);
        drop(active);
        drop(original);
        drop(fence);
        Ok(finished)
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
            #[cfg(feature = "test-fixtures")]
            let trace_nodes = pending.iter().map(|item| (item.transition.node_id.clone(),
                item.expected_binding.as_ref().map(|binding| binding.attempt))).collect::<Vec<_>>();
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
                                            active.binding.clone() == *binding
                                        })
                            })
                        {
                            #[cfg(feature = "test-fixtures")]
                            build16_callback_trace(graph_id, &pending.transition.node_id,
                                pending.expected_binding.as_ref().map(|binding| binding.attempt),
                                "terminal_wave_membership", "member=admitted");
                            admitted.push(pending.transition);
                            admitted_responses.push(pending.response);
                        } else {
                            #[cfg(feature = "test-fixtures")]
                            build16_callback_trace(graph_id, &pending.transition.node_id,
                                pending.expected_binding.as_ref().map(|binding| binding.attempt),
                                "terminal_wave_membership", "member=superseded");
                            superseded_responses.push(pending.response);
                        }
                    }
                    drop(active);
                    #[cfg(feature = "test-fixtures")]
                    let admitted_count = admitted.len();
                    let commit = if admitted.is_empty() {
                        Ok(())
                    } else {
                        self.commit_service
                            .transition_nodes_async(current, admitted)
                            .await
                            .map(|_| ())
                    };
                    #[cfg(feature = "test-fixtures")]
                    for (node, attempt) in &trace_nodes {
                        let detail = match &commit {
                            Ok(()) if admitted_count == 0 => "scope=batch result=no_admitted_transition".to_string(),
                            Ok(()) => format!("scope=batch result=committed admitted={admitted_count}"),
                            Err(_) => format!("scope=batch result=error admitted={admitted_count} original_detail=private"),
                        };
                        build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_commit", &detail);
                    }
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
                            #[cfg(feature = "test-fixtures")]
                            for (node, attempt) in &trace_nodes {
                                build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_converge",
                                    &match &convergence { Ok(()) => "scope=batch result=ok".into(), Err(error) => format!("scope=batch result=error kind={} original_detail=private", build16_runner_error_kind(error)) });
                            }
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
                    #[cfg(feature = "test-fixtures")]
                    for (node, attempt) in &trace_nodes {
                        build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_load", "scope=batch result=error original_detail=private");
                    }
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
        let Some((node_id, mut outcome)) = self
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
        #[cfg(feature = "test-fixtures")]
        let diagnostic_ticket = if std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some() {
            self.active.lock().await.get(&(graph_id.to_string(), node_id.clone()))
                .filter(|active| Some(&active.binding) == expected_binding.as_ref())
                .map(|active| active.ticket.clone()).filter(k05_completion_trace_enabled)
        } else { None };
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "resolve_pump_return", None, None); }
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
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_check", None, None); }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_enter", None, None); }
            waiter.await;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_return", None, None); }
        }
        let active_wave = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.clone()))
            .filter(|active| {
                Some(active.binding.clone()).as_ref()
                    == expected_binding.as_ref()
            })
            .map(|active| {
                (
                    Arc::clone(&active.executor),
                    active.ticket.clone(),
                    active.effect_receipt_required,
                )
            });
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "active_wave_checked", None, Some(active_wave.is_some())); }
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
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_wave_enter", None, None); }
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
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_wave_return", None, None); }
            #[cfg(feature = "test-fixtures")]
            build16_callback_trace(graph_id, &node_id, Some(ticket.attempt), "terminal_wave_disposition",
                match &disposition {
                    TerminalCommitDisposition::Committed => "wave=committed",
                    TerminalCommitDisposition::Superseded => "wave=superseded",
                    TerminalCommitDisposition::Failed(_) => "wave=failed source=unknown original_detail=private",
                });
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
                    if let Err(cleanup) = self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("terminal_wave_commit_failed"),
                    ).await {
                        return Err(ExecutionRunnerError::TerminalWaveCleanup {
                            reason, phase: "original callback", cleanup: Box::new(cleanup),
                        });
                    }
                    if let Err(cleanup) = self.take_bound_active_node(
                        graph_id, &node_id, expected_binding.as_ref(),
                    ).await {
                        return Err(ExecutionRunnerError::TerminalWaveCleanup {
                            reason, phase: "original owner", cleanup: Box::new(cleanup),
                        });
                    }
                    return Err(ExecutionRunnerError::Driver(reason));
                }
            }
        }

        // Commit terminal graph truth before waking the pump or publishing
        // process-local executor output.
        let committed_executor = {
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "coordination_enter", None, None); }
            let mut coordination = self.graph_coordination_without_command(graph_id).await;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "coordination_acquired", None, None); }
            let mut current = self.state_store.load_async(graph_id).await?;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "graph_loaded", Some(current.revision), None); }
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
                drop(coordination);
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
            let (original_executor, ticket, _) = active_wave.as_ref().ok_or_else(|| {
                ExecutionRunnerError::Driver("atomic outcome binding missing".into())
            })?;
            // Rebuild from durable state after an explicitly retryable source
            // change. Every attempt still validates the original carrier tail;
            // an incompatible tail cannot be made valid by retrying it.
            const MAX_SOURCE_PREPARATION_ATTEMPTS: usize = 3;
            let mut preparation_attempt = 0;
            let source_result = loop {
                preparation_attempt += 1;
                let result = self.commit_service.execution_outcome_sources(&current, ticket, &outcome);
                if !matches!(&result, Err(error) if retryable_proof_source_change(error))
                    || preparation_attempt >= MAX_SOURCE_PREPARATION_ATTEMPTS
                {
                    break result;
                }
                drop(coordination);
                tokio::task::yield_now().await;
                coordination = self.graph_coordination_without_command(graph_id).await;
                current = self.state_store.load_async(graph_id).await?;
                let original_owner_live = self.active.lock().await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .is_some_and(|owner| {
                        Some(&owner.binding) == expected_binding.as_ref()
                            && owner.ticket == *ticket
                            && Arc::ptr_eq(&owner.executor, original_executor)
                            && !owner.callback_completed
                    });
                if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                    || !expected_binding.as_ref().is_some_and(|binding| {
                        current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt)
                    })
                    || !original_owner_live
                {
                    return Err(ExecutionRunnerError::Driver(
                        "node transition superseded during source preparation retry".into(),
                    ));
                }
            };
            let mut outcome_sources = match source_result {
                Ok(sources) => sources,
                Err(error) => {
                    // No outcome was committed.  This exact inconsistent
                    // source set cannot be retried as one transaction; retire
                    // its original binding through the fenced cancellation
                    // owner rather than leaving an apparently live owner.
                    let source_conflict = matches!(&error,
                        ExecutionCommitError::InvalidCommand(reason)
                        if reason == "observation source conflicts with causal read-set");
                    let revision = current.revision;
                    drop(coordination);
                    if source_conflict {
                        let retirement = self.command(graph_id, ExecutionGraphCommand::CancelNode {
                            expected_revision: revision,
                            node_id: node_id.clone(),
                            reason: format!("original outcome preparation failed: {error}"),
                        }).await;
                        if let Err(retirement_error) = retirement {
                            return Err(ExecutionRunnerError::Driver(format!(
                                "{error}; original binding retirement failed: {retirement_error}"
                            )));
                        }
                    } else if let Err(settlement_error) = self
                        .isolate_node_failure(
                            graph_id,
                            &node_id,
                            expected_binding.as_ref(),
                            format!("original outcome preparation failed: {error}"),
                        )
                        .await
                    {
                        // The settlement itself failed, so the node may still
                        // look live.  Report both errors instead of losing the
                        // original cause behind an unsettled binding.
                        return Err(ExecutionRunnerError::Driver(format!(
                            "{error}; original binding settlement failed: {settlement_error}"
                        )));
                    }
                    return Err(error.into());
                }
            };
            // A concurrent writer can advance this graph stream between the
            // load and the atomic outcome commit. Keep the captured outcome and
            // its original source fences while retrying instead of failing the whole
            // execution driver with a raw `revision mismatch`.
            const MAX_TRANSITION_ATTEMPTS: usize = 8;
            let mut attempt = 0;
            let mut owner_superseded = false;
            let transition = loop {
                attempt += 1;
                #[cfg(feature = "test-fixtures")]
                if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_cas_enter", Some(current.revision), None); }
                let result = self
                    .commit_service
                    .transition_execution_outcome_async(
                        current.clone(),
                        ticket.clone(),
                        outcome.clone(),
                        outcome_sources.clone(),
                    )
                    .await;
                #[cfg(feature = "test-fixtures")]
                if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_cas_return", Some(current.revision), Some(result.is_ok())); }
                #[cfg(feature = "test-fixtures")]
                if let (Some(ticket), Err(error)) = (&diagnostic_ticket, &result) { k05_completion_cas_error(ticket, error); }
                match result {
                    Ok(receipt) => break Ok(receipt),
                    Err(error @ ExecutionCommitError::EventStore(
                        crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { .. },
                    )) if attempt < MAX_TRANSITION_ATTEMPTS => {
                        let (stream, expected, actual) = match &error {
                            ExecutionCommitError::EventStore(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { stream_id, expected, actual }) => (stream_id.clone(), *expected, *actual),
                            _ => unreachable!(),
                        };
                        if stream != graph_id {
                            let (executor, original_ticket, _) = active_wave.as_ref().expect("checked active binding");
                            if !self.registry.is_builtin_tool_batch(executor)
                                || outcome_sources.get(&stream) != Some(&expected)
                                || !stream.starts_with("goal:")
                            {
                                break Err(error);
                            }
                            // The original owner may do bounded persistent reads;
                            // never hold graph coordination across that work.
                            drop(coordination);
                            let rebuilt = executor.revalidate_goal_source(
                                original_ticket, &outcome, &stream, expected, actual,
                            ).await;
                            coordination = self.graph_coordination_without_command(graph_id).await;
                            current = self.state_store.load_async(graph_id).await?;
                            let original_owner_live = self.active.lock().await
                                .get(&(graph_id.to_string(), node_id.clone()))
                                .is_some_and(|owner| {
                                    Some(&owner.binding) == expected_binding.as_ref()
                                        && owner.ticket == *original_ticket
                                        && Arc::ptr_eq(&owner.executor, executor)
                                        && !owner.callback_completed
                                });
                            if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                                || !expected_binding.as_ref().is_some_and(|binding| current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt))
                                || !original_owner_live
                            {
                                owner_superseded = !original_owner_live;
                                break Err(ExecutionCommitError::InvalidCommand("node transition superseded during Goal revalidation".into()));
                            }
                            let Ok(Some(candidate)) = rebuilt else { break Err(error); };
                            if validate_outcome(&node_id, &candidate).is_err()
                                || candidate.replan.as_ref().is_some_and(|plan| self.registry.validate_nodes(&plan.nodes).is_err())
                            {
                                break Err(error);
                            }
                            let candidate_sources = match self.commit_service.execution_outcome_sources(&current, original_ticket, &candidate) {
                                Ok(sources) => sources,
                                Err(source_error) => break Err(source_error),
                            };
                            if !accepts_goal_revalidation(&outcome, &candidate, &outcome_sources, &candidate_sources, &stream, expected, actual) {
                                break Err(error);
                            }
                            outcome_sources = candidate_sources;
                            outcome = candidate;
                            continue;
                        }
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
                if owner_superseded {
                    // A same-key replacement is not the original owner. Do
                    // not call its callback or release its leases.
                    return Err(error.into());
                }
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
                        Some(active.binding.clone()).as_ref()
                            == expected_binding.as_ref()
                    })
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                drop(coordination);
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
                    Some(active.binding.clone()).as_ref()
                        == expected_binding.as_ref()
                })
                .map(|active| (Arc::clone(&active.executor), active.ticket.clone()))
        };
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "post_commit_converge_enter", None, None); }
        let converged = self.converge_after_terminal_commit(graph_id).await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "post_commit_converge_return", None, Some(converged.is_ok())); }
        converged?;
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
                        Some(active.binding.clone()).as_ref() == expected_binding
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
            .map(|active| active.binding.clone())
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
        {
            let active = self.active.lock().await;
            let Some(owner) = active.get(&(graph_id.to_string(), ticket.node_id.clone()))
                .filter(|owner| owner.ticket == ticket && Some(&owner.binding) == expected_binding) else {
                return Err(ExecutionRunnerError::CommandSuperseded { node_id: ticket.node_id.clone() });
            };
            if self.command_intent_waiter(graph_id).is_some() {
                return Err(ExecutionRunnerError::CommandSuperseded { node_id: ticket.node_id.clone() });
            }
            owner.cancel_outcome.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).polled = true;
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
                Some(active.binding.clone()).as_ref() == expected_binding
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
                active.binding.clone(),
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
                    .is_some_and(|active| active.binding.clone() == *binding)
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
            let handoffs = if matches!(command, ExecutionGraphCommand::Cancel { .. } | ExecutionGraphCommand::CancelNode { .. }) {
                let table = self.active.lock().await;
                active.iter().filter_map(|(node_id, _, ticket)| {
                    table.get(&(graph_id.to_string(), node_id.clone()))
                        .filter(|owner| owner.ticket == *ticket && owner.effect_receipt_required)
                        .map(|owner| (node_id.clone(), ticket.clone(), Arc::clone(&owner.cancel_outcome)))
                }).collect::<Vec<_>>()
            } else { Vec::new() };
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

            if let Some((_, error)) = cancellation_errors.into_iter().next() {
                return Err(error);
            }
            let mut cancelled_outcomes = Vec::new();
            for (node_id, ticket, handoff) in handoffs {
                let deadline = tokio::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
                let outcome = loop {
                    let ready = handoff.ready.notified();
                    let snapshot = {
                        let state = handoff.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        (state.polled, state.finished, state.outcome.clone())
                    };
                    if !snapshot.0 { break None; }
                    if snapshot.1 {
                        let Some(outcome) = snapshot.2 else { break None; };
                        // A completed operation remains completed. An error,
                        // nonterminal result or outcome requiring another
                        // domain transition remains unknown to this command.
                        if !matches!(outcome.result.status, ExecutionNodeStatus::Cancelled | ExecutionNodeStatus::Completed)
                            || outcome.replan.is_some() || outcome.delivery_envelope.is_some()
                            || outcome.terminal_presentation.is_some() || outcome.no_dispatch.is_some() {
                            break None;
                        }
                        break Some(outcome);
                    }
                    if tokio::time::timeout_at(deadline, ready).await.is_err() {
                        return Err(ExecutionRunnerError::Driver("original cancelled executor outcome remains pending".into()));
                    }
                };
                if let Some(outcome) = outcome {
                    cancelled_outcomes.push(ExecutionTerminalCommit { node_id, outcome, effect_ticket:Some(ticket) });
                }
            }
            let coordination = self.graph_coordination(graph_id).await;
            let current = self.state_store.load_async(graph_id).await?;
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
                .apply_command_with_cancelled_outcomes_async(current, command.clone(), cancelled_outcomes)
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
                        .map(|active| active.binding.clone());
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
    #[cfg(feature = "test-fixtures")]
    let k05_trace = k05_completion_trace_enabled(ticket);
    #[cfg(feature = "test-fixtures")]
    if k05_trace { k05_completion_trace(ticket, "after_commit_enter", None, None); }
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
    #[cfg(feature = "test-fixtures")]
    if k05_trace { k05_completion_trace(ticket, "after_commit_return", None, Some(result.is_ok())); }
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
mod goal_revalidation_fence_tests {
    use super::*;

    #[test]
    fn terminal_wave_cleanup_keeps_original_reason_and_typed_error_source() {
        let cleanup = ExecutionRunnerError::Executor(NodeExecutorError::Cancel {
            node_id: "node".into(), reason: "physical close pending".into(),
        });
        let error = ExecutionRunnerError::TerminalWaveCleanup {
            reason: "terminal commit failed".into(), phase: "original owner", cleanup: Box::new(cleanup),
        };
        assert!(error.to_string().contains("terminal commit failed"));
        let source = std::error::Error::source(&error).unwrap()
            .downcast_ref::<ExecutionRunnerError>().unwrap();
        assert!(matches!(source, ExecutionRunnerError::Executor(NodeExecutorError::Cancel { .. })));
    }

    #[test]
    fn proof_source_retry_classifies_all_six_branches_without_accepting_other_errors() {
        for message in [
            "proof source changed while paging; retry current inputs",
            "proof source changed at final page boundary; retry current inputs",
            "proof Program source changed; retry current inputs",
            "proof Program source changed; retry current inputs; advanced=6 head=24 [event]",
            "proof physical lineage changed during discovery; retry current inputs",
            "proof source changed during validation; retry current inputs; source=goal:x expected=1 actual=2 phase=final_source_fence; advanced=1 head=2 []",
        ] {
            assert!(retryable_proof_source_change(&ExecutionCommitError::InvalidCommand(message.into())), "{message}");
        }
        for message in ["proof Goal spec changed", "observation source conflicts with causal read-set", "proof Program source changed; retry current inputs; forged"] {
            assert!(!retryable_proof_source_change(&ExecutionCommitError::InvalidCommand(message.into())), "{message}");
        }
    }

    #[test]
    fn goal_revalidation_preserves_physical_fact_and_every_other_source() {
        let original = deadline_exceeded_outcome("node", 42);
        let old = BTreeMap::from([("goal:one".to_owned(), 62), ("effect:one".to_owned(), 7)]);
        let next = BTreeMap::from([("goal:one".to_owned(), 63), ("effect:one".to_owned(), 7)]);
        let candidate = original.clone();
        assert!(accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:one", 62, 63));
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:foreign", 62, 63));
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:one", 62, 64));
        let mut changed = candidate.clone();
        changed.result.summary = Some("rewritten physical result".into());
        assert!(!accepts_goal_revalidation(&original, &changed, &old, &next, "goal:one", 62, 63));
        let mut changed_sources = next.clone();
        changed_sources.remove("effect:one");
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &changed_sources, "goal:one", 62, 63));
        changed_sources.insert("effect:one".into(), 8);
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &changed_sources, "goal:one", 62, 63));
    }
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
        revalidation: std::sync::Mutex<Option<Arc<GoalRevalidationProbe>>>,
    }

    struct GoalRevalidationProbe {
        entered: Notify,
        resume: Notify,
        calls: AtomicUsize,
        accept: bool,
    }

    #[async_trait::async_trait]
    impl ScopedNodeBackend for RefusedBackend617 {
        async fn revalidate_goal_source(
            &self, _ticket: &NodeExecutionTicket, outcome: &NodeExecutionOutcome,
            stream: &str, _expected: u64, actual: u64,
        ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
            let probe = self.revalidation.lock().unwrap().clone();
            let Some(probe) = probe else { return Ok(None); };
            if probe.calls.fetch_add(1, Ordering::SeqCst) != 0 { return Ok(None); }
            probe.entered.notify_one();
            probe.resume.notified().await;
            if !probe.accept { return Ok(None); }
            let mut rebuilt = outcome.clone();
            rebuilt.source_revisions.insert(stream.into(), actual);
            Ok(Some(rebuilt))
        }
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

    struct ClosureProbeExecutor;

    #[async_trait::async_trait]
    impl NodeExecutor for ClosureProbeExecutor {
        fn kind(&self) -> &str {
            "k05-closure-probe"
        }
        fn validate(
            &self,
            _: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
            Ok(())
        }
        async fn start(
            &self,
            context: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            Ok(NodeExecutionTicket {
                graph_id: context.graph.id.clone(),
                node_id: context.node.id.clone(),
                executor_kind: self.kind().into(),
                service_class: context.graph.service_class,
                attempt: context.attempt,
                idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
                payload_ref: context.node.payload_ref.clone(),
            })
        }
        async fn poll_or_await(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("k05 closure probe".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    struct CancelOutcomeProbeK05 {
        entered: Notify,
        stop: Notify,
        finished: Notify,
        returned: std::sync::atomic::AtomicBool,
        completed: bool,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for CancelOutcomeProbeK05 {
        fn kind(&self) -> &str { "k05-cancel-outcome-probe" }
        fn validate(&self, _: &harness_contract::execution_graph::ExecutionNodeSpec) -> Result<(), NodeExecutorError> { Ok(()) }
        async fn start(&self, context: NodeExecutionContext) -> Result<NodeExecutionTicket, NodeExecutorError> {
            Ok(NodeExecutionTicket { graph_id:context.graph.id.clone(), node_id:context.node.id.clone(),
                executor_kind:self.kind().into(), service_class:context.graph.service_class,
                attempt:context.attempt, idempotency_key:context.node.idempotency_key.clone(),
                payload_ref:context.node.payload_ref.clone() })
        }
        async fn poll_or_await(&self, _: &NodeExecutionTicket) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.entered.notify_one();
            self.stop.notified().await;
            self.returned.store(true, Ordering::SeqCst);
            self.finished.notify_one();
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status:if self.completed { ExecutionNodeStatus::Completed } else { ExecutionNodeStatus::Cancelled }, result_ref:None, wait_class:None,
                summary:Some("original executor cancellation outcome".into()), evidence_refs:Vec::new(),
                failure:None, usage:Default::default(), finished_at_ms:1,
            }))
        }
        async fn cancel(&self, _: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.stop.notify_one();
            self.finished.notified().await;
            Ok(())
        }
    }

    #[test]
    fn k05_host_transcript_command_requires_original_node_attempt_status_and_transaction() {
        use super::super::commit_service::host_transcript_cancel_command_result as extract;
        use super::super::events::{ExecutionGraphDelta, ExecutionGraphEvent};
        let mut before = ExecutionGraph::new("cancel transcript source");
        let node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel, "inline_model", "model");
        let node_id = node.id.clone();
        before.nodes.push(node);
        before.revision = 4;
        before.node_statuses.insert(node_id.clone(), ExecutionNodeStatus::Running);
        before.recovery_cursor.node_attempts.insert(node_id.clone(), 1);
        let ticket = NodeExecutionTicket { graph_id:before.id.clone(), node_id:node_id.clone(),
            executor_kind:"inline_model".into(), service_class:before.service_class,
            attempt:1, idempotency_key:"original-ticket".into(), payload_ref:"original-payload".into() };
        let mut after = before.clone();
        after.revision = 5;
        after.node_statuses.insert(node_id.clone(), ExecutionNodeStatus::Cancelled);
        after.node_results.insert(node_id.clone(), ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:None, evidence_refs:Vec::new(), failure:None,
            usage:Default::default(), finished_at_ms:1 });
        let delta = ExecutionGraphDelta::between(&before, &after);
        let transaction = format!("{}:command:cancel:5", before.id);
        let event = ExecutionGraphEvent::CommandApplied { command:"cancel".into(), reason:None, delta:delta.clone() };
        assert_eq!(extract(&event, &ticket, &transaction).unwrap().status, ExecutionNodeStatus::Cancelled);
        let mut foreign = ticket.clone(); foreign.attempt = 2;
        assert!(extract(&event, &foreign, &transaction).is_none());
        foreign = ticket.clone(); foreign.node_id = "foreign".into();
        assert!(extract(&event, &foreign, &transaction).is_none());
        assert!(extract(&event, &ticket, &format!("{}:command:cancel:6", before.id)).is_none());
        let wrong_command = ExecutionGraphEvent::CommandApplied { command:"pause".into(), reason:None, delta:delta.clone() };
        assert!(extract(&wrong_command, &ticket, &transaction).is_none());
        let mut wrong_delta = delta.clone();
        wrong_delta.node_status_updates.insert(node_id.clone(), ExecutionNodeStatus::Completed);
        let wrong_status = ExecutionGraphEvent::CommandApplied { command:"cancel".into(), reason:None, delta:wrong_delta };
        assert!(extract(&wrong_status, &ticket, &transaction).is_none());
        let checkpoint = ExecutionGraphEvent::Checkpoint { cause:"execution_graph.command_applied".into(),
            graph:after, bindings:Default::default(), lease_closures:Vec::new() };
        assert!(extract(&checkpoint, &ticket, &transaction).is_some());
        assert!(extract(&checkpoint, &ticket, "foreign:command:cancel:5").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn k05_host_transcript_original_cancel_command_is_atomic_and_foreign_rejected() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let mut source = ExecutionGraph::new("original inline model transcript cancellation");
        source.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"session-617".into(), turn_id:"turn-617".into(),
            root_task_id:"task-617".into(), task_id:"task-617".into(), generation:1 });
        source.nodes.push(harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel, "inline_model", "original-model"));
        let graph_id = runner.register(source).await.unwrap().id;
        let snapshot = runner.pump_snapshot(&graph_id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&graph_id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().ticket.clone();
        let graph = runner.state_store.load(&graph_id).unwrap();
        let event = services.commit_service().prepare_host_transcript(&ticket, "session-617", "turn-617", 0,
            format!("sha256:{}", "0".repeat(64)), Vec::new(), None).unwrap();
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:None, evidence_refs:Vec::new(), failure:None,
            usage:Default::default(), finished_at_ms:1 });
        let mut missing_session_proof = outcome.result.clone();
        missing_session_proof.summary = Some("original local provider response observed authorized cancellation".into());
        missing_session_proof.result_ref = Some(format!("session:session-617:physical:5:sha256:{}", "0".repeat(64)));
        assert!(services.commit_service().validate_cold_provider_cancellation(&ticket, &missing_session_proof).is_err());
        super::super::commit_service::ExecutionCommitService::attach_host_transcript(&mut outcome, event).unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:graph.revision, reason:"original cancellation".into() };
        let mut foreign = outcome.clone();
        foreign.domain_events[0].event.payload["attempt"] = serde_json::json!(ticket.attempt + 1);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:foreign, effect_ticket:Some(ticket.clone())}]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome, effect_ticket:Some(ticket.clone())}]).unwrap();
        assert!(services.commit_service().committed_host_transcript(&ticket).unwrap().messages.is_empty());
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&graph_id).unwrap();
        assert!(graph_events.iter().any(|event| event.transaction_id == receipt.transaction_id && event.commit_cursor == receipt.commit_cursor));
    }

    // Original runner/cancel command counterexample: the physical executor
    // returned its actual outcome, but cancel removed the original binding
    // before the pump could publish the effect receipt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_original_cancel_drops_actual_executor_outcome_before_effect_receipt() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let executor = Arc::new(CancelOutcomeProbeK05 { entered:Notify::new(), stop:Notify::new(),
            finished:Notify::new(), returned:std::sync::atomic::AtomicBool::new(false), completed:false });
        services.executor_registry().register(executor.clone()).unwrap();
        let mut graph = ExecutionGraph::new("k05 real cancellation effect handoff");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"k05-cancel-session".into(), turn_id:"k05-cancel-turn".into(),
            root_task_id:"k05-cancel-task".into(), task_id:"k05-cancel-task".into(), generation:1 });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify, "k05-cancel-outcome-probe", "probe");
        node.resource_scopes.push("write:k05-cancel".into());
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner.pump_snapshot(&registered.id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&registered.id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let binding = prepared.binding.clone().unwrap();
        let ticket = runner.active.lock().await.get(&(registered.id.clone(), node_id.clone())).unwrap().ticket.clone();
        let worker_runner = runner.clone(); let worker_graph = registered.id.clone();
        let worker = tokio::spawn(async move { worker_runner.execute_prepared_pump_node(&worker_graph, prepared, Arc::new(Notify::new())).await });
        tokio::time::timeout(Duration::from_secs(3), executor.entered.notified()).await.unwrap();
        let current = runner.state_store.load_async(&registered.id).await.unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:current.revision, reason:"controlled original cancellation".into() };
        let fixture_outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:Some("original executor cancellation outcome".into()),
            evidence_refs:Vec::new(), failure:None, usage:Default::default(), finished_at_ms:1 });
        let mut foreign = ticket.clone(); foreign.attempt += 1;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(foreign) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        let mut noncancel = fixture_outcome.clone(); noncancel.result.status = ExecutionNodeStatus::Failed;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:noncancel, effect_ticket:Some(ticket.clone()) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        runner.command(&registered.id, command.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker).await.unwrap().unwrap().unwrap();
        assert!(executor.returned.load(Ordering::SeqCst));
        let graph = runner.state_store.load(&registered.id).unwrap();
        let start = runner.state_store.binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(runner.state_store.binding_lease_closure(&graph, &node_id, &binding, start).unwrap());
        let actual_outcome = match services.commit_service().inspect_execution_effect(&ticket).unwrap() {
            ExecutionEffectState::Completed(outcome) => outcome,
            _ => panic!("the original executor outcome must be atomically recorded with cancellation"),
        };
        assert_eq!(actual_outcome.result.status, ExecutionNodeStatus::Cancelled);
        assert_eq!(actual_outcome.result.summary, fixture_outcome.result.summary);
        assert!(actual_outcome.result.usage.duration_ms >= 1);
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        assert_eq!(effects.len(), 2);
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&registered.id).unwrap();
        let command_event = graph_events.iter().find(|event| event.transaction_id == receipt.transaction_id).unwrap();
        assert_eq!(command_event.commit_cursor, receipt.commit_cursor);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(ticket.clone())}]).is_err());
        assert_eq!(services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap().len(), 2);
        let cold_store = ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let replayed = cold_store.load(&registered.id).unwrap();
        assert_eq!(replayed.node_statuses.get(&node_id), Some(&ExecutionNodeStatus::Cancelled));
        assert_eq!(replayed.node_results.get(&node_id), Some(&actual_outcome.result));
        let cold_commits = super::super::commit_service::ExecutionCommitService::new(Arc::clone(services.event_store()));
        assert!(matches!(cold_commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(outcome) if outcome == actual_outcome));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_cancel_preserves_actual_completed_outcome_in_atomic_receipt() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let executor = Arc::new(CancelOutcomeProbeK05 { entered:Notify::new(), stop:Notify::new(),
            finished:Notify::new(), returned:std::sync::atomic::AtomicBool::new(false), completed:true });
        services.executor_registry().register(executor.clone()).unwrap();
        let mut graph = ExecutionGraph::new("k05 real cancellation effect handoff");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"k05-cancel-session".into(), turn_id:"k05-cancel-turn".into(),
            root_task_id:"k05-cancel-task".into(), task_id:"k05-cancel-task".into(), generation:1 });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify, "k05-cancel-outcome-probe", "probe");
        node.resource_scopes.push("write:k05-cancel".into());
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner.pump_snapshot(&registered.id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&registered.id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let binding = prepared.binding.clone().unwrap();
        let ticket = runner.active.lock().await.get(&(registered.id.clone(), node_id.clone())).unwrap().ticket.clone();
        let worker_runner = runner.clone(); let worker_graph = registered.id.clone();
        let worker = tokio::spawn(async move { worker_runner.execute_prepared_pump_node(&worker_graph, prepared, Arc::new(Notify::new())).await });
        tokio::time::timeout(Duration::from_secs(3), executor.entered.notified()).await.unwrap();
        let current = runner.state_store.load_async(&registered.id).await.unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:current.revision, reason:"controlled original cancellation".into() };
        let fixture_outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Completed,
            result_ref:None, wait_class:None, summary:Some("original executor cancellation outcome".into()),
            evidence_refs:Vec::new(), failure:None, usage:Default::default(), finished_at_ms:1 });
        let mut foreign = ticket.clone(); foreign.attempt += 1;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(foreign) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        let mut noncancel = fixture_outcome.clone(); noncancel.result.status = ExecutionNodeStatus::Failed;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:noncancel, effect_ticket:Some(ticket.clone()) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        runner.command(&registered.id, command.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker).await.unwrap().unwrap().unwrap();
        assert!(executor.returned.load(Ordering::SeqCst));
        let graph = runner.state_store.load(&registered.id).unwrap();
        let start = runner.state_store.binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(runner.state_store.binding_lease_closure(&graph, &node_id, &binding, start).unwrap());
        let actual_outcome = match services.commit_service().inspect_execution_effect(&ticket).unwrap() {
            ExecutionEffectState::Completed(outcome) => outcome,
            _ => panic!("the original executor outcome must be atomically recorded with cancellation"),
        };
        assert_eq!(actual_outcome.result.status, ExecutionNodeStatus::Completed);
        assert_eq!(actual_outcome.result.summary, fixture_outcome.result.summary);
        assert!(actual_outcome.result.usage.duration_ms >= 1);
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        assert_eq!(effects.len(), 2);
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&registered.id).unwrap();
        let command_event = graph_events.iter().find(|event| event.transaction_id == receipt.transaction_id).unwrap();
        assert_eq!(command_event.commit_cursor, receipt.commit_cursor);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(ticket.clone())}]).is_err());
        assert_eq!(services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap().len(), 2);
        let cold_store = ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let replayed = cold_store.load(&registered.id).unwrap();
        assert_eq!(replayed.node_statuses.get(&node_id), Some(&ExecutionNodeStatus::Completed));
        assert_eq!(replayed.node_results.get(&node_id), Some(&actual_outcome.result));
        let cold_commits = super::super::commit_service::ExecutionCommitService::new(Arc::clone(services.event_store()));
        assert!(matches!(cold_commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(outcome) if outcome == actual_outcome));
    }

    async fn prepare_scoped_k05(
        runner: &ExecutionGraphRunner,
        session: &str,
        path: &str,
    ) -> (String, ExecutionPreparedPumpNode) {
        let mut graph = ExecutionGraph::new("k05 scoped closure");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session.into(),
            turn_id: format!("{session}-turn"),
            root_task_id: format!("{session}-task"),
            task_id: format!("{session}-task"),
            generation: 1,
        });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify,
            "k05-closure-probe",
            "k05",
        );
        node.resource_scopes.push(format!("write:{path}"));
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        let mut ready = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        let prepared = ready.remove(0);
        assert!(prepared.binding.as_ref().unwrap().scope_lease_ref.is_some());
        (registered.id, prepared)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_shared_runner_slow_original_closure_does_not_block_other_session() {
        let (_workspace, services, runner, graph_a, prepared_a, _) = original_prepared617().await;
        // Retire the fixture's original unpolled owner before creating two scoped
        // leaf graphs on the very same runner and resource managers.
        runner
            .execute_prepared_pump_node(&graph_a, prepared_a, Arc::new(Notify::new()))
            .await
            .unwrap();
        services
            .executor_registry()
            .register(Arc::new(ClosureProbeExecutor))
            .unwrap();
        let (a, a_node) = prepare_scoped_k05(&runner, "k05-session-a", "a").await;
        let (b, b_node) = prepare_scoped_k05(&runner, "k05-session-b", "b").await;
        let a_binding = a_node.binding.clone().unwrap();
        let b_binding = b_node.binding.clone().unwrap();
        let a_id = a_node.node.id.clone();
        let b_id = b_node.node.id.clone();
        let entered = Arc::new(Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = a.clone();
        let probe_entered = entered.clone();
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |graph| {
            if graph == probe_graph {
                probe_entered.notify_one();
                // A bounded fallback guarantees that a broken assertion cannot
                // leave the original owner holding the shared lock indefinitely.
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15));
            }
        }));
        let a_runner = runner.clone();
        let a_graph = a.clone();
        let task_a = tokio::spawn(async move {
            a_runner
                .execute_prepared_pump_node(&a_graph, a_node, Arc::new(Notify::new()))
                .await
        });
        let hit = tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .is_ok();
        let b_runner = runner.clone();
        let b_graph = b.clone();
        let mut task_b = tokio::spawn(async move {
            b_runner
                .execute_prepared_pump_node(&b_graph, b_node, Arc::new(Notify::new()))
                .await
        });
        let progress = if hit {
            tokio::time::timeout(Duration::from_secs(2), &mut task_b)
                .await
                .ok()
        } else {
            None
        };
        let b_progressed = progress.is_some();
        let _ = release_tx.send(());
        let result_a = task_a.await.unwrap();
        let result_b = match progress {
            Some(result) => result.unwrap(),
            None => task_b.await.unwrap(),
        };
        *runner.closure_probe.lock().unwrap() = None;
        result_a.unwrap();
        result_b.unwrap();
        for (graph, node, binding) in [(&a, &a_id, &a_binding), (&b, &b_id, &b_binding)] {
            let state = runner.state_store.load(graph).unwrap();
            let position = runner
                .state_store
                .binding_start_position(&state, node, binding)
                .unwrap()
                .unwrap();
            assert!(runner
                .state_store
                .binding_lease_closure(&state, node, binding, position)
                .unwrap());
            assert!(
                !runner
                    .active_binding_matches(graph, node, Some(binding))
                    .await
            );
        }
        assert!(runner.drain_physical_execution().await.is_empty());
        assert!(hit, "original A closure barrier was not reached");
        assert!(
            b_progressed,
            "independent B could not finish while A held the original closure barrier"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_cancelled_closure_waiter_retains_original_owner_until_retry() {
        let (_workspace, services, runner, original_graph, original_node, _) =
            original_prepared617().await;
        runner
            .execute_prepared_pump_node(&original_graph, original_node, Arc::new(Notify::new()))
            .await
            .unwrap();
        services
            .executor_registry()
            .register(Arc::new(ClosureProbeExecutor))
            .unwrap();
        let (graph, node) = prepare_scoped_k05(&runner, "k05-cancel-session", "cancel").await;
        let binding = node.binding.clone().unwrap();
        let node_id = node.node.id.clone();
        let mut foreign = binding.clone();
        foreign.attempt += 1;
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&foreign))
            .await
            .unwrap()
            .is_none());
        assert!(
            runner
                .active_binding_matches(&graph, &node_id, Some(&binding))
                .await
        );
        let entered = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = graph.clone();
        let probe_entered = entered.clone();
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == probe_graph {
                probe_entered.notify_one();
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15));
            }
        }));
        let completed_graph = graph.clone();
        let completed_notify = completed.clone();
        *runner.closure_finished_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == completed_graph {
                completed_notify.notify_one();
            }
        }));
        let task_runner = runner.clone();
        let task_graph = graph.clone();
        let task = tokio::spawn(async move {
            task_runner
                .execute_prepared_pump_node(&task_graph, node, Arc::new(Notify::new()))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        // Hold the table only after the closure has left it; this forces the
        // first waiter to suspend after durable closure while the entry and
        // original fence remain owned by the table.
        let active = runner.active.lock().await;
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), completed.notified())
            .await
            .unwrap();
        task.abort();
        assert!(tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        let retained = active.get(&(graph.clone(), node_id.clone())).unwrap();
        assert_eq!(retained.binding, binding);
        assert!(retained.resources.lock().unwrap().owner_fence.is_some());
        drop(active);
        *runner.closure_probe.lock().unwrap() = None;
        *runner.closure_finished_probe.lock().unwrap() = None;
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&binding))
            .await
            .unwrap()
            .is_some());
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&binding))
            .await
            .unwrap()
            .is_none());
        let state = runner.state_store.load(&graph).unwrap();
        let position = runner
            .state_store
            .binding_start_position(&state, &node_id, &binding)
            .unwrap()
            .unwrap();
        assert!(runner
            .state_store
            .binding_lease_closure(&state, &node_id, &binding, position)
            .unwrap());
        assert!(runner.drain_physical_execution().await.is_empty());
    }

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
            revalidation: std::sync::Mutex::new(None),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn k05_original_runner_goal_revalidation_releases_coordination_and_keeps_fences() {
        for mode in ["accept", "unknown", "source_advances", "binding_changes", "ticket_changes", "executor_changes"] {
            let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
            let probe = Arc::new(GoalRevalidationProbe {
                entered: Notify::new(), resume: Notify::new(), calls: AtomicUsize::new(0),
                accept: mode != "unknown",
            });
            *backend.revalidation.lock().unwrap() = Some(probe.clone());
            let binding = prepared.binding.clone().unwrap();
            let (node_id, mut outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
            outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
                "envelope_id":"k05-envelope","revision":1,"objective_id":"k05-objective","created_at_ms":1
            })).unwrap());
            let goal = format!("goal:k05-revalidation-{mode}");
            outcome.source_revisions.insert(goal.clone(), 0);
            let append_goal = || services.event_store().append(crate::RuntimeEventInput {
                stream_id: goal.clone(), scope: crate::RuntimeEventScope::Goal,
                kind: "test.k05_goal_advance".into(), status: None, actor: None,
                refs: Vec::new(), payload: serde_json::json!({}),
            }).unwrap();
            append_goal();
            let task_runner = runner.clone();
            let task_graph = graph_id.clone();
            let task_node = node_id.clone();
            let task = tokio::spawn(async move {
                task_runner.finish_pump_node(&task_graph, Ok((task_node, outcome)), Arc::new(Notify::new()), Some(binding)).await
            });
            tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await.unwrap();
            // A competing graph operation must be able to acquire the actual
            // coordination guard while the original backend is held here.
            let guard = tokio::time::timeout(Duration::from_secs(5), runner.graph_coordination_without_command(&graph_id)).await.unwrap();
            assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&prepared.binding.unwrap())).await);
            if mode == "source_advances" { append_goal(); }
            if mode == "binding_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().binding.attempt += 1;
            }
            if mode == "ticket_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().ticket.payload_ref = "foreign".into();
            }
            if mode == "executor_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().executor = Arc::new(ClosureProbeExecutor);
            }
            drop(guard);
            probe.resume.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
            if mode == "accept" { assert!(result.is_ok(), "{result:?}"); }
            else { assert!(result.is_err(), "{mode} unexpectedly committed"); }
            if matches!(mode, "binding_changes" | "ticket_changes" | "executor_changes") {
                assert!(runner.active.lock().await.contains_key(&(graph_id.clone(), node_id.clone())), "foreign owner must not be released");
            }
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1, "physical tool must not replay");
            let graph = services.graph_state_store().load(&graph_id).unwrap();
            if mode == "accept" { assert_eq!(graph.node_statuses[&node_id], ExecutionNodeStatus::Completed); }
            else { assert_ne!(graph.node_statuses[&node_id], ExecutionNodeStatus::Completed); }
        }
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
    async fn r601_mixed_goal_cut_retires_only_original_binding_with_physical_closure() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, mut outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
            "envelope_id":"601-envelope","revision":1,"objective_id":"601-objective","created_at_ms":1
        })).unwrap());
        outcome.source_revisions.insert("goal:601-conflict".into(), 1);
        outcome.domain_events.push(crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "goal:601-conflict".into(),
                scope: crate::RuntimeEventScope::Goal,
                kind: "goal.observation".into(),
                status: None, actor: None, refs: Vec::new(),
                payload: serde_json::json!({"goal_source":{"stream_id":"goal:601-conflict","sequence":2}}),
            },
            schema_version: 2,
            idempotency_key: Some("601-conflict".into()),
        });
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let stale = runner.command(&graph_id, ExecutionGraphCommand::CancelNode {
            expected_revision: graph.revision + 1,
            node_id: node_id.clone(),
            reason: "r601 stale owner must not retire".into(),
        }).await;
        assert!(stale.is_err());
        assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        let error = runner.finish_pump_node(&graph_id, Ok((node_id.clone(), outcome)), Arc::new(Notify::new()), Some(binding.clone())).await.unwrap_err();
        assert!(error.to_string().contains("observation source conflicts with causal read-set"), "{error}");
        let retired = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(retired.node_statuses[&node_id], ExecutionNodeStatus::Cancelled);
        assert!(services.graph_state_store().binding_retirement_record(&retired, &node_id, &binding).unwrap().is_some());
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert!(runner.drain_physical_execution().await.is_empty());
        assert!(runner.recover_graph(&graph_id).await.is_ok());
    }

    #[tokio::test]
    async fn r623_foreign_conflicting_outcome_cannot_retire_original_binding() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, mut outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
            "envelope_id":"623-envelope","revision":1,"objective_id":"623-objective","created_at_ms":1
        })).unwrap());
        outcome.source_revisions.insert("goal:623-conflict".into(), 1);
        outcome.domain_events.push(crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "goal:623-conflict".into(), scope: crate::RuntimeEventScope::Goal,
                kind: "goal.observation".into(), status: None, actor: None, refs: Vec::new(),
                payload: serde_json::json!({"goal_source":{"stream_id":"goal:623-conflict","sequence":2}}),
            },
            schema_version: 2, idempotency_key: Some("623-conflict".into()),
        });
        let before = services.graph_state_store().load(&graph_id).unwrap();
        let mut foreign = binding.clone();
        foreign.attempt += 1;
        runner.finish_pump_node(&graph_id, Ok((node_id.clone(), outcome)), Arc::new(Notify::new()), Some(foreign)).await.unwrap();
        let after = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(after, before);
        assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert!(services.graph_state_store().binding_retirement_record(&after, &node_id, &binding).unwrap().is_none());
        let retired = runner.command(&graph_id, ExecutionGraphCommand::CancelNode {
            expected_revision: after.revision, node_id: node_id.clone(), reason: "r623 fixture cleanup".into(),
        }).await.unwrap();
        assert!(services.graph_state_store().binding_retirement_record(&retired, &node_id, &binding).unwrap().is_some());
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
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
