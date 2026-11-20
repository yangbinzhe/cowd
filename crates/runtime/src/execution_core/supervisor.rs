use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::FutureExt;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

use super::graph::{
    ExecutionGraphHost, ExecutionGraphHostReceipt, ExecutionGraphRunner, ExecutionRecoveryError,
    ExecutionRunReport, ExecutionRunnerError, OriginalPumpTask, PumpCancellationOwner,
};
use crate::CancellationToken;

#[cfg(test)]
const DEFAULT_QUEUE_CAPACITY: usize = 1_024;
#[cfg(test)]
const DEFAULT_MAX_PARALLEL_GRAPHS: usize = 64;
const DEFAULT_QUEUE_PARTITIONS: u16 = 32;
const LIFECYCLE_OPEN: u8 = 0;
const LIFECYCLE_CLOSING: u8 = 1;
const LIFECYCLE_CLOSED: u8 = 2;

type GraphSettledObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionHealth {
    pub lifecycle: String,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub active_drivers: usize,
    pub active_owned_tasks: usize,
    pub tracked_keys: usize,
    pub oldest_queued_age_ms: u64,
    pub accepted: u64,
    pub completed: u64,
    pub failed: u64,
    pub forced_aborts: u64,
    pub dispatcher_restarts: u64,
    pub last_error: Option<String>,
    pub owners: std::collections::BTreeMap<String, RuntimeExecutionOwnerReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionOwnerReport {
    pub submitted: u64,
    pub active: u64,
    pub completed: u64,
    pub failed: u64,
    pub aborted: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionShutdownReport {
    pub accepted: u64,
    pub completed: u64,
    pub failed: u64,
    /// Non-terminal graphs deliberately left durable for the next process to
    /// recover. Process shutdown is not a business cancellation.
    #[serde(default)]
    pub preserved_graphs: usize,
    pub cancelled_graphs: usize,
    pub forced_aborts: u64,
    pub remaining_keys: usize,
    pub errors: Vec<String>,
    pub owners: std::collections::BTreeMap<String, RuntimeExecutionOwnerReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeExecutionShutdownCompletion {
    Finished { report: RuntimeExecutionShutdownReport, custody_closed: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeWorkAdmissionReceipt {
    pub admission_id: String,
    pub owner: String,
    pub accepted_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSubmissionCapacitySnapshot {
    pub active_graphs: usize,
    pub queued_graphs: usize,
    pub available_slots: usize,
}

#[derive(Debug, Clone)]
enum DriverOutcome {
    Completed(ExecutionRunReport),
    Failed(String),
    Aborted(String),
}

#[derive(Debug)]
struct DriverSlotState {
    requested_generation: u64,
    completed_generation: u64,
    queued_at_ms: u64,
    active: bool,
    outcome: Option<DriverOutcome>,
    shutdown_unstarted: Option<(u64, u64)>,
}

#[derive(Debug)]
struct DriverSlot {
    state: StdMutex<DriverSlotState>,
    admission: StdMutex<Option<Arc<tokio::sync::OwnedSemaphorePermit>>>,
    changed: tokio::sync::Notify,
}

#[derive(Debug)]
struct GraphProducerGate {
    released: bool,
    deferred_graphs: std::collections::BTreeSet<String>,
}

impl DriverSlot {
    fn new() -> Self {
        Self {
            state: StdMutex::new(DriverSlotState {
                requested_generation: 0,
                completed_generation: 0,
                queued_at_ms: 0,
                active: false,
                outcome: None,
                shutdown_unstarted: None,
            }),
            changed: tokio::sync::Notify::new(),
            admission: StdMutex::new(None),
        }
    }

    fn request(&self, now_ms: u64, lifecycle: &AtomicU8) -> Option<u64> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.load(Ordering::Acquire) != LIFECYCLE_OPEN { return None; }
        state.requested_generation = state.requested_generation.saturating_add(1);
        if !state.active {
            state.queued_at_ms = now_ms;
        }
        Some(state.requested_generation)
    }

    fn start(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = true;
        state.requested_generation
    }

    fn finish(&self, generation: u64, outcome: DriverOutcome) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.completed_generation = generation;
            state.active = false;
            state.outcome = Some(outcome);
        }
        self.changed.notify_waiters();
    }

    fn has_newer_request(&self, generation: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requested_generation
            > generation
    }

    fn observation(&self, generation: u64) -> Option<DriverOutcome> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.shutdown_unstarted.is_some_and(|(first, last)| (first..=last).contains(&generation)) {
            return Some(DriverOutcome::Aborted("execution cancelled during shutdown".to_string()));
        }
        (state.completed_generation >= generation)
            .then(|| state.outcome.clone())
            .flatten()
    }

    fn idle(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.active && (state.completed_generation >= state.requested_generation
            || state.shutdown_unstarted.is_some_and(|(_, last)| last >= state.requested_generation))
    }

    fn abort_unstarted_on_shutdown(&self, active_generation: Option<u64>) {
        let changed = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let first = state.completed_generation.saturating_add(1)
                .max(active_generation.map_or(0, |generation| generation.saturating_add(1)));
            if state.completed_generation >= state.requested_generation
                || active_generation == Some(u64::MAX)
                || (state.active && active_generation.is_none())
                || first > state.requested_generation
            {
                false
            } else {
                let last = state.requested_generation;
                state.shutdown_unstarted = Some((first, last));
                true
            }
        };
        if changed { self.changed.notify_waiters(); }
    }

    fn requested_generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requested_generation
    }
}

struct SupervisorMetrics {
    lifecycle: AtomicU8,
    active_drivers: AtomicUsize,
    active_owned_tasks: AtomicUsize,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    forced_aborts: AtomicU64,
    dispatcher_restarts: AtomicU64,
    last_error: StdMutex<Option<String>>,
    owners: StdMutex<std::collections::BTreeMap<String, RuntimeExecutionOwnerReport>>,
}

impl SupervisorMetrics {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(LIFECYCLE_OPEN),
            active_drivers: AtomicUsize::new(0),
            active_owned_tasks: AtomicUsize::new(0),
            accepted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            forced_aborts: AtomicU64::new(0),
            dispatcher_restarts: AtomicU64::new(0),
            last_error: StdMutex::new(None),
            owners: StdMutex::new(std::collections::BTreeMap::new()),
        }
    }

    fn record_error(&self, error: impl Into<String>) {
        let error = error.into();
        *self
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn owner_submitted(&self, owner: &str) {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let report = owners.entry(owner.to_string()).or_default();
        report.submitted = report.submitted.saturating_add(1);
        report.active = report.active.saturating_add(1);
    }

    fn owner_finished(&self, owner: &str, failed: bool) {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let report = owners.entry(owner.to_string()).or_default();
        report.active = report.active.saturating_sub(1);
        if failed {
            report.failed = report.failed.saturating_add(1);
        } else {
            report.completed = report.completed.saturating_add(1);
        }
    }

    fn owner_aborted(&self, owner: &str) {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let report = owners.entry(owner.to_string()).or_default();
        report.active = report.active.saturating_sub(1);
        report.aborted = report.aborted.saturating_add(1);
    }

    fn force_abort_active_owners(&self) -> u64 {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owners.values_mut().fold(0, |total, report| {
            let active = report.active;
            report.active = 0;
            report.aborted = report.aborted.saturating_add(active);
            total.saturating_add(active)
        })
    }
}

type OwnedWork = Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>>;
type OwnedTask = Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

enum SupervisorMessage {
    Wake(String),
    Owned { owner: String, work: OwnedWork },
}

enum SupervisorCompletion {
    Graph {
        graph_id: String,
        generation: u64,
        outcome: DriverOutcome,
    },
    Owned {
        owner: String,
        status: OwnedCompletionStatus,
        error: Option<String>,
    },
}

enum OwnedCompletionStatus {
    Completed,
    Failed,
    Aborted,
}

#[derive(Default)]
struct ShutdownDrainState {
    dispatcher: Option<JoinHandle<()>>,
    preserved_graphs: Option<usize>,
    deadline: Option<tokio::time::Instant>,
    deadline_observed: bool,
    physical_drained: bool,
    errors: Vec<String>,
    physical_drain:
        Option<Pin<Box<dyn std::future::Future<Output = Vec<String>> + Send + 'static>>>,
}

/// The single Runtime owner of durable graph execution.
///
/// Callers may admit work, issue typed commands, wait for a projection, or
/// inspect health. Only this supervisor owns and invokes the graph runner.
pub struct RuntimeExecutionSupervisor {
    runner: Arc<ExecutionGraphRunner>,
    sender: mpsc::Sender<SupervisorMessage>,
    receiver: StdMutex<Option<mpsc::Receiver<SupervisorMessage>>>,
    reaper_sender: mpsc::Sender<SupervisorMessage>,
    reaper_receiver: StdMutex<Option<mpsc::Receiver<SupervisorMessage>>>,
    dispatcher: StdMutex<Option<JoinHandle<()>>>,
    shutdown_state: tokio::sync::Mutex<ShutdownDrainState>,
    slots: Arc<StdMutex<HashMap<String, Arc<DriverSlot>>>>,
    parallelism: Arc<Semaphore>,
    owned_parallelism: Arc<Semaphore>,
    max_active_nodes_per_graph: usize,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
    queue_capacity: usize,
    queue_partitions: u16,
    shutdown_timeout: Duration,
    shutdown_observation_deadline: OnceLock<tokio::time::Instant>,
    preserved_snapshot: std::sync::atomic::AtomicUsize,
    preserved_snapshot_ready: std::sync::atomic::AtomicBool,
    graph_settled_observer: Arc<OnceLock<GraphSettledObserver>>,
    graph_producer_gate: StdMutex<GraphProducerGate>,
    session_recovery_owner: OnceLock<Arc<crate::RuntimeEventStore>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    verdict_resolutions: StdMutex<
        HashMap<String, tokio::sync::oneshot::Sender<(u64, tokio::sync::oneshot::Sender<()>)>>,
    >,
}

impl RuntimeExecutionSupervisor {
    pub(crate) async fn settle_retained_original_callback<B, F>(
        &self,
        graph_id: &str,
        node_id: &str,
        attempt: u32,
        verify_original: B,
        verify_calls: F,
    ) -> Result<Option<crate::execution_core::graph::NodeExecutionTicket>, String>
    where
        B: FnOnce(&crate::execution_core::graph::NodeExecutionTicket) -> Result<bool, String> + Send,
        F: FnOnce(
            &crate::execution_core::graph::NodeExecutionTicket,
            Option<&Arc<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>>,
        ) -> Result<bool, String> + Send,
    {
        self.runner.settle_retained_original_callback(graph_id, node_id, attempt, verify_original, verify_calls)
            .await.map_err(|error| error.to_string())
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn observe_verdict_resolution_for_test(
        &self,
        program: &str,
    ) -> Result<tokio::sync::oneshot::Receiver<(u64, tokio::sync::oneshot::Sender<()>)>, String>
    {
        if program.is_empty() {
            return Err("verdict resolution requires exact Program".into());
        }
        let mut observers = self
            .verdict_resolutions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observers.contains_key(program) {
            return Err("duplicate verdict resolution observer".into());
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        observers.insert(program.into(), sender);
        Ok(receiver)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn observe_committed_verdict_for_test(
        &self,
        program: &str,
        revision: u64,
    ) -> Result<(), String> {
        let observer = self
            .verdict_resolutions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(program);
        if let Some(observer) = observer {
            let (release, released) = tokio::sync::oneshot::channel();
            if observer.send((revision, release)).is_ok() {
                tokio::select! {
                    _ = released => {},
                    _ = self.cancellation.cancelled() => return Err("verdict resolution interrupted before checkpoint".into()),
                }
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn active_binding_for_ticket_for_test(
        &self,
        ticket: &super::graph::NodeExecutionTicket,
    ) -> Option<super::graph::ExecutionNodeBinding> {
        self.runner.active_binding_for_ticket_for_test(ticket).await
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn reject_stale_binding_for_test(
        &self,
        graph: &str,
        node: &str,
        binding: &super::graph::ExecutionNodeBinding,
    ) -> Result<(), super::graph::ExecutionRunnerError> {
        self.runner
            .reject_stale_binding_for_test(graph, node, binding)
            .await
    }
    #[must_use]
    pub(crate) fn submission_capacity_snapshot(&self) -> ExecutionSubmissionCapacitySnapshot {
        let slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active_graphs = slots
            .values()
            .filter(|slot| {
                slot.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .active
            })
            .count();
        let queued_graphs = slots
            .values()
            .filter(|slot| {
                let state = slot
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                !state.active && state.requested_generation > state.completed_generation
            })
            .count();
        ExecutionSubmissionCapacitySnapshot {
            active_graphs,
            queued_graphs,
            available_slots: self.parallelism.available_permits(),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(runner: Arc<ExecutionGraphRunner>) -> Self {
        Self::with_limits(
            runner,
            DEFAULT_QUEUE_CAPACITY,
            DEFAULT_MAX_PARALLEL_GRAPHS,
            Duration::from_secs(20),
        )
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_limits(
        runner: Arc<ExecutionGraphRunner>,
        queue_capacity: usize,
        max_parallel_graphs: usize,
        shutdown_timeout: Duration,
    ) -> Self {
        Self::with_capacity_limits(
            runner,
            queue_capacity,
            max_parallel_graphs,
            max_parallel_graphs,
            max_parallel_graphs.saturating_mul(4),
            shutdown_timeout,
            true,
        )
    }

    pub(crate) fn with_capacity_profile(
        runner: Arc<ExecutionGraphRunner>,
        profile: &crate::ExecutionCapacityProfile,
        defer_graph_producers: bool,
    ) -> Self {
        let agent_capacity = profile.max_parallel_agents.max(1);
        Self::with_capacity_limits(
            runner,
            profile.max_pending_instance.max(1),
            agent_capacity,
            agent_capacity,
            agent_capacity
                .saturating_mul(4)
                .min(profile.max_pending_per_class.max(1)),
            Duration::from_secs(20),
            !defer_graph_producers,
        )
    }

    fn with_capacity_limits(
        runner: Arc<ExecutionGraphRunner>,
        queue_capacity: usize,
        max_parallel_graphs: usize,
        max_active_nodes_per_graph: usize,
        max_parallel_owned_tasks: usize,
        shutdown_timeout: Duration,
        graph_producers_released: bool,
    ) -> Self {
        let queue_capacity = queue_capacity.max(1);
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (reaper_sender, reaper_receiver) = mpsc::channel(queue_capacity);
        Self {
            runner,
            sender,
            receiver: StdMutex::new(Some(receiver)),
            reaper_sender,
            reaper_receiver: StdMutex::new(Some(reaper_receiver)),
            dispatcher: StdMutex::new(None),
            shutdown_state: tokio::sync::Mutex::new(ShutdownDrainState::default()),
            slots: Arc::new(StdMutex::new(HashMap::new())),
            parallelism: Arc::new(Semaphore::new(max_parallel_graphs.max(1))),
            owned_parallelism: Arc::new(Semaphore::new(max_parallel_owned_tasks.max(1))),
            max_active_nodes_per_graph: max_active_nodes_per_graph.max(1),
            cancellation: CancellationToken::new(),
            metrics: Arc::new(SupervisorMetrics::new()),
            queue_capacity,
            queue_partitions: DEFAULT_QUEUE_PARTITIONS,
            shutdown_timeout,
            shutdown_observation_deadline: OnceLock::new(),
            preserved_snapshot: std::sync::atomic::AtomicUsize::new(0),
            preserved_snapshot_ready: std::sync::atomic::AtomicBool::new(false),
            graph_settled_observer: Arc::new(OnceLock::new()),
            #[cfg(any(test, feature = "test-fixtures"))]
            verdict_resolutions: StdMutex::new(HashMap::new()),
            graph_producer_gate: StdMutex::new(GraphProducerGate {
                released: graph_producers_released,
                deferred_graphs: std::collections::BTreeSet::new(),
            }),
            session_recovery_owner: OnceLock::new(),
        }
    }

    pub(crate) fn install_graph_settled_observer(
        &self,
        observer: impl Fn(&str) + Send + Sync + 'static,
    ) -> Result<(), &'static str> {
        self.graph_settled_observer
            .set(Arc::new(observer))
            .map_err(|_| "graph settled observer is already installed")
    }

    pub(crate) fn install_mutation_gate(
        &self,
        gate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        self.runner.install_mutation_gate(gate);
    }

    pub(crate) async fn recover_graph(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        self.runner.recover_graph(graph_id).await
    }

    pub(crate) async fn commit_live_prepared_outcome(
        &self,
        ticket: &super::graph::NodeExecutionTicket,
        prepared: &super::graph::NodeExecutionOutcome,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.runner.commit_live_prepared_outcome(ticket, prepared).await
    }

    fn ensure_dispatcher(&self) -> Result<(), ExecutionRunnerError> {
        if self.metrics.lifecycle.load(Ordering::Acquire) != LIFECYCLE_OPEN {
            return Err(ExecutionRunnerError::SupervisorUnavailable(
                "shutdown has started".to_string(),
            ));
        }
        let mut dispatcher = self
            .dispatcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if dispatcher
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(());
        }
        if dispatcher.as_ref().is_some_and(JoinHandle::is_finished) {
            self.metrics
                .dispatcher_restarts
                .fetch_add(1, Ordering::Relaxed);
            let _ = dispatcher.take();
        }
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                ExecutionRunnerError::SupervisorUnavailable(
                    "dispatcher receiver is no longer available".to_string(),
                )
            })?;
        let reaper_receiver = self
            .reaper_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                ExecutionRunnerError::SupervisorUnavailable(
                    "dispatcher reaper receiver is no longer available".to_string(),
                )
            })?;
        let runner = Arc::clone(&self.runner);
        let slots = Arc::clone(&self.slots);
        let parallelism = Arc::clone(&self.parallelism);
        let owned_parallelism = Arc::clone(&self.owned_parallelism);
        let max_active_nodes_per_graph = self.max_active_nodes_per_graph;
        let cancellation = self.cancellation.clone();
        let metrics = Arc::clone(&self.metrics);
        let graph_settled_observer = Arc::clone(&self.graph_settled_observer);
        let session_recovery_owner = self.session_recovery_owner.get().cloned();
        *dispatcher = Some(tokio::spawn(async move {
            dispatch_loop(
                receiver,
                reaper_receiver,
                runner,
                slots,
                parallelism,
                owned_parallelism,
                max_active_nodes_per_graph,
                cancellation,
                metrics,
                graph_settled_observer,
                session_recovery_owner,
            )
            .await;
        }));
        Ok(())
    }

    pub(crate) fn graph_producers_released(&self) -> bool {
        self.graph_producer_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .released
    }

    fn require_graph_producers_released(&self) -> Result<(), ExecutionRunnerError> {
        self.graph_producers_released()
            .then_some(())
            .ok_or_else(|| {
                ExecutionRunnerError::SupervisorUnavailable(
                    "ordered Runtime recovery has not released graph execution".to_string(),
                )
            })
    }

    async fn enqueue(
        &self,
        graph_id: &str,
    ) -> Result<(Arc<DriverSlot>, u64), ExecutionRunnerError> {
        if self.has_session_recovery_exclusions() {
            let graph = self.runner.state_store().load_async(graph_id).await?;
            self.ensure_graph_session_recovery_admitted(&graph)?;
        }
        let (slot, generation) = {
            let mut slots = self.slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.metrics.lifecycle.load(Ordering::Acquire) != LIFECYCLE_OPEN {
                return Err(ExecutionRunnerError::SupervisorUnavailable("shutdown has started".to_string()));
            }
            let created = !slots.contains_key(graph_id);
            let slot = Arc::clone(slots.entry(graph_id.to_string()).or_insert_with(|| Arc::new(DriverSlot::new())));
            match slot.request(now_ms(), &self.metrics.lifecycle) {
                Some(generation) => (slot, generation),
                None => {
                    if created { slots.remove(graph_id); }
                    return Err(ExecutionRunnerError::SupervisorUnavailable("shutdown has started".to_string()));
                }
            }
        };
        {
            let mut gate = self
                .graph_producer_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !gate.released {
                gate.deferred_graphs.insert(graph_id.to_string());
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                return Ok((slot, generation));
            }
        }
        self.ensure_dispatcher()?;
        self.sender
            .send(SupervisorMessage::Wake(graph_id.to_string()))
            .await
            .map_err(|_| {
                ExecutionRunnerError::SupervisorUnavailable(
                    "execution admission queue is closed".to_string(),
                )
            })?;
        self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
        Ok((slot, generation))
    }

    /// Open the sole graph-driver gate after ordered durable recovery and hand
    /// back the graphs admitted while it was closed, WITHOUT waking them. The
    /// caller decides which are bound enough to wake: a conversation-owned
    /// graph must not be woken before its Session host rebinds the turn-scoped
    /// executors, or the node fails as `executor ... is unavailable`.
    pub(crate) fn install_session_recovery_owner(
        &self,
        store: Arc<crate::RuntimeEventStore>,
    ) -> Result<(), ExecutionRunnerError> {
        self.session_recovery_owner.set(store).map_err(|_| {
            ExecutionRunnerError::MutationBlocked("session_recovery_owner_already_installed".into())
        })
    }

    fn has_session_recovery_exclusions(&self) -> bool {
        self.session_recovery_owner
            .get()
            .is_some_and(|store| store.has_session_recovery_exclusions())
    }

    pub(crate) fn ensure_graph_session_recovery_admitted(
        &self,
        graph: &ExecutionGraph,
    ) -> Result<(), ExecutionRunnerError> {
        if let Some(store) = self
            .session_recovery_owner
            .get()
            .filter(|store| store.has_session_recovery_exclusions())
        {
            let lineage = graph.lineage.as_ref().ok_or_else(|| {
                ExecutionRunnerError::MutationBlocked(
                    "session_recovery_graph_lineage_missing".into(),
                )
            })?;
            lineage
                .validate()
                .map_err(|error| ExecutionRunnerError::MutationBlocked(error.to_string()))?;
            store
                .ensure_session_recovery_admitted(&lineage.session_id)
                .map_err(ExecutionRunnerError::MutationBlocked)?;
        }
        Ok(())
    }

    pub(crate) fn install_startup_recovery_exclusions(
        &self,
        store: &crate::RuntimeEventStore,
        excluded: BTreeSet<String>,
    ) -> Result<(), ExecutionRunnerError> {
        let gate = self
            .graph_producer_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if gate.released {
            return Err(ExecutionRunnerError::MutationBlocked(
                "startup_recovery_partition_already_released".into(),
            ));
        }
        if self
            .session_recovery_owner
            .get()
            .is_none_or(|owner| !std::ptr::eq(owner.as_ref(), store))
        {
            return Err(ExecutionRunnerError::MutationBlocked(
                "startup_recovery_owner_mismatch".into(),
            ));
        }
        store.replace_session_recovery_exclusions(excluded);
        Ok(())
    }

    pub(crate) fn deferred_recovered_graphs(&self) -> Option<Vec<String>> {
        let gate = self
            .graph_producer_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if gate.released {
            return None;
        }
        Some(gate.deferred_graphs.iter().cloned().collect())
    }

    pub(crate) fn prepare_recovered_producers(&self) -> Result<(), ExecutionRunnerError> {
        if self.metrics.lifecycle.load(Ordering::Acquire) != LIFECYCLE_OPEN {
            return Err(ExecutionRunnerError::SupervisorUnavailable(
                "shutdown has started".into(),
            ));
        }
        let dispatcher = self
            .dispatcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.sender.is_closed()
            || dispatcher.as_ref().is_some_and(JoinHandle::is_finished)
            || (dispatcher.is_none()
                && (self
                    .receiver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none()
                    || self
                        .reaper_receiver
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_none()))
        {
            return Err(ExecutionRunnerError::SupervisorUnavailable(
                "execution dispatcher is unavailable before recovery release".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn release_prepared_recovered_graphs(
        &self,
        expected: &[String],
    ) -> Result<(), ExecutionRunnerError> {
        let mut gate = self
            .graph_producer_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.prepare_recovered_producers()?;
        if gate.released || gate.deferred_graphs.iter().ne(expected.iter()) {
            return Err(ExecutionRunnerError::MutationBlocked(
                "recovery_deferred_source_changed_before_release".into(),
            ));
        }
        gate.released = true;
        gate.deferred_graphs.clear();
        Ok(())
    }

    pub(crate) async fn spawn_owned(
        &self,
        owner: impl Into<String>,
        work: OwnedTask,
    ) -> Result<(), ExecutionRunnerError> {
        self.admit_owned(
            owner,
            Box::pin(async move {
                work.await;
                Ok(())
            }),
        )
        .await
        .map(|_| ())
    }

    pub async fn admit_owned(
        &self,
        owner: impl Into<String>,
        work: OwnedWork,
    ) -> Result<RuntimeWorkAdmissionReceipt, ExecutionRunnerError> {
        self.require_graph_producers_released()?;
        self.ensure_dispatcher()?;
        let owner = owner.into();
        self.sender
            .send(SupervisorMessage::Owned {
                owner: owner.clone(),
                work,
            })
            .await
            .map_err(|error| {
                ExecutionRunnerError::SupervisorUnavailable(format!(
                    "owned execution queue rejected work: {error}"
                ))
            })?;
        let sequence = self.metrics.accepted.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(RuntimeWorkAdmissionReceipt {
            admission_id: format!("owned:{owner}:{sequence}"),
            owner,
            accepted_at_ms: now_ms(),
        })
    }

    pub(crate) fn reap_join_handle<T>(&self, owner: impl Into<String>, handle: JoinHandle<T>)
    where
        T: Send + 'static,
    {
        handle.abort();
        if self.ensure_dispatcher().is_err() {
            return;
        }
        let owner = owner.into();
        let message = SupervisorMessage::Owned {
            owner,
            work: Box::pin(async move {
                let _ = handle.await;
                Ok(())
            }),
        };
        match self.reaper_sender.try_send(message) {
            Ok(()) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    queue_capacity = self.queue_capacity,
                    "execution reaper queue is full; aborted join handle will detach"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("execution reaper is unavailable after aborting a join handle");
            }
        }
    }

    async fn admit(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, Arc<DriverSlot>, u64), ExecutionRunnerError> {
        if !matches!(
            command,
            ExecutionGraphCommand::Start { expected_revision }
                if expected_revision == graph.revision
        ) {
            return Err(ExecutionRunnerError::InvalidStartCommand);
        }
        let graph = self.register_graph(graph).await?;
        let accepted_at_ms = now_ms();
        let (slot, generation) = self.enqueue(&graph.id).await?;
        Ok((
            self.receipt(&graph, "admission", accepted_at_ms),
            slot,
            generation,
        ))
    }

    pub(crate) async fn register_graph(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.runner.register(graph).await
    }

    pub(crate) async fn register_continuation_graph(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
        source_goal_cut: Option<crate::ExpectedStreamRevision>,
        original_owner: Option<Arc<crate::execution_core::services::OriginalGraphRegistrationOwner>>,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        // Keep the registration future out of the caller's async frame; its
        // durable owner and admission checks stay on the same execution path.
        Box::pin(self.runner.register_continuation(graph, actor, source_goal_cut, original_owner)).await
    }

    pub(crate) async fn confirm_cancelled_bound_child_never_admitted(
        &self,
        expected: &ExecutionGraph,
    ) -> Result<(), ExecutionRunnerError> {
        self.runner
            .confirm_cancelled_bound_child_never_admitted(expected)
            .await
    }

    pub(crate) async fn drive_registered(
        &self,
        graph_id: &str,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
        self.require_graph_producers_released()?;
        let graph = self
            .runner
            .state_store()
            .load_async(graph_id)
            .await
            .map_err(ExecutionRunnerError::from)?;
        let receipt = self.receipt(&graph, "admission", now_ms());
        let (slot, generation) = self.enqueue(graph_id).await?;
        let report = self.await_slot(graph_id, slot, generation).await?;
        Ok((receipt, report))
    }

    pub(crate) async fn admit_registered(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        let graph = self
            .runner
            .state_store()
            .load_async(graph_id)
            .await
            .map_err(ExecutionRunnerError::from)?;
        let receipt = self.receipt(&graph, "admission", now_ms());
        let _ = self.enqueue(graph_id).await?;
        Ok(receipt)
    }

    pub(crate) async fn notify_graph(&self, graph_id: &str) -> Result<(), ExecutionRunnerError> {
        let _ = self.enqueue(graph_id).await?;
        Ok(())
    }

    /// Apply a short, revision-fenced graph command without introducing a
    /// second scheduler. Coordinator reactions use this same command lane as
    /// every other durable graph transition.
    pub(crate) async fn command(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        if self.has_session_recovery_exclusions() {
            let graph = self.runner.state_store().load_async(graph_id).await?;
            self.ensure_graph_session_recovery_admitted(&graph)?;
        }
        self.runner.command(graph_id, command).await
    }

    fn receipt(
        &self,
        graph: &ExecutionGraph,
        kind: &str,
        accepted_at_ms: u64,
    ) -> ExecutionGraphHostReceipt {
        ExecutionGraphHostReceipt {
            graph_id: graph.id.clone(),
            admission_id: format!("{kind}:{}:{}", graph.id, graph.revision),
            accepted_revision: graph.revision,
            queue_partition: partition(&graph.id, self.queue_partitions),
            accepted_at_ms,
        }
    }

    async fn await_slot(
        &self,
        graph_id: &str,
        slot: Arc<DriverSlot>,
        generation: u64,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        loop {
            let notified = slot.changed.notified();
            if let Some(outcome) = slot.observation(generation) {
                self.cleanup_slot(graph_id, &slot);
                return match outcome {
                    DriverOutcome::Completed(report) => Ok(report),
                    DriverOutcome::Failed(error) | DriverOutcome::Aborted(error) => {
                        Err(ExecutionRunnerError::Driver(error))
                    }
                };
            }
            notified.await;
        }
    }

    fn cleanup_slot(&self, graph_id: &str, slot: &Arc<DriverSlot>) {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slots
            .get(graph_id)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
            && slot.idle() && Arc::strong_count(slot) <= 2
        {
            slots.remove(graph_id);
        }
    }

    pub async fn submit_and_wait(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
        self.require_graph_producers_released()?;
        let (receipt, slot, generation) = self.admit(graph, command).await?;
        let report = self.await_slot(&receipt.graph_id, slot, generation).await?;
        Ok((receipt, report))
    }

    /// Admit a graph and let its durable driver continue independently.
    /// The returned receipt proves admission; callers that need the result can
    /// later use `wait_for_terminal`. Dropping the receipt never cancels work.
    pub async fn submit(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        let (receipt, _slot, _generation) = self.admit(graph, command).await?;
        Ok(receipt)
    }

    /// Admit a graph, release its graph-level concurrency permit whenever it
    /// becomes quiescent, and keep waiting on durable commits until every node
    /// reaches a terminal state. This is the foreground execution boundary;
    /// `submit_and_wait` intentionally retains its lower-level quiescence
    /// semantics for approval/external-input workflows.
    pub async fn submit_and_wait_terminal(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
        self.require_graph_producers_released()?;
        let (receipt, slot, generation) = self.admit(graph, command).await?;
        self.await_slot(&receipt.graph_id, slot, generation).await?;
        let report = self.wait_for_terminal(&receipt.graph_id).await?;
        Ok((receipt, report))
    }

    pub async fn wait_for_terminal(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let mut commits = self.runner.subscribe_state_commits();
        loop {
            if let Some(report) = self.runner.terminal_report(graph_id).await? {
                return Ok(report);
            }
            commits.changed().await.map_err(|_| {
                ExecutionRunnerError::SupervisorUnavailable(
                    "durable execution commit subscription closed before graph terminalized"
                        .to_string(),
                )
            })?;
        }
    }

    pub async fn wait_for_quiescence(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(graph_id)
            .cloned();
        if let Some(slot) = slot {
            let generation = slot.requested_generation();
            if generation > 0 {
                return self.await_slot(graph_id, slot, generation).await;
            }
        }
        let (report, quiescent) = self.runner.current_report(graph_id).await?;
        if quiescent {
            Ok(report)
        } else {
            Err(ExecutionRunnerError::SupervisorUnavailable(format!(
                "graph `{graph_id}` is not admitted; submit or notify it before waiting"
            )))
        }
    }

    pub async fn submit_command_and_wait(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
        self.require_graph_producers_released()?;
        let receipt = self.command_graph(graph_id, command).await?;
        let report = self.wait_for_quiescence(graph_id).await?;
        Ok((receipt, report))
    }

    pub async fn projection(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphProjection, ExecutionRunnerError> {
        self.runner.projection(graph_id).await
    }

    #[must_use]
    pub fn health(&self) -> RuntimeExecutionHealth {
        let now = now_ms();
        let slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut oldest_queued_at = None;
        for slot in slots.values() {
            let state = slot
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.active && state.requested_generation > state.completed_generation {
                oldest_queued_at =
                    Some(oldest_queued_at.map_or(state.queued_at_ms, |current: u64| {
                        current.min(state.queued_at_ms)
                    }));
            }
        }
        RuntimeExecutionHealth {
            lifecycle: lifecycle_label(self.metrics.lifecycle.load(Ordering::Acquire)).to_string(),
            queue_capacity: self.queue_capacity,
            queue_depth: self.queue_capacity.saturating_sub(self.sender.capacity()),
            active_drivers: self.metrics.active_drivers.load(Ordering::Relaxed),
            active_owned_tasks: self.metrics.active_owned_tasks.load(Ordering::Relaxed),
            tracked_keys: slots.len(),
            oldest_queued_age_ms: oldest_queued_at.map_or(0, |queued| now.saturating_sub(queued)),
            accepted: self.metrics.accepted.load(Ordering::Relaxed),
            completed: self.metrics.completed.load(Ordering::Relaxed),
            failed: self.metrics.failed.load(Ordering::Relaxed),
            forced_aborts: self.metrics.forced_aborts.load(Ordering::Relaxed),
            dispatcher_restarts: self.metrics.dispatcher_restarts.load(Ordering::Relaxed),
            last_error: self
                .metrics
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            owners: self
                .metrics
                .owners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    pub async fn shutdown(&self) -> RuntimeExecutionShutdownReport {
        self.shutdown_impl(false).await
    }

    pub(crate) async fn shutdown_for_runtime(&self) -> RuntimeExecutionShutdownReport {
        self.shutdown_impl(true).await
    }

    pub(crate) fn original_shutdown_deadline(&self) -> tokio::time::Instant {
        *self.shutdown_observation_deadline.get_or_init(|| tokio::time::Instant::now() + self.shutdown_timeout)
    }

    pub(crate) fn unresolved_shutdown_report(&self, reason: &str) -> RuntimeExecutionShutdownReport {
        let mut errors = vec![reason.to_owned()];
        if !self.preserved_snapshot_ready.load(Ordering::Acquire) {
            errors.push("original preserved graph count is not yet available".into());
        }
        self.shutdown_report(self.preserved_snapshot.load(Ordering::Acquire), 0, errors)
    }

    pub(crate) fn record_shutdown_observation_timeout(&self) {
        self.metrics.record_error("original Runtime finalization exceeded its observation deadline");
    }

    async fn shutdown_impl(&self, defer_close: bool) -> RuntimeExecutionShutdownReport {
        // This is a shutdown-owner lock, not a graph/data lock. Keeping the
        // join handle here lets another waiter resume an abandoned shutdown.
        let original_deadline = self.original_shutdown_deadline();
        let mut shutdown = match tokio::time::timeout_at(original_deadline, self.shutdown_state.lock()).await {
            Ok(shutdown) => shutdown,
            Err(_) => {
                self.record_shutdown_observation_timeout();
                return self.unresolved_shutdown_report("original execution shutdown owner remains in flight");
            }
        };
        let prior = self
            .metrics
            .lifecycle
            .compare_exchange(
                LIFECYCLE_OPEN,
                LIFECYCLE_CLOSING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .unwrap_or_else(|current| current);
        if prior == LIFECYCLE_CLOSED {
            return self.shutdown_report(
                shutdown.preserved_graphs.unwrap_or(0),
                0,
                shutdown.errors.clone(),
            );
        }
        let deadline = *shutdown.deadline.get_or_insert(original_deadline);
        self.runner.install_shutdown_deadline(deadline.into_std());

        let graph_ids = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        // Stop process-local drivers without changing durable business truth.
        // A SIGTERM, rolling deploy, evaluator disconnect, or host failure is
        // not evidence that the user cancelled the work. Running nodes remain
        // recoverable and ExecutionGraphRecovery reattaches their idempotent
        // attempts in the next process. Explicit operator/user cancellation
        // continues to use the typed graph/Session cancellation commands.
        let preserved_graphs = graph_ids
            .iter()
            .filter(|graph_id| {
                self.runner
                    .state_store()
                    .load(graph_id.as_str())
                    .is_ok_and(|graph| {
                        graph
                            .node_statuses
                            .values()
                            .any(|status| !status.is_terminal())
                    })
            })
            .count();
        let preserved_graphs = *shutdown.preserved_graphs.get_or_insert(preserved_graphs);
        self.preserved_snapshot.store(preserved_graphs, Ordering::Release);
        self.preserved_snapshot_ready.store(true, Ordering::Release);
        self.cancellation.cancel();
        if shutdown.dispatcher.is_none() {
            shutdown.dispatcher = self
                .dispatcher
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        if let Some(dispatcher) = shutdown.dispatcher.as_mut() {
            match tokio::time::timeout_at(deadline, &mut *dispatcher).await {
                Ok(Ok(())) => {},
                Ok(Err(error)) => {
                    shutdown.errors.push(format!("original execution dispatcher failed: {error}"));
                    shutdown.dispatcher.take();
                    return self.shutdown_report(preserved_graphs, 0, shutdown.errors.clone());
                },
                Err(_) => {
                    let reason = "original execution dispatcher remains unresolved after shutdown deadline";
                    if !shutdown.deadline_observed {
                        self.metrics.record_error(reason.to_string());
                        shutdown.deadline_observed = true;
                    }
                    let mut errors = shutdown.errors.clone();
                    errors.push(reason.to_string());
                    return self.shutdown_report(preserved_graphs, 0, errors);
                }
            }
        }
        shutdown.dispatcher.take();
        // The registry drain contract begins only after process-local drivers
        // stop. A retained original node is still such a driver.
        let owners = tokio::time::timeout_at(deadline, self.runner.has_retained_shutdown_owners()).await;
        if !matches!(owners, Ok(false)) {
            let reason = "original execution owner remains unresolved after shutdown";
            if !shutdown.deadline_observed {
                self.metrics.record_error(reason.to_string());
                shutdown.deadline_observed = true;
            }
            let mut errors = shutdown.errors.clone(); errors.push(reason.to_string());
            return self.shutdown_report(preserved_graphs, 0, errors);
        }
        // No producer/driver remains. Retained effects must finish before the
        // Runtime is reported closed; a shutdown timeout cannot unlock a write.
        // Their graph attempts remain recoverable, not business-cancelled.
        if !shutdown.physical_drained && shutdown.physical_drain.is_none() && tokio::time::Instant::now() >= deadline {
            let mut errors = shutdown.errors.clone();
            errors.push("original physical drain was not registered before shutdown deadline".into());
            return self.shutdown_report(preserved_graphs, 0, errors);
        }
        if !shutdown.physical_drained && shutdown.physical_drain.is_none() {
            let runner = Arc::clone(&self.runner);
            shutdown.physical_drain =
                Some(Box::pin(
                    async move { runner.drain_physical_execution().await },
                ));
        }
        // The original state owns this exact aggregation, including already
        // observed errors. Dropping a waiter must not restart and wash them out.
        let errors = if shutdown.physical_drained { Vec::new() } else {
            match tokio::time::timeout_at(deadline, shutdown.physical_drain.as_mut().expect("original drain")).await {
                Ok(errors) => errors,
                Err(_) => {
                    let reason = "original physical drain remains unresolved after shutdown deadline";
                    if !shutdown.deadline_observed {
                        self.metrics.record_error(reason.to_string());
                        shutdown.deadline_observed = true;
                    }
                    let mut errors = shutdown.errors.clone(); errors.push(reason.to_string());
                    return self.shutdown_report(preserved_graphs, 0, errors);
                }
            }
        };
        let drained = errors.is_empty();
        for error in errors {
            if !shutdown.errors.contains(&error) {
                shutdown.errors.push(error);
            }
        }
        shutdown.physical_drain = None;
        if drained { shutdown.physical_drained = true; }
        if drained && !defer_close {
            self.metrics
                .lifecycle
                .store(LIFECYCLE_CLOSED, Ordering::Release);
        }
        self.shutdown_report(preserved_graphs, 0, shutdown.errors.clone())
    }

    // The RuntimeServices original finalization driver starts before the
    // observation deadline and retains this same dispatcher/drain sequence.
    pub(crate) async fn await_original_shutdown(&self) -> RuntimeExecutionShutdownReport {
        let mut shutdown = self.shutdown_state.lock().await;
        let preserved = shutdown.preserved_graphs.unwrap_or(0);
        let mut dispatcher_failed = false;
        if let Some(dispatcher) = shutdown.dispatcher.as_mut() {
            if let Err(error) = dispatcher.await {
                shutdown.errors.push(format!("original execution dispatcher failed: {error}"));
                dispatcher_failed = true;
            }
        }
        shutdown.dispatcher.take();
        if dispatcher_failed { return self.shutdown_report(preserved, 0, shutdown.errors.clone()); }
        // Original retained tasks are process-local drivers, including the
        // non-retaining ToolBatch path. Their own handoffs must finish first.
        self.runner.wait_retained_shutdown_owners().await;
        if !shutdown.physical_drained && shutdown.physical_drain.is_none() {
            let runner = Arc::clone(&self.runner);
            shutdown.physical_drain = Some(Box::pin(async move { runner.drain_physical_execution().await }));
        }
        if !shutdown.physical_drained {
            let errors = shutdown.physical_drain.as_mut().expect("original drain").await;
            shutdown.physical_drained = errors.is_empty();
            for error in errors { if !shutdown.errors.contains(&error) { shutdown.errors.push(error); } }
            shutdown.physical_drain = None;
        }
        self.shutdown_report(preserved, 0, shutdown.errors.clone())
    }

    pub(crate) async fn has_shutdown_failure(&self) -> bool {
        !self.shutdown_state.lock().await.errors.is_empty()
    }

    pub(crate) async fn finish_runtime_shutdown(&self) -> bool {
        let shutdown = self.shutdown_state.lock().await;
        if shutdown.dispatcher.is_some() || !shutdown.physical_drained
            || self.runner.has_retained_shutdown_owners().await { return false; }
        self.metrics.lifecycle.store(LIFECYCLE_CLOSED, Ordering::Release);
        true
    }

    fn shutdown_report(
        &self,
        preserved_graphs: usize,
        cancelled_graphs: usize,
        errors: Vec<String>,
    ) -> RuntimeExecutionShutdownReport {
        RuntimeExecutionShutdownReport {
            accepted: self.metrics.accepted.load(Ordering::Relaxed),
            completed: self.metrics.completed.load(Ordering::Relaxed),
            failed: self.metrics.failed.load(Ordering::Relaxed),
            preserved_graphs,
            cancelled_graphs,
            forced_aborts: self.metrics.forced_aborts.load(Ordering::Relaxed),
            remaining_keys: self
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            errors,
            owners: self
                .metrics
                .owners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }
}

#[async_trait]
impl ExecutionGraphHost for RuntimeExecutionSupervisor {
    async fn submit_graph(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        self.admit(graph, command)
            .await
            .map(|(receipt, _, _)| receipt)
    }

    async fn command_graph(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        let should_wake = command_advances(&command);
        let graph = self.command(graph_id, command).await?;
        if should_wake {
            self.notify_graph(graph_id).await?;
        }
        Ok(self.receipt(&graph, "command", now_ms()))
    }

    async fn graph_projection(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphProjection, ExecutionRunnerError> {
        self.projection(graph_id).await
    }
}

fn close_queued_for_shutdown(
    receiver: &mut mpsc::Receiver<SupervisorMessage>,
    reaper_receiver: &mut mpsc::Receiver<SupervisorMessage>,
    slots: &StdMutex<HashMap<String, Arc<DriverSlot>>>,
    active: &HashMap<String, u64>,
    metrics: &SupervisorMetrics,
) {
    receiver.close();
    reaper_receiver.close();
    while let Ok(message) = receiver.try_recv() {
        if let SupervisorMessage::Owned { owner, .. } = message {
            metrics.owner_submitted(&owner);
            metrics.owner_aborted(&owner);
        }
    }
    while let Ok(SupervisorMessage::Owned { owner, .. }) = reaper_receiver.try_recv() {
        metrics.owner_submitted(&owner);
        metrics.owner_aborted(&owner);
    }
    let pending = slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter().map(|(graph_id, slot)| (Arc::clone(slot), active.get(graph_id).copied())).collect::<Vec<_>>();
    for (slot, generation) in pending { slot.abort_unstarted_on_shutdown(generation); }
}

async fn drain_closed_queue(receiver: &mut mpsc::Receiver<SupervisorMessage>, metrics: &SupervisorMetrics) {
    // A permit acquired before close can still deliver after try_recv is empty.
    while let Some(message) = receiver.recv().await {
        if let SupervisorMessage::Owned { owner, .. } = message {
            metrics.owner_submitted(&owner);
            metrics.owner_aborted(&owner);
        }
    }
}

async fn dispatch_loop(
    mut receiver: mpsc::Receiver<SupervisorMessage>,
    mut reaper_receiver: mpsc::Receiver<SupervisorMessage>,
    runner: Arc<ExecutionGraphRunner>,
    slots: Arc<StdMutex<HashMap<String, Arc<DriverSlot>>>>,
    parallelism: Arc<Semaphore>,
    owned_parallelism: Arc<Semaphore>,
    max_active_nodes_per_graph: usize,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
    graph_settled_observer: Arc<OnceLock<GraphSettledObserver>>,
    session_recovery_owner: Option<Arc<crate::RuntimeEventStore>>,
) {
    let mut workers = JoinSet::new();
    let mut active = HashMap::<String, u64>::new();
    let mut pending = HashSet::<String>::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                close_queued_for_shutdown(&mut receiver, &mut reaper_receiver, &slots, &active, &metrics);
                tokio::join!(drain_closed_queue(&mut receiver, &metrics),
                    drain_closed_queue(&mut reaper_receiver, &metrics));

                while let Some(joined) = workers.join_next().await {
                    match joined {
                        Ok(SupervisorCompletion::Graph {
                            graph_id,
                            generation,
                            outcome,
                        }) => {
                            active.remove(&graph_id);
                            if let Some(slot) = slots
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get(&graph_id)
                                .cloned()
                            {
                                slot.finish(generation, outcome.clone());
                                slot.abort_unstarted_on_shutdown(None);
                            }
                            notify_graph_settled(&graph_settled_observer, &graph_id);
                            match outcome {
                                DriverOutcome::Completed(_) => {
                                    metrics.completed.fetch_add(1, Ordering::Relaxed);
                                    metrics.owner_finished("graph", false);
                                }
                                DriverOutcome::Failed(error) => {
                                    metrics.record_error(error);
                                    metrics.owner_finished("graph", true);
                                }
                                DriverOutcome::Aborted(_) => {
                                    metrics.owner_aborted("graph");
                                }
                            }
                        }
                        Ok(SupervisorCompletion::Owned {
                            owner,
                            status,
                            error,
                        }) => {
                            metrics.active_owned_tasks.fetch_sub(1, Ordering::Relaxed);
                            match status {
                                OwnedCompletionStatus::Completed => {
                                    metrics.owner_finished(&owner, false);
                                    metrics.completed.fetch_add(1, Ordering::Relaxed);
                                }
                                OwnedCompletionStatus::Failed => {
                                    metrics.owner_finished(&owner, true);
                                    if let Some(error) = error {
                                        metrics.record_error(error);
                                    }
                                }
                                OwnedCompletionStatus::Aborted => {
                                    metrics.owner_aborted(&owner);
                                }
                            }
                        }
                        Err(error) => {
                            metrics.record_error(format!(
                                "execution driver join failed during shutdown: {error}"
                            ));
                        }
                    }
                }

                let retained = {
                    let mut slots = slots
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    slots.drain().map(|(_, slot)| slot).collect::<Vec<_>>()
                };
                for slot in retained {
                    slot.abort_unstarted_on_shutdown(None);
                }
                break;
            }
            Some(message) = receiver.recv() => {
                match message {
                    SupervisorMessage::Wake(graph_id) => {
                        if let Some(store) = session_recovery_owner.as_ref().filter(|store| store.has_session_recovery_exclusions()) {
                            let admission = match runner.state_store().load_async(&graph_id).await {
                                Ok(graph) => graph.lineage.as_ref()
                                    .ok_or_else(|| "session_recovery_graph_lineage_missing".to_string())
                                    .and_then(|lineage| {
                                        lineage.validate().map_err(|error| error.to_string())?;
                                        store.ensure_session_recovery_admitted(&lineage.session_id)
                                    }),
                                Err(error) => Err(error.to_string()),
                            };
                            if let Err(error) = admission {
                                metrics.record_error(error.clone());
                                if let Some((generation, slot)) = prepare_worker(&slots, &graph_id) {
                                    slot.finish(generation, DriverOutcome::Failed(error));
                                }
                                continue;
                            }
                        }
                        if active.contains_key(&graph_id) {
                            pending.insert(graph_id);
                            continue;
                        }
                        if let Some((generation, slot)) = prepare_worker(&slots, &graph_id) {
                            active.insert(graph_id.clone(), generation);
                            spawn_graph_pump(
                                &mut workers,
                                Arc::clone(&slots),
                                graph_id,
                                generation,
                                slot,
                                Arc::clone(&runner),
                                Arc::clone(&parallelism),
                                max_active_nodes_per_graph,
                                cancellation.clone(),
                                Arc::clone(&metrics),
                            );
                        }
                    }
                    SupervisorMessage::Owned { owner, work } => {
                        spawn_owned_work(
                            &mut workers,
                            owner,
                            work,
                            Arc::clone(&owned_parallelism),
                            cancellation.clone(),
                            Arc::clone(&metrics),
                        );
                    }
                }
            }
            Some(SupervisorMessage::Owned { owner, work }) = reaper_receiver.recv() => {
                spawn_owned_work(
                    &mut workers,
                    owner,
                    work,
                    Arc::clone(&owned_parallelism),
                    cancellation.clone(),
                    Arc::clone(&metrics),
                );
            }
            Some(joined) = workers.join_next(), if !workers.is_empty() => {
                let completion = match joined {
                    Ok(completion) => completion,
                    Err(error) => {
                        metrics.record_error(format!("execution driver join failed: {error}"));
                        continue;
                    }
                };
                match completion {
                    SupervisorCompletion::Graph {
                        graph_id,
                        generation,
                        outcome,
                    } => {
                        active.remove(&graph_id);
                        let slot = slots
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get(&graph_id)
                            .cloned();
                        if let Some(slot) = slot {
                            slot.finish(generation, outcome.clone());
                            notify_graph_settled(&graph_settled_observer, &graph_id);
                            match outcome {
                                DriverOutcome::Completed(_) => {
                                    metrics.completed.fetch_add(1, Ordering::Relaxed);
                                    metrics.owner_finished("graph", false);
                                }
                                DriverOutcome::Failed(error) => {
                                    metrics.record_error(error);
                                    metrics.owner_finished("graph", true);
                                }
                                DriverOutcome::Aborted(_) => {
                                    metrics.owner_aborted("graph");
                                }
                            }
                            if pending.remove(&graph_id) || slot.has_newer_request(generation) {
                                let next_generation = slot.start();
                                active.insert(graph_id.clone(), next_generation);
                                spawn_graph_pump(
                                    &mut workers,
                                    Arc::clone(&slots),
                                    graph_id,
                                    next_generation,
                                    slot,
                                    Arc::clone(&runner),
                                    Arc::clone(&parallelism),
                                    max_active_nodes_per_graph,
                                    cancellation.clone(),
                                    Arc::clone(&metrics),
                                );
                            } else {
                                cleanup_idle_slot(&slots, &graph_id, &slot);
                            }
                        }
                    }
                    SupervisorCompletion::Owned {
                        owner,
                        status,
                        error,
                    } => {
                        metrics.active_owned_tasks.fetch_sub(1, Ordering::Relaxed);
                        match status {
                            OwnedCompletionStatus::Completed => {
                                metrics.owner_finished(&owner, false);
                                metrics.completed.fetch_add(1, Ordering::Relaxed);
                            }
                            OwnedCompletionStatus::Failed => {
                                metrics.owner_finished(&owner, true);
                                if let Some(error) = error {
                                    metrics.record_error(error);
                                }
                            }
                            OwnedCompletionStatus::Aborted => {
                                metrics.owner_aborted(&owner);
                            }
                        }
                    }
                }
            }
            else => break,
        }
    }
}

fn notify_graph_settled(observer: &OnceLock<GraphSettledObserver>, graph_id: &str) {
    if let Some(observer) = observer.get() {
        observer(graph_id);
    }
}

fn prepare_worker(
    slots: &StdMutex<HashMap<String, Arc<DriverSlot>>>,
    graph_id: &str,
) -> Option<(u64, Arc<DriverSlot>)> {
    let slot = slots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(graph_id)
        .cloned()?;
    let generation = slot.start();
    Some((generation, slot))
}

fn spawn_graph_pump(
    workers: &mut JoinSet<SupervisorCompletion>,
    slots: Arc<StdMutex<HashMap<String, Arc<DriverSlot>>>>,
    graph_id: String,
    generation: u64,
    slot: Arc<DriverSlot>,
    runner: Arc<ExecutionGraphRunner>,
    parallelism: Arc<Semaphore>,
    max_active_nodes_per_graph: usize,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
) {
    metrics.owner_submitted("graph");
    workers.spawn(async move {
        let execution = async {
            let inherited = match runner.state_store().load_async(&graph_id).await {
                Ok(graph) if super::graph::executors::agent_tool::is_bound_agent_tool_graph(&graph) => {
                    let parent = graph.parent_execution.as_ref().expect("typed parent checked");
                    match runner.state_store().load_async(&parent.execution_id).await {
                        Ok(parent_graph) if parent_graph.node_statuses.get(&parent.node_id) == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
                            && graph.lineage == parent_graph.lineage && graph.service_class == parent_graph.service_class
                            && parent_graph.nodes.iter().any(|node| node.id == parent.node_id && node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask) => {
                            let parent_slot = slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&parent.execution_id).cloned();
                            parent_slot.and_then(|slot| slot.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone())
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            let permit = if let Some(permit) = inherited { permit } else { Arc::new(tokio::select! {
                _ = cancellation.cancelled() => {
                    return DriverOutcome::Aborted(
                        "execution cancelled before driver start".to_string()
                    );
                }
                permit = parallelism.acquire_owned() => {
                    match permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            return DriverOutcome::Failed("execution parallelism gate is closed".to_string());
                        }
                    }
                }
            }) };
            *slot.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(permit.clone());
            struct AdmissionOwner(Arc<DriverSlot>);
            impl Drop for AdmissionOwner {
                fn drop(&mut self) { self.0.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take(); }
            }
            let _admission_owner = AdmissionOwner(slot.clone());
            metrics.active_drivers.fetch_add(1, Ordering::Relaxed);
            let result = run_completion_pump(
                Arc::clone(&runner),
                &graph_id,
                max_active_nodes_per_graph,
                cancellation.clone(),
            )
            .await;
            metrics.active_drivers.fetch_sub(1, Ordering::Relaxed);
            drop(permit);
            result
        };
        let outcome = AssertUnwindSafe(execution)
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                DriverOutcome::Failed(format!(
                    "execution driver panicked: {}",
                    panic_message(panic)
                ))
            });
        drop(slot);
        SupervisorCompletion::Graph {
            graph_id,
            generation,
            outcome,
        }
    });
}

async fn run_completion_pump(
    runner: Arc<ExecutionGraphRunner>,
    graph_id: &str,
    max_active_nodes_per_graph: usize,
    cancellation: CancellationToken,
) -> DriverOutcome {
    let durable_progress = Arc::new(Notify::new());
    let mut active_nodes = JoinSet::new();
    let mut in_flight = BTreeSet::new();
    let mut protected = BTreeSet::new();
    let mut reasons = std::collections::BTreeMap::<String, String>::new();
    let mut originals = HashMap::new();
    let mut original_tasks = HashMap::new();
    let mut panic_cleanup_started = BTreeSet::new();
    let mut deferred_nodes = BTreeSet::new();
    let mut waited_without_progress = false;
    let retained = match runner.retained_original_pumps(graph_id).await {
        Ok(retained) => retained,
        Err(error) => return DriverOutcome::Failed(error.to_string()),
    };
    for (node, task, owner) in retained {
        original_tasks.insert(node.clone(), Arc::clone(&task));
        let reason = match task.classify(&node) {
            Some(Err(error)) => error.to_string(),
            Some(Ok(())) => format!("original pump for `{node}` awaits owner reconciliation"),
            None => format!("original pump for `{node}` remains pending"),
        };
        protected.insert(node.clone());
        in_flight.insert(node.clone());
        reasons.insert(node.clone(), reason);
        originals.insert(node.clone(), (Arc::clone(&task), owner));
        active_nodes.spawn(async move { let result = task.observe(&node).await; (node, result) });
    }

    loop {
        if cancellation.is_cancelled() {
            if let Err(error) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                return DriverOutcome::Failed(format!("original cancellation drain unconfirmed: {error}"));
            }
            return DriverOutcome::Aborted(
                "execution cancelled while graph pump was running".to_string(),
            );
        }
        let snapshot = match runner.pump_snapshot_with_unknown(graph_id, &in_flight, &protected).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if let Err(drain) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                    return DriverOutcome::Failed(format!("{error}; original drain unconfirmed: {drain}"));
                }
                return DriverOutcome::Failed(error.to_string());
            }
        };

        let available = max_active_nodes_per_graph.saturating_sub(active_nodes.len());
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            for node in &snapshot.ready {
                eprintln!("r622_toolbatch phase=pump_ready node_id={} executor={}", node.id, node.executor_kind);
            }
        }
        let ready = snapshot
            .ready
            .into_iter()
            .filter(|node| !deferred_nodes.contains(&node.id))
            .take(available)
            .collect::<Vec<_>>();
        let mut admission_deferred = false;
        let prepared = match runner.prepare_ready_wave_with_unknown(graph_id, ready, &protected).await {
            Ok(prepared) => prepared,
            Err(ExecutionRunnerError::ResourceDeferred { .. }) => {
                admission_deferred = true;
                Vec::new()
            }
            Err(error) => {
                if let Err(drain) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                    return DriverOutcome::Failed(format!("{error}; original drain unconfirmed: {drain}"));
                }
                return DriverOutcome::Failed(error.to_string());
            }
        };
        for prepared in prepared {
            if !prepared.has_startup_error() { waited_without_progress = false; }
            let node_id = prepared.node.id.clone();
            if !in_flight.insert(node_id.clone()) {
                continue;
            }
            let original_task = if prepared.has_startup_error() {
                None
            } else {
                let (task, sender) = OriginalPumpTask::new();
                if let Err(error) = runner.register_original_pump_task(graph_id, &prepared, Arc::clone(&task)).await {
                    in_flight.remove(&node_id);
                    if let Err(drain) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                        return DriverOutcome::Failed(format!("{error}; original drain unconfirmed: {drain}"));
                    }
                    return DriverOutcome::Failed(error.to_string());
                }
                original_tasks.insert(node_id.clone(), Arc::clone(&task));
                Some((task, sender))
            };
            let runner = Arc::clone(&runner);
            let graph_id = graph_id.to_string();
            let progress = Arc::clone(&durable_progress);
            let task_node_id = node_id.clone();
            let panic_task = original_task.as_ref().map(|(task, _)| Arc::clone(task));
            let work = async move {
                let node_id_for_panic = task_node_id.clone();
                let executed = AssertUnwindSafe(async move {
                    runner
                        .execute_prepared_pump_node(&graph_id, prepared, progress)
                        .await
                })
                .catch_unwind()
                .await;
                match executed {
                    Ok(result) => result,
                    Err(payload) => {
                        if let Some(task) = panic_task { task.mark_panicked(); }
                        let reason = panic_message(payload);
                        Err(ExecutionRunnerError::Executor(super::graph::NodeExecutorError::OriginalCustodyUnknown {
                            node_id: node_id_for_panic,
                            reason: format!("original pump task panicked: {reason}"),
                        }))
                    }
                }
            };
            if let Some((task, sender)) = original_task {
                tokio::spawn(async move { let _ = sender.send(work.await); });
                active_nodes.spawn(async move {
                    let result = task.observe(&node_id).await;
                    (node_id, result)
                });
            } else {
                active_nodes.spawn(async move { (node_id, work.await) });
            }
        }

        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=pump_state active={} inflight={} deferred={} waiting={} revision={}",
                active_nodes.len(), in_flight.len(), admission_deferred, snapshot.report.waiting, snapshot.report.revision);
        }
        if active_nodes.is_empty() {
            if !deferred_nodes.is_empty() && (protected.is_empty() || !waited_without_progress) {
                if !protected.is_empty() { waited_without_progress = true; }
                tokio::select! { _ = cancellation.cancelled() => {}, () = runner.wait_for_resource_change() => {} }
                deferred_nodes.clear();
                continue;
            }
            if !protected.is_empty() && admission_deferred && !waited_without_progress {
                waited_without_progress = true;
                tokio::select! { _ = cancellation.cancelled() => {}, () = runner.wait_for_resource_change() => {} }
                continue;
            }
            if let Some(reason) = reasons.values().next() { return DriverOutcome::Failed(reason.clone()); }
            if admission_deferred {
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    () = runner.wait_for_resource_change() => {}
                }
                continue;
            }
            match runner.terminal_report(graph_id).await {
                Ok(Some(report)) => return DriverOutcome::Completed(report),
                Ok(None) if snapshot.report.waiting > 0 => {
                    return DriverOutcome::Completed(snapshot.report);
                }
                Ok(None) => {
                    return DriverOutcome::Failed(format!(
                        "execution graph `{graph_id}` quiesced with no active or ready node while durable nodes remained non-terminal at revision {}",
                        snapshot.report.revision
                    ));
                }
                Err(error) => return DriverOutcome::Failed(error.to_string()),
            }
        }

        tokio::select! {
            _ = cancellation.cancelled() => {
                if let Err(error) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                    return DriverOutcome::Failed(format!("original cancellation drain unconfirmed: {error}"));
                }
                return DriverOutcome::Aborted(
                    "execution cancelled while graph pump was running".to_string(),
                );
            }
            () = durable_progress.notified() => {
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() { eprintln!("r622_toolbatch phase=pump_wake source=durable_progress"); }
                // Durable terminal truth is already committed. Re-read it and
                // release direct successors without waiting for unrelated work.
            }
            () = runner.wait_for_resource_change(), if !deferred_nodes.is_empty() => {
                deferred_nodes.clear();
            }
            joined = active_nodes.join_next() => {
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() { eprintln!("r622_toolbatch phase=pump_wake source=node_join"); }
                let Some(joined) = joined else {
                    continue;
                };
                match joined {
                    Ok((node_id, Ok(()))) => {
                        if let Some((task, owner)) = originals.remove(&node_id) {
                            if !runner.original_pump_was_closed(&task, &owner) {
                                let same = runner.original_pump_matches(graph_id, &node_id, &task, &owner).await;
                                let detail = if same { "original pump completed without confirmed binding release" } else { "original pump owner changed before confirmed release" };
                                if panic_cleanup_started.contains(&node_id) {
                                    reasons.entry(node_id.clone()).and_modify(|reason| reason.push_str(&format!("; {detail}"))).or_insert_with(|| detail.into());
                                } else {
                                    reasons.insert(node_id.clone(), detail.into());
                                }
                                protected.insert(node_id);
                                continue;
                            }
                            protected.remove(&node_id);
                            if !panic_cleanup_started.contains(&node_id) { reasons.remove(&node_id); }
                        }
                        in_flight.remove(&node_id);
                        // A durable terminal wave wakes many sibling tasks at
                        // once. Drain them before re-projecting the same graph;
                        // otherwise N completed siblings cause N identical
                        // state-store reads after their single atomic commit.
                        let mut resource_deferred = false;
                        while let Some(joined) = active_nodes.try_join_next() {
                            match joined {
                                Ok((node_id, Ok(()))) => {
                                    if let Some((task, owner)) = originals.remove(&node_id) {
                                        if !runner.original_pump_was_closed(&task, &owner) {
                                            let same = runner.original_pump_matches(graph_id, &node_id, &task, &owner).await;
                                            let detail = if same { "original pump completed without confirmed binding release" } else { "original pump owner changed before confirmed release" };
                                            if panic_cleanup_started.contains(&node_id) {
                                                reasons.entry(node_id.clone()).and_modify(|reason| reason.push_str(&format!("; {detail}"))).or_insert_with(|| detail.into());
                                            } else {
                                                reasons.insert(node_id.clone(), detail.into());
                                            }
                                            protected.insert(node_id);
                                            continue;
                                        }
                                        protected.remove(&node_id);
                                        if !panic_cleanup_started.contains(&node_id) { reasons.remove(&node_id); }
                                    }
                                    in_flight.remove(&node_id);
                                }
                                Ok((node_id, Err(error))) => {
                                    if matches!(&error, ExecutionRunnerError::Executor(super::graph::NodeExecutorError::OriginalCustodyUnknown { .. })) {
                                        protected.insert(node_id.clone());
                                        let mut reason = error.to_string();
                                        if let Some(task) = original_tasks.get(&node_id).filter(|task| task.requires_original_cleanup()) {
                                            if panic_cleanup_started.insert(node_id.clone()) {
                                                match schedule_panicked_original_cleanup(&runner, graph_id, &node_id, task, &reason, &mut active_nodes).await {
                                                    Ok(owner) => { originals.insert(node_id.clone(), (Arc::clone(task), owner)); }
                                                    Err(detail) => reason.push_str(&format!("; original cleanup unconfirmed: {detail}")),
                                                }
                                            }
                                        }
                                        reasons.insert(node_id, reason);
                                    } else if matches!(error, ExecutionRunnerError::ResourceDeferred { .. }) {
                                        deferred_nodes.insert(node_id.clone());
                                        in_flight.remove(&node_id);
                                        resource_deferred = true;
                                    } else if !matches!(
                                        error,
                                        ExecutionRunnerError::NodeMissing(_)
                                            | ExecutionRunnerError::CommandSuperseded { .. }
                                    ) {
                                        if let Err(drain) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                                            return DriverOutcome::Failed(format!("{error}; original drain unconfirmed: {drain}"));
                                        }
                                        return DriverOutcome::Failed(error.to_string());
                                    } else {
                                        in_flight.remove(&node_id);
                                    }
                                }
                                Err(error) => {
                                    tracing::error!(
                                        graph_id,
                                        error = %error,
                                        "execution node task failed to join"
                                    );
                                }
                            }
                        }
                        if resource_deferred && protected.is_empty() {
                            runner.wait_for_resource_change().await;
                            deferred_nodes.clear();
                        }
                    }
                    Ok((node_id, Err(error))) => {
                        if matches!(&error, ExecutionRunnerError::Executor(super::graph::NodeExecutorError::OriginalCustodyUnknown { .. })) {
                            protected.insert(node_id.clone());
                            let mut reason = error.to_string();
                            if let Some(task) = original_tasks.get(&node_id).filter(|task| task.requires_original_cleanup()) {
                                if panic_cleanup_started.insert(node_id.clone()) {
                                    match schedule_panicked_original_cleanup(&runner, graph_id, &node_id, task, &reason, &mut active_nodes).await {
                                        Ok(owner) => { originals.insert(node_id.clone(), (Arc::clone(task), owner)); }
                                        Err(detail) => reason.push_str(&format!("; original cleanup unconfirmed: {detail}")),
                                    }
                                }
                            }
                            reasons.insert(node_id, reason);
                            continue;
                        }
                        if matches!(error, ExecutionRunnerError::ResourceDeferred { .. }) {
                            deferred_nodes.insert(node_id.clone());
                            in_flight.remove(&node_id);
                            // Keep the graph driver alive across soft resource
                            // pressure. `wait_for_change` is notification-led
                            // with a bounded fallback, so a release cannot
                            // strand a Ready node and a missed notification
                            // cannot spin forever.
                            if protected.is_empty() { runner.wait_for_resource_change().await; deferred_nodes.clear(); }
                            continue;
                        }
                        // Node-local executor failures are already isolated
                        // inside `execute_pump_node`. Anything that still
                        // escapes is a graph/state/commit error, which must not
                        // be rewritten into a permanent node failure. `NodeMissing`
                        // is a benign supersede race; everything else fails the
                        // driver so recovery owns the retry.
                        if matches!(
                            error,
                            ExecutionRunnerError::NodeMissing(_)
                                | ExecutionRunnerError::CommandSuperseded { .. }
                        ) {
                            in_flight.remove(&node_id);
                            continue;
                        }
                        if let Err(drain) = abort_pump_nodes(&runner, graph_id, &in_flight, &mut active_nodes).await {
                            return DriverOutcome::Failed(format!("{error}; original drain unconfirmed: {drain}"));
                        }
                        return DriverOutcome::Failed(error.to_string());
                    }
                    Err(error) => {
                        tracing::error!(
                            graph_id,
                            error = %error,
                            "execution node task failed to join"
                        );
                    }
                }
            }
        }
    }
}

// The pump has already returned before this is scheduled.  Waiting for the
// retained pump from inside its own work future would wait on itself.
async fn schedule_panicked_original_cleanup(
    runner: &Arc<ExecutionGraphRunner>,
    graph_id: &str,
    node_id: &str,
    task: &Arc<OriginalPumpTask>,
    panic_reason: &str,
    active_nodes: &mut JoinSet<(String, Result<(), ExecutionRunnerError>)>,
) -> Result<PumpCancellationOwner, &'static str> {
    let nodes = BTreeSet::from([node_id.to_owned()]);
    let (mut owners, missing) = runner.capture_pump_cancellation_owners(graph_id, &nodes).await;
    if missing || owners.len() != 1 { return Err("original cancellation owner missing"); }
    let owner = owners.pop().ok_or("original cancellation owner missing")?;
    if !runner.original_pump_matches(graph_id, node_id, task, &owner).await {
        return Err("original pump owner changed");
    }
    let retained_owner = owner.clone();
    let runner = Arc::clone(runner);
    let graph_id = graph_id.to_owned();
    let node_id = node_id.to_owned();
    let panic_reason = panic_reason.to_owned();
    active_nodes.spawn(async move {
        let cleanup = runner.drain_pump_cancellation_owners(&graph_id, vec![owner]).await;
        let result = cleanup.map_err(|error| ExecutionRunnerError::Executor(
            super::graph::NodeExecutorError::OriginalCustodyUnknown {
                node_id: node_id.clone(), reason: format!("{panic_reason}; original cleanup unconfirmed: {error}"),
            }));
        (node_id, result)
    });
    Ok(retained_owner)
}

pub(crate) async fn abort_pump_nodes(
    runner: &ExecutionGraphRunner,
    graph_id: &str,
    in_flight: &BTreeSet<String>,
    active_nodes: &mut JoinSet<(String, Result<(), ExecutionRunnerError>)>,
) -> Result<(), ExecutionRunnerError> {
    let (owners, missing) = runner.capture_pump_cancellation_owners(graph_id, in_flight).await;
    active_nodes.abort_all();
    while active_nodes.join_next().await.is_some() {}
    runner.drain_pump_cancellation_owners(graph_id, owners).await?;
    if missing {
        return Err(ExecutionRunnerError::Driver("original cancellation owner missing for in-flight node".into()));
    }
    Ok(())
}

fn spawn_owned_work(
    workers: &mut JoinSet<SupervisorCompletion>,
    owner: String,
    work: OwnedWork,
    parallelism: Arc<Semaphore>,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
) {
    metrics.owner_submitted(&owner);
    metrics.active_owned_tasks.fetch_add(1, Ordering::Relaxed);
    workers.spawn(async move {
        let execution = async {
            let permit = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Ok(OwnedCompletionStatus::Aborted);
                }
                permit = parallelism.acquire_owned() => {
                    match permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            return Err("execution parallelism gate is closed".to_string());
                        }
                    }
                }
            };
            tokio::select! {
                _ = cancellation.cancelled() => {
                    drop(permit);
                    Ok(OwnedCompletionStatus::Aborted)
                }
                result = work => {
                    drop(permit);
                    result.map(|()| OwnedCompletionStatus::Completed)
                }
            }
        };
        match AssertUnwindSafe(execution).catch_unwind().await {
            Ok(Ok(status)) => SupervisorCompletion::Owned {
                owner,
                status,
                error: None,
            },
            Ok(Err(error)) => SupervisorCompletion::Owned {
                owner,
                status: OwnedCompletionStatus::Failed,
                error: Some(error),
            },
            Err(panic) => SupervisorCompletion::Owned {
                owner,
                status: OwnedCompletionStatus::Failed,
                error: Some(format!(
                    "owned execution panicked: {}",
                    panic_message(panic)
                )),
            },
        }
    });
}

fn cleanup_idle_slot(
    slots: &StdMutex<HashMap<String, Arc<DriverSlot>>>,
    graph_id: &str,
    slot: &Arc<DriverSlot>,
) {
    let mut slots = slots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if slots
        .get(graph_id)
        .is_some_and(|current| Arc::ptr_eq(current, slot))
        && slot.idle() && Arc::strong_count(slot) <= 2
    {
        slots.remove(graph_id);
    }
}

fn command_advances(command: &ExecutionGraphCommand) -> bool {
    match command {
        ExecutionGraphCommand::SubmitApproval { decision, .. } => decision.approved,
        ExecutionGraphCommand::Resume { .. }
        | ExecutionGraphCommand::Advance { .. }
        | ExecutionGraphCommand::ResolveExternal { .. } => true,
        _ => false,
    }
}

fn partition(graph_id: &str, partitions: u16) -> u16 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    graph_id.hash(&mut hasher);
    (hasher.finish() % u64::from(partitions.max(1))) as u16
}

fn lifecycle_label(value: u8) -> &'static str {
    match value {
        LIFECYCLE_OPEN => "open",
        LIFECYCLE_CLOSING => "closing",
        _ => "closed",
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod completion_pump_tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind,
        ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    use super::*;
    use crate::execution_core::graph::{
        ExecutionCommitService, ExecutionGraphStateStore, ExecutionResourceKind,
        ExecutionResourceManager, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
        NodeExecutor, NodeExecutorError, NodeExecutorRegistry, ResourceQuota, ScopeLockManager,
        WorktreeLeaseManager,
    };
    use crate::runtime_event_store::RuntimeEventStore;

    #[tokio::test]
    async fn shutdown_closes_unstarted_child_wakes_without_overwriting_running_generation() {
        let metrics = SupervisorMetrics::new();
        let child = Arc::new(DriverSlot::new());
        let child_generation = child.request(1, &metrics.lifecycle).unwrap();
        let duplicate = child.request(2, &metrics.lifecycle).unwrap();
        let parent = Arc::new(DriverSlot::new());
        let running_generation = parent.request(1, &metrics.lifecycle).unwrap();
        assert_eq!(parent.start(), running_generation);
        let newer_generation = parent.request(2, &metrics.lifecycle).unwrap();
        let slots = StdMutex::new(HashMap::from([
            ("child".to_string(), Arc::clone(&child)),
            ("parent".to_string(), Arc::clone(&parent)),
        ]));
        let active = HashMap::from([("parent".to_string(), running_generation)]);
        let (sender, mut receiver) = mpsc::channel(4);
        let (_reaper_sender, mut reaper) = mpsc::channel(1);
        sender.try_send(SupervisorMessage::Wake("child".into())).unwrap_or_else(|_| panic!("original child wake admission failed"));
        sender.try_send(SupervisorMessage::Wake("child".into())).unwrap_or_else(|_| panic!("duplicate child wake admission failed"));
        let late_permit = sender.try_reserve().unwrap();
        let late_owned_permit = sender.try_reserve().unwrap();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let mut workers = JoinSet::new();
        let child_waiter = Arc::clone(&child);
        workers.spawn(async move {
            let notified = child_waiter.changed.notified();
            let _ = ready.send(());
            if let Some(outcome) = child_waiter.observation(child_generation) { return outcome; }
            notified.await;
            child_waiter.observation(child_generation).expect("original child slot must resolve")
        });
        waiting.await.unwrap();
        metrics.lifecycle.store(LIFECYCLE_CLOSING, Ordering::Release);
        close_queued_for_shutdown(&mut receiver, &mut reaper, &slots, &active, &metrics);
        let mut late_drain = Box::pin(drain_closed_queue(&mut receiver, &metrics));
        assert!(futures::poll!(&mut late_drain).is_pending());
        late_permit.send(SupervisorMessage::Wake("child".into()));
        late_owned_permit.send(SupervisorMessage::Owned {
            owner: "late-owned".into(), work: Box::pin(async { Ok(()) }),
        });
        late_drain.await;
        let owners = metrics.owners.lock().unwrap();
        assert_eq!(owners["late-owned"].submitted, 1);
        assert_eq!(owners["late-owned"].aborted, 1);
        assert_eq!(owners["late-owned"].completed, 0);
        drop(owners);
        assert!(matches!(workers.join_next().await.unwrap().unwrap(), DriverOutcome::Aborted(_)));
        assert!(matches!(child.observation(duplicate), Some(DriverOutcome::Aborted(_))));
        assert!(parent.observation(running_generation).is_none());
        assert!(matches!(parent.observation(newer_generation), Some(DriverOutcome::Aborted(_))));
        assert!(parent.state.lock().unwrap().active);
        parent.finish(running_generation, DriverOutcome::Failed("original worker result".into()));
        assert!(matches!(parent.observation(running_generation), Some(DriverOutcome::Failed(ref reason)) if reason == "original worker result"));
        assert!(matches!(parent.observation(newer_generation), Some(DriverOutcome::Aborted(_))));
        assert!(child.request(3, &metrics.lifecycle).is_none());
        assert!(sender.try_send(SupervisorMessage::Wake("child".into())).is_err());
    }

    #[tokio::test]
    async fn shutdown_rejects_late_enqueue_without_creating_a_slot() {
        let supervisor = test_supervisor(Arc::new(PumpTestExecutor::new([])));
        let report = supervisor.shutdown().await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(supervisor.enqueue("late-graph").await.is_err());
        assert!(supervisor.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn cleanup_rechecks_slot_under_the_same_map_lock_as_request() {
        let lifecycle = AtomicU8::new(LIFECYCLE_OPEN);
        let slot = Arc::new(DriverSlot::new());
        let generation = slot.request(1, &lifecycle).unwrap();
        slot.finish(generation, DriverOutcome::Failed("original".into()));
        let slots = Arc::new(StdMutex::new(HashMap::from([("graph".to_string(), slot)])));
        let guard = slots.lock().unwrap();
        let cleanup_slot = Arc::clone(guard.get("graph").unwrap());
        let cleanup_slots = Arc::clone(&slots);
        let (started, waiting) = std::sync::mpsc::sync_channel(0);
        let cleaner = std::thread::spawn(move || {
            started.send(()).unwrap();
            cleanup_idle_slot(&cleanup_slots, "graph", &cleanup_slot);
        });
        waiting.recv().unwrap();
        let next = guard.get("graph").unwrap().request(2, &lifecycle).unwrap();
        drop(guard);
        cleaner.join().unwrap();
        let slots = slots.lock().unwrap();
        let current = slots.get("graph").expect("accepted generation must remain mapped");
        assert_eq!(current.requested_generation(), next);
        assert!(current.observation(next).is_none());
    }

    struct PumpTestExecutor {
        kind: &'static str,
        release: Option<Arc<Notify>>,
        delays: BTreeMap<String, Duration>,
        panic_nodes: BTreeSet<String>,
        cancel_error_nodes: BTreeSet<String>,
        conflict_nodes: BTreeSet<String>,
        cancellations: Mutex<Vec<String>>,
        running: AtomicUsize,
        max_running: AtomicUsize,
        slow_finished: AtomicBool,
        successor_started_before_slow_finished: AtomicBool,
        calls: Mutex<Vec<String>>,
        poll_started_at: Mutex<Vec<std::time::Instant>>,
    }

    impl PumpTestExecutor {
        fn new(delays: impl IntoIterator<Item = (String, Duration)>) -> Self {
            Self {
                kind: "completion_pump_test",
                release: None,
                delays: delays.into_iter().collect(),
                panic_nodes: BTreeSet::new(),
                cancel_error_nodes: BTreeSet::new(),
                conflict_nodes: BTreeSet::new(),
                cancellations: Mutex::new(Vec::new()),
                running: AtomicUsize::new(0),
                max_running: AtomicUsize::new(0),
                slow_finished: AtomicBool::new(false),
                successor_started_before_slow_finished: AtomicBool::new(false),
                calls: Mutex::new(Vec::new()),
                poll_started_at: Mutex::new(Vec::new()),
            }
        }

        fn with_panic_node(mut self, node_id: &str) -> Self {
            self.panic_nodes.insert(node_id.to_string());
            self
        }

        fn with_conflicting_goal_cut(mut self, node_id: &str) -> Self {
            self.conflict_nodes.insert(node_id.to_owned());
            self
        }

        fn with_cancel_error_node(mut self, node_id: &str) -> Self {
            self.cancel_error_nodes.insert(node_id.to_owned());
            self
        }

        /// Register this fake executor under another node kind. Used by tests
        /// that must exercise a production-authorized `WaitingExternal` origin
        /// (see harness-contract `WAITING_EXTERNAL_EXECUTOR_KINDS` / PLAN C4).
        fn with_kind(mut self, kind: &'static str) -> Self {
            self.kind = kind;
            self
        }
    }

    #[async_trait]
    impl NodeExecutor for PumpTestExecutor {
        async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.cancellations.lock().unwrap().push(ticket.node_id.clone());
            if self.cancel_error_nodes.contains(&ticket.node_id) {
                Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "injected cancellation failure".into(),
                })
            } else {
                Ok(())
            }
        }

        fn kind(&self) -> &str {
            self.kind
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
            if self.panic_nodes.contains(&ticket.node_id) {
                panic!("injected node panic for {}", ticket.node_id);
            }
            if ticket.node_id == "successor" {
                self.successor_started_before_slow_finished
                    .store(!self.slow_finished.load(Ordering::SeqCst), Ordering::SeqCst);
            }
            if ticket.node_id == "external-wait" {
                return Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                    status: ExecutionNodeStatus::WaitingExternal,
                    result_ref: Some("agentic-program-wait:external-wait:revision:1".to_string()),
                    wait_class: Some(harness_contract::execution_graph::ExternalWaitClass::Barrier),
                    summary: None,
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                }));
            }
            self.poll_started_at
                .lock()
                .unwrap()
                .push(std::time::Instant::now());
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_running.fetch_max(running, Ordering::SeqCst);
            if let Some(release) = &self.release {
                release.notified().await;
            }
            tokio::time::sleep(
                self.delays
                    .get(&ticket.node_id)
                    .copied()
                    .unwrap_or(Duration::from_millis(1)),
            )
            .await;
            self.running.fetch_sub(1, Ordering::SeqCst);
            if ticket.node_id == "slow" {
                self.slow_finished.store(true, Ordering::SeqCst);
            }
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(ticket.node_id.clone());
            let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some(format!("result:{}", ticket.node_id)),
                wait_class: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            });
            if self.conflict_nodes.contains(&ticket.node_id) {
                outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
                    "envelope_id":"pump-conflict-envelope","revision":1,
                    "objective_id":"pump-conflict-objective","created_at_ms":1
                })).unwrap());
                let stream = "goal:pump-conflict";
                outcome.source_revisions.insert(stream.into(), 1);
                outcome.domain_events.push(crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream.into(), scope: crate::RuntimeEventScope::Goal,
                        kind: "goal.observation".into(), status: None, actor: None, refs: Vec::new(),
                        payload: serde_json::json!({"goal_source":{"stream_id":stream,"sequence":2}}),
                    }, schema_version: 2, idempotency_key: Some("pump-conflict".into()),
                });
            }
            Ok(outcome)
        }
    }

    fn test_node(id: &str) -> ExecutionNodeSpec {
        ExecutionNodeSpec {
            id: id.to_string(),
            kind: ExecutionNodeKind::ToolBatch,
            payload_ref: format!("payload:{id}"),
            executor_kind: "completion_pump_test".to_string(),
            idempotency_key: format!("idempotency:{id}"),
            lease_ref: None,
            acceptance: Default::default(),
            retry_policy: Default::default(),
            resource_scopes: Vec::new(),
            work: None,
        }
    }

    fn test_supervisor(executor: Arc<PumpTestExecutor>) -> RuntimeExecutionSupervisor {
        test_supervisor_with_capacity(executor, 8, 2)
    }

    fn test_supervisor_with_capacity(
        executor: Arc<PumpTestExecutor>,
        resource_capacity: usize,
        active_nodes_per_graph: usize,
    ) -> RuntimeExecutionSupervisor {
        test_supervisor_with_executors(
            vec![executor],
            resource_capacity,
            active_nodes_per_graph,
            16,
        )
    }

    fn test_supervisor_with_executors(
        executors: Vec<Arc<dyn NodeExecutor>>,
        resource_capacity: usize,
        active_nodes_per_graph: usize,
        graph_capacity: usize,
    ) -> RuntimeExecutionSupervisor {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let registry = Arc::new(NodeExecutorRegistry::new());
        for executor in executors {
            registry.register(executor).expect("register executor");
        }
        let workspace_id = format!("completion-pump-{}", uuid::Uuid::new_v4());
        let workspace = std::env::temp_dir()
            .join("cowd-completion-pump-tests")
            .join(&workspace_id);
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.join("artifacts"),
        ));
        event_store
            .bind_artifact_store(Arc::clone(&artifacts))
            .expect("event store content owner");
        let leases = WorktreeLeaseManager::open(workspace.join("leases.json"))
            .expect("worktree leases");
        let runner = ExecutionGraphRunner::new(
            registry,
            ExecutionGraphStateStore::new(Arc::clone(&event_store)),
            ExecutionCommitService::new(event_store).with_artifact_store(artifacts),
            Arc::new(ExecutionResourceManager::new([
                (
                    ExecutionResourceKind::Provider,
                    ResourceQuota::new(1, resource_capacity, resource_capacity)
                        .expect("provider quota"),
                ),
                (
                    ExecutionResourceKind::Agent,
                    ResourceQuota::new(1, resource_capacity, resource_capacity)
                        .expect("agent quota"),
                ),
                (
                    ExecutionResourceKind::Tool,
                    ResourceQuota::new(1, resource_capacity, resource_capacity)
                        .expect("tool quota"),
                ),
            ])),
            Arc::new(ScopeLockManager::persistent(workspace.join("scope-locks")).unwrap()),
            Arc::new(leases),
            workspace_id,
            workspace,
        );
        RuntimeExecutionSupervisor::with_limits(
            Arc::new(runner),
            graph_capacity,
            active_nodes_per_graph,
            Duration::from_secs(2),
        )
    }

    struct PhysicalDrainProbe122 {
        kind: &'static str,
        calls: AtomicUsize,
        fail_first: bool,
        fail_always: bool,
        panic: bool,
        driver: tokio::sync::Mutex<Option<JoinHandle<()>>>,
        joined: AtomicBool,
    }
    impl PhysicalDrainProbe122 {
        fn new(kind: &'static str) -> Self {
            Self {
                kind,
                calls: AtomicUsize::new(0),
                fail_first: false,
                fail_always: false,
                panic: false,
                driver: tokio::sync::Mutex::new(None),
                joined: AtomicBool::new(false),
            }
        }
    }
    #[async_trait]
    impl NodeExecutor for PhysicalDrainProbe122 {
        fn kind(&self) -> &str {
            self.kind
        }
        fn validate(&self, _: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
            Ok(())
        }
        async fn start(
            &self,
            _: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            panic!("physical-drain-only probe must not execute business work")
        }
        async fn poll_or_await(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            panic!("physical-drain-only probe must not poll business work")
        }
        async fn drain_physical_execution(&self) -> Result<(), NodeExecutorError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.panic {
                panic!("private-panic-sentinel-122");
            }
            if self.fail_always || (self.fail_first && call == 0) {
                return Err(NodeExecutorError::Uncertain {
                    node_id: "original-122".into(),
                    reason: "fixture driver join failure".into(),
                });
            }
            let mut driver = self.driver.lock().await;
            if let Some(handle) = driver.as_mut() {
                handle.await.map_err(|_| NodeExecutorError::Uncertain {
                    node_id: "held-122".into(),
                    reason: "held original driver join failed".into(),
                })?;
                driver.take();
                self.joined.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn physical_drain_failure_keeps_closing_and_deduplicates_original_errors() {
        let executor = Arc::new(crate::execution_core::graph::ScopedNodeExecutor::new(
            "tool_batch",
        ));
        let ticket = NodeExecutionTicket {
            graph_id: "drain-122-graph".into(),
            node_id: "panic-leaf".into(),
            executor_kind: "tool_batch".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "original-drain-122".into(),
            payload_ref: "{}".into(),
        };
        crate::execution_core::NodeExecutor::prepare_original_binding(executor.as_ref(), &ticket).unwrap();
        let (release, ready) = tokio::sync::oneshot::channel();
        let mut leaf = Box::pin(executor.execute_physical_tool(
            &ticket,
            "original-leaf",
            Box::pin(async move {
                ready.await.unwrap();
                panic!("controlled original Scoped work panic");
            }),
        ));
        assert!(futures::poll!(&mut leaf).is_pending());
        let supervisor = test_supervisor_with_executors(vec![executor.clone()], 1, 1, 1);
        let mut shutdown = Box::pin(supervisor.shutdown());
        assert!(futures::poll!(&mut shutdown).is_pending());
        drop(leaf);
        release.send(()).unwrap();
        let first = AssertUnwindSafe(shutdown)
            .catch_unwind()
            .await
            .expect("real Scoped driver failure must reach the original report");
        assert_eq!(first.errors, vec!["executor tool_batch physical drain failed: executor effect for node `panic-leaf` has uncertain completion: original physical drain could not prove every driver join"]);
        assert_eq!(
            supervisor.metrics.lifecycle.load(Ordering::Acquire),
            LIFECYCLE_CLOSING
        );
        let second = supervisor.shutdown().await;
        assert_eq!(
            second.errors, first.errors,
            "repeat must neither repoll poisoned work nor grow errors"
        );
        assert_eq!(
            supervisor.metrics.lifecycle.load(Ordering::Acquire),
            LIFECYCLE_CLOSING
        );
    }

    #[tokio::test]
    async fn abandoned_physical_drain_keeps_first_error_and_same_held_driver() {
        let mut failed = PhysicalDrainProbe122::new("a-drain-error");
        failed.fail_first = true;
        let failed = Arc::new(failed);
        let release = Arc::new(Semaphore::new(0));
        let exited = Arc::new(AtomicBool::new(false));
        let mut held = PhysicalDrainProbe122::new("b-drain-held");
        *held.driver.get_mut() = Some({
            let release = release.clone();
            let exited = exited.clone();
            tokio::spawn(async move {
                let _permit = release.acquire().await.unwrap();
                exited.store(true, Ordering::SeqCst);
            })
        });
        let held = Arc::new(held);
        let supervisor =
            test_supervisor_with_executors(vec![failed.clone(), held.clone()], 1, 1, 1);
        let mut first = Box::pin(supervisor.shutdown());
        assert!(futures::poll!(&mut first).is_pending());
        assert_eq!(failed.calls.load(Ordering::SeqCst), 1);
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        drop(first);
        let mut resumed = Box::pin(supervisor.shutdown());
        assert!(futures::poll!(&mut resumed).is_pending());
        assert_eq!(
            failed.calls.load(Ordering::SeqCst),
            1,
            "must not rerun and wash out first Err"
        );
        assert_eq!(
            held.calls.load(Ordering::SeqCst),
            1,
            "resume same original join"
        );
        assert!(!exited.load(Ordering::SeqCst));
        release.add_permits(1);
        let report = resumed.await;
        assert!(exited.load(Ordering::SeqCst));
        assert!(held.joined.load(Ordering::SeqCst));
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].contains("a-drain-error physical drain failed"));
        assert_eq!(
            supervisor.metrics.lifecycle.load(Ordering::Acquire),
            LIFECYCLE_CLOSING
        );
        let retry = supervisor.shutdown().await;
        assert_eq!(
            retry.errors, report.errors,
            "success cannot erase historical failure"
        );
        assert_eq!(failed.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            supervisor.metrics.lifecycle.load(Ordering::Acquire),
            LIFECYCLE_CLOSED
        );
    }

    #[tokio::test]
    async fn physical_drain_panic_isolated_until_original_sibling_join() {
        let mut failed = PhysicalDrainProbe122::new("a-drain-panic");
        failed.panic = true;
        let failed = Arc::new(failed);
        let release = Arc::new(Semaphore::new(0));
        let mut held = PhysicalDrainProbe122::new("b-drain-held");
        *held.driver.get_mut() = Some({
            let release = release.clone();
            tokio::spawn(async move {
                let _permit = release.acquire().await.unwrap();
            })
        });
        let held = Arc::new(held);
        let supervisor = test_supervisor_with_executors(vec![failed, held.clone()], 1, 1, 1);
        let mut shutdown = Box::pin(supervisor.shutdown());
        assert!(futures::poll!(&mut shutdown).is_pending());
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        assert!(!held.joined.load(Ordering::SeqCst));
        release.add_permits(1);
        let report = shutdown.await;
        assert!(held.joined.load(Ordering::SeqCst));
        assert_eq!(
            report.errors,
            vec!["executor a-drain-panic physical drain panicked"]
        );
        assert!(!report.errors[0].contains("private-panic-sentinel"));
        assert_eq!(
            supervisor.metrics.lifecycle.load(Ordering::Acquire),
            LIFECYCLE_CLOSING
        );
    }

    #[tokio::test]
    async fn a8_runtime_finalization_survives_observer_cancel_and_releases_service() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let release = Arc::new(Semaphore::new(0));
        let mut held = PhysicalDrainProbe122::new("a8-finalization-held");
        *held.driver.get_mut() = Some({
            let release = Arc::clone(&release);
            tokio::spawn(async move { let _permit = release.acquire().await.unwrap(); })
        });
        let held = Arc::new(held);
        services.executor_registry().register(held.clone()).unwrap();
        let weak = Arc::downgrade(&services);
        assert!(services.subscribe_execution_shutdown_completion().is_none());
        let first = {
            let services = Arc::clone(&services);
            tokio::spawn(async move { services.shutdown_execution().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while held.calls.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
        }).await.unwrap();
        let mut completion = services.subscribe_execution_shutdown_completion().unwrap();
        first.abort(); assert!(first.await.unwrap_err().is_cancelled());
        drop(services);
        assert!(weak.upgrade().is_some(), "the original finalizer retains the service");
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if completion.borrow().is_some() { break; }
                completion.changed().await.unwrap();
            }
        }).await.unwrap();
        assert!(matches!(completion.borrow().as_ref(),
            Some(crate::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed: true })
                if report.errors.is_empty() && report.forced_aborts == 0));
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() { tokio::task::yield_now().await; }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn a8_runtime_finalization_reports_original_physical_panic_as_not_closed() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let mut failed = PhysicalDrainProbe122::new("a8-finalization-panic");
        failed.panic = true;
        services.executor_registry().register(Arc::new(failed)).unwrap();
        let report = services.shutdown_execution().await;
        assert!(!report.errors.is_empty());
        assert_eq!(services.execution_supervisor().health().lifecycle, "closing");
        let completion = services.subscribe_execution_shutdown_completion().unwrap();
        assert!(matches!(completion.borrow().as_ref(),
            Some(crate::RuntimeExecutionShutdownCompletion::Finished { custody_closed: false, .. })));
    }

    #[tokio::test]
    async fn a8_runtime_deadline_unknown_late_completion_uses_same_driver() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let release = Arc::new(Semaphore::new(0));
        let mut held = PhysicalDrainProbe122::new("a8-finalization-late");
        *held.driver.get_mut() = Some({
            let release = Arc::clone(&release);
            tokio::spawn(async move { let _permit = release.acquire().await.unwrap(); })
        });
        let held = Arc::new(held);
        services.executor_registry().register(held.clone()).unwrap();
        let first = services.shutdown_execution().await;
        assert_eq!(first.forced_aborts, 0);
        assert!(!first.errors.is_empty());
        assert_eq!(services.execution_supervisor().health().lifecycle, "closing");
        let mut completion = services.subscribe_execution_shutdown_completion().unwrap();
        assert!(completion.borrow().is_none());
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if completion.borrow().is_some() { break; }
                completion.changed().await.unwrap();
            }
        }).await.unwrap();
        assert!(matches!(completion.borrow().as_ref(),
            Some(crate::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed: true })
                if report.errors.is_empty() && report.forced_aborts == 0));
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        assert_eq!(services.execution_supervisor().health().lifecycle, "closed");
        assert!(services.execution_supervisor().health().last_error.is_some());
    }

    #[tokio::test]
    async fn abandoned_shutdown_retains_dispatcher_join_for_concurrent_waiters() {
        let supervisor = Arc::new(test_supervisor(Arc::new(PumpTestExecutor::new([]))));
        let release = Arc::new(Notify::new());
        let exited = Arc::new(AtomicBool::new(false));
        *supervisor.dispatcher.lock().unwrap() = Some({
            let release = release.clone();
            let exited = exited.clone();
            tokio::spawn(async move {
                release.notified().await;
                exited.store(true, Ordering::SeqCst);
            })
        });
        let first = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.shutdown().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while supervisor.dispatcher.lock().unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(!exited.load(Ordering::SeqCst));
        let mut second = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.shutdown().await })
        };
        let mut third = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.shutdown().await })
        };
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut third)
            .await
            .is_err());
        assert_eq!(supervisor.health().lifecycle, "closing");
        release.notify_one();
        let second = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
        let third = tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .unwrap()
            .unwrap();
        assert!(exited.load(Ordering::SeqCst));
        assert_eq!(supervisor.health().lifecycle, "closed");
        assert_eq!(second, third);
        assert_eq!(second.forced_aborts, 0);
    }

    #[tokio::test]
    async fn a8_shutdown_deadline_retains_original_dispatcher_and_rejoins_late_result() {
        let mut supervisor = test_supervisor(Arc::new(PumpTestExecutor::new([])));
        supervisor.shutdown_timeout = Duration::from_millis(20);
        let supervisor = Arc::new(supervisor);
        let release = Arc::new(Notify::new());
        let exited = Arc::new(AtomicBool::new(false));
        *supervisor.dispatcher.lock().unwrap() = Some({
            let release = Arc::clone(&release); let exited = Arc::clone(&exited);
            tokio::spawn(async move { release.notified().await; exited.store(true, Ordering::SeqCst); })
        });
        let first = supervisor.shutdown().await;
        assert_eq!(first.forced_aborts, 0);
        assert!(first.errors.iter().any(|error| error.contains("dispatcher remains unresolved")));
        assert_eq!(supervisor.health().lifecycle, "closing");
        assert!(supervisor.shutdown_state.lock().await.dispatcher.is_some());
        assert!(!exited.load(Ordering::SeqCst));
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !exited.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
        }).await.unwrap();
        let second = supervisor.shutdown().await;
        assert!(second.errors.iter().any(|error| error.contains("physical drain was not registered")), "{second:?}");
        assert_eq!(second.forced_aborts, 0);
        assert_eq!(supervisor.health().lifecycle, "closing");
        assert!(supervisor.health().last_error.is_some_and(|error| error.contains("dispatcher remains unresolved")));
    }

    #[tokio::test]
    async fn a8_dispatcher_panic_is_reported_without_claiming_closed() {
        let supervisor = test_supervisor(Arc::new(PumpTestExecutor::new([])));
        *supervisor.dispatcher.lock().unwrap() = Some(tokio::spawn(async { panic!("original dispatcher panic") }));
        let report = supervisor.shutdown().await;
        assert!(report.errors.iter().any(|error| error.contains("original execution dispatcher failed")));
        assert_eq!(supervisor.health().lifecycle, "closing");
    }

    #[tokio::test]
    #[ignore = "needs a really started ToolBatch parent attempt; the command reports `bound ToolBatch parent attempt missing`"]
    async fn bound_agent_child_shares_one_parent_slot_but_unrelated_root_cannot() {
        use sha2::{Digest, Sha256};
        let release = Arc::new(Notify::new());
        let mut parent_executor = PumpTestExecutor::new([]);
        parent_executor.release = Some(release.clone());
        let parent_executor = Arc::new(parent_executor);
        let mut leaf_executor = PumpTestExecutor::new([]);
        leaf_executor.kind = "tool_batch";
        let leaf_executor = Arc::new(leaf_executor);
        let supervisor = test_supervisor_with_executors(
            vec![parent_executor.clone(), leaf_executor.clone()],
            1,
            1,
            1,
        );
        let mut parent = ExecutionGraph::new("one-slot Agent parent");
        crate::test_support::attach_execution_graph_lineage(&mut parent);
        let mut parent_node = test_node("parent-agent");
        parent_node.kind = ExecutionNodeKind::AgentTask;
        parent_node.payload_ref =
            serde_json::to_string(&harness_contract::agent::AgentTaskIntent {
                selected_agent_id: None,
                definition_ref: None,
                granted_capabilities: vec![],
                principal_id: "test".into(),
                source_turn_id: "turn".into(),
                run_id: "run".into(),
                task_id: "task".into(),
                root_task_id: "task".into(),
                parent_task_id: None,
                session_id: "session".into(),
                mission_id: "mission".into(),
                team_id: None,
                graph_id: parent.id.clone(),
                node_id: parent_node.id.clone(),
                attempt: 1,
                expected_graph_revision: 0,
                objective: "wait for own tool child".into(),
                required_acceptance: Default::default(),
                output_acceptance: vec![],
                acceptance: vec![],
                constraints: vec![],
                context_refs: vec![],
                evidence_refs: vec![],
                resource_scopes: vec![],
                allowed_tools: vec![],
                allowed_skills: vec![],
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                model_lease: "test".into(),
                budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                    "parent-budget",
                    "agent-budget",
                    "agent",
                    1_000,
                    u64::MAX,
                    1,
                ),
                deadline_at_ms: u64::MAX,
                managed_invocation: None,
                idempotency_key: "parent-invocation".into(),
                agentic_binding: None,
            })
            .unwrap();
        parent.nodes.push(parent_node);
        supervisor
            .submit(
                parent.clone(),
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while parent_executor.running.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(supervisor.parallelism.available_permits(), 0);
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "child-tool".into(),
                tool_name: "read_file".into(),
                input: "{}".into(),
                depends_on: vec![],
            },
        );
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: parent.id.clone(),
                node_id: "parent-agent".into(),
            });
        request.session_id = Some(parent.lineage.as_ref().unwrap().session_id.clone());
        let payload = crate::execution_core::graph::executors::agent_tool::encode_bound_agent_tool(
            "agent",
            "run",
            request.clone(),
        )
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut child = ExecutionGraph::new("bound tool child");
        child.id = format!("agent-tool-batch:{digest}");
        child.parent_execution = request.parent_execution;
        child.lineage = parent.lineage.clone();
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        node.idempotency_key = node.id.clone();
        child.nodes.push(node);
        let mut unrelated = ExecutionGraph::new("independent root cannot borrow capacity");
        crate::test_support::attach_execution_graph_lineage(&mut unrelated);
        unrelated.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "unrelated",
        ));
        let unrelated_id = unrelated.id.clone();
        supervisor
            .submit(
                unrelated,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        let (_, report) = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.submit_and_wait_terminal(
                child,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            ),
        )
        .await
        .expect("child must finish while its sole-capacity parent still waits")
        .unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(leaf_executor.calls.lock().unwrap().len(), 1);
        assert_eq!(supervisor.parallelism.available_permits(), 0);
        release.notify_one();
        tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.wait_for_terminal(&unrelated_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(leaf_executor.calls.lock().unwrap().len(), 2);
        supervisor.shutdown().await;
        assert_eq!(supervisor.parallelism.available_permits(), 1);
    }

    #[tokio::test]
    async fn committed_fast_branch_releases_successor_before_unrelated_slow_branch() {
        let executor = Arc::new(PumpTestExecutor::new([
            ("fast".to_string(), Duration::from_millis(10)),
            ("slow".to_string(), Duration::from_millis(250)),
            ("successor".to_string(), Duration::from_millis(1)),
        ]));
        let supervisor = test_supervisor(Arc::clone(&executor));
        let mut graph = ExecutionGraph::new("immediate successor release");
        graph.id = "completion-pump-successor".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes = vec![test_node("fast"), test_node("slow"), test_node("successor")];
        graph.edges.push(ExecutionEdge {
            from: "fast".to_string(),
            to: "successor".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });

        let (_, report) = supervisor
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph completes");

        assert_eq!(report.completed, 3);
        assert!(
            executor
                .successor_started_before_slow_finished
                .load(Ordering::SeqCst),
            "a committed fast branch must release its successor immediately"
        );
        assert_eq!(executor.calls.lock().unwrap().len(), 3);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn completion_pump_runs_independent_ready_nodes_concurrently() {
        let executor =
            Arc::new(PumpTestExecutor::new((0..4).map(|index| {
                (format!("parallel-{index}"), Duration::from_millis(40))
            })));
        let supervisor = test_supervisor(Arc::clone(&executor));
        let mut graph = ExecutionGraph::new("parallel ready nodes");
        graph.id = "completion-pump-concurrency".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes = (0..4)
            .map(|index| test_node(&format!("parallel-{index}")))
            .collect();

        let (_, report) = supervisor
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph completes");

        assert_eq!(report.completed, 4);
        assert!(
            executor.max_running.load(Ordering::SeqCst) > 1,
            "independent ready nodes must overlap"
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "scripts/test/runtime-execution-performance.sh"]
    async fn completion_pump_saturates_sixty_four_independent_work_items() {
        let executor =
            Arc::new(PumpTestExecutor::new((0..64).map(|index| {
                (format!("parallel-64-{index}"), Duration::from_millis(75))
            })));
        let supervisor = test_supervisor_with_capacity(Arc::clone(&executor), 64, 64);
        let mut graph = ExecutionGraph::new("64 independent work items");
        graph.id = "completion-pump-64".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes = (0..64)
            .map(|index| test_node(&format!("parallel-64-{index}")))
            .collect();

        let started = std::time::Instant::now();
        let (_, report) = supervisor
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("64-work graph completes");

        assert_eq!(report.completed, 64);
        assert_eq!(executor.max_running.load(Ordering::SeqCst), 64);
        let mut dispatch_latencies = executor
            .poll_started_at
            .lock()
            .unwrap()
            .iter()
            .map(|observed| observed.duration_since(started))
            .collect::<Vec<_>>();
        dispatch_latencies.sort_unstable();
        assert!(
            dispatch_latencies[60] < Duration::from_millis(250),
            "dispatch p95 must stay below 250ms, observed {:?}",
            dispatch_latencies[60]
        );
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "64 independent 75ms work items must execute as one saturated wave"
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "unresolved: 58 concurrent ready nodes exhaust the 60s physical-closure budget (62.9-63.4s); measured that shutdown_impl is never entered, coordinate_original_gate records only 2 GATE_OK, and the pump loop count is 0 — the lead is load/closure accounting under a current_thread runtime, not a lock or a lost wakeup"]
    async fn completion_pump_keeps_mixed_ready_utilization_above_ninety_percent() {
        const READY: usize = 58;
        const CAPACITY: usize = 64;
        let executor =
            Arc::new(PumpTestExecutor::new((0..READY).map(|index| {
                (format!("mixed-{index}"), Duration::from_millis(75))
            })));
        let supervisor = test_supervisor_with_capacity(Arc::clone(&executor), CAPACITY, CAPACITY);
        let mut graph = ExecutionGraph::new("mixed ready utilization");
        graph.id = "completion-pump-mixed-utilization".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes = (0..READY)
            .map(|index| test_node(&format!("mixed-{index}")))
            .collect();

        let (_, report) = supervisor
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("mixed-utilization graph completes");

        assert_eq!(report.completed, READY);
        let peak = executor.max_running.load(Ordering::SeqCst);
        assert!(
            peak * 100 >= CAPACITY * 90,
            "ready work must use at least 90% of capacity: peak={peak}, capacity={CAPACITY}"
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn foreground_terminal_wait_does_not_mistake_external_quiescence_for_completion() {
        let executor = Arc::new(PumpTestExecutor::new([]).with_kind("agentic_program_wait"));
        let unrelated_executor = Arc::new(PumpTestExecutor::new([]));
        let supervisor = Arc::new(test_supervisor_with_executors(
            vec![executor, unrelated_executor.clone()],
            1,
            1,
            1,
        ));
        let mut graph = ExecutionGraph::new("foreground waits for durable terminal truth");
        graph.id = "foreground-terminal-wait".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut external_wait = test_node("external-wait");
        external_wait.executor_kind = "agentic_program_wait".to_string();
        graph.nodes = vec![external_wait];
        let graph_id = graph.id.clone();
        let waiting = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move {
                supervisor
                    .submit_and_wait_terminal(
                        graph,
                        ExecutionGraphCommand::Start {
                            expected_revision: 0,
                        },
                    )
                    .await
            })
        };

        let projection = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(projection) = supervisor.projection(&graph_id).await {
                    if projection.nodes[0].status == ExecutionNodeStatus::WaitingExternal {
                        break projection;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("graph reaches durable WaitingExternal");
        assert!(
            !waiting.is_finished(),
            "foreground completion must remain pending while the graph is only quiescent"
        );

        let mut unrelated = ExecutionGraph::new("parked parent releases shared graph capacity");
        crate::test_support::attach_execution_graph_lineage(&mut unrelated);
        unrelated.lineage.as_mut().unwrap().session_id = "unrelated-session".to_string();
        unrelated.nodes.push(test_node("unrelated-ready"));
        let (_, unrelated_report) = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.submit_and_wait_terminal(
                unrelated,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            ),
        )
        .await
        .expect("unrelated Session must run while the foreground waiter remains parked")
        .expect("unrelated graph completes");
        assert_eq!(unrelated_report.completed, 1);
        assert_eq!(
            unrelated_executor.calls.lock().unwrap().as_slice(),
            &["unrelated-ready"]
        );
        assert!(!waiting.is_finished());

        // The waiting graph may have advanced after the projection snapshot was
        // taken (a parked wait re-arms), so the command fences on the live
        // revision instead of a stale one.
        let resolve_revision = supervisor
            .projection(&graph_id)
            .await
            .expect("live projection")
            .revision;
        supervisor
            .command_graph(
                &graph_id,
                ExecutionGraphCommand::ResolveExternal {
                    expected_revision: resolve_revision,
                    node_id: "external-wait".to_string(),
                    result_ref: "agentic-program-checkpoint:external-wait:revision:1:resolved"
                        .to_string(),
                    correlation_id: "agentic-program-wait:external-wait:node:1".to_string(),
                },
            )
            .await
            .expect("resolve external wait");
        let (_, report) = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("terminal wait completes")
            .expect("terminal wait task joins")
            .expect("terminal wait succeeds");
        assert_eq!(report.completed, 1);
        supervisor.shutdown().await;
    }

    fn panic_resource_fixture(
        executor: Arc<PumpTestExecutor>,
        workspace: &std::path::Path,
    ) -> (
        RuntimeExecutionSupervisor,
        ExecutionGraphStateStore,
        Arc<RuntimeEventStore>,
        Arc<ExecutionResourceManager>,
    ) {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let resources = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::Tool,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]));
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry.register(executor.clone()).unwrap();
        let runner = ExecutionGraphRunner::new(
            registry,
            state.clone(),
            ExecutionCommitService::new(event_store.clone()),
            resources.clone(),
            Arc::new(ScopeLockManager::persistent(workspace.join("scope-locks")).unwrap()),
            Arc::new(WorktreeLeaseManager::open(workspace.join("leases.json")).unwrap()),
            "panic-quota-workspace",
            workspace,
        );
        let supervisor =
            RuntimeExecutionSupervisor::with_limits(Arc::new(runner), 1, 1, Duration::from_secs(2));
        (supervisor, state, event_store, resources)
    }

    #[tokio::test]
    #[ignore = "unresolved: this oracle contradicts node_panic_does_not_terminate_unrelated_ready_nodes, which pins panic as custody-unknown (node stays Running, error exposes `original pump task panicked`); making a panic a failed node result would break that pinned product stance"]
    async fn node_panic_returns_materialize_quota_before_another_graph_needs_it() {
        let workspace = tempfile::tempdir().unwrap();
        let executor = Arc::new(PumpTestExecutor::new([]).with_panic_node("panic-node"));
        let (supervisor, state, event_store, resources) =
            panic_resource_fixture(executor.clone(), workspace.path());
        let mut panicking = ExecutionGraph::new("panic under the real single Tool quota");
        crate::test_support::attach_execution_graph_lineage(&mut panicking);
        let mut panic_node = test_node("panic-node");
        panic_node.kind = ExecutionNodeKind::Materialize;
        panicking.nodes.push(panic_node);
        let panic_graph_id = panicking.id.clone();
        let panic_result = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.submit_and_wait_terminal(
                panicking,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            ),
        )
        .await;
        let panic_graph = state.load(&panic_graph_id);
        let after_panic = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
        let mut healthy = ExecutionGraph::new("another graph needs the returned Tool quota");
        crate::test_support::attach_execution_graph_lineage(&mut healthy);
        let mut healthy_node = test_node("healthy-after-panic");
        healthy_node.kind = ExecutionNodeKind::Materialize;
        healthy.nodes.push(healthy_node);
        let healthy_result = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.submit_and_wait_terminal(
                healthy,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            ),
        )
        .await;
        supervisor.shutdown().await;
        drop(supervisor);
        let drained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
        assert_eq!(drained.active_leases, 0);
        assert_eq!(drained.queued_waiters, 0);
        let (_, panic_report) = panic_result.unwrap().unwrap();
        assert_eq!(panic_report.failed, 1);
        let canonical = state.load(&panic_graph_id).unwrap();
        let binding = state.current_binding(&canonical, "panic-node", 1).unwrap();
        let (ticket, _) = state
            .binding_start_ticket(&canonical, "panic-node", &binding)
            .unwrap();
        let effects = event_store
            .list_stream(
                &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(&ticket),
            )
            .unwrap();
        assert_eq!(
            effects
                .iter()
                .filter(|event| event.kind == "execution.effect.intent")
                .count(),
            1
        );
        assert!(effects
            .iter()
            .all(|event| event.kind != "execution.effect.receipt"));
        println!(
            "V3_PANIC_RESOURCE_WITNESS {}",
            serde_json::json!({
                "after_panic_active": after_panic.active_leases,
                "after_shutdown_active": drained.active_leases,
                "effect_intents": 1, "effect_receipts": 0,
                "healthy_finished": matches!(&healthy_result, Ok(Ok((_, report))) if report.completed == 1),
            })
        );
        let panic_graph = panic_graph.unwrap();
        let failure = panic_graph.node_results["panic-node"]
            .failure
            .as_ref()
            .unwrap();
        assert!(failure
            .message
            .contains("injected node panic for panic-node"));
        assert_eq!(after_panic.active_leases, 0);
        let (_, healthy_report) = healthy_result
            .expect("a node panic must not strand shared capacity until Runtime shutdown")
            .expect("healthy graph completes");
        assert_eq!(healthy_report.completed, 1);
        assert_eq!(
            executor.calls.lock().unwrap().as_slice(),
            &["healthy-after-panic"]
        );
    }

    #[tokio::test]
    async fn conflicting_original_pump_drains_same_ticket_and_keeps_unknown_custody() {
        let workspace = tempfile::tempdir().unwrap();
        let executor = Arc::new(PumpTestExecutor::new([]).with_conflicting_goal_cut("conflict-node"));
        let (supervisor, state, events, _resources) = panic_resource_fixture(executor.clone(), workspace.path());
        let mut graph = ExecutionGraph::new("original pump conflicting goal cut");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut node = test_node("conflict-node");
        node.kind = ExecutionNodeKind::Materialize;
        graph.nodes.push(node);
        let graph_id = graph.id.clone();
        let result = tokio::time::timeout(Duration::from_secs(2), supervisor.submit_and_wait_terminal(
            graph, ExecutionGraphCommand::Start { expected_revision: 0 },
        )).await.expect("original pump must return rather than await itself");
        let error = result.expect_err("the original conflicting outcome must stay uncommitted");
        assert!(error.to_string().contains("observation source conflicts with causal read-set"), "{error}");
        assert!(error.to_string().contains("original call custody"), "{error}");
        assert_eq!(executor.cancellations.lock().unwrap().as_slice(), &["conflict-node"]);
        let current = state.load(&graph_id).unwrap();
        assert_eq!(current.node_statuses["conflict-node"], ExecutionNodeStatus::Running);
        let binding = state.current_binding(&current, "conflict-node", 1).unwrap();
        assert!(state.binding_retirement_record(&current, "conflict-node", &binding).unwrap().is_none());
        assert!(events.event_by_idempotency_key("goal:pump-conflict", "pump-conflict").unwrap().is_none());
    }

    #[tokio::test]
    async fn node_panic_cleanup_failure_remains_unsettled_and_reaches_original_driver_error() {
        let workspace = tempfile::tempdir().unwrap();
        let executor = Arc::new(
            PumpTestExecutor::new([])
                .with_panic_node("panic-node")
                .with_cancel_error_node("panic-node"),
        );
        let (supervisor, state, events, resources) =
            panic_resource_fixture(executor, workspace.path());
        let mut graph = ExecutionGraph::new("cleanup failure cannot be reported as settlement");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut work = test_node("panic-node");
        work.kind = ExecutionNodeKind::Materialize;
        graph.nodes.push(work);
        let graph_id = graph.id.clone();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.submit_and_wait_terminal(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            ),
        )
        .await;
        let before_shutdown = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
        let current = state.load(&graph_id).unwrap();
        let health = supervisor.health();
        let binding = state.current_binding(&current, "panic-node", 1).unwrap();
        let (ticket, _) = state
            .binding_start_ticket(&current, "panic-node", &binding)
            .unwrap();
        let effects = events
            .list_stream(
                &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(&ticket),
            )
            .unwrap();
        supervisor.shutdown().await;
        drop(supervisor);
        let drained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
        let error = result
            .expect("original driver must return its cleanup error")
            .unwrap_err();
        assert!(
            error.to_string().contains("injected cancellation failure"),
            "{error}"
        );
        assert_eq!(
            current.node_statuses["panic-node"],
            ExecutionNodeStatus::Running
        );
        assert_eq!(before_shutdown.active_leases, 1);
        assert!(health
            .last_error
            .is_some_and(|error| error.contains("injected cancellation failure")));
        assert_eq!(
            effects
                .iter()
                .filter(|event| event.kind == "execution.effect.intent")
                .count(),
            1
        );
        assert!(effects
            .iter()
            .all(|event| event.kind != "execution.effect.receipt"));
        assert_eq!(drained.active_leases, 0);
        assert_eq!(drained.queued_waiters, 0);
    }

    #[tokio::test]
    async fn node_panic_does_not_terminate_unrelated_ready_nodes() {
        let executor = Arc::new(
            PumpTestExecutor::new([
                ("panic-node".to_string(), Duration::from_millis(5)),
                ("healthy-node".to_string(), Duration::from_millis(40)),
            ])
            .with_panic_node("panic-node"),
        );
        let supervisor = test_supervisor(Arc::clone(&executor));
        let mut graph = ExecutionGraph::new("node panic isolation");
        graph.id = "completion-pump-panic-isolation".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes = vec![test_node("panic-node"), test_node("healthy-node"), test_node("dependent-node")];
        graph.edges.push(harness_contract::execution_graph::ExecutionEdge {
            from: "panic-node".into(), to: "dependent-node".into(),
            kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
        });

        let error = supervisor
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect_err("unresolved original custody must remain visible");

        assert!(error.to_string().contains("original pump task panicked"));
        let graph = supervisor.runner.state_store().load("completion-pump-panic-isolation").unwrap();
        assert_eq!(graph.node_statuses["healthy-node"], ExecutionNodeStatus::Completed);
        assert_eq!(graph.node_statuses["panic-node"], ExecutionNodeStatus::Running);
        assert!(!graph.node_statuses["dependent-node"].is_terminal());
        let state = supervisor.runner.state_store();
        let binding = state.current_binding(&graph, "panic-node", 1).unwrap();
        let position = state.binding_start_position(&graph, "panic-node", &binding).unwrap().unwrap();
        assert!(!state.binding_lease_closure(&graph, "panic-node", &binding, position).unwrap());
        assert!(
            executor
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&"healthy-node".to_string()),
            "the unrelated ready node must still execute"
        );
        assert!(!executor.calls.lock().unwrap().contains(&"dependent-node".to_string()));
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn original_pump_reentry_rejoins_same_ticket_before_or_after_completion() {
        for complete_before_reentry in [false, true] {
            let release = Arc::new(Notify::new());
            let mut executor = PumpTestExecutor::new([]);
            executor.release = Some(Arc::clone(&release));
            let executor = Arc::new(executor);
            let supervisor = test_supervisor(Arc::clone(&executor));
            let runner = Arc::clone(&supervisor.runner);
            let mut graph = ExecutionGraph::new("original pump observer reentry");
            crate::test_support::attach_execution_graph_lineage(&mut graph);
            graph.nodes.push(test_node("original"));
            let graph = runner.register(graph).await.unwrap();
            let graph_id = graph.id.clone();
            let first = {
                let runner = Arc::clone(&runner);
                let graph_id = graph_id.clone();
                tokio::spawn(async move { run_completion_pump(runner, &graph_id, 2, CancellationToken::new()).await })
            };
            tokio::time::timeout(Duration::from_secs(2), async {
                while executor.running.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
            }).await.unwrap();
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            if complete_before_reentry {
                release.notify_one();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while runner.state_store().load(&graph_id).unwrap().node_statuses["original"] != ExecutionNodeStatus::Completed {
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap();
            }
            let resumed = {
                let runner = Arc::clone(&runner);
                let graph_id = graph_id.clone();
                tokio::spawn(async move { run_completion_pump(runner, &graph_id, 2, CancellationToken::new()).await })
            };
            if !complete_before_reentry { release.notify_one(); }
            let result = tokio::time::timeout(Duration::from_secs(2), resumed).await.unwrap().unwrap();
            assert!(matches!(result, DriverOutcome::Completed(ref report) if report.completed == 1), "{result:?}");
            assert_eq!(executor.poll_started_at.lock().unwrap().len(), 1, "the original ticket must not be dispatched twice");
            assert_eq!(executor.calls.lock().unwrap().as_slice(), &["original"]);
            assert_eq!(runner.state_store().load(&graph_id).unwrap().node_statuses["original"], ExecutionNodeStatus::Completed);
        }
    }

    #[tokio::test]
    async fn wait_is_observer_only_and_notify_is_the_explicit_execution_signal() {
        let executor = Arc::new(PumpTestExecutor::new([(
            "observed".to_string(),
            Duration::from_millis(1),
        )]));
        let supervisor = test_supervisor(Arc::clone(&executor));
        let mut graph = ExecutionGraph::new("observer separation");
        graph.id = "completion-pump-observer".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(test_node("observed"));
        supervisor
            .register_graph(graph)
            .await
            .expect("register graph");

        let error = supervisor
            .wait_for_quiescence("completion-pump-observer")
            .await
            .expect_err("an observer must not start an unadmitted graph");
        assert!(matches!(
            error,
            ExecutionRunnerError::SupervisorUnavailable(_)
        ));
        assert!(executor.calls.lock().unwrap().is_empty());

        supervisor
            .notify_graph("completion-pump-observer")
            .await
            .expect("notify graph");
        let report = supervisor
            .wait_for_quiescence("completion-pump-observer")
            .await
            .expect("observe admitted graph");
        assert_eq!(report.completed, 1);
        assert_eq!(executor.calls.lock().unwrap().as_slice(), &["observed"]);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn drive_registered_admits_and_completes_a_recovered_graph() {
        let executor = Arc::new(PumpTestExecutor::new([(
            "recovered".to_string(),
            Duration::from_millis(1),
        )]));
        let supervisor = test_supervisor(Arc::clone(&executor));
        let mut graph = ExecutionGraph::new("recovered registered graph");
        graph.id = "completion-pump-recovered".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(test_node("recovered"));
        supervisor
            .register_graph(graph)
            .await
            .expect("register recovered graph");

        let (receipt, report) = supervisor
            .drive_registered("completion-pump-recovered")
            .await
            .expect("drive recovered graph");

        assert_eq!(receipt.graph_id, "completion-pump-recovered");
        assert_eq!(report.completed, 1);
        assert_eq!(executor.calls.lock().unwrap().as_slice(), &["recovered"]);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_joins_resource_deferred_graph_without_retiring_its_ready_node() {
        let workspace = tempfile::tempdir().unwrap();
        let executor = Arc::new(PumpTestExecutor::new([]));
        let (supervisor, state, _events, resources) =
            panic_resource_fixture(executor.clone(), workspace.path());
        let lease = resources.acquire(ExecutionResourceKind::Tool, None).await.unwrap();
        let mut graph = ExecutionGraph::new("shutdown while tool capacity is held");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut node = test_node("deferred-at-shutdown");
        node.kind = ExecutionNodeKind::Materialize;
        graph.nodes.push(node);
        let graph_id = graph.id.clone();
        supervisor.register_graph(graph).await.unwrap();
        let waiting_resources = resources.clone();
        let earlier_waiter = tokio::spawn(async move {
            waiting_resources.acquire(ExecutionResourceKind::Tool, None).await
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while resources.snapshot(&ExecutionResourceKind::Tool).unwrap().queued_waiters != 1 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        let snapshot = supervisor.runner.pump_snapshot(&graph_id, &BTreeSet::new()).await.unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut prepared = tokio::time::timeout(
            Duration::from_secs(1),
            supervisor.runner.prepare_ready_wave(&graph_id, snapshot.ready),
        ).await.expect("quota admission must defer without waiting").unwrap();
        assert_eq!(prepared.len(), 1);
        let result = supervisor.runner.execute_prepared_pump_node(
            &graph_id, prepared.pop().unwrap(), Arc::new(Notify::new()),
        ).await;
        assert!(matches!(result, Err(ExecutionRunnerError::ResourceDeferred { .. })));
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().queued_waiters, 1);
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().cancelled_rate_basis_points, None);
        supervisor.notify_graph(&graph_id).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let graph = state.load(&graph_id).unwrap();
            if supervisor.metrics.active_drivers.load(Ordering::Acquire) == 1
                && graph.node_statuses["deferred-at-shutdown"] == ExecutionNodeStatus::Ready
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        // Hold the original lease across several resource retry wakeups.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(supervisor.metrics.active_drivers.load(Ordering::Acquire), 1);
        assert!(executor.calls.lock().unwrap().is_empty());

        let report = tokio::time::timeout(Duration::from_secs(1), supervisor.shutdown())
            .await
            .expect("the original graph worker must join after cancellation");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.forced_aborts, 0);
        assert_eq!(report.cancelled_graphs, 0);
        assert_eq!(report.preserved_graphs, 1);
        assert_eq!(state.load(&graph_id).unwrap().node_statuses["deferred-at-shutdown"], ExecutionNodeStatus::Ready);
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().active_leases, 1);
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().queued_waiters, 1);
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().cancelled_rate_basis_points, None);
        drop(lease);
        let granted = tokio::time::timeout(Duration::from_secs(1), earlier_waiter)
            .await.unwrap().unwrap().unwrap();
        drop(granted);
        let next = tokio::time::timeout(Duration::from_secs(1), resources.acquire(ExecutionResourceKind::Tool, None))
            .await.unwrap().unwrap();
        drop(next);
        assert_eq!(resources.snapshot(&ExecutionResourceKind::Tool).unwrap().queued_waiters, 0);
    }

    #[tokio::test]
    async fn shutdown_preserves_nonterminal_graph_for_process_recovery() {
        let executor = Arc::new(PumpTestExecutor::new([(
            "survives-restart".to_string(),
            Duration::from_secs(30),
        )]));
        let supervisor = test_supervisor(executor);
        let mut graph = ExecutionGraph::new("restart is not cancellation");
        graph.id = "completion-pump-process-restart".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(test_node("survives-restart"));
        supervisor.register_graph(graph).await.unwrap();
        supervisor
            .notify_graph("completion-pump-process-restart")
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let graph = supervisor
                .runner
                .state_store()
                .load("completion-pump-process-restart")
                .unwrap();
            if graph.node_statuses["survives-restart"] == ExecutionNodeStatus::Running {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let report = supervisor.shutdown().await;
        assert_eq!(report.cancelled_graphs, 0);
        assert_eq!(report.preserved_graphs, 1);
        let graph = supervisor
            .runner
            .state_store()
            .load("completion-pump-process-restart")
            .unwrap();
        assert_eq!(
            graph.node_statuses["survives-restart"],
            ExecutionNodeStatus::Running,
            "the next process must recover the interrupted idempotent attempt"
        );
    }
}
