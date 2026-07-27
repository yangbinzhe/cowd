use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::FutureExt;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

use super::graph::{
    ExecutionGraphHost, ExecutionGraphHostReceipt, ExecutionGraphRunner, ExecutionRecoveryError,
    ExecutionRunReport, ExecutionRunnerError,
};
use crate::CancellationToken;

const DEFAULT_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_MAX_PARALLEL_GRAPHS: usize = 64;
const DEFAULT_MAX_PARALLEL_OWNED_TASKS: usize = 256;
const DEFAULT_QUEUE_PARTITIONS: u16 = 32;
const LIFECYCLE_OPEN: u8 = 0;
const LIFECYCLE_CLOSING: u8 = 1;
const LIFECYCLE_CLOSED: u8 = 2;

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
    pub cancelled_graphs: usize,
    pub forced_aborts: u64,
    pub remaining_keys: usize,
    pub errors: Vec<String>,
    pub owners: std::collections::BTreeMap<String, RuntimeExecutionOwnerReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeWorkAdmissionReceipt {
    pub admission_id: String,
    pub owner: String,
    pub accepted_at_ms: u64,
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
}

#[derive(Debug)]
struct DriverSlot {
    state: StdMutex<DriverSlotState>,
    changed: tokio::sync::Notify,
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
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn request(&self, now_ms: u64) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.requested_generation = state.requested_generation.saturating_add(1);
        if !state.active {
            state.queued_at_ms = now_ms;
        }
        state.requested_generation
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
        (state.completed_generation >= generation)
            .then(|| state.outcome.clone())
            .flatten()
    }

    fn idle(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.active && state.completed_generation >= state.requested_generation
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

/// The single Runtime owner of durable graph execution.
///
/// Callers may admit work, issue typed commands, wait for a projection, or
/// inspect health. Only this supervisor owns and invokes the graph runner.
pub struct RuntimeExecutionSupervisor {
    runner: Arc<ExecutionGraphRunner>,
    sender: mpsc::Sender<SupervisorMessage>,
    receiver: StdMutex<Option<mpsc::Receiver<SupervisorMessage>>>,
    reaper_sender: mpsc::UnboundedSender<SupervisorMessage>,
    reaper_receiver: StdMutex<Option<mpsc::UnboundedReceiver<SupervisorMessage>>>,
    dispatcher: StdMutex<Option<JoinHandle<()>>>,
    slots: Arc<StdMutex<HashMap<String, Arc<DriverSlot>>>>,
    parallelism: Arc<Semaphore>,
    owned_parallelism: Arc<Semaphore>,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
    queue_capacity: usize,
    queue_partitions: u16,
    shutdown_timeout: Duration,
}

impl RuntimeExecutionSupervisor {
    #[must_use]
    pub(crate) fn new(runner: Arc<ExecutionGraphRunner>) -> Self {
        Self::with_limits(
            runner,
            DEFAULT_QUEUE_CAPACITY,
            DEFAULT_MAX_PARALLEL_GRAPHS,
            Duration::from_secs(20),
        )
    }

    #[must_use]
    pub(crate) fn with_limits(
        runner: Arc<ExecutionGraphRunner>,
        queue_capacity: usize,
        max_parallel_graphs: usize,
        shutdown_timeout: Duration,
    ) -> Self {
        let queue_capacity = queue_capacity.max(1);
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (reaper_sender, reaper_receiver) = mpsc::unbounded_channel();
        Self {
            runner,
            sender,
            receiver: StdMutex::new(Some(receiver)),
            reaper_sender,
            reaper_receiver: StdMutex::new(Some(reaper_receiver)),
            dispatcher: StdMutex::new(None),
            slots: Arc::new(StdMutex::new(HashMap::new())),
            parallelism: Arc::new(Semaphore::new(max_parallel_graphs.max(1))),
            owned_parallelism: Arc::new(Semaphore::new(DEFAULT_MAX_PARALLEL_OWNED_TASKS)),
            cancellation: CancellationToken::new(),
            metrics: Arc::new(SupervisorMetrics::new()),
            queue_capacity,
            queue_partitions: DEFAULT_QUEUE_PARTITIONS,
            shutdown_timeout,
        }
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
        let cancellation = self.cancellation.clone();
        let metrics = Arc::clone(&self.metrics);
        *dispatcher = Some(tokio::spawn(async move {
            dispatch_loop(
                receiver,
                reaper_receiver,
                runner,
                slots,
                parallelism,
                owned_parallelism,
                cancellation,
                metrics,
            )
            .await;
        }));
        Ok(())
    }

    fn slot(&self, graph_id: &str) -> Arc<DriverSlot> {
        Arc::clone(
            self.slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(graph_id.to_string())
                .or_insert_with(|| Arc::new(DriverSlot::new())),
        )
    }

    async fn enqueue(
        &self,
        graph_id: &str,
    ) -> Result<(Arc<DriverSlot>, u64), ExecutionRunnerError> {
        self.ensure_dispatcher()?;
        let slot = self.slot(graph_id);
        let generation = slot.request(now_ms());
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

    pub(crate) async fn spawn_owned(
        &self,
        owner: impl Into<String>,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), ExecutionRunnerError> {
        self.admit_owned(owner, async move {
            work.await;
            Ok(())
        })
        .await
        .map(|_| ())
    }

    pub async fn admit_owned(
        &self,
        owner: impl Into<String>,
        work: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
    ) -> Result<RuntimeWorkAdmissionReceipt, ExecutionRunnerError> {
        self.ensure_dispatcher()?;
        let owner = owner.into();
        self.sender
            .send(SupervisorMessage::Owned {
                owner: owner.clone(),
                work: Box::pin(work),
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
        match self.reaper_sender.send(message) {
            Ok(()) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
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

    pub(crate) async fn drive_registered(
        &self,
        graph_id: &str,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
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
        if !slot.idle() || Arc::strong_count(slot) > 2 {
            return;
        }
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slots
            .get(graph_id)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            slots.remove(graph_id);
        }
    }

    pub async fn submit_and_wait(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
        let (receipt, slot, generation) = self.admit(graph, command).await?;
        let report = self.await_slot(&receipt.graph_id, slot, generation).await?;
        Ok((receipt, report))
    }

    pub async fn wait_for_quiescence(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let (slot, generation) = self.enqueue(graph_id).await?;
        self.await_slot(graph_id, slot, generation).await
    }

    pub async fn submit_command_and_wait(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<(ExecutionGraphHostReceipt, ExecutionRunReport), ExecutionRunnerError> {
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
            return self.shutdown_report(0, Vec::new());
        }

        let graph_ids = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut cancelled_graphs = 0;
        let mut errors = Vec::new();
        for graph_id in graph_ids {
            let Ok(projection) = self.runner.projection(&graph_id).await else {
                continue;
            };
            if projection
                .nodes
                .iter()
                .all(|node| node.status.is_terminal())
            {
                continue;
            }
            match self
                .runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: projection.revision,
                        reason: "Runtime execution supervisor shutdown".to_string(),
                    },
                )
                .await
            {
                Ok(_) => cancelled_graphs += 1,
                Err(error) => errors.push(format!("{graph_id}: {error}")),
            }
        }
        self.cancellation.cancel();
        let dispatcher = self
            .dispatcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut dispatcher) = dispatcher {
            if tokio::time::timeout(self.shutdown_timeout, &mut dispatcher)
                .await
                .is_err()
            {
                dispatcher.abort();
                let _ = dispatcher.await;
                let forced = self.metrics.force_abort_active_owners().max(1);
                self.metrics
                    .forced_aborts
                    .fetch_add(forced, Ordering::Relaxed);
                self.metrics.active_drivers.store(0, Ordering::Relaxed);
                self.metrics.active_owned_tasks.store(0, Ordering::Relaxed);
                errors.push("execution dispatcher exceeded shutdown timeout".to_string());
            }
        }
        self.metrics
            .lifecycle
            .store(LIFECYCLE_CLOSED, Ordering::Release);
        self.shutdown_report(cancelled_graphs, errors)
    }

    fn shutdown_report(
        &self,
        cancelled_graphs: usize,
        errors: Vec<String>,
    ) -> RuntimeExecutionShutdownReport {
        RuntimeExecutionShutdownReport {
            accepted: self.metrics.accepted.load(Ordering::Relaxed),
            completed: self.metrics.completed.load(Ordering::Relaxed),
            failed: self.metrics.failed.load(Ordering::Relaxed),
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
        let graph = self.runner.command(graph_id, command).await?;
        if should_wake {
            let _ = self.enqueue(graph_id).await?;
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

async fn dispatch_loop(
    mut receiver: mpsc::Receiver<SupervisorMessage>,
    mut reaper_receiver: mpsc::UnboundedReceiver<SupervisorMessage>,
    runner: Arc<ExecutionGraphRunner>,
    slots: Arc<StdMutex<HashMap<String, Arc<DriverSlot>>>>,
    parallelism: Arc<Semaphore>,
    owned_parallelism: Arc<Semaphore>,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
) {
    let mut workers = JoinSet::new();
    let mut active = HashMap::<String, u64>::new();
    let mut pending = HashSet::<String>::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                receiver.close();
                reaper_receiver.close();
                while let Ok(message) = receiver.try_recv() {
                    if let SupervisorMessage::Owned { owner, .. } = message {
                        metrics.owner_submitted(&owner);
                        metrics.owner_aborted(&owner);
                    }
                }
                while let Ok(SupervisorMessage::Owned { owner, .. }) =
                    reaper_receiver.try_recv()
                {
                    metrics.owner_submitted(&owner);
                    metrics.owner_aborted(&owner);
                }

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
                            }
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
                    let generation = slot
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .requested_generation;
                    if !slot.idle() {
                        slot.finish(
                            generation,
                            DriverOutcome::Aborted(
                                "execution cancelled during shutdown".to_string(),
                            ),
                        );
                    }
                }
                break;
            }
            Some(message) = receiver.recv() => {
                match message {
                    SupervisorMessage::Wake(graph_id) => {
                        if active.contains_key(&graph_id) {
                            pending.insert(graph_id);
                            continue;
                        }
                        if let Some((generation, slot)) = prepare_worker(&slots, &graph_id) {
                            active.insert(graph_id.clone(), generation);
                            spawn_driver(
                                &mut workers,
                                graph_id,
                                generation,
                                slot,
                                Arc::clone(&runner),
                                Arc::clone(&parallelism),
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
                                spawn_driver(
                                    &mut workers,
                                    graph_id,
                                    next_generation,
                                    slot,
                                    Arc::clone(&runner),
                                    Arc::clone(&parallelism),
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

fn spawn_driver(
    workers: &mut JoinSet<SupervisorCompletion>,
    graph_id: String,
    generation: u64,
    slot: Arc<DriverSlot>,
    runner: Arc<ExecutionGraphRunner>,
    parallelism: Arc<Semaphore>,
    cancellation: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
) {
    metrics.owner_submitted("graph");
    workers.spawn(async move {
        let execution = async {
            let permit = tokio::select! {
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
            };
            metrics.active_drivers.fetch_add(1, Ordering::Relaxed);
            let result = tokio::select! {
                _ = cancellation.cancelled() => {
                    DriverOutcome::Aborted(
                        "execution cancelled while graph was running".to_string()
                    )
                }
                result = runner.run_until_quiescent(&graph_id) => {
                    match result {
                        Ok(report) => DriverOutcome::Completed(report),
                        Err(error) => DriverOutcome::Failed(error.to_string()),
                    }
                }
            };
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
    if !slot.idle() || Arc::strong_count(slot) > 2 {
        return;
    }
    let mut slots = slots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if slots
        .get(graph_id)
        .is_some_and(|current| Arc::ptr_eq(current, slot))
    {
        slots.remove(graph_id);
    }
}

fn command_advances(command: &ExecutionGraphCommand) -> bool {
    matches!(
        command,
        ExecutionGraphCommand::Resume { .. }
            | ExecutionGraphCommand::Advance { .. }
            | ExecutionGraphCommand::SubmitApproval { approved: true, .. }
            | ExecutionGraphCommand::ResolveExternal { .. }
    )
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
