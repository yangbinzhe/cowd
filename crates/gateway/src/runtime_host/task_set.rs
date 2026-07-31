use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures::FutureExt;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GatewayTaskKind {
    HttpServer,
    ConfigReload,
    SurfaceIngress,
    SurfaceIngressWork,
    SurfaceMonitor,
    SurfaceSupervisor,
    SurfaceTransport,
    SurfaceStream,
    LiveSubscription,
    EvalWorker,
    MissionSchedule,
    MemoryGovernance,
    EventLoopProbe,
    RuntimeRestoration,
    SessionEventRelay,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GatewayTaskOwner {
    Process,
    Session(String),
    Surface(String),
    LiveSubscription(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatewayTaskSetPhase {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GatewayTaskShutdownReport {
    pub(crate) joined: usize,
    pub(crate) panicked: usize,
    pub(crate) forced_aborts: usize,
    pub(crate) rejected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct GatewayTaskHealthSnapshot {
    pub(crate) phase: &'static str,
    pub(crate) shutdown_phase: String,
    pub(crate) shutdown_failures: Vec<String>,
    pub(crate) active: usize,
    pub(crate) oldest_active_age_ms: Option<u64>,
    pub(crate) forced_aborts: usize,
    pub(crate) active_by_kind: BTreeMap<String, usize>,
    pub(crate) active_by_owner: BTreeMap<String, usize>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum GatewayTaskSpawnError {
    #[error("Gateway background task admission is closed")]
    Closed,
    #[error("Gateway background tasks for session {0} are closed")]
    SessionClosed(String),
    #[error("Gateway background task requires a Tokio runtime")]
    RuntimeUnavailable,
}

struct GatewayTaskEntry {
    id: u64,
    kind: GatewayTaskKind,
    owner: GatewayTaskOwner,
    accepted_at: Instant,
    cancellation: runtime::CancellationToken,
    outcome: Arc<AtomicU8>,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
struct GatewayTaskMetadata {
    kind: GatewayTaskKind,
    owner: GatewayTaskOwner,
    accepted_at: Instant,
}

struct GatewayTaskCompletion {
    id: u64,
    panicked: bool,
}

struct GatewayTaskSetState {
    phase: GatewayTaskSetPhase,
    shutdown_phase: String,
    shutdown_failures: Vec<String>,
    closed_owners: BTreeSet<GatewayTaskOwner>,
    tasks: BTreeMap<u64, GatewayTaskEntry>,
    settling: BTreeMap<u64, GatewayTaskMetadata>,
    completed: GatewayTaskShutdownReport,
}

impl Default for GatewayTaskSetState {
    fn default() -> Self {
        Self {
            phase: GatewayTaskSetPhase::Open,
            shutdown_phase: "open".to_string(),
            shutdown_failures: Vec::new(),
            closed_owners: BTreeSet::new(),
            tasks: BTreeMap::new(),
            settling: BTreeMap::new(),
            completed: GatewayTaskShutdownReport::default(),
        }
    }
}

/// Process-level owner for Gateway background tasks.
///
/// Admission and task registration share one lock. Closing either the process
/// or one Session therefore cannot race with a later task insertion. Every
/// accepted task retains a cancellation token and JoinHandle until the reaper
/// or an explicit drain awaits it.
pub(crate) struct GatewayRuntimeTaskSet {
    state: Mutex<GatewayTaskSetState>,
    next_id: AtomicU64,
    rejected: AtomicU64,
    completion_tx: mpsc::UnboundedSender<GatewayTaskCompletion>,
    completion_rx: Mutex<Option<mpsc::UnboundedReceiver<GatewayTaskCompletion>>>,
    reaper: Mutex<Option<JoinHandle<()>>>,
    reaper_cancellation: runtime::CancellationToken,
    shutdown_gate: tokio::sync::RwLock<()>,
    session_lifecycle_locks: Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    default_timeout: Duration,
}

impl std::fmt::Debug for GatewayRuntimeTaskSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        formatter
            .debug_struct("GatewayRuntimeTaskSet")
            .field("phase", &state.phase)
            .field(
                "tracked_tasks",
                &state.tasks.len().saturating_add(state.settling.len()),
            )
            .field("closed_owners", &state.closed_owners.len())
            .field("rejected", &self.rejected.load(Ordering::Relaxed))
            .field("default_timeout", &self.default_timeout)
            .finish()
    }
}

impl GatewayRuntimeTaskSet {
    pub(crate) fn new(default_timeout: Duration) -> Arc<Self> {
        let (completion_tx, completion_rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            state: Mutex::new(GatewayTaskSetState::default()),
            next_id: AtomicU64::new(1),
            rejected: AtomicU64::new(0),
            completion_tx,
            completion_rx: Mutex::new(Some(completion_rx)),
            reaper: Mutex::new(None),
            reaper_cancellation: runtime::CancellationToken::new(),
            shutdown_gate: tokio::sync::RwLock::new(()),
            session_lifecycle_locks: Mutex::new(BTreeMap::new()),
            default_timeout,
        })
    }

    pub(crate) fn phase(&self) -> GatewayTaskSetPhase {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase
    }

    pub(crate) fn stop_accepting(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == GatewayTaskSetPhase::Open {
            state.phase = GatewayTaskSetPhase::Closing;
        }
    }

    pub(crate) fn observe_shutdown_phase(&self, phase: impl Into<String>, failures: &[String]) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.shutdown_phase = phase.into();
        state.shutdown_failures = failures.to_vec();
    }

    pub(crate) async fn open_session(&self, session_id: &str) -> Result<(), GatewayTaskSpawnError> {
        self.open_owner(GatewayTaskOwner::Session(session_id.to_string()))
            .await
    }

    pub(crate) async fn open_owner(
        &self,
        owner: GatewayTaskOwner,
    ) -> Result<(), GatewayTaskSpawnError> {
        let _shutdown = self.shutdown_gate.read().await;
        let lifecycle = self.owner_lifecycle_lock(&owner);
        let _lifecycle = lifecycle.lock().await;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != GatewayTaskSetPhase::Open {
            return Err(GatewayTaskSpawnError::Closed);
        }
        state.closed_owners.remove(&owner);
        Ok(())
    }

    pub(crate) fn spawn<F, Fut>(
        self: &Arc<Self>,
        kind: GatewayTaskKind,
        session_id: Option<String>,
        build: F,
    ) -> Result<u64, GatewayTaskSpawnError>
    where
        F: FnOnce(runtime::CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_owned(
            kind,
            session_id
                .map(GatewayTaskOwner::Session)
                .unwrap_or(GatewayTaskOwner::Process),
            build,
        )
    }

    pub(crate) fn spawn_owned<F, Fut>(
        self: &Arc<Self>,
        kind: GatewayTaskKind,
        owner: GatewayTaskOwner,
        build: F,
    ) -> Result<u64, GatewayTaskSpawnError>
    where
        F: FnOnce(runtime::CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(GatewayTaskSpawnError::RuntimeUnavailable);
        }
        self.ensure_reaper()?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != GatewayTaskSetPhase::Open {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(GatewayTaskSpawnError::Closed);
        }
        if state.closed_owners.contains(&owner) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(match &owner {
                GatewayTaskOwner::Session(session_id) => {
                    GatewayTaskSpawnError::SessionClosed(session_id.clone())
                }
                _ => GatewayTaskSpawnError::Closed,
            });
        }
        let cancellation = runtime::CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let outcome = Arc::new(AtomicU8::new(0));
        let task_outcome = Arc::clone(&outcome);
        let completion_tx = self.completion_tx.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let future = build(task_cancellation);
        let handle = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return;
            }
            let panicked = AssertUnwindSafe(future).catch_unwind().await.is_err();
            task_outcome.store(if panicked { 2 } else { 1 }, Ordering::Release);
            let _ = completion_tx.send(GatewayTaskCompletion { id, panicked });
        });

        state.tasks.insert(
            id,
            GatewayTaskEntry {
                id,
                kind,
                owner,
                accepted_at: Instant::now(),
                cancellation,
                outcome,
                handle,
            },
        );
        let _ = start_tx.send(());
        Ok(id)
    }

    pub(crate) async fn replace_session_task<F, Fut>(
        self: &Arc<Self>,
        kind: GatewayTaskKind,
        session_id: &str,
        build: F,
    ) -> Result<u64, GatewayTaskSpawnError>
    where
        F: FnOnce(runtime::CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _shutdown = self.shutdown_gate.read().await;
        let owner = GatewayTaskOwner::Session(session_id.to_string());
        let lifecycle = self.owner_lifecycle_lock(&owner);
        let _lifecycle = lifecycle.lock().await;
        let previous = self.take_matching(|entry| entry.kind == kind && entry.owner == owner);
        let report = self.settle(previous, self.default_timeout).await;
        self.record_report(&report);
        self.spawn(kind, Some(session_id.to_string()), build)
    }

    pub(crate) async fn close_session_and_drain(
        &self,
        session_id: &str,
        timeout: Duration,
    ) -> GatewayTaskShutdownReport {
        self.close_owner_and_drain(GatewayTaskOwner::Session(session_id.to_string()), timeout)
            .await
    }

    pub(crate) async fn close_owner_and_drain(
        &self,
        owner: GatewayTaskOwner,
        timeout: Duration,
    ) -> GatewayTaskShutdownReport {
        let _shutdown = self.shutdown_gate.read().await;
        let lifecycle = self.owner_lifecycle_lock(&owner);
        let _lifecycle = lifecycle.lock().await;
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed_owners.insert(owner.clone());
            take_matching_locked(&mut state, |entry| entry.owner == owner)
        };
        let report = self.settle(tasks, timeout).await;
        self.record_report(&report);
        report
    }

    /// Cancel and join one explicit shutdown phase without closing unrelated
    /// owners. The process admission gate must be closed separately so the
    /// caller can stop ingress before draining Surface, Eval, Session and
    /// Runtime work in dependency order.
    pub(crate) async fn cancel_and_drain_kinds(
        &self,
        kinds: &[GatewayTaskKind],
        timeout: Duration,
    ) -> GatewayTaskShutdownReport {
        let _shutdown = self.shutdown_gate.read().await;
        let kinds = kinds.iter().copied().collect::<BTreeSet<_>>();
        let tasks = self.take_matching(|entry| kinds.contains(&entry.kind));
        let report = self.settle(tasks, timeout).await;
        self.record_report(&report);
        report
    }

    pub(crate) async fn shutdown(&self) -> GatewayTaskShutdownReport {
        // Keep the exclusive guard until the final report is published. A
        // concurrent follower therefore cannot observe Closing, take an empty
        // task map and return before the leader has joined tasks and reaper.
        let _shutdown = self.shutdown_gate.write().await;
        if self.phase() == GatewayTaskSetPhase::Closed {
            return self.report();
        }
        self.stop_accepting();
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            take_matching_locked(&mut state, |_| true)
        };
        let report = self.settle(tasks, self.default_timeout).await;
        self.reaper_cancellation.cancel();
        self.drain_reaper().await;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.phase = GatewayTaskSetPhase::Closed;
        merge_report(&mut state.completed, &report);
        state.completed.rejected = self.rejected.load(Ordering::Relaxed);
        state.completed.clone()
    }

    pub(crate) fn report(&self) -> GatewayTaskShutdownReport {
        let mut report = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed
            .clone();
        report.rejected = self.rejected.load(Ordering::Relaxed);
        report
    }

    pub(crate) fn health(&self) -> GatewayTaskHealthSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let mut active_by_kind = BTreeMap::new();
        let mut active_by_owner = BTreeMap::new();
        let mut oldest_active_age_ms = None;
        for metadata in state
            .tasks
            .values()
            .map(GatewayTaskMetadata::from)
            .chain(state.settling.values().cloned())
        {
            *active_by_kind
                .entry(task_kind_label(metadata.kind).to_string())
                .or_insert(0) += 1;
            *active_by_owner
                .entry(task_owner_label(&metadata.owner))
                .or_insert(0) += 1;
            let age = now
                .saturating_duration_since(metadata.accepted_at)
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            oldest_active_age_ms =
                Some(oldest_active_age_ms.map_or(age, |oldest: u64| oldest.max(age)));
        }
        GatewayTaskHealthSnapshot {
            phase: task_phase_label(state.phase),
            shutdown_phase: state.shutdown_phase.clone(),
            shutdown_failures: state.shutdown_failures.clone(),
            active: state.tasks.len().saturating_add(state.settling.len()),
            oldest_active_age_ms,
            forced_aborts: state.completed.forced_aborts,
            active_by_kind,
            active_by_owner,
        }
    }

    #[cfg(test)]
    pub(crate) fn tracked_task_count(&self) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.tasks.len().saturating_add(state.settling.len())
    }

    fn ensure_reaper(self: &Arc<Self>) -> Result<(), GatewayTaskSpawnError> {
        let mut reaper = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reaper.is_some() {
            return Ok(());
        }
        let mut receiver = self
            .completion_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(GatewayTaskSpawnError::Closed)?;
        let owner = Arc::downgrade(self);
        let cancellation = self.reaper_cancellation.clone();
        *reaper = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    completion = receiver.recv() => {
                        let Some(completion) = completion else {
                            break;
                        };
                        reap_completed_task(&owner, completion).await;
                    }
                }
            }
        }));
        Ok(())
    }

    fn take_matching(
        &self,
        predicate: impl FnMut(&GatewayTaskEntry) -> bool,
    ) -> Vec<GatewayTaskEntry> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        take_matching_locked(&mut state, predicate)
    }

    fn record_report(&self, report: &GatewayTaskShutdownReport) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        merge_report(&mut state.completed, report);
    }

    fn owner_lifecycle_lock(&self, owner: &GatewayTaskOwner) -> Arc<tokio::sync::Mutex<()>> {
        let owner = match owner {
            GatewayTaskOwner::Session(session_id) => format!("session:{session_id}"),
            GatewayTaskOwner::Surface(surface_id) => format!("surface:{surface_id}"),
            GatewayTaskOwner::LiveSubscription(subscription_id) => {
                format!("live:{subscription_id}")
            }
            GatewayTaskOwner::Process => "process".to_string(),
        };
        let mut locks = self
            .session_lifecycle_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&owner).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(owner, Arc::downgrade(&lock));
        lock
    }

    async fn settle(
        &self,
        mut tasks: Vec<GatewayTaskEntry>,
        timeout: Duration,
    ) -> GatewayTaskShutdownReport {
        let mut report = GatewayTaskShutdownReport::default();
        for task in &tasks {
            task.cancellation.cancel();
        }
        let deadline = tokio::time::Instant::now() + timeout;
        for task in &mut tasks {
            match tokio::time::timeout_at(deadline, &mut task.handle).await {
                Ok(Ok(())) => {
                    report.joined += 1;
                    if task.outcome.load(Ordering::Acquire) == 2 {
                        report.panicked += 1;
                    }
                }
                Ok(Err(error)) => {
                    report.joined += 1;
                    if error.is_panic() {
                        report.panicked += 1;
                    }
                }
                Err(_) => {
                    task.handle.abort();
                    let _ = (&mut task.handle).await;
                    report.forced_aborts += 1;
                }
            }
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .settling
                .remove(&task.id);
        }
        report
    }

    async fn drain_reaper(&self) {
        let handle = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut handle) = handle {
            if tokio::time::timeout(self.default_timeout, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
    }
}

fn take_matching_locked(
    state: &mut GatewayTaskSetState,
    mut predicate: impl FnMut(&GatewayTaskEntry) -> bool,
) -> Vec<GatewayTaskEntry> {
    let ids = state
        .tasks
        .iter()
        .filter_map(|(id, entry)| predicate(entry).then_some(*id))
        .collect::<Vec<_>>();
    ids.into_iter()
        .filter_map(|id| {
            let task = state.tasks.remove(&id)?;
            state.settling.insert(id, GatewayTaskMetadata::from(&task));
            Some(task)
        })
        .collect()
}

impl From<&GatewayTaskEntry> for GatewayTaskMetadata {
    fn from(task: &GatewayTaskEntry) -> Self {
        Self {
            kind: task.kind,
            owner: task.owner.clone(),
            accepted_at: task.accepted_at,
        }
    }
}

fn task_phase_label(phase: GatewayTaskSetPhase) -> &'static str {
    match phase {
        GatewayTaskSetPhase::Open => "open",
        GatewayTaskSetPhase::Closing => "closing",
        GatewayTaskSetPhase::Closed => "closed",
    }
}

fn task_kind_label(kind: GatewayTaskKind) -> &'static str {
    match kind {
        GatewayTaskKind::HttpServer => "http_server",
        GatewayTaskKind::ConfigReload => "config_reload",
        GatewayTaskKind::SurfaceIngress => "surface_ingress",
        GatewayTaskKind::SurfaceIngressWork => "surface_ingress_work",
        GatewayTaskKind::SurfaceMonitor => "surface_monitor",
        GatewayTaskKind::SurfaceSupervisor => "surface_supervisor",
        GatewayTaskKind::SurfaceTransport => "surface_transport",
        GatewayTaskKind::SurfaceStream => "surface_stream",
        GatewayTaskKind::LiveSubscription => "live_subscription",
        GatewayTaskKind::EvalWorker => "eval_worker",
        GatewayTaskKind::MissionSchedule => "mission_schedule",
        GatewayTaskKind::MemoryGovernance => "memory_governance",
        GatewayTaskKind::EventLoopProbe => "event_loop_probe",
        GatewayTaskKind::RuntimeRestoration => "runtime_restoration",
        GatewayTaskKind::SessionEventRelay => "session_event_relay",
    }
}

fn task_owner_label(owner: &GatewayTaskOwner) -> String {
    match owner {
        GatewayTaskOwner::Process => "process".to_string(),
        GatewayTaskOwner::Session(session_id) => format!("session:{session_id}"),
        GatewayTaskOwner::Surface(surface_id) => format!("surface:{surface_id}"),
        GatewayTaskOwner::LiveSubscription(subscription_id) => {
            format!("live_subscription:{subscription_id}")
        }
    }
}

async fn reap_completed_task(
    owner: &Weak<GatewayRuntimeTaskSet>,
    completion: GatewayTaskCompletion,
) {
    let Some(owner) = owner.upgrade() else {
        return;
    };
    let task = owner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .tasks
        .remove(&completion.id);
    let Some(mut task) = task else {
        return;
    };
    let joined = (&mut task.handle).await;
    let mut state = owner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.completed.joined += 1;
    if completion.panicked || joined.is_err_and(|error| error.is_panic()) {
        state.completed.panicked += 1;
    }
}

fn merge_report(target: &mut GatewayTaskShutdownReport, source: &GatewayTaskShutdownReport) {
    target.joined += source.joined;
    target.panicked += source.panicked;
    target.forced_aborts += source.forced_aborts;
    target.rejected = target.rejected.max(source.rejected);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_close_is_atomic_with_concurrent_restoration_registration() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        tasks.open_session("session-a").await.unwrap();
        let (accepted_started, accepted_wait) = tokio::sync::oneshot::channel();
        let active = Arc::new(AtomicUsize::new(0));
        let accepted_active = Arc::clone(&active);
        tasks
            .spawn(
                GatewayTaskKind::RuntimeRestoration,
                Some("session-a".to_string()),
                move |cancellation| async move {
                    accepted_active.fetch_add(1, Ordering::SeqCst);
                    let _ = accepted_started.send(());
                    cancellation.cancelled().await;
                    accepted_active.fetch_sub(1, Ordering::SeqCst);
                },
            )
            .unwrap();
        accepted_wait.await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(33));
        let mut registrars = Vec::new();
        for _ in 0..32 {
            let tasks = Arc::clone(&tasks);
            let barrier = Arc::clone(&barrier);
            let active = Arc::clone(&active);
            registrars.push(tokio::spawn(async move {
                barrier.wait().await;
                let active_for_task = Arc::clone(&active);
                let _ = tasks.spawn(
                    GatewayTaskKind::RuntimeRestoration,
                    Some("session-a".to_string()),
                    move |cancellation| async move {
                        active_for_task.fetch_add(1, Ordering::SeqCst);
                        cancellation.cancelled().await;
                        active_for_task.fetch_sub(1, Ordering::SeqCst);
                    },
                );
            }));
        }
        barrier.wait().await;
        let report = tasks
            .close_session_and_drain("session-a", Duration::from_secs(1))
            .await;
        for registrar in registrars {
            registrar.await.unwrap();
        }
        let second = tasks
            .close_session_and_drain("session-a", Duration::from_secs(1))
            .await;

        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(tasks.tracked_task_count(), 0);
        assert_eq!(second.forced_aborts, 0);
        assert_eq!(
            tasks.spawn(
                GatewayTaskKind::RuntimeRestoration,
                Some("session-a".to_string()),
                |_| async {},
            ),
            Err(GatewayTaskSpawnError::SessionClosed(
                "session-a".to_string()
            ))
        );
        assert_eq!(report.forced_aborts, 0);
        tasks.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_reopen_and_replacement_wait_for_close_settlement() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        tasks.open_session("session-a").await.unwrap();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        tasks
            .spawn(
                GatewayTaskKind::SessionEventRelay,
                Some("session-a".to_string()),
                move |cancellation| async move {
                    cancellation.cancelled().await;
                    let _ = cancelled_tx.send(());
                    let _ = release_rx.await;
                },
            )
            .unwrap();

        let closing_tasks = Arc::clone(&tasks);
        let close = tokio::spawn(async move {
            closing_tasks
                .close_session_and_drain("session-a", Duration::from_secs(1))
                .await
        });
        cancelled_rx
            .await
            .expect("close should cancel the old session task");

        let opening_tasks = Arc::clone(&tasks);
        let open = tokio::spawn(async move { opening_tasks.open_session("session-a").await });
        let replacing_tasks = Arc::clone(&tasks);
        let replacement_started = Arc::new(AtomicBool::new(false));
        let replacement_started_in_task = Arc::clone(&replacement_started);
        let replace = tokio::spawn(async move {
            replacing_tasks
                .replace_session_task(
                    GatewayTaskKind::SessionEventRelay,
                    "session-a",
                    move |cancellation| async move {
                        replacement_started_in_task.store(true, Ordering::SeqCst);
                        cancellation.cancelled().await;
                    },
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!open.is_finished());
        assert!(!replace.is_finished());
        assert!(!replacement_started.load(Ordering::SeqCst));
        let settling = tasks.health();
        assert_eq!(settling.active, 1);
        assert_eq!(settling.active_by_owner["session:session-a"], 1);

        release_tx
            .send(())
            .expect("old session task should still be settling");
        assert_eq!(close.await.unwrap().forced_aborts, 0);
        open.await.unwrap().unwrap();
        let replacement = replace.await.unwrap();
        assert!(
            replacement.is_ok()
                || matches!(replacement, Err(GatewayTaskSpawnError::SessionClosed(_)))
        );
        tasks.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn process_shutdown_waits_for_an_in_flight_session_drain() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        tasks.open_session("session-a").await.unwrap();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        tasks
            .spawn(
                GatewayTaskKind::RuntimeRestoration,
                Some("session-a".to_string()),
                move |cancellation| async move {
                    cancellation.cancelled().await;
                    let _ = cancelled_tx.send(());
                    let _ = release_rx.await;
                },
            )
            .unwrap();

        let closing_tasks = Arc::clone(&tasks);
        let close = tokio::spawn(async move {
            closing_tasks
                .close_session_and_drain("session-a", Duration::from_secs(1))
                .await
        });
        cancelled_rx
            .await
            .expect("session drain should cancel the restoration");

        let shutdown_tasks = Arc::clone(&tasks);
        let shutdown = tokio::spawn(async move { shutdown_tasks.shutdown().await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!shutdown.is_finished());

        release_tx
            .send(())
            .expect("restoration should still be settling");
        assert_eq!(close.await.unwrap().forced_aborts, 0);
        assert_eq!(
            shutdown.await.unwrap().forced_aborts,
            0,
            "process shutdown must observe completion of the session-owned drain"
        );
        assert_eq!(tasks.phase(), GatewayTaskSetPhase::Closed);
    }

    #[tokio::test]
    async fn panicking_task_is_reaped_and_reported() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        tasks
            .spawn(GatewayTaskKind::EventLoopProbe, None, |_| async move {
                panic!("injected Gateway task panic");
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while tasks.tracked_task_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicking task must be reaped");

        let report = tasks.shutdown().await;

        assert_eq!(report.panicked, 1);
        assert_eq!(report.joined, 1);
        assert_eq!(report.forced_aborts, 0);
    }

    #[tokio::test]
    async fn shutdown_aborts_and_awaits_a_task_that_ignores_cancellation() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_millis(25));
        let finished = Arc::new(AtomicBool::new(false));
        let finished_in_task = Arc::clone(&finished);
        tasks
            .spawn(
                GatewayTaskKind::MissionSchedule,
                None,
                move |_| async move {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    finished_in_task.store(true, Ordering::SeqCst);
                },
            )
            .unwrap();

        let report = tasks.shutdown().await;

        assert_eq!(report.forced_aborts, 1);
        assert!(!finished.load(Ordering::SeqCst));
        assert_eq!(tasks.phase(), GatewayTaskSetPhase::Closed);
        assert_eq!(tasks.tracked_task_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_is_atomic_with_concurrent_task_registration() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let barrier = Arc::new(tokio::sync::Barrier::new(65));
        let active = Arc::new(AtomicUsize::new(0));
        let mut registrars = Vec::new();
        for _ in 0..64 {
            let tasks = Arc::clone(&tasks);
            let barrier = Arc::clone(&barrier);
            let active = Arc::clone(&active);
            registrars.push(tokio::spawn(async move {
                barrier.wait().await;
                let active_for_task = Arc::clone(&active);
                let _ = tasks.spawn(
                    GatewayTaskKind::SurfaceIngressWork,
                    None,
                    move |cancellation| async move {
                        active_for_task.fetch_add(1, Ordering::SeqCst);
                        cancellation.cancelled().await;
                        active_for_task.fetch_sub(1, Ordering::SeqCst);
                    },
                );
            }));
        }
        barrier.wait().await;
        let report = tasks.shutdown().await;
        for registrar in registrars {
            registrar.await.unwrap();
        }

        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(tasks.tracked_task_count(), 0);
        assert_eq!(report.forced_aborts, 0);
        assert!(matches!(
            tasks.spawn(GatewayTaskKind::SurfaceIngressWork, None, |_| async {}),
            Err(GatewayTaskSpawnError::Closed)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_session_task_replacement_keeps_one_live_owner() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_millis(100));
        let mut replacements = Vec::new();
        for _ in 0..32 {
            let tasks = Arc::clone(&tasks);
            replacements.push(tokio::spawn(async move {
                tasks
                    .replace_session_task(
                        GatewayTaskKind::SessionEventRelay,
                        "session-a",
                        |cancellation| async move {
                            cancellation.cancelled().await;
                        },
                    )
                    .await
            }));
        }

        for replacement in replacements {
            replacement
                .await
                .expect("replacement task should join")
                .expect("replacement should remain admitted while the Gateway task set is open");
        }

        assert_eq!(tasks.tracked_task_count(), 1);
        let report = tasks.shutdown().await;
        assert_eq!(report.forced_aborts, 0);
        assert_eq!(tasks.phase(), GatewayTaskSetPhase::Closed);
    }

    #[tokio::test]
    async fn startup_failure_uses_the_same_idempotent_shutdown_path() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_in_task = Arc::clone(&cancelled);
        tasks
            .spawn(
                GatewayTaskKind::ConfigReload,
                None,
                move |token| async move {
                    token.cancelled().await;
                    cancelled_in_task.store(true, Ordering::SeqCst);
                },
            )
            .unwrap();
        tasks.stop_accepting();
        assert!(matches!(
            tasks.spawn(GatewayTaskKind::SurfaceIngress, None, |_| async {}),
            Err(GatewayTaskSpawnError::Closed)
        ));
        assert!(
            !cancelled.load(Ordering::SeqCst),
            "closing admission must not cancel an already accepted task"
        );

        let first = tasks.shutdown().await;
        let second = tasks.shutdown().await;

        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(first, second);
        assert_eq!(tasks.phase(), GatewayTaskSetPhase::Closed);
        assert!(matches!(
            tasks.spawn(GatewayTaskKind::EventLoopProbe, None, |_| async {}),
            Err(GatewayTaskSpawnError::Closed)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_shutdown_followers_wait_for_one_identical_final_report() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let release_task = Arc::clone(&release);
        tasks
            .spawn(
                GatewayTaskKind::MissionSchedule,
                None,
                move |cancellation| async move {
                    cancellation.cancelled().await;
                    let _ = cancelled_tx.send(());
                    release_task.notified().await;
                },
            )
            .unwrap();

        let mut shutdowns = Vec::new();
        for _ in 0..16 {
            let tasks = Arc::clone(&tasks);
            shutdowns.push(tokio::spawn(async move { tasks.shutdown().await }));
        }
        cancelled_rx
            .await
            .expect("the shutdown leader must cancel the accepted task");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            shutdowns.iter().all(|shutdown| !shutdown.is_finished()),
            "no shutdown follower may return before the leader has settled the task"
        );

        release.notify_waiters();
        let mut reports = Vec::new();
        for shutdown in shutdowns {
            reports.push(shutdown.await.expect("shutdown caller joins"));
        }
        assert!(reports.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(reports[0].joined, 1);
        assert_eq!(reports[0].forced_aborts, 0);
        assert_eq!(tasks.phase(), GatewayTaskSetPhase::Closed);
    }

    #[tokio::test]
    async fn phased_drain_cancels_only_selected_task_kinds() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let admission_cancelled = Arc::new(AtomicBool::new(false));
        let eval_cancelled = Arc::new(AtomicBool::new(false));
        let runtime_cancelled = Arc::new(AtomicBool::new(false));
        for (kind, flag) in [
            (
                GatewayTaskKind::HttpServer,
                Arc::clone(&admission_cancelled),
            ),
            (GatewayTaskKind::EvalWorker, Arc::clone(&eval_cancelled)),
            (
                GatewayTaskKind::RuntimeRestoration,
                Arc::clone(&runtime_cancelled),
            ),
        ] {
            tasks
                .spawn(kind, None, move |cancellation| async move {
                    cancellation.cancelled().await;
                    flag.store(true, Ordering::SeqCst);
                })
                .unwrap();
        }

        tasks.stop_accepting();
        assert!(!admission_cancelled.load(Ordering::SeqCst));
        assert!(!eval_cancelled.load(Ordering::SeqCst));
        assert!(!runtime_cancelled.load(Ordering::SeqCst));
        let closing = tasks.health();
        assert_eq!(closing.phase, "closing");
        assert_eq!(closing.active, 3);
        assert_eq!(closing.active_by_kind["http_server"], 1);
        assert_eq!(closing.active_by_kind["eval_worker"], 1);
        assert_eq!(closing.active_by_kind["runtime_restoration"], 1);

        let admission = tasks
            .cancel_and_drain_kinds(&[GatewayTaskKind::HttpServer], Duration::from_secs(1))
            .await;
        assert_eq!(admission.joined, 1);
        assert!(admission_cancelled.load(Ordering::SeqCst));
        assert!(!eval_cancelled.load(Ordering::SeqCst));
        assert!(!runtime_cancelled.load(Ordering::SeqCst));
        assert_eq!(tasks.health().active, 2);

        let eval = tasks
            .cancel_and_drain_kinds(&[GatewayTaskKind::EvalWorker], Duration::from_secs(1))
            .await;
        assert_eq!(eval.joined, 1);
        assert!(eval_cancelled.load(Ordering::SeqCst));
        assert!(!runtime_cancelled.load(Ordering::SeqCst));

        let report = tasks.shutdown().await;
        assert!(runtime_cancelled.load(Ordering::SeqCst));
        assert_eq!(report.joined, 3);
        assert_eq!(report.forced_aborts, 0);
    }

    #[tokio::test]
    async fn eval_shutdown_uses_the_same_bounded_abort_contract_as_other_tasks() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_millis(10));
        let body_finished = Arc::new(AtomicBool::new(false));
        let body_finished_in_task = Arc::clone(&body_finished);
        tasks
            .spawn(
                GatewayTaskKind::EvalWorker,
                None,
                move |cancellation| async move {
                    cancellation.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    body_finished_in_task.store(true, Ordering::SeqCst);
                },
            )
            .unwrap();

        let report = tasks.shutdown().await;

        assert!(!body_finished.load(Ordering::SeqCst));
        assert_eq!(report.joined, 0);
        assert_eq!(report.forced_aborts, 1);
    }
}
