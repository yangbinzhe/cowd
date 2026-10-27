//! Unified control plane for durable Runtime event projections.
//!
//! The reactor owns worker lifecycle, wake-up, background admission, retry,
//! shutdown and operational health. Domain lanes still own their reducer,
//! checkpoint payload, dead-letter policy and idempotency rules.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use serde::Serialize;

use crate::{
    CancellationToken, RuntimeEventStore, RuntimeProjectionInterest, RuntimeProjectionWorkClass,
};

pub type RuntimeProjectionFuture =
    Pin<Box<dyn Future<Output = Result<RuntimeProjectionPass, String>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeProjectionLatencyClass {
    #[default]
    ReadModel,
    Maintenance,
}

// Maintenance lanes are durable but never latency-critical. Coalescing their
// backlog passes for this interval prevents a busy foreground stream from
// turning each commit notification into a competing SQLite scan/write. At the
// maximum 128-commit maintenance batch this still drains at least 640 commits
// per second when the foreground is quiet.
const MAINTENANCE_PASS_INTERVAL: Duration = Duration::from_millis(200);
// A maintenance pass starts only after a short foreground-commit quiet window.
// Under a continuous write stream the bounded deferral still admits one pass,
// preventing permanent projection starvation.
const MAINTENANCE_FOREGROUND_QUIET: Duration = Duration::from_millis(50);
const MAINTENANCE_MAX_FOREGROUND_DEFERRAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProjectionDescriptor {
    pub projection_id: String,
    pub interest: RuntimeProjectionInterest,
    pub batch_size: usize,
    pub safety_tick: Duration,
    pub latency_class: RuntimeProjectionLatencyClass,
}

impl RuntimeProjectionDescriptor {
    pub fn new(
        projection_id: impl Into<String>,
        interest: RuntimeProjectionInterest,
        batch_size: usize,
        safety_tick: Duration,
    ) -> Result<Self, String> {
        let projection_id = projection_id.into();
        if projection_id.trim().is_empty() {
            return Err("projection lane id must not be empty".to_string());
        }
        if interest.events.is_empty() {
            return Err(format!(
                "projection lane `{projection_id}` must declare at least one event interest"
            ));
        }
        if batch_size == 0 {
            return Err(format!(
                "projection lane `{projection_id}` batch size must be positive"
            ));
        }
        if safety_tick.is_zero() {
            return Err(format!(
                "projection lane `{projection_id}` safety tick must be positive"
            ));
        }
        Ok(Self {
            projection_id,
            interest,
            batch_size,
            safety_tick,
            latency_class: RuntimeProjectionLatencyClass::ReadModel,
        })
    }

    #[must_use]
    pub const fn with_latency_class(
        mut self,
        latency_class: RuntimeProjectionLatencyClass,
    ) -> Self {
        self.latency_class = latency_class;
        self
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeProjectionPass {
    pub scanned_commits: usize,
    pub matched_events: usize,
    pub backlog: bool,
}

impl RuntimeProjectionPass {
    #[must_use]
    pub const fn scanned(scanned_commits: usize, batch_size: usize) -> Self {
        Self {
            scanned_commits,
            matched_events: 0,
            backlog: scanned_commits >= batch_size,
        }
    }

    #[must_use]
    pub const fn with_matches(mut self, matched_events: usize) -> Self {
        self.matched_events = matched_events;
        self
    }
}

enum RuntimeProjectionRunner {
    Blocking(Arc<dyn Fn(usize) -> Result<RuntimeProjectionPass, String> + Send + Sync + 'static>),
    Async(Arc<dyn Fn(usize) -> RuntimeProjectionFuture + Send + Sync + 'static>),
}

pub struct RuntimeProjectionLane {
    descriptor: RuntimeProjectionDescriptor,
    runner: RuntimeProjectionRunner,
}

impl std::fmt::Debug for RuntimeProjectionLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeProjectionLane")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl RuntimeProjectionLane {
    #[must_use]
    pub fn blocking(
        descriptor: RuntimeProjectionDescriptor,
        runner: impl Fn(usize) -> Result<RuntimeProjectionPass, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor,
            runner: RuntimeProjectionRunner::Blocking(Arc::new(runner)),
        }
    }

    #[must_use]
    pub fn asynchronous(
        descriptor: RuntimeProjectionDescriptor,
        runner: impl Fn(usize) -> RuntimeProjectionFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor,
            runner: RuntimeProjectionRunner::Async(Arc::new(runner)),
        }
    }

    #[must_use]
    pub fn descriptor(&self) -> &RuntimeProjectionDescriptor {
        &self.descriptor
    }

    async fn run_once(
        self: Arc<Self>,
        event_store: Arc<RuntimeEventStore>,
    ) -> Result<RuntimeProjectionPass, String> {
        let batch_size = self.descriptor.batch_size;
        match &self.runner {
            RuntimeProjectionRunner::Blocking(runner) => {
                let runner = Arc::clone(runner);
                tokio::task::spawn_blocking(move || {
                    event_store.run_projection_work(RuntimeProjectionWorkClass::Background, || {
                        runner(batch_size)
                    })
                })
                .await
                .map_err(|error| format!("projection blocking worker failed: {error}"))?
            }
            RuntimeProjectionRunner::Async(runner) => runner(batch_size).await,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeProjectionLaneHealth {
    pub projection_id: String,
    pub worker_running: bool,
    pub admission_open: bool,
    pub admitted_passes: u64,
    pub checkpoint_cursor: u64,
    pub latest_commit_cursor: u64,
    pub lag_commits: u64,
    pub consecutive_failures: u32,
    pub total_passes: u64,
    pub total_scanned_commits: u64,
    pub total_matched_events: u64,
    pub last_pass_duration_ms: u64,
    pub last_success_at_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeEventReactorHealth {
    pub sealed: bool,
    pub admission_capacity: usize,
    pub lanes: Vec<RuntimeProjectionLaneHealth>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RuntimeEventReactorShutdownReport {
    pub drained_lanes: Vec<String>,
    pub timed_out_lanes: Vec<String>,
    pub join_errors: Vec<String>,
}

/// A completed physical join is distinct from a successful domain pass. The
/// domain keeps its durable claim/receipt and must expose this error instead
/// of interpreting a returned worker as successful business settlement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeProjectionLaneDrainReport {
    pub projection_id: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
struct LaneRuntimeState {
    worker_running: AtomicBool,
    admission_open: Mutex<bool>,
    admitted_passes: AtomicU64,
    consecutive_failures: AtomicU32,
    total_passes: AtomicU64,
    total_scanned_commits: AtomicU64,
    total_matched_events: AtomicU64,
    last_pass_duration_ms: AtomicU64,
    last_success_at_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

#[derive(Debug, Clone)]
struct RegisteredLane {
    lane: Arc<RuntimeProjectionLane>,
    state: Arc<LaneRuntimeState>,
    wake: Arc<tokio::sync::Notify>,
    stop: CancellationToken,
}

impl RegisteredLane {
    fn new(lane: RuntimeProjectionLane) -> Self {
        Self {
            lane: Arc::new(lane),
            state: Arc::new(LaneRuntimeState {
                admission_open: Mutex::new(true),
                ..LaneRuntimeState::default()
            }),
            wake: Arc::new(tokio::sync::Notify::new()),
            stop: CancellationToken::new(),
        }
    }
}

struct RetainedProjectionWorker {
    handle: Option<tokio::task::JoinHandle<()>>,
    outcome: Option<Result<(), String>>,
}

pub struct RuntimeEventReactor {
    event_store: Arc<RuntimeEventStore>,
    lanes: Mutex<BTreeMap<String, RegisteredLane>>,
    admission_capacity: usize,
    admission: Arc<tokio::sync::Semaphore>,
    cancellation: CancellationToken,
    started: AtomicBool,
    workers: Mutex<Vec<(String, Arc<tokio::sync::Mutex<RetainedProjectionWorker>>)>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    notification_sources:
        Arc<Mutex<BTreeMap<String, (tokio::sync::watch::Receiver<u64>, Arc<AtomicU64>)>>>,
}

impl std::fmt::Debug for RuntimeEventReactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeEventReactor")
            .field("lane_count", &self.lane_snapshot().len())
            .field("admission_capacity", &self.admission_capacity)
            .field("started", &self.started.load(Ordering::Relaxed))
            .finish()
    }
}

impl RuntimeEventReactor {
    fn lane_snapshot(&self) -> BTreeMap<String, RegisteredLane> {
        self.lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Register a domain lane only after its durable resume cursor is ready.
    pub fn register_lane(self: &Arc<Self>, lane: RuntimeProjectionLane) -> Result<(), String> {
        self.register_lanes(std::iter::once(lane))
    }

    /// A domain with several lanes becomes registered as one set. Validate all
    /// identities before publishing any of them: a duplicate must not leave a
    /// partially activated domain or start its otherwise-valid new worker.
    pub fn register_lanes(
        self: &Arc<Self>,
        lanes: impl IntoIterator<Item = RuntimeProjectionLane>,
    ) -> Result<(), String> {
        let mut pending = BTreeMap::new();
        for lane in lanes {
            let id = lane.descriptor.projection_id.clone();
            if pending.contains_key(&id) {
                return Err(format!("duplicate Runtime projection lane `{id}`"));
            }
            pending.insert(id, RegisteredLane::new(lane));
        }
        let handle = {
            let mut lanes = self
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.cancellation.is_cancelled() {
                return Err("Runtime event reactor admission is closed".into());
            }
            if let Some(id) = pending.keys().find(|id| lanes.contains_key(*id)) {
                return Err(format!("duplicate Runtime projection lane `{id}`"));
            }
            // Read started under the same lock as the complete insertion. A
            // concurrent initial start must either observe this whole set or
            // leave it to this call; no pre-lock flag snapshot may strand it.
            let handle = if self.started.load(Ordering::Acquire) {
                Some(tokio::runtime::Handle::try_current().map_err(|error| error.to_string())?)
            } else {
                None
            };
            lanes.extend(pending.iter().map(|(id, lane)| (id.clone(), lane.clone())));
            handle
        };
        if let Some(handle) = handle {
            self.start_lanes(handle, pending)?;
        }
        Ok(())
    }

    /// Stop new passes synchronously. The currently admitted pass remains owned
    /// by its retained worker; this does not publish a completed drain.
    pub fn stop_lane(&self, projection_id: &str) -> bool {
        let lanes = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(lane) = lanes.get(projection_id) else {
            return false;
        };
        *lane
            .state
            .admission_open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        lane.stop.cancel();
        true
    }

    /// Await the original retained worker without consuming its handle on
    /// timeout or waiter cancellation. Only a successful physical join permits
    /// removing the registration and later creating its single successor.
    pub async fn drain_lane(
        &self,
        projection_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<RuntimeProjectionLaneDrainReport, String> {
        let absent = || RuntimeProjectionLaneDrainReport {
            projection_id: projection_id.into(),
            last_error: None,
        };
        let snapshot = self.lane_snapshot();
        let Some(lane) = snapshot.get(projection_id) else {
            return Ok(absent());
        };
        if *lane
            .state
            .admission_open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return Err("lane admission must be stopped before drain".into());
        }
        let worker = {
            // The selected handle must belong to this snapshot's generation.
            // A concurrent drain may already have joined/removed it and let a
            // successor register; never await that successor as the old lane.
            let current = self
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !current
                .get(projection_id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.state, &lane.state))
            {
                let last_error = lane
                    .state
                    .last_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                return Ok(RuntimeProjectionLaneDrainReport {
                    projection_id: projection_id.into(),
                    last_error,
                });
            }
            self.workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .find(|(id, _)| id == projection_id)
                .map(|(_, worker)| worker.clone())
        };
        if let Some(worker) = worker {
            tokio::time::timeout_at(deadline, async {
                let mut worker = worker.lock().await;
                if let Some(handle) = worker.handle.as_mut() {
                    let outcome = handle.await.map_err(|error| error.to_string());
                    worker.handle = None;
                    worker.outcome = Some(outcome);
                }
                worker
                    .outcome
                    .clone()
                    .expect("joined projection worker outcome")
            })
            .await
            .map_err(|_| format!("projection lane `{projection_id}` is still draining"))??;
        }
        let mut lanes = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lanes
            .get(projection_id)
            .is_some_and(|current| Arc::ptr_eq(&current.state, &lane.state))
        {
            lanes.remove(projection_id);
            self.workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|(id, _)| id != projection_id);
        }
        let last_error = lane
            .state
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(RuntimeProjectionLaneDrainReport {
            projection_id: projection_id.into(),
            last_error,
        })
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn use_lane_notifications_for_test(
        &self,
        lane: &str,
        source: tokio::sync::watch::Receiver<u64>,
    ) -> Result<Arc<AtomicU64>, String> {
        if !self.lane_snapshot().contains_key(lane) {
            return Err("unknown notification lane".into());
        }
        let mut sources = self
            .notification_sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sources.contains_key(lane) {
            return Err("duplicate lane notification source".into());
        }
        let hits = Arc::new(AtomicU64::new(0));
        sources.insert(lane.into(), (source, hits.clone()));
        Ok(hits)
    }
    pub fn sealed(
        event_store: Arc<RuntimeEventStore>,
        lanes: impl IntoIterator<Item = RuntimeProjectionLane>,
    ) -> Result<Self, String> {
        let mut registered = BTreeMap::new();
        for lane in lanes {
            let projection_id = lane.descriptor.projection_id.clone();
            if registered.contains_key(&projection_id) {
                return Err(format!(
                    "duplicate Runtime projection lane `{projection_id}`"
                ));
            }
            registered.insert(projection_id, RegisteredLane::new(lane));
        }
        if registered.is_empty() {
            return Err("Runtime event reactor requires at least one lane".to_string());
        }
        let logical_cpus = std::thread::available_parallelism().map_or(1, usize::from);
        let admission_capacity = admission_capacity(
            registered.len(),
            logical_cpus,
            event_store.background_projection_capacity_hint(),
        );
        Ok(Self {
            event_store,
            lanes: Mutex::new(registered),
            admission_capacity,
            admission: Arc::new(tokio::sync::Semaphore::new(admission_capacity)),
            cancellation: CancellationToken::new(),
            started: AtomicBool::new(false),
            workers: Mutex::new(Vec::new()),
            #[cfg(any(test, feature = "test-fixtures"))]
            notification_sources: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Coalesced scheduling hint only: the lane rereads its durable sources.
    /// One retained permit covers notifications arriving during a pass, without
    /// spawning work or retaining a queue of graph identities.
    pub(crate) fn wake_lane(&self, projection_id: &str) -> bool {
        if self.cancellation.is_cancelled() {
            return false;
        }
        let snapshot = self.lane_snapshot();
        let Some(registered) = snapshot.get(projection_id) else {
            return false;
        };
        registered.wake.notify_one();
        true
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        if self.cancellation.is_cancelled() {
            return Err("Runtime event reactor admission is closed".to_string());
        }
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // Preserve the synchronous fixture contract: the sealed reactor is
            // still fully inspectable and direct projector calls remain
            // available, while production composition starts it under Tokio.
            return Ok(());
        };
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        self.start_lanes(handle, self.lane_snapshot())
    }

    fn start_lanes(
        &self,
        handle: tokio::runtime::Handle,
        lanes: BTreeMap<String, RegisteredLane>,
    ) -> Result<(), String> {
        let admission = Arc::clone(&self.admission);
        // Registration and physical-handle insertion share this lock order
        // with drain. A stopped/removed generation must never spawn after a
        // successor was registered, or consume the successor's worker slot.
        let current_lanes = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancellation.is_cancelled() {
            return Err("Runtime event reactor admission is closed".to_string());
        }
        for (projection_id, registered) in &lanes {
            let projection_id = projection_id.clone();
            if !current_lanes
                .get(&projection_id)
                .is_some_and(|current| Arc::ptr_eq(&current.state, &registered.state))
                || !*registered
                    .state
                    .admission_open
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                continue;
            }
            if workers.iter().any(|(id, _)| id == &projection_id) {
                continue;
            }
            let lane = Arc::clone(&registered.lane);
            let state = Arc::clone(&registered.state);
            let event_store = Arc::clone(&self.event_store);
            let cancellation = registered.stop.clone();
            let admission = Arc::clone(&admission);
            let wake = Arc::clone(&registered.wake);
            let worker_projection_id = projection_id.clone();
            #[cfg(any(test, feature = "test-fixtures"))]
            let notification_sources = self.notification_sources.clone();
            let worker = handle.spawn(async move {
                struct RunningGuard(Arc<LaneRuntimeState>);
                impl Drop for RunningGuard {
                    fn drop(&mut self) {
                        self.0.worker_running.store(false, Ordering::Release);
                    }
                }

                state.worker_running.store(true, Ordering::Release);
                let _running = RunningGuard(Arc::clone(&state));
                let mut commits = event_store.subscribe_commits();
                loop {
                    if cancellation.is_cancelled() {
                        break;
                    }
                    let _ = commits.borrow_and_update();
                    // A maintenance lane is throughput-oriented, so debounce
                    // every pass, including the first commit-triggered pass.
                    // Sleeping only after a pass lets the first scan race a
                    // foreground burst before coalescing can take effect.
                    if lane.descriptor.latency_class == RuntimeProjectionLatencyClass::Maintenance {
                        tokio::select! {
                            _ = cancellation.cancelled() => break,
                            _ = tokio::time::sleep(MAINTENANCE_PASS_INTERVAL) => {}
                        }
                        let deferral_started = tokio::time::Instant::now();
                        loop {
                            let remaining = event_store
                                .foreground_quiet_remaining(MAINTENANCE_FOREGROUND_QUIET);
                            if remaining.is_zero()
                                || deferral_started.elapsed() >= MAINTENANCE_MAX_FOREGROUND_DEFERRAL
                            {
                                break;
                            }
                            tokio::select! {
                                _ = cancellation.cancelled() => return,
                                _ = tokio::time::sleep(remaining) => {}
                            }
                        }
                    }
                    let permit = tokio::select! {
                        _ = cancellation.cancelled() => break,
                        permit = Arc::clone(&admission).acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        }
                    };
                    // Stop-new and admission linearize on the same original
                    // lane state. Once admitted, its future is retained and is
                    // never cancelled by an off request or a dropped waiter.
                    {
                        let open = state.admission_open.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        if !*open {
                            break;
                        }
                        state.admitted_passes.fetch_add(1, Ordering::AcqRel);
                    }
                    struct PassGuard(Arc<LaneRuntimeState>);
                    impl Drop for PassGuard {
                        fn drop(&mut self) { self.0.admitted_passes.fetch_sub(1, Ordering::AcqRel); }
                    }
                    let admitted = PassGuard(Arc::clone(&state));
                    let started = std::time::Instant::now();
                    let pass = std::panic::AssertUnwindSafe(
                        Arc::clone(&lane).run_once(Arc::clone(&event_store)),
                    )
                    .catch_unwind()
                    .await;
                    drop(admitted);
                    drop(permit);
                    state.total_passes.fetch_add(1, Ordering::Relaxed);
                    state.last_pass_duration_ms.store(
                        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    match pass {
                        Ok(Ok(pass)) => {
                            state.consecutive_failures.store(0, Ordering::Relaxed);
                            state
                                .total_scanned_commits
                                .fetch_add(pass.scanned_commits as u64, Ordering::Relaxed);
                            state
                                .total_matched_events
                                .fetch_add(pass.matched_events as u64, Ordering::Relaxed);
                            state
                                .last_success_at_ms
                                .store(now_ms().max(1), Ordering::Relaxed);
                            *state
                                .last_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                            if pass.backlog {
                                match lane.descriptor.latency_class {
                                    RuntimeProjectionLatencyClass::ReadModel => {
                                        tokio::task::yield_now().await;
                                    }
                                    RuntimeProjectionLatencyClass::Maintenance => {
                                        // The next loop iteration applies the
                                        // same coalescing interval before every
                                        // maintenance pass.
                                    }
                                }
                                continue;
                            }
                            #[cfg(any(test, feature = "test-fixtures"))]
                            let safety_hits = {
                                let sources = notification_sources.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                sources.get(&worker_projection_id).map(|(source, hits)| {
                                    commits = source.clone();
                                    hits.clone()
                                })
                            };
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = wake.notified() => {}
                                changed = commits.changed() => {
                                    if changed.is_err() {
                                        break;
                                    }
                                }
                                _ = tokio::time::sleep(lane.descriptor.safety_tick) => {
                                    #[cfg(any(test, feature = "test-fixtures"))]
                                    if let Some(hits) = safety_hits { hits.fetch_add(1, Ordering::SeqCst); }
                                }
                            }
                        }
                        Ok(Err(error)) => {
                            record_failure(&state, &worker_projection_id, error);
                            let delay = failure_backoff(
                                &worker_projection_id,
                                state.consecutive_failures.load(Ordering::Relaxed),
                            );
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                        Err(_) => {
                            record_failure(
                                &state,
                                &worker_projection_id,
                                "projection lane panicked".to_string(),
                            );
                            let delay = failure_backoff(
                                &worker_projection_id,
                                state.consecutive_failures.load(Ordering::Relaxed),
                            );
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                    }
                }
            });
            workers.push((
                projection_id,
                Arc::new(tokio::sync::Mutex::new(RetainedProjectionWorker {
                    handle: Some(worker),
                    outcome: None,
                })),
            ));
        }
        Ok(())
    }

    pub fn lane_health(
        &self,
        projection_id: &str,
    ) -> Result<Option<RuntimeProjectionLaneHealth>, String> {
        if !self.lane_snapshot().contains_key(projection_id) {
            return Ok(None);
        }
        let latest_commit_cursor = self
            .event_store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        self.lane_health_at(projection_id, latest_commit_cursor)
    }

    fn lane_health_at(
        &self,
        projection_id: &str,
        latest_commit_cursor: u64,
    ) -> Result<Option<RuntimeProjectionLaneHealth>, String> {
        let snapshot = self.lane_snapshot();
        let Some(registered) = snapshot.get(projection_id) else {
            return Ok(None);
        };
        let checkpoint_cursor = self
            .event_store
            .projection_checkpoint(projection_id)
            .map_err(|error| error.to_string())?
            .map_or(0, |checkpoint| checkpoint.source_cursor);
        let last_success_at_ms = registered.state.last_success_at_ms.load(Ordering::Relaxed);
        let admission_open = *registered
            .state
            .admission_open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let health = RuntimeProjectionLaneHealth {
            projection_id: projection_id.to_string(),
            worker_running: registered.state.worker_running.load(Ordering::Acquire),
            admission_open,
            admitted_passes: registered.state.admitted_passes.load(Ordering::Acquire),
            checkpoint_cursor,
            latest_commit_cursor,
            lag_commits: latest_commit_cursor.saturating_sub(checkpoint_cursor),
            consecutive_failures: registered
                .state
                .consecutive_failures
                .load(Ordering::Relaxed),
            total_passes: registered.state.total_passes.load(Ordering::Relaxed),
            total_scanned_commits: registered
                .state
                .total_scanned_commits
                .load(Ordering::Relaxed),
            total_matched_events: registered
                .state
                .total_matched_events
                .load(Ordering::Relaxed),
            last_pass_duration_ms: registered
                .state
                .last_pass_duration_ms
                .load(Ordering::Relaxed),
            last_success_at_ms: (last_success_at_ms > 0).then_some(last_success_at_ms),
            last_error: registered
                .state
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        };
        Ok(Some(health))
    }

    pub fn health(&self) -> Result<RuntimeEventReactorHealth, String> {
        let snapshot = self.lane_snapshot();
        let mut lanes = Vec::with_capacity(snapshot.len());
        let latest_commit_cursor = self
            .event_store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        for projection_id in snapshot.keys() {
            if let Some(health) = self.lane_health_at(projection_id, latest_commit_cursor)? {
                lanes.push(health);
            }
        }
        Ok(RuntimeEventReactorHealth {
            sealed: true,
            admission_capacity: self.admission_capacity,
            lanes,
        })
    }

    pub async fn shutdown(&self) -> RuntimeEventReactorShutdownReport {
        self.cancellation.cancel();
        for lane in self.lane_snapshot().values() {
            *lane
                .state
                .admission_open
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
            lane.stop.cancel();
        }
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut report = RuntimeEventReactorShutdownReport::default();
        for (projection_id, worker) in workers {
            let joined = tokio::time::timeout_at(deadline, async {
                let mut worker = worker.lock().await;
                if let Some(handle) = worker.handle.as_mut() {
                    // Borrow the retained handle: dropping this shutdown waiter
                    // or reaching its deadline must not detach physical work.
                    let outcome = handle.await.map_err(|error| error.to_string());
                    worker.handle = None;
                    worker.outcome = Some(outcome);
                }
                worker
                    .outcome
                    .clone()
                    .expect("joined projection worker outcome")
            })
            .await;
            match joined {
                Ok(Ok(())) => report.drained_lanes.push(projection_id),
                Ok(Err(error)) => report.join_errors.push(format!("{projection_id}: {error}")),
                Err(_) => report.timed_out_lanes.push(projection_id),
            }
        }
        report
    }
}

fn record_failure(state: &LaneRuntimeState, projection_id: &str, error: String) {
    let failures = state
        .consecutive_failures
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    *state
        .last_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
    tracing::warn!(projection_id, %error, failures, "Runtime projection lane pass failed");
}

fn failure_backoff(projection_id: &str, failures: u32) -> Duration {
    let base_ms = 100_u64
        .saturating_mul(1_u64 << failures.saturating_sub(1).min(8))
        .min(30_000);
    let digest = projection_id.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(109).wrapping_add(u64::from(byte))
    });
    let jitter_percent = 100_u64.saturating_add(digest % 21);
    Duration::from_millis(
        base_ms
            .saturating_mul(jitter_percent)
            .saturating_div(100)
            .min(30_000),
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn admission_capacity(lane_count: usize, logical_cpus: usize, backend_capacity: usize) -> usize {
    lane_count
        .max(1)
        .min(logical_cpus.max(1))
        .min(backend_capacity.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeEventScope, RuntimeProjectionEventInterest};
    use std::sync::atomic::AtomicUsize;

    fn descriptor(id: &str) -> RuntimeProjectionDescriptor {
        RuntimeProjectionDescriptor::new(
            id,
            RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Task,
                "test.event",
            )]),
            8,
            Duration::from_millis(10),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn domain_lane_registration_is_atomic_before_any_new_worker_starts() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let lane = |id| {
            RuntimeProjectionLane::blocking(
                descriptor(id),
                |_| Ok(RuntimeProjectionPass::default()),
            )
        };
        let reactor =
            Arc::new(RuntimeEventReactor::sealed(store, [lane("projector:base")]).unwrap());
        reactor.start().unwrap();
        assert!(reactor
            .register_lanes([lane("projector:new-a"), lane("projector:base")])
            .is_err());
        assert!(reactor.lane_health("projector:new-a").unwrap().is_none());
        assert!(reactor
            .register_lanes([lane("projector:new-b"), lane("projector:new-b")])
            .is_err());
        assert!(reactor.lane_health("projector:new-b").unwrap().is_none());

        let a = Arc::new(tokio::sync::Notify::new());
        let b = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let ready_lane = |id, entered: Arc<tokio::sync::Notify>| {
            let mut descriptor = descriptor(id);
            descriptor.safety_tick = Duration::from_secs(60);
            let calls = calls.clone();
            RuntimeProjectionLane::asynchronous(descriptor, move |_| {
                let entered = entered.clone();
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    Ok(RuntimeProjectionPass::default())
                })
            })
        };
        reactor
            .register_lanes([
                ready_lane("projector:new-a", a.clone()),
                ready_lane("projector:new-b", b.clone()),
            ])
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            a.notified().await;
            b.notified().await;
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
        assert!(report.join_errors.is_empty());
    }

    #[tokio::test]
    async fn stopped_lane_retains_admitted_work_across_drain_timeout_and_registers_one_successor() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let exited = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let lane = RuntimeProjectionLane::asynchronous(descriptor("projector:controlled"), {
            let entered = entered.clone();
            let release = release.clone();
            let exited = exited.clone();
            let calls = calls.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                let exited = exited.clone();
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    exited.store(true, Ordering::SeqCst);
                    Ok(RuntimeProjectionPass {
                        backlog: true,
                        ..RuntimeProjectionPass::default()
                    })
                })
            }
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store, [lane]).unwrap());
        reactor.start().unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(reactor
            .drain_lane("projector:controlled", tokio::time::Instant::now())
            .await
            .unwrap_err()
            .contains("stopped"));
        assert!(reactor.stop_lane("projector:controlled"));
        let health = reactor
            .lane_health("projector:controlled")
            .unwrap()
            .unwrap();
        assert!(!health.admission_open);
        assert_eq!(health.admitted_passes, 1);
        assert!(reactor
            .drain_lane("projector:controlled", tokio::time::Instant::now())
            .await
            .unwrap_err()
            .contains("draining"));
        assert!(!exited.load(Ordering::SeqCst));
        let successor_entered = Arc::new(tokio::sync::Notify::new());
        let successor = || {
            RuntimeProjectionLane::asynchronous(descriptor("projector:controlled"), {
                let entered = successor_entered.clone();
                move |_| {
                    let entered = entered.clone();
                    Box::pin(async move {
                        entered.notify_one();
                        Ok(RuntimeProjectionPass::default())
                    })
                }
            })
        };
        assert!(reactor
            .register_lane(successor())
            .unwrap_err()
            .contains("duplicate"));
        release.notify_one();
        reactor
            .drain_lane(
                "projector:controlled",
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(exited.load(Ordering::SeqCst));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "backlog must not reopen stopped admission"
        );
        assert!(reactor
            .lane_health("projector:controlled")
            .unwrap()
            .is_none());
        reactor.register_lane(successor()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), successor_entered.notified())
            .await
            .unwrap();
        assert!(reactor.shutdown().await.timed_out_lanes.is_empty());
        assert!(reactor
            .register_lane(successor())
            .unwrap_err()
            .contains("closed"));
    }

    #[test]
    fn stopped_unstarted_lane_cannot_acquire_a_worker_after_its_registration_is_removed() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let called = Arc::new(AtomicBool::new(false));
        let lane = RuntimeProjectionLane::blocking(descriptor("projector:cold"), {
            let called = called.clone();
            move |_| {
                called.store(true, Ordering::SeqCst);
                Ok(RuntimeProjectionPass::default())
            }
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store, [lane]).unwrap());
        let old_generation = reactor.lane_snapshot();
        assert!(reactor.stop_lane("projector:cold"));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            reactor
                .drain_lane(
                    "projector:cold",
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
            reactor
                .start_lanes(tokio::runtime::Handle::current(), old_generation)
                .unwrap();
            assert!(reactor.workers.lock().unwrap().is_empty());
            assert!(!called.load(Ordering::SeqCst));
        });
    }

    #[tokio::test]
    async fn lane_physical_drain_preserves_domain_failure_for_its_control_consumer() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let lane = RuntimeProjectionLane::asynchronous(descriptor("projector:failure"), {
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Err("original-domain-receipt-write-failed".into())
                })
            }
        });
        let reactor = Arc::new(
            RuntimeEventReactor::sealed(Arc::new(RuntimeEventStore::for_test()), [lane]).unwrap(),
        );
        reactor.start().unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        reactor.stop_lane("projector:failure");
        release.notify_one();
        let report = reactor
            .drain_lane(
                "projector:failure",
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(
            report.last_error.as_deref(),
            Some("original-domain-receipt-write-failed")
        );
        assert!(reactor.lane_health("projector:failure").unwrap().is_none());
        assert!(reactor.shutdown().await.timed_out_lanes.is_empty());
    }

    #[tokio::test]
    async fn a_commit_during_a_scan_triggers_another_pass_without_waiting_for_the_safety_tick() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let rescanned = Arc::new(tokio::sync::Notify::new());
        let passes = Arc::new(AtomicUsize::new(0));
        let mut descriptor = descriptor("projector:scan-doorbell-race");
        descriptor.safety_tick = Duration::from_secs(30);
        let lane_store = store.clone();
        let lane_entered = entered.clone();
        let lane_release = release.clone();
        let lane_rescanned = rescanned.clone();
        let lane_passes = passes.clone();
        let lane = RuntimeProjectionLane::asynchronous(descriptor, move |_| {
            let store = lane_store.clone();
            let entered = lane_entered.clone();
            let release = lane_release.clone();
            let rescanned = lane_rescanned.clone();
            let passes = lane_passes.clone();
            Box::pin(async move {
                let scanned_cursor = store.current_commit_cursor();
                if passes.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(scanned_cursor, 0);
                    entered.notify_one();
                    release.notified().await;
                } else {
                    assert!(scanned_cursor > 0);
                    rescanned.notify_one();
                }
                Ok(RuntimeProjectionPass::default())
            })
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store.clone(), [lane]).unwrap());
        reactor.start().unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        store
            .append(crate::RuntimeEventInput {
                stream_id: "scan-doorbell-race".into(),
                scope: RuntimeEventScope::Task,
                kind: "test.event".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), rescanned.notified())
            .await
            .unwrap();
        assert_eq!(passes.load(Ordering::SeqCst), 2);
        assert!(reactor.shutdown().await.timed_out_lanes.is_empty());
    }

    #[test]
    fn sealed_reactor_rejects_duplicate_lane_ids() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let lane = || {
            RuntimeProjectionLane::blocking(descriptor("projector:test"), |_| {
                Ok(RuntimeProjectionPass::default())
            })
        };
        let error = RuntimeEventReactor::sealed(store, [lane(), lane()]).unwrap_err();
        assert!(error.contains("duplicate Runtime projection lane"));
    }

    #[tokio::test]
    async fn explicit_wakes_during_a_pass_coalesce_and_stop_at_shutdown() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let rescanned = Arc::new(tokio::sync::Notify::new());
        let passes = Arc::new(AtomicUsize::new(0));
        let mut description = descriptor("projector:explicit-wake");
        description.safety_tick = Duration::from_secs(60);
        let lane = RuntimeProjectionLane::asynchronous(description, {
            let entered = entered.clone();
            let release = release.clone();
            let rescanned = rescanned.clone();
            let passes = passes.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                let rescanned = rescanned.clone();
                let passes = passes.clone();
                Box::pin(async move {
                    if passes.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.notify_one();
                        release.notified().await;
                    } else {
                        rescanned.notify_one();
                    }
                    Ok(RuntimeProjectionPass::default())
                })
            }
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store, [lane]).unwrap());
        assert!(!reactor.wake_lane("missing"));
        reactor.start().unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        for _ in 0..10_000 {
            assert!(reactor.wake_lane("projector:explicit-wake"));
        }
        assert_eq!(passes.load(Ordering::SeqCst), 1, "no concurrent reentry");
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), rescanned.notified())
            .await
            .unwrap();
        let report = reactor.shutdown().await;
        assert_eq!(report.drained_lanes, ["projector:explicit-wake"]);
        assert!(report.timed_out_lanes.is_empty());
        assert!(report.join_errors.is_empty());
        assert_eq!(passes.load(Ordering::SeqCst), 2, "bounded coalescing");
        assert!(!reactor.wake_lane("projector:explicit-wake"));
    }

    #[test]
    fn admission_capacity_respects_every_resource_bound() {
        assert_eq!(admission_capacity(8, 16, 4), 4);
        assert_eq!(admission_capacity(8, 2, 16), 2);
        assert_eq!(admission_capacity(1, 16, 16), 1);
        assert_eq!(admission_capacity(0, 0, 0), 1);
    }

    #[tokio::test]
    async fn abandoned_shutdown_retains_blocking_work_until_physical_return() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Arc::new(Mutex::new(wait));
        let lane = RuntimeProjectionLane::blocking(descriptor("projector:physical-drain"), {
            let entered = entered.clone();
            move |_| {
                entered.notify_one();
                wait.lock().unwrap().recv().unwrap();
                Ok(RuntimeProjectionPass::default())
            }
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store, [lane]).unwrap());
        reactor.start().unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        // Polling then dropping the shutdown future is a cancelled caller, not
        // permission to abandon the worker's join or its blocking IO.
        let mut first = Box::pin(reactor.shutdown());
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        let mut second = Box::pin(reactor.shutdown());
        assert!(futures::poll!(&mut second).is_pending());
        assert!(!reactor.wake_lane("projector:physical-drain"));
        release.send(()).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap();
        assert_eq!(report.drained_lanes, ["projector:physical-drain"]);
        assert!(report.timed_out_lanes.is_empty());
        assert!(report.join_errors.is_empty());
        assert_eq!(reactor.shutdown().await, report);
    }

    #[tokio::test(start_paused = true)]
    async fn n1_shutdown_timeout_retains_join_and_rejects_new_admission() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let lane = RuntimeProjectionLane::asynchronous(descriptor("projector:late-exit"), {
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(RuntimeProjectionPass::default())
                })
            }
        });
        let reactor = Arc::new(RuntimeEventReactor::sealed(store, [lane]).unwrap());
        reactor.start().unwrap();
        entered.notified().await;
        let first = reactor.shutdown().await;
        assert!(first.drained_lanes.is_empty());
        assert_eq!(first.timed_out_lanes, ["projector:late-exit"]);
        assert!(reactor.start().is_err());
        assert!(!reactor.wake_lane("projector:late-exit"));
        release.notify_one();
        let second = reactor.shutdown().await;
        assert_eq!(second.drained_lanes, ["projector:late-exit"]);
        assert!(second.timed_out_lanes.is_empty());
        assert!(second.join_errors.is_empty());
    }

    #[tokio::test]
    async fn one_failing_lane_does_not_stop_another_lane() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let successful_passes = Arc::new(AtomicU64::new(0));
        let successful = Arc::clone(&successful_passes);
        let reactor = Arc::new(
            RuntimeEventReactor::sealed(
                store,
                [
                    RuntimeProjectionLane::blocking(descriptor("projector:failed"), |_| {
                        Err("injected".to_string())
                    }),
                    RuntimeProjectionLane::blocking(descriptor("projector:healthy"), move |_| {
                        successful.fetch_add(1, Ordering::Relaxed);
                        Ok(RuntimeProjectionPass::default())
                    }),
                ],
            )
            .unwrap(),
        );
        reactor.start().unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let failed = reactor.lane_health("projector:failed").unwrap().unwrap();
        let healthy = reactor.lane_health("projector:healthy").unwrap().unwrap();
        assert!(failed.consecutive_failures > 0);
        assert!(healthy.worker_running);
        assert!(successful_passes.load(Ordering::Relaxed) > 0);
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
    }

    #[tokio::test]
    async fn one_lane_is_never_reentered_while_backlog_remains() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let run_active = Arc::clone(&active);
        let run_maximum = Arc::clone(&maximum);
        let reactor = Arc::new(
            RuntimeEventReactor::sealed(
                store,
                [RuntimeProjectionLane::blocking(
                    descriptor("projector:single-flight"),
                    move |batch_size| {
                        let current = run_active.fetch_add(1, Ordering::SeqCst) + 1;
                        run_maximum.fetch_max(current, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(5));
                        run_active.fetch_sub(1, Ordering::SeqCst);
                        Ok(RuntimeProjectionPass::scanned(batch_size, batch_size))
                    },
                )],
            )
            .unwrap(),
        );
        reactor.start().unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }
}
