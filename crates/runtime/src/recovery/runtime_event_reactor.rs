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

#[derive(Debug, Default)]
struct LaneRuntimeState {
    worker_running: AtomicBool,
    consecutive_failures: AtomicU32,
    total_passes: AtomicU64,
    total_scanned_commits: AtomicU64,
    total_matched_events: AtomicU64,
    last_pass_duration_ms: AtomicU64,
    last_success_at_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

#[derive(Debug)]
struct RegisteredLane {
    lane: Arc<RuntimeProjectionLane>,
    state: Arc<LaneRuntimeState>,
}

pub struct RuntimeEventReactor {
    event_store: Arc<RuntimeEventStore>,
    lanes: BTreeMap<String, RegisteredLane>,
    admission_capacity: usize,
    cancellation: CancellationToken,
    started: AtomicBool,
    workers: Mutex<Vec<(String, tokio::task::JoinHandle<()>)>>,
}

impl std::fmt::Debug for RuntimeEventReactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeEventReactor")
            .field("lane_count", &self.lanes.len())
            .field("admission_capacity", &self.admission_capacity)
            .field("started", &self.started.load(Ordering::Relaxed))
            .finish()
    }
}

impl RuntimeEventReactor {
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
            registered.insert(
                projection_id,
                RegisteredLane {
                    lane: Arc::new(lane),
                    state: Arc::new(LaneRuntimeState::default()),
                },
            );
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
            lanes: registered,
            admission_capacity,
            cancellation: CancellationToken::new(),
            started: AtomicBool::new(false),
            workers: Mutex::new(Vec::new()),
        })
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
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
        let admission = Arc::new(tokio::sync::Semaphore::new(self.admission_capacity));
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (projection_id, registered) in &self.lanes {
            let projection_id = projection_id.clone();
            let lane = Arc::clone(&registered.lane);
            let state = Arc::clone(&registered.state);
            let event_store = Arc::clone(&self.event_store);
            let cancellation = self.cancellation.clone();
            let admission = Arc::clone(&admission);
            let worker_projection_id = projection_id.clone();
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
                    let permit = tokio::select! {
                        _ = cancellation.cancelled() => break,
                        permit = Arc::clone(&admission).acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        }
                    };
                    let started = std::time::Instant::now();
                    let pass = std::panic::AssertUnwindSafe(
                        Arc::clone(&lane).run_once(Arc::clone(&event_store)),
                    )
                    .catch_unwind()
                    .await;
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
                            let _ = commits.borrow_and_update();
                            if pass.backlog {
                                match lane.descriptor.latency_class {
                                    RuntimeProjectionLatencyClass::ReadModel => {
                                        tokio::task::yield_now().await;
                                    }
                                    RuntimeProjectionLatencyClass::Maintenance => {
                                        tokio::time::sleep(Duration::from_millis(1)).await;
                                    }
                                }
                                continue;
                            }
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                changed = commits.changed() => {
                                    if changed.is_err() {
                                        break;
                                    }
                                }
                                _ = tokio::time::sleep(lane.descriptor.safety_tick) => {}
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
            workers.push((projection_id, worker));
        }
        Ok(())
    }

    pub fn lane_health(
        &self,
        projection_id: &str,
    ) -> Result<Option<RuntimeProjectionLaneHealth>, String> {
        let Some(registered) = self.lanes.get(projection_id) else {
            return Ok(None);
        };
        let checkpoint_cursor = self
            .event_store
            .projection_checkpoint(projection_id)
            .map_err(|error| error.to_string())?
            .map_or(0, |checkpoint| checkpoint.source_cursor);
        let latest_commit_cursor = self.event_store.current_commit_cursor();
        let last_success_at_ms = registered.state.last_success_at_ms.load(Ordering::Relaxed);
        Ok(Some(RuntimeProjectionLaneHealth {
            projection_id: projection_id.to_string(),
            worker_running: registered.state.worker_running.load(Ordering::Acquire),
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
        }))
    }

    pub fn health(&self) -> Result<RuntimeEventReactorHealth, String> {
        let mut lanes = Vec::with_capacity(self.lanes.len());
        for projection_id in self.lanes.keys() {
            if let Some(health) = self.lane_health(projection_id)? {
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
        let mut workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut report = RuntimeEventReactorShutdownReport::default();
        for (projection_id, mut worker) in workers.drain(..) {
            match tokio::time::timeout_at(deadline, &mut worker).await {
                Ok(Ok(())) => report.drained_lanes.push(projection_id),
                Ok(Err(error)) => report.join_errors.push(format!("{projection_id}: {error}")),
                Err(_) => {
                    worker.abort();
                    report.timed_out_lanes.push(projection_id);
                }
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

    #[test]
    fn sealed_reactor_rejects_duplicate_lane_ids() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let lane = || {
            RuntimeProjectionLane::blocking(descriptor("projector:test"), |_| {
                Ok(RuntimeProjectionPass::default())
            })
        };
        let error = RuntimeEventReactor::sealed(store, [lane(), lane()]).unwrap_err();
        assert!(error.contains("duplicate Runtime projection lane"));
    }

    #[test]
    fn admission_capacity_respects_every_resource_bound() {
        assert_eq!(admission_capacity(8, 16, 4), 4);
        assert_eq!(admission_capacity(8, 2, 16), 2);
        assert_eq!(admission_capacity(1, 16, 16), 1);
        assert_eq!(admission_capacity(0, 0, 0), 1);
    }

    #[tokio::test]
    async fn one_failing_lane_does_not_stop_another_lane() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
