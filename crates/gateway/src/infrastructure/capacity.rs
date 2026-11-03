use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const HISTOGRAM_CAPACITY: usize = 512;
const DEFAULT_RUNTIME_STACK_BYTES: usize = 4 * 1024 * 1024;
static RUNTIME_THREADS: OnceLock<RuntimeThreadConfig> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeThreadConfig {
    pub(crate) workers: usize,
    pub(crate) stack_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HttpCapacityLane {
    Control,
    Data,
    Stream,
    Blocking,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayCapacityConfig {
    pub(crate) runtime_workers: usize,
    pub(crate) runtime_workers_source: &'static str,
    pub(crate) runtime_stack_bytes: usize,
    pub(crate) control_requests: usize,
    pub(crate) data_requests: usize,
    pub(crate) stream_connections: usize,
    pub(crate) blocking_requests: usize,
    pub(crate) queue_timeout_ms: u64,
}

impl GatewayCapacityConfig {
    pub(crate) fn resolve(config: &runtime::GatewayCapacityConfig) -> Self {
        let logical_cpus = std::thread::available_parallelism().map_or(2, usize::from);
        let (runtime_workers, runtime_workers_source) = config.runtime_workers.map_or_else(
            || (logical_cpus.clamp(2, 16), "available_parallelism"),
            |value| (value.clamp(1, 32), "gateway.capacity.runtime_workers"),
        );
        Self {
            runtime_workers,
            runtime_workers_source,
            runtime_stack_bytes: config.runtime_stack_bytes.unwrap_or(DEFAULT_RUNTIME_STACK_BYTES),
            control_requests: config.control_requests.unwrap_or(16).clamp(4, 128),
            data_requests: config
                .data_requests
                .unwrap_or_else(|| logical_cpus.saturating_mul(8))
                .clamp(16, 512),
            stream_connections: config
                .stream_connections
                .unwrap_or_else(|| logical_cpus.saturating_mul(4))
                .clamp(8, 256),
            blocking_requests: config
                .blocking_requests
                .unwrap_or_else(|| logical_cpus.saturating_mul(4))
                .clamp(8, 128),
            queue_timeout_ms: config.queue_timeout_ms.unwrap_or(250).clamp(10, 30_000),
        }
    }
}

fn install_runtime_threads(slot: &OnceLock<RuntimeThreadConfig>, requested: RuntimeThreadConfig) -> Result<RuntimeThreadConfig, String> {
    if requested.stack_bytes == 0 { return Err("gateway runtime stack size must be nonzero".into()); }
    let installed = *slot.get_or_init(|| requested);
    if installed != requested {
        return Err(format!("gateway runtime thread configuration already initialized: installed {installed:?}, requested {requested:?}"));
    }
    Ok(installed)
}

pub(crate) fn configure_runtime_threads(config: &runtime::GatewayCapacityConfig) -> Result<RuntimeThreadConfig, String> {
    let effective = GatewayCapacityConfig::resolve(config);
    install_runtime_threads(&RUNTIME_THREADS, RuntimeThreadConfig {
        workers: effective.runtime_workers, stack_bytes: effective.runtime_stack_bytes,
    })
}

pub(crate) fn configured_runtime_threads() -> RuntimeThreadConfig {
    *RUNTIME_THREADS.get_or_init(|| {
        let effective = GatewayCapacityConfig::resolve(&runtime::GatewayCapacityConfig::default());
        RuntimeThreadConfig { workers: effective.runtime_workers, stack_bytes: effective.runtime_stack_bytes }
    })
}

#[derive(Debug, Default)]
struct BoundedHistogram {
    values: Mutex<VecDeque<u64>>,
}

impl BoundedHistogram {
    fn observe(&self, value: u64) {
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if values.len() == HISTOGRAM_CAPACITY {
            values.pop_front();
        }
        values.push_back(value);
    }

    fn snapshot(&self) -> LatencySnapshot {
        let values = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sorted = values.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        let percentile = |numerator: usize| {
            if sorted.is_empty() {
                return 0;
            }
            sorted[(sorted.len().saturating_sub(1) * numerator) / 100]
        };
        LatencySnapshot {
            samples: sorted.len(),
            p50_ms: percentile(50),
            p95_ms: percentile(95),
            max_ms: sorted.last().copied().unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LatencySnapshot {
    pub(crate) samples: usize,
    pub(crate) p50_ms: u64,
    pub(crate) p95_ms: u64,
    pub(crate) max_ms: u64,
}

#[derive(Debug)]
struct LaneState {
    capacity: usize,
    semaphore: Arc<Semaphore>,
    active: AtomicUsize,
    queued: AtomicUsize,
    rejected: AtomicU64,
    queue_wait: BoundedHistogram,
    run: BoundedHistogram,
}

impl LaneState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
            active: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            rejected: AtomicU64::new(0),
            queue_wait: BoundedHistogram::default(),
            run: BoundedHistogram::default(),
        }
    }

    fn snapshot(&self) -> LaneSnapshot {
        LaneSnapshot {
            capacity: self.capacity,
            active: self.active.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            queue_wait: self.queue_wait.snapshot(),
            run: self.run.snapshot(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LaneSnapshot {
    pub(crate) capacity: usize,
    pub(crate) active: usize,
    pub(crate) queued: usize,
    pub(crate) rejected: u64,
    pub(crate) queue_wait: LatencySnapshot,
    pub(crate) run: LatencySnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct GatewayCapacityController {
    config: GatewayCapacityConfig,
    control: Arc<LaneState>,
    data: Arc<LaneState>,
    stream: Arc<LaneState>,
    blocking: Arc<LaneState>,
    resources: Arc<runtime::execution_core::graph::ExecutionResourceManager>,
}

impl GatewayCapacityController {
    pub(crate) fn new(
        config: GatewayCapacityConfig,
        resources: Arc<runtime::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        Self {
            control: Arc::new(LaneState::new(config.control_requests)),
            data: Arc::new(LaneState::new(config.data_requests)),
            stream: Arc::new(LaneState::new(config.stream_connections)),
            blocking: Arc::new(LaneState::new(config.blocking_requests)),
            config,
            resources,
        }
    }

    pub(crate) fn defaults(
        resources: Arc<runtime::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        Self::new(
            GatewayCapacityConfig::resolve(&runtime::GatewayCapacityConfig::default()),
            resources,
        )
    }

    fn lane(&self, lane: HttpCapacityLane) -> &Arc<LaneState> {
        match lane {
            HttpCapacityLane::Control => &self.control,
            HttpCapacityLane::Data => &self.data,
            HttpCapacityLane::Stream => &self.stream,
            HttpCapacityLane::Blocking => &self.blocking,
        }
    }

    pub(crate) async fn admit_http(
        &self,
        lane: HttpCapacityLane,
    ) -> Result<GatewayCapacityLease, CapacityOverload> {
        self.admit(lane, Arc::clone(self.lane(lane))).await
    }

    pub(crate) async fn admit_blocking(&self) -> Result<GatewayCapacityLease, CapacityOverload> {
        self.admit(HttpCapacityLane::Blocking, Arc::clone(&self.blocking))
            .await
    }

    async fn admit(
        &self,
        lane: HttpCapacityLane,
        state: Arc<LaneState>,
    ) -> Result<GatewayCapacityLease, CapacityOverload> {
        state.queued.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let permit = tokio::time::timeout(
            Duration::from_millis(self.config.queue_timeout_ms),
            Arc::clone(&state.semaphore).acquire_owned(),
        )
        .await;
        state.queued.fetch_sub(1, Ordering::Relaxed);
        let wait_ms = elapsed_ms(started);
        state.queue_wait.observe(wait_ms);
        let permit = match permit {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                state.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(CapacityOverload {
                    lane,
                    retry_after_ms: self.config.queue_timeout_ms.max(100),
                });
            }
        };
        state.active.fetch_add(1, Ordering::Relaxed);
        Ok(GatewayCapacityLease {
            _permit: permit,
            state,
            started: Instant::now(),
        })
    }

    pub(crate) fn snapshot(&self) -> GatewayCapacitySnapshot {
        GatewayCapacitySnapshot {
            status: if self.data.queued.load(Ordering::Relaxed) > self.data.capacity {
                "overloaded"
            } else {
                "ready"
            },
            config: self.config.clone(),
            control: self.control.snapshot(),
            data: self.data.snapshot(),
            stream: self.stream.snapshot(),
            blocking: self.blocking.snapshot(),
            resources: self.resources.snapshots().unwrap_or_default(),
            histogram_capacity: HISTOGRAM_CAPACITY,
        }
    }
}

pub(crate) struct GatewayCapacityLease {
    _permit: OwnedSemaphorePermit,
    state: Arc<LaneState>,
    started: Instant,
}

impl Drop for GatewayCapacityLease {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::Relaxed);
        self.state.run.observe(elapsed_ms(self.started));
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CapacityOverload {
    pub(crate) lane: HttpCapacityLane,
    pub(crate) retry_after_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayCapacitySnapshot {
    pub(crate) status: &'static str,
    pub(crate) config: GatewayCapacityConfig,
    pub(crate) control: LaneSnapshot,
    pub(crate) data: LaneSnapshot,
    pub(crate) stream: LaneSnapshot,
    pub(crate) blocking: LaneSnapshot,
    pub(crate) resources: Vec<runtime::execution_core::graph::ExecutionResourceSnapshot>,
    pub(crate) histogram_capacity: usize,
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::execution_core::graph::{ExecutionResourceKind, ResourceQuota};

    fn controller(config: runtime::GatewayCapacityConfig) -> GatewayCapacityController {
        let resources = Arc::new(
            runtime::execution_core::graph::ExecutionResourceManager::new([
                (
                    ExecutionResourceKind::Provider,
                    ResourceQuota::new(1, 2, 4).unwrap(),
                ),
                (
                    ExecutionResourceKind::Tool,
                    ResourceQuota::new(1, 2, 4).unwrap(),
                ),
                (
                    ExecutionResourceKind::Agent,
                    ResourceQuota::new(1, 2, 4).unwrap(),
                ),
            ]),
        );
        GatewayCapacityController::new(GatewayCapacityConfig::resolve(&config), resources)
    }

    #[tokio::test]
    async fn control_lane_remains_available_when_data_is_saturated() {
        let controller = controller(runtime::GatewayCapacityConfig {
            control_requests: Some(4),
            data_requests: Some(16),
            queue_timeout_ms: Some(10),
            ..Default::default()
        });
        let mut data = Vec::new();
        for _ in 0..16 {
            data.push(controller.admit_http(HttpCapacityLane::Data).await.unwrap());
        }
        assert!(controller.admit_http(HttpCapacityLane::Data).await.is_err());
        assert!(controller
            .admit_http(HttpCapacityLane::Control)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn stream_budget_is_independent_and_cancel_returns_permit() {
        let controller = controller(runtime::GatewayCapacityConfig {
            stream_connections: Some(8),
            queue_timeout_ms: Some(10),
            ..Default::default()
        });
        let permits = futures::future::join_all(
            (0..8).map(|_| controller.admit_http(HttpCapacityLane::Stream)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert!(controller
            .admit_http(HttpCapacityLane::Stream)
            .await
            .is_err());
        drop(permits);
        assert!(controller
            .admit_http(HttpCapacityLane::Stream)
            .await
            .is_ok());
    }

    #[test]
    fn overrides_are_clamped_and_histograms_are_bounded() {
        let config = GatewayCapacityConfig::resolve(&runtime::GatewayCapacityConfig {
            runtime_workers: Some(999),
            queue_timeout_ms: Some(1),
            ..Default::default()
        });
        assert_eq!(config.runtime_workers, 32);
        assert_eq!(config.queue_timeout_ms, 10);
        let histogram = BoundedHistogram::default();
        for value in 0..1000 {
            histogram.observe(value);
        }
        assert_eq!(histogram.snapshot().samples, HISTOGRAM_CAPACITY);
    }

    #[test]
    fn runtime_worker_default_and_explicit_single_worker_are_deterministic() {
        let automatic = GatewayCapacityConfig::resolve(&runtime::GatewayCapacityConfig::default());
        assert!((2..=16).contains(&automatic.runtime_workers));
        assert_eq!(automatic.runtime_workers_source, "available_parallelism");

        let explicit = GatewayCapacityConfig::resolve(&runtime::GatewayCapacityConfig {
            runtime_workers: Some(1),
            ..Default::default()
        });
        assert_eq!(explicit.runtime_workers, 1);
        assert_eq!(
            explicit.runtime_workers_source,
            "gateway.capacity.runtime_workers"
        );
    }

    #[test]
    fn runtime_thread_settings_install_together_and_reject_conflicts() {
        let slot = OnceLock::new();
        let requested = RuntimeThreadConfig { workers: 3, stack_bytes: 4 * 1024 * 1024 };
        assert_eq!(install_runtime_threads(&slot, requested).unwrap(), requested);
        assert_eq!(install_runtime_threads(&slot, requested).unwrap(), requested);
        assert!(install_runtime_threads(&slot, RuntimeThreadConfig { workers: 4, ..requested }).is_err());
        assert!(install_runtime_threads(&slot, RuntimeThreadConfig { stack_bytes: 8 * 1024 * 1024, ..requested }).is_err());
        assert!(install_runtime_threads(&OnceLock::new(), RuntimeThreadConfig { stack_bytes: 0, ..requested }).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shared_runtime_worker_and_blocking_threads_receive_configured_stack() {
        fn stack_bytes() -> usize {
            // SAFETY: pthread initializes the attributes before they are read.
            unsafe {
                let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
                assert_eq!(libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()), 0);
                let mut attr = attr.assume_init();
                let mut bytes = 0;
                assert_eq!(libc::pthread_attr_getstacksize(&attr, &mut bytes), 0);
                assert_eq!(libc::pthread_attr_destroy(&mut attr), 0);
                bytes
            }
        }
        let configured = configure_runtime_threads(&runtime::GatewayCapacityConfig::default()).unwrap();
        assert_eq!(configured.stack_bytes, 4 * 1024 * 1024);
        let (worker, blocking) = crate::SHARED_RT.block_on(async {
            let worker = tokio::spawn(async { stack_bytes() }).await.unwrap();
            let blocking = tokio::task::spawn_blocking(stack_bytes).await.unwrap();
            (worker, blocking)
        });
        assert!(worker >= configured.stack_bytes);
        assert!(blocking >= configured.stack_bytes);
    }
}
