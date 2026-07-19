use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

/// Independently throttled execution resource families.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionResourceKind {
    Provider,
    Agent,
    Tool,
    Custom(String),
}

/// Bounds used when adapting an effective concurrency limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceQuota {
    pub minimum: usize,
    pub target: usize,
    pub maximum: usize,
}

impl ResourceQuota {
    pub fn new(
        minimum: usize,
        target: usize,
        maximum: usize,
    ) -> Result<Self, ResourceAcquireError> {
        if minimum == 0 || minimum > target || target > maximum {
            return Err(ResourceAcquireError::InvalidQuota {
                minimum,
                target,
                maximum,
            });
        }
        Ok(Self {
            minimum,
            target,
            maximum,
        })
    }
}

/// Runtime pressure input. Values are normalized to `0.0..=1.0`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourcePressure {
    pub saturation: f32,
    pub failure_rate: f32,
    pub latency_pressure: f32,
}

impl ResourcePressure {
    pub const HEALTHY: Self = Self {
        saturation: 0.0,
        failure_rate: 0.0,
        latency_pressure: 0.0,
    };

    fn score(self) -> f32 {
        let saturation = self.saturation.clamp(0.0, 1.0);
        let failure = self.failure_rate.clamp(0.0, 1.0);
        let latency = self.latency_pressure.clamp(0.0, 1.0);
        (saturation * 0.35 + failure * 0.40 + latency * 0.25).clamp(0.0, 1.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionResourceSnapshot {
    pub kind: ExecutionResourceKind,
    pub minimum: usize,
    pub target: usize,
    pub maximum: usize,
    pub effective_limit: usize,
    pub active_leases: usize,
    pub queued_waiters: usize,
    pub queue_wait: ResourceLatencySnapshot,
    pub run: ResourceLatencySnapshot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLatencySnapshot {
    pub samples: usize,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub max_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResourceAcquireError {
    #[error(
        "invalid quota: expected 0 < minimum <= target <= maximum, got {minimum}/{target}/{maximum}"
    )]
    InvalidQuota {
        minimum: usize,
        target: usize,
        maximum: usize,
    },
    #[error("resource kind is not configured: {0:?}")]
    UnknownResource(ExecutionResourceKind),
    #[error("timed out waiting for resource {kind:?} after {waited_ms} ms")]
    TimedOut {
        kind: ExecutionResourceKind,
        waited_ms: u64,
    },
    #[error("resource manager lock is poisoned")]
    Poisoned,
}

#[derive(Debug)]
struct ResourceState {
    quota: ResourceQuota,
    effective_limit: usize,
    active: HashMap<Uuid, Instant>,
    waiters: VecDeque<Uuid>,
    queue_wait_ms: VecDeque<u64>,
    run_ms: VecDeque<u64>,
}

const LATENCY_SAMPLE_CAPACITY: usize = 256;

#[derive(Debug, Default)]
struct ManagerState {
    resources: HashMap<ExecutionResourceKind, ResourceState>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<ManagerState>,
    adaptive: Mutex<HashMap<ExecutionResourceKind, AdaptiveState>>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct AdaptiveState {
    pressure_streak: u8,
    healthy_streak: u8,
    last_adjustment: Option<Instant>,
}

/// Instance-owned dynamic quota and backpressure manager.
#[derive(Clone, Debug, Default)]
pub struct ExecutionResourceManager {
    shared: Arc<Shared>,
}

impl ExecutionResourceManager {
    pub fn new(quotas: impl IntoIterator<Item = (ExecutionResourceKind, ResourceQuota)>) -> Self {
        let resources = quotas
            .into_iter()
            .map(|(kind, quota)| {
                (
                    kind,
                    ResourceState {
                        quota,
                        effective_limit: quota.target,
                        active: HashMap::new(),
                        waiters: VecDeque::new(),
                        queue_wait_ms: VecDeque::new(),
                        run_ms: VecDeque::new(),
                    },
                )
            })
            .collect();
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(ManagerState { resources }),
                adaptive: Mutex::new(HashMap::new()),
                changed: Notify::new(),
            }),
        }
    }

    /// Adapt the effective limit without revoking already-running leases.
    ///
    /// Severe pressure converges toward `minimum`; healthy capacity converges
    /// toward `maximum`. Existing leases finish naturally while new work is
    /// backpressured against the new limit.
    pub fn observe_pressure(
        &self,
        kind: &ExecutionResourceKind,
        pressure: ResourcePressure,
    ) -> Result<ExecutionResourceSnapshot, ResourceAcquireError> {
        let snapshot = {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            let state = guard
                .resources
                .get_mut(kind)
                .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
            let span = state.quota.maximum - state.quota.minimum;
            let desired =
                state.quota.maximum - ((span as f32 * pressure.score()).round() as usize).min(span);
            state.effective_limit = desired.clamp(state.quota.minimum, state.quota.maximum);
            snapshot_for(kind.clone(), state)
        };
        self.shared.changed.notify_waiters();
        Ok(snapshot)
    }

    /// Feed real queue/run/error observations into the existing quota owner.
    /// Three consecutive pressure samples lower capacity; eight healthy samples
    /// raise it. A five-second cooldown prevents request-by-request oscillation.
    pub fn observe_runtime_pressure(
        &self,
        kind: &ExecutionResourceKind,
        latency: Duration,
        failed: bool,
    ) -> Result<ExecutionResourceSnapshot, ResourceAcquireError> {
        let snapshot = self.snapshot(kind)?;
        let saturation = if snapshot.effective_limit == 0 {
            1.0
        } else {
            (snapshot.active_leases + snapshot.queued_waiters) as f32
                / snapshot.effective_limit as f32
        }
        .clamp(0.0, 1.0);
        let latency_pressure = (latency.as_secs_f32() / 60.0).clamp(0.0, 1.0);
        let pressured = failed || saturation >= 1.0 || latency_pressure >= 0.5;
        let healthy = !failed && snapshot.queued_waiters == 0 && latency_pressure < 0.15;
        let adjustment = {
            let mut states = self
                .shared
                .adaptive
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            let state = states.entry(kind.clone()).or_default();
            if pressured {
                state.pressure_streak = state.pressure_streak.saturating_add(1);
                state.healthy_streak = 0;
            } else if healthy {
                state.healthy_streak = state.healthy_streak.saturating_add(1);
                state.pressure_streak = 0;
            } else {
                state.pressure_streak = 0;
                state.healthy_streak = 0;
            }
            let cooldown_ready = state
                .last_adjustment
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(5));
            let pressure = if cooldown_ready && state.pressure_streak >= 3 {
                state.pressure_streak = 0;
                Some(ResourcePressure {
                    saturation,
                    failure_rate: if failed { 1.0 } else { 0.0 },
                    latency_pressure,
                })
            } else if cooldown_ready && state.healthy_streak >= 8 {
                state.healthy_streak = 0;
                Some(ResourcePressure::HEALTHY)
            } else {
                None
            };
            if pressure.is_some() {
                state.last_adjustment = Some(Instant::now());
            }
            pressure
        };
        adjustment.map_or(Ok(snapshot), |pressure| {
            self.observe_pressure(kind, pressure)
        })
    }

    /// Replace quota bounds. Running leases are never cancelled when shrinking.
    pub fn update_quota(
        &self,
        kind: &ExecutionResourceKind,
        quota: ResourceQuota,
    ) -> Result<ExecutionResourceSnapshot, ResourceAcquireError> {
        let snapshot = {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            let state = guard
                .resources
                .get_mut(kind)
                .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
            state.quota = quota;
            state.effective_limit = quota.target;
            snapshot_for(kind.clone(), state)
        };
        self.shared.changed.notify_waiters();
        Ok(snapshot)
    }

    pub async fn acquire(
        &self,
        kind: ExecutionResourceKind,
        timeout: Option<Duration>,
    ) -> Result<ExecutionResourceLease, ResourceAcquireError> {
        let waiter_id = Uuid::new_v4();
        {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            let state = guard
                .resources
                .get_mut(&kind)
                .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
            state.waiters.push_back(waiter_id);
        }

        let mut registration = WaiterRegistration {
            shared: Arc::clone(&self.shared),
            kind: kind.clone(),
            waiter_id,
            active: true,
        };
        let started = Instant::now();

        loop {
            let notified = self.shared.changed.notified();
            let acquired = {
                let mut guard = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| ResourceAcquireError::Poisoned)?;
                let state = guard
                    .resources
                    .get_mut(&kind)
                    .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
                let is_front = state.waiters.front().copied() == Some(waiter_id);
                if is_front && state.active.len() < state.effective_limit {
                    state.waiters.pop_front();
                    observe_latency(&mut state.queue_wait_ms, duration_millis(started.elapsed()));
                    state.active.insert(waiter_id, Instant::now());
                    true
                } else {
                    false
                }
            };

            if acquired {
                registration.active = false;
                return Ok(ExecutionResourceLease {
                    shared: Arc::clone(&self.shared),
                    kind,
                    lease_id: waiter_id,
                    released: false,
                });
            }

            if let Some(limit) = timeout {
                let Some(remaining) = limit.checked_sub(started.elapsed()) else {
                    return Err(ResourceAcquireError::TimedOut {
                        kind,
                        waited_ms: duration_millis(limit),
                    });
                };
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    return Err(ResourceAcquireError::TimedOut {
                        kind,
                        waited_ms: duration_millis(limit),
                    });
                }
            } else {
                notified.await;
            }
        }
    }

    pub fn snapshot(
        &self,
        kind: &ExecutionResourceKind,
    ) -> Result<ExecutionResourceSnapshot, ResourceAcquireError> {
        let guard = self
            .shared
            .state
            .lock()
            .map_err(|_| ResourceAcquireError::Poisoned)?;
        let state = guard
            .resources
            .get(kind)
            .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
        Ok(snapshot_for(kind.clone(), state))
    }

    pub fn snapshots(&self) -> Result<Vec<ExecutionResourceSnapshot>, ResourceAcquireError> {
        let guard = self
            .shared
            .state
            .lock()
            .map_err(|_| ResourceAcquireError::Poisoned)?;
        let mut snapshots = guard
            .resources
            .iter()
            .map(|(kind, state)| snapshot_for(kind.clone(), state))
            .collect::<Vec<_>>();
        snapshots
            .sort_by(|left, right| format!("{:?}", left.kind).cmp(&format!("{:?}", right.kind)));
        Ok(snapshots)
    }
}

pub struct ExecutionResourceLease {
    shared: Arc<Shared>,
    kind: ExecutionResourceKind,
    lease_id: Uuid,
    released: bool,
}

impl ExecutionResourceLease {
    pub fn id(&self) -> Uuid {
        self.lease_id
    }

    pub fn kind(&self) -> &ExecutionResourceKind {
        &self.kind
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            if let Some(state) = guard.resources.get_mut(&self.kind) {
                if let Some(started) = state.active.remove(&self.lease_id) {
                    observe_latency(&mut state.run_ms, duration_millis(started.elapsed()));
                }
            }
        }
        self.released = true;
        self.shared.changed.notify_waiters();
    }
}

impl Drop for ExecutionResourceLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

struct WaiterRegistration {
    shared: Arc<Shared>,
    kind: ExecutionResourceKind,
    waiter_id: Uuid,
    active: bool,
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            if let Some(state) = guard.resources.get_mut(&self.kind) {
                state.waiters.retain(|id| *id != self.waiter_id);
            }
        }
        self.shared.changed.notify_waiters();
    }
}

fn snapshot_for(kind: ExecutionResourceKind, state: &ResourceState) -> ExecutionResourceSnapshot {
    ExecutionResourceSnapshot {
        kind,
        minimum: state.quota.minimum,
        target: state.quota.target,
        maximum: state.quota.maximum,
        effective_limit: state.effective_limit,
        active_leases: state.active.len(),
        queued_waiters: state.waiters.len(),
        queue_wait: latency_snapshot(&state.queue_wait_ms),
        run: latency_snapshot(&state.run_ms),
    }
}

fn observe_latency(values: &mut VecDeque<u64>, value: u64) {
    if values.len() == LATENCY_SAMPLE_CAPACITY {
        values.pop_front();
    }
    values.push_back(value);
}

fn latency_snapshot(values: &VecDeque<u64>) -> ResourceLatencySnapshot {
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let percentile = |numerator: usize| {
        sorted
            .get((sorted.len().saturating_sub(1) * numerator) / 100)
            .copied()
            .unwrap_or_default()
    };
    ResourceLatencySnapshot {
        samples: sorted.len(),
        p50_ms: percentile(50),
        p95_ms: percentile(95),
        max_ms: sorted.last().copied().unwrap_or_default(),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(limit: usize) -> ExecutionResourceManager {
        ExecutionResourceManager::new([(
            ExecutionResourceKind::Tool,
            ResourceQuota::new(1, limit, limit).unwrap(),
        )])
    }

    #[test]
    fn runtime_pressure_uses_streak_and_cooldown_instead_of_oscillating() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 4).unwrap(),
        )]);
        for _ in 0..2 {
            let snapshot = manager
                .observe_runtime_pressure(
                    &ExecutionResourceKind::Provider,
                    Duration::from_secs(60),
                    true,
                )
                .unwrap();
            assert_eq!(snapshot.effective_limit, 4);
        }
        let pressured = manager
            .observe_runtime_pressure(
                &ExecutionResourceKind::Provider,
                Duration::from_secs(60),
                true,
            )
            .unwrap();
        assert!(pressured.effective_limit < 4);
        for _ in 0..16 {
            let stable = manager
                .observe_runtime_pressure(
                    &ExecutionResourceKind::Provider,
                    Duration::from_millis(1),
                    false,
                )
                .unwrap();
            assert_eq!(stable.effective_limit, pressured.effective_limit);
        }
    }

    #[tokio::test]
    async fn lease_drop_releases_capacity_for_waiter() {
        let manager = manager(1);
        let first = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let waiting_manager = manager.clone();
        let waiter = tokio::spawn(async move {
            waiting_manager
                .acquire(ExecutionResourceKind::Tool, Some(Duration::from_secs(1)))
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .queued_waiters,
            1
        );
        drop(first);
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn pressure_shrinks_without_revoking_active_leases() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 3, 4).unwrap(),
        )]);
        let first = manager
            .acquire(ExecutionResourceKind::Provider, None)
            .await
            .unwrap();
        let second = manager
            .acquire(ExecutionResourceKind::Provider, None)
            .await
            .unwrap();
        let snapshot = manager
            .observe_pressure(
                &ExecutionResourceKind::Provider,
                ResourcePressure {
                    saturation: 1.0,
                    failure_rate: 1.0,
                    latency_pressure: 1.0,
                },
            )
            .unwrap();
        assert_eq!(snapshot.effective_limit, 1);
        assert_eq!(snapshot.active_leases, 2);
        assert!(matches!(
            manager
                .acquire(
                    ExecutionResourceKind::Provider,
                    Some(Duration::from_millis(10)),
                )
                .await,
            Err(ResourceAcquireError::TimedOut { .. })
        ));
        drop((first, second));
    }

    #[tokio::test]
    async fn cancelled_waiter_is_removed() {
        let manager = manager(1);
        let _lease = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let waiting_manager = manager.clone();
        let waiter = tokio::spawn(async move {
            waiting_manager
                .acquire(ExecutionResourceKind::Tool, None)
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .queued_waiters,
            0
        );
    }

    #[tokio::test]
    async fn snapshots_report_bounded_queue_and_run_latency() {
        let manager = manager(1);
        for _ in 0..(LATENCY_SAMPLE_CAPACITY + 20) {
            drop(
                manager
                    .acquire(ExecutionResourceKind::Tool, None)
                    .await
                    .unwrap(),
            );
        }
        let snapshot = manager.snapshot(&ExecutionResourceKind::Tool).unwrap();
        assert_eq!(snapshot.queue_wait.samples, LATENCY_SAMPLE_CAPACITY);
        assert_eq!(snapshot.run.samples, LATENCY_SAMPLE_CAPACITY);
    }
}
