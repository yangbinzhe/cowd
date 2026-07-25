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

/// One completed resource operation, split into capacity and service signals.
///
/// Service latency is retained for diagnostics but deliberately does not
/// reduce concurrency on its own. Capacity adapts only when work waited for
/// admission, the resource was saturated, or a bounded producer was blocked.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceObservation {
    pub queue_wait: Duration,
    pub service_time: Duration,
    pub producer_wait: Duration,
    pub queue_depth: usize,
    pub saturation: f32,
    pub result_class: ResourceResultClass,
}

impl ResourceObservation {
    #[must_use]
    pub fn completed(service_time: Duration) -> Self {
        Self {
            queue_wait: Duration::ZERO,
            service_time,
            producer_wait: Duration::ZERO,
            queue_depth: 0,
            saturation: 0.0,
            result_class: ResourceResultClass::Completed,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceResultClass {
    Completed,
    Failed,
    Cancelled,
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
    #[error("resource demand {demand} exceeds {kind:?} maximum quota {maximum}")]
    DemandExceedsQuota {
        kind: ExecutionResourceKind,
        demand: usize,
        maximum: usize,
    },
    #[error("resource waiter registration was lost before acquisition")]
    RegistrationLost,
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
    active: HashMap<Uuid, ActiveResourceDemand>,
    queue_wait_ms: VecDeque<u64>,
    run_ms: VecDeque<u64>,
}

#[derive(Debug)]
struct ActiveResourceDemand {
    started: Instant,
    weight: usize,
}

#[derive(Debug)]
struct PendingResourceDemand {
    id: Uuid,
    demands: Vec<(ExecutionResourceKind, usize)>,
}

const LATENCY_SAMPLE_CAPACITY: usize = 256;

#[derive(Debug, Default)]
struct ManagerState {
    resources: HashMap<ExecutionResourceKind, ResourceState>,
    waiters: VecDeque<PendingResourceDemand>,
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
                        queue_wait_ms: VecDeque::new(),
                        run_ms: VecDeque::new(),
                    },
                )
            })
            .collect();
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(ManagerState {
                    resources,
                    waiters: VecDeque::new(),
                }),
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
            let queued_waiters = queued_waiter_count(&guard.waiters, kind);
            let state = guard
                .resources
                .get_mut(kind)
                .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
            let span = state.quota.maximum - state.quota.minimum;
            let desired =
                state.quota.maximum - ((span as f32 * pressure.score()).round() as usize).min(span);
            state.effective_limit = desired.clamp(state.quota.minimum, state.quota.maximum);
            snapshot_for(kind.clone(), state, queued_waiters)
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
        observation: ResourceObservation,
    ) -> Result<ExecutionResourceSnapshot, ResourceAcquireError> {
        let snapshot = self.snapshot(kind)?;
        let observed_saturation = if snapshot.effective_limit == 0 {
            1.0
        } else {
            (snapshot.active_leases + snapshot.queued_waiters) as f32
                / snapshot.effective_limit as f32
        }
        .clamp(0.0, 1.0);
        let saturation = observation
            .saturation
            .max(observed_saturation)
            .clamp(0.0, 1.0);
        let queue_pressure =
            observation.queue_depth > 0 || observation.queue_wait >= Duration::from_millis(250);
        let producer_pressure = observation.producer_wait >= Duration::from_millis(50);
        let pressured = saturation >= 1.0 && (queue_pressure || producer_pressure);
        let healthy = !queue_pressure && !producer_pressure && snapshot.queued_waiters == 0;
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
                    failure_rate: 0.0,
                    latency_pressure: if producer_pressure { 1.0 } else { 0.0 },
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
            let queued_waiters = queued_waiter_count(&guard.waiters, kind);
            let state = guard
                .resources
                .get_mut(kind)
                .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
            state.quota = quota;
            state.effective_limit = quota.target;
            snapshot_for(kind.clone(), state, queued_waiters)
        };
        self.shared.changed.notify_waiters();
        Ok(snapshot)
    }

    pub async fn acquire(
        &self,
        kind: ExecutionResourceKind,
        timeout: Option<Duration>,
    ) -> Result<ExecutionResourceLease, ResourceAcquireError> {
        self.acquire_bundle([(kind, 1)], timeout).await
    }

    /// Atomically acquires a weighted set of resource families.
    ///
    /// A waiter may bypass an earlier waiter only when their demand sets are
    /// disjoint. This preserves FIFO fairness for contended resources without
    /// serializing independent Provider, Agent, Tool, process, or network work.
    pub async fn acquire_bundle(
        &self,
        demands: impl IntoIterator<Item = (ExecutionResourceKind, usize)>,
        timeout: Option<Duration>,
    ) -> Result<ExecutionResourceLease, ResourceAcquireError> {
        let demands = normalize_demands(demands);
        let waiter_id = Uuid::new_v4();
        {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            for (kind, weight) in &demands {
                let state = guard
                    .resources
                    .get(kind)
                    .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
                if *weight > state.quota.maximum {
                    return Err(ResourceAcquireError::DemandExceedsQuota {
                        kind: kind.clone(),
                        demand: *weight,
                        maximum: state.quota.maximum,
                    });
                }
            }
            guard.waiters.push_back(PendingResourceDemand {
                id: waiter_id,
                demands: demands.clone(),
            });
        }

        let mut registration = WaiterRegistration {
            shared: Arc::clone(&self.shared),
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
                let position = guard
                    .waiters
                    .iter()
                    .position(|waiter| waiter.id == waiter_id)
                    .ok_or(ResourceAcquireError::RegistrationLost)?;
                let blocked_by_earlier_conflict = guard
                    .waiters
                    .iter()
                    .take(position)
                    .any(|earlier| demand_sets_overlap(&demands, &earlier.demands));
                let has_capacity = demands.iter().all(|(kind, weight)| {
                    guard.resources.get(kind).is_some_and(|state| {
                        active_weight(state).saturating_add(*weight) <= state.effective_limit
                    })
                });
                if !blocked_by_earlier_conflict && has_capacity {
                    guard.waiters.remove(position);
                    for (kind, weight) in &demands {
                        let Some(state) = guard.resources.get_mut(kind) else {
                            return Err(ResourceAcquireError::RegistrationLost);
                        };
                        observe_latency(
                            &mut state.queue_wait_ms,
                            duration_millis(started.elapsed()),
                        );
                        state.active.insert(
                            waiter_id,
                            ActiveResourceDemand {
                                started: Instant::now(),
                                weight: *weight,
                            },
                        );
                    }
                    true
                } else {
                    false
                }
            };

            if acquired {
                registration.active = false;
                return Ok(ExecutionResourceLease {
                    shared: Arc::clone(&self.shared),
                    demands,
                    lease_id: waiter_id,
                    queue_wait: started.elapsed(),
                    released: false,
                });
            }

            if let Some(limit) = timeout {
                let Some(remaining) = limit.checked_sub(started.elapsed()) else {
                    return Err(ResourceAcquireError::TimedOut {
                        kind: demands[0].0.clone(),
                        waited_ms: duration_millis(limit),
                    });
                };
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    return Err(ResourceAcquireError::TimedOut {
                        kind: demands[0].0.clone(),
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
        Ok(snapshot_for(
            kind.clone(),
            state,
            queued_waiter_count(&guard.waiters, kind),
        ))
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
            .map(|(kind, state)| {
                snapshot_for(
                    kind.clone(),
                    state,
                    queued_waiter_count(&guard.waiters, kind),
                )
            })
            .collect::<Vec<_>>();
        snapshots
            .sort_by(|left, right| format!("{:?}", left.kind).cmp(&format!("{:?}", right.kind)));
        Ok(snapshots)
    }
}

pub struct ExecutionResourceLease {
    shared: Arc<Shared>,
    demands: Vec<(ExecutionResourceKind, usize)>,
    lease_id: Uuid,
    queue_wait: Duration,
    released: bool,
}

impl ExecutionResourceLease {
    pub fn id(&self) -> Uuid {
        self.lease_id
    }

    pub fn kind(&self) -> &ExecutionResourceKind {
        &self.demands[0].0
    }

    #[must_use]
    pub fn demands(&self) -> &[(ExecutionResourceKind, usize)] {
        &self.demands
    }

    #[must_use]
    pub fn queue_wait(&self) -> Duration {
        self.queue_wait
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            for (kind, _) in &self.demands {
                if let Some(state) = guard.resources.get_mut(kind) {
                    if let Some(active) = state.active.remove(&self.lease_id) {
                        observe_latency(
                            &mut state.run_ms,
                            duration_millis(active.started.elapsed()),
                        );
                    }
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
    waiter_id: Uuid,
    active: bool,
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            guard.waiters.retain(|waiter| waiter.id != self.waiter_id);
        }
        self.shared.changed.notify_waiters();
    }
}

fn snapshot_for(
    kind: ExecutionResourceKind,
    state: &ResourceState,
    queued_waiters: usize,
) -> ExecutionResourceSnapshot {
    ExecutionResourceSnapshot {
        kind: kind.clone(),
        minimum: state.quota.minimum,
        target: state.quota.target,
        maximum: state.quota.maximum,
        effective_limit: state.effective_limit,
        active_leases: active_weight(state),
        queued_waiters,
        queue_wait: latency_snapshot(&state.queue_wait_ms),
        run: latency_snapshot(&state.run_ms),
    }
}

fn queued_waiter_count(
    waiters: &VecDeque<PendingResourceDemand>,
    kind: &ExecutionResourceKind,
) -> usize {
    waiters
        .iter()
        .filter(|waiter| waiter.demands.iter().any(|(demand, _)| demand == kind))
        .count()
}

fn active_weight(state: &ResourceState) -> usize {
    state.active.values().map(|active| active.weight).sum()
}

fn normalize_demands(
    demands: impl IntoIterator<Item = (ExecutionResourceKind, usize)>,
) -> Vec<(ExecutionResourceKind, usize)> {
    let mut normalized = HashMap::<ExecutionResourceKind, usize>::new();
    for (kind, weight) in demands {
        if weight > 0 {
            let entry = normalized.entry(kind).or_default();
            *entry = entry.saturating_add(weight);
        }
    }
    let mut normalized = normalized.into_iter().collect::<Vec<_>>();
    normalized.sort_by(|left, right| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)));
    if normalized.is_empty() {
        normalized.push((ExecutionResourceKind::Tool, 1));
    }
    normalized
}

fn demand_sets_overlap(
    left: &[(ExecutionResourceKind, usize)],
    right: &[(ExecutionResourceKind, usize)],
) -> bool {
    left.iter()
        .any(|(left, _)| right.iter().any(|(right, _)| left == right))
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
                    ResourceObservation {
                        queue_wait: Duration::from_secs(1),
                        service_time: Duration::from_secs(60),
                        producer_wait: Duration::ZERO,
                        queue_depth: 1,
                        saturation: 1.0,
                        result_class: ResourceResultClass::Failed,
                    },
                )
                .unwrap();
            assert_eq!(snapshot.effective_limit, 4);
        }
        let pressured = manager
            .observe_runtime_pressure(
                &ExecutionResourceKind::Provider,
                ResourceObservation {
                    queue_wait: Duration::from_secs(1),
                    service_time: Duration::from_secs(60),
                    producer_wait: Duration::ZERO,
                    queue_depth: 1,
                    saturation: 1.0,
                    result_class: ResourceResultClass::Failed,
                },
            )
            .unwrap();
        assert!(pressured.effective_limit < 4);
        for _ in 0..16 {
            let stable = manager
                .observe_runtime_pressure(
                    &ExecutionResourceKind::Provider,
                    ResourceObservation::completed(Duration::from_millis(1)),
                )
                .unwrap();
            assert_eq!(stable.effective_limit, pressured.effective_limit);
        }
    }

    #[test]
    fn slow_service_without_capacity_pressure_does_not_reduce_limit() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 4).unwrap(),
        )]);
        for _ in 0..8 {
            let snapshot = manager
                .observe_runtime_pressure(
                    &ExecutionResourceKind::Provider,
                    ResourceObservation::completed(Duration::from_secs(60)),
                )
                .unwrap();
            assert_eq!(snapshot.effective_limit, 4);
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
    async fn bundle_acquisition_is_atomic_across_resource_families() {
        let process = ExecutionResourceKind::Custom("tool.process".to_string());
        let manager = ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 2, 2).unwrap(),
            ),
            (process.clone(), ResourceQuota::new(1, 1, 1).unwrap()),
        ]);
        let first = manager
            .acquire_bundle(
                [(ExecutionResourceKind::Tool, 1), (process.clone(), 1)],
                None,
            )
            .await
            .unwrap();
        let waiting = {
            let manager = manager.clone();
            let process = process.clone();
            tokio::spawn(async move {
                manager
                    .acquire_bundle(
                        [(ExecutionResourceKind::Tool, 1), (process, 1)],
                        Some(Duration::from_secs(1)),
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .active_leases,
            1,
            "blocked bundles must not reserve a partial Tool lease"
        );
        drop(first);
        assert!(waiting.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn disjoint_waiter_can_bypass_without_starving_contended_fifo() {
        let tool = ExecutionResourceKind::Tool;
        let network = ExecutionResourceKind::Custom("tool.network".to_string());
        let manager = ExecutionResourceManager::new([
            (tool.clone(), ResourceQuota::new(1, 1, 1).unwrap()),
            (network.clone(), ResourceQuota::new(1, 1, 1).unwrap()),
        ]);
        let occupied = manager.acquire(tool.clone(), None).await.unwrap();
        let blocked = {
            let manager = manager.clone();
            let tool = tool.clone();
            tokio::spawn(async move { manager.acquire(tool, None).await })
        };
        tokio::task::yield_now().await;
        let disjoint = manager
            .acquire(network, Some(Duration::from_millis(100)))
            .await
            .expect("disjoint resource may bypass");
        assert!(!blocked.is_finished());
        drop(disjoint);
        drop(occupied);
        assert!(blocked.await.unwrap().is_ok());
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
