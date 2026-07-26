use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

/// Independently throttled execution resource families.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionResourceKind {
    SessionTurn,
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

/// One terminal resource operation. Producers classify the result; the
/// resource manager owns aggregation and effective-capacity decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceObservation {
    pub observed_at_ms: u64,
    pub queue_wait: Duration,
    pub service_time: Duration,
    pub result_class: ResourceResultClass,
}

impl ResourceObservation {
    #[must_use]
    pub fn terminal(
        queue_wait: Duration,
        service_time: Duration,
        result_class: ResourceResultClass,
    ) -> Self {
        Self::at(now_ms(), queue_wait, service_time, result_class)
    }

    #[must_use]
    pub fn completed(service_time: Duration) -> Self {
        Self::terminal(Duration::ZERO, service_time, ResourceResultClass::Completed)
    }

    #[must_use]
    pub const fn at(
        observed_at_ms: u64,
        queue_wait: Duration,
        service_time: Duration,
        result_class: ResourceResultClass,
    ) -> Self {
        Self {
            observed_at_ms,
            queue_wait,
            service_time,
            result_class,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceResultClass {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    DownstreamOverload,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceObservationFreshness {
    #[default]
    Unknown,
    Fresh,
    Stale,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLimitAdjustment {
    #[default]
    Hold,
    Increased,
    DecreasedOverload,
    DecreasedFailureUpperBound,
    DecreasedServiceRegression,
    ResetToConfiguredTarget,
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
    pub service_time: ResourceLatencySnapshot,
    pub throughput_per_minute: u64,
    pub failure_rate_basis_points: Option<u16>,
    pub timeout_rate_basis_points: Option<u16>,
    pub overload_rate_basis_points: Option<u16>,
    pub cancelled_rate_basis_points: Option<u16>,
    pub failure_timeout_upper_bound_basis_points: Option<u16>,
    pub sample_count: usize,
    pub last_observed_at_ms: Option<u64>,
    pub freshness: ResourceObservationFreshness,
    pub last_adjustment: ResourceLimitAdjustment,
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
    samples: VecDeque<ResourceSample>,
    adaptive: AdaptiveState,
}

#[derive(Debug)]
struct ActiveResourceDemand {
    weight: usize,
}

#[derive(Debug)]
struct PendingResourceDemand {
    id: Uuid,
    demands: Vec<(ExecutionResourceKind, usize)>,
}

const RESOURCE_SAMPLE_CAPACITY: usize = 256;
const ADAPTATION_WINDOW: usize = 16;
const ADAPTATION_HALF_WINDOW: usize = ADAPTATION_WINDOW / 2;
const ADJUSTMENT_COOLDOWN_MS: u64 = 5_000;
const FRESHNESS_WINDOW_MS: u64 = 60_000;
const HIGH_QUEUE_P95_MS: u64 = 100;
const FAILURE_TIMEOUT_UCB_THRESHOLD_BP: u16 = 3_500;

#[derive(Debug, Default)]
struct ManagerState {
    resources: HashMap<ExecutionResourceKind, ResourceState>,
    waiters: VecDeque<PendingResourceDemand>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<ManagerState>,
    changed: Notify,
}

#[derive(Clone, Copy, Debug)]
struct ResourceSample {
    observed_at_ms: u64,
    queue_wait_ms: u64,
    service_time_ms: u64,
    result_class: ResourceResultClass,
}

#[derive(Debug, Default)]
struct AdaptiveState {
    healthy_streak: u8,
    total_samples: u64,
    last_adjustment_at_ms: Option<u64>,
    last_adjustment: ResourceLimitAdjustment,
    increase_baseline: Option<IncreaseBaseline>,
}

#[derive(Clone, Copy, Debug)]
struct IncreaseBaseline {
    sample_sequence: u64,
    service_p95_ms: u64,
    throughput_per_minute: u64,
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
                        samples: VecDeque::new(),
                        adaptive: AdaptiveState::default(),
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
                changed: Notify::new(),
            }),
        }
    }

    /// Records one typed terminal observation and applies the sole adaptive
    /// capacity policy. Queueing alone never decreases concurrency.
    pub fn record_observation(
        &self,
        kind: &ExecutionResourceKind,
        observation: ResourceObservation,
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
            let observed_at_ms = state
                .samples
                .back()
                .map_or(observation.observed_at_ms, |sample| {
                    sample.observed_at_ms.max(observation.observed_at_ms)
                });
            push_sample(
                &mut state.samples,
                ResourceSample {
                    observed_at_ms,
                    queue_wait_ms: duration_millis(observation.queue_wait),
                    service_time_ms: duration_millis(observation.service_time),
                    result_class: observation.result_class,
                },
            );
            state.adaptive.total_samples = state.adaptive.total_samples.saturating_add(1);
            apply_adaptive_policy(state, observed_at_ms);
            snapshot_for(kind.clone(), state, queued_waiters, observed_at_ms)
        };
        self.shared.changed.notify_waiters();
        Ok(snapshot)
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
            state.samples.clear();
            state.adaptive = AdaptiveState {
                last_adjustment: ResourceLimitAdjustment::ResetToConfiguredTarget,
                ..AdaptiveState::default()
            };
            snapshot_for(kind.clone(), state, queued_waiters, now_ms())
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

        let started = Instant::now();
        let mut registration = WaiterRegistration {
            shared: Arc::clone(&self.shared),
            waiter_id,
            demands: demands.clone(),
            started,
            active: true,
        };

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
                        state
                            .active
                            .insert(waiter_id, ActiveResourceDemand { weight: *weight });
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
                    registration.finish(ResourceResultClass::TimedOut);
                    return Err(ResourceAcquireError::TimedOut {
                        kind: demands[0].0.clone(),
                        waited_ms: duration_millis(limit),
                    });
                };
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    registration.finish(ResourceResultClass::TimedOut);
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
            now_ms(),
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
                    now_ms(),
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
                    state.active.remove(&self.lease_id);
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
    demands: Vec<(ExecutionResourceKind, usize)>,
    started: Instant,
    active: bool,
}

impl WaiterRegistration {
    fn finish(&mut self, result_class: ResourceResultClass) {
        if !self.active {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            guard.waiters.retain(|waiter| waiter.id != self.waiter_id);
            let observed_at_ms = now_ms();
            let queue_wait_ms = duration_millis(self.started.elapsed());
            for (kind, _) in &self.demands {
                if let Some(state) = guard.resources.get_mut(kind) {
                    push_sample(
                        &mut state.samples,
                        ResourceSample {
                            observed_at_ms,
                            queue_wait_ms,
                            service_time_ms: 0,
                            result_class,
                        },
                    );
                    state.adaptive.total_samples = state.adaptive.total_samples.saturating_add(1);
                    apply_adaptive_policy(state, observed_at_ms);
                }
            }
        }
        self.active = false;
        self.shared.changed.notify_waiters();
    }
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        self.finish(ResourceResultClass::Cancelled);
    }
}

fn snapshot_for(
    kind: ExecutionResourceKind,
    state: &ResourceState,
    queued_waiters: usize,
    now_ms: u64,
) -> ExecutionResourceSnapshot {
    let metrics = window_metrics(state.samples.iter().copied());
    let last_observed_at_ms = state.samples.back().map(|sample| sample.observed_at_ms);
    ExecutionResourceSnapshot {
        kind: kind.clone(),
        minimum: state.quota.minimum,
        target: state.quota.target,
        maximum: state.quota.maximum,
        effective_limit: state.effective_limit,
        active_leases: active_weight(state),
        queued_waiters,
        queue_wait: latency_snapshot_from_samples(&state.samples, |sample| sample.queue_wait_ms),
        service_time: latency_snapshot_from_samples(&state.samples, |sample| {
            sample.service_time_ms
        }),
        throughput_per_minute: metrics.throughput_per_minute,
        failure_rate_basis_points: rate_basis_points(metrics.failed, metrics.samples),
        timeout_rate_basis_points: rate_basis_points(metrics.timed_out, metrics.samples),
        overload_rate_basis_points: rate_basis_points(metrics.overloaded, metrics.samples),
        cancelled_rate_basis_points: rate_basis_points(metrics.cancelled, metrics.samples),
        failure_timeout_upper_bound_basis_points: failure_timeout_upper_bound(&metrics),
        sample_count: metrics.samples,
        last_observed_at_ms,
        freshness: last_observed_at_ms.map_or(ResourceObservationFreshness::Unknown, |last| {
            if now_ms.saturating_sub(last) <= FRESHNESS_WINDOW_MS {
                ResourceObservationFreshness::Fresh
            } else {
                ResourceObservationFreshness::Stale
            }
        }),
        last_adjustment: state.adaptive.last_adjustment,
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

fn push_sample(samples: &mut VecDeque<ResourceSample>, sample: ResourceSample) {
    if samples.len() == RESOURCE_SAMPLE_CAPACITY {
        samples.pop_front();
    }
    samples.push_back(sample);
}

fn latency_snapshot_from_samples(
    samples: &VecDeque<ResourceSample>,
    value: impl Fn(&ResourceSample) -> u64,
) -> ResourceLatencySnapshot {
    let mut sorted = samples.iter().map(value).collect::<Vec<_>>();
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

#[derive(Clone, Copy, Debug, Default)]
struct ResourceWindowMetrics {
    samples: usize,
    failed: usize,
    cancelled: usize,
    timed_out: usize,
    overloaded: usize,
    queue_p95_ms: u64,
    service_p95_ms: u64,
    throughput_per_minute: u64,
}

fn window_metrics(samples: impl IntoIterator<Item = ResourceSample>) -> ResourceWindowMetrics {
    let mut samples = samples.into_iter().collect::<Vec<_>>();
    if samples.is_empty() {
        return ResourceWindowMetrics::default();
    }
    samples.sort_by_key(|sample| sample.observed_at_ms);
    let mut queue = samples
        .iter()
        .map(|sample| sample.queue_wait_ms)
        .collect::<Vec<_>>();
    let mut service = samples
        .iter()
        .map(|sample| sample.service_time_ms)
        .collect::<Vec<_>>();
    queue.sort_unstable();
    service.sort_unstable();
    let percentile = |values: &[u64], numerator: usize| {
        values
            .get((values.len().saturating_sub(1) * numerator) / 100)
            .copied()
            .unwrap_or_default()
    };
    let first_at = samples.first().map_or(0, |sample| sample.observed_at_ms);
    let last = samples.last().copied().unwrap_or(ResourceSample {
        observed_at_ms: first_at,
        queue_wait_ms: 0,
        service_time_ms: 0,
        result_class: ResourceResultClass::Cancelled,
    });
    let elapsed_ms = last
        .observed_at_ms
        .saturating_sub(first_at)
        .saturating_add(last.service_time_ms)
        .max(1);
    let completed = samples
        .iter()
        .filter(|sample| sample.result_class == ResourceResultClass::Completed)
        .count();
    ResourceWindowMetrics {
        samples: samples.len(),
        failed: samples
            .iter()
            .filter(|sample| sample.result_class == ResourceResultClass::Failed)
            .count(),
        cancelled: samples
            .iter()
            .filter(|sample| sample.result_class == ResourceResultClass::Cancelled)
            .count(),
        timed_out: samples
            .iter()
            .filter(|sample| sample.result_class == ResourceResultClass::TimedOut)
            .count(),
        overloaded: samples
            .iter()
            .filter(|sample| sample.result_class == ResourceResultClass::DownstreamOverload)
            .count(),
        queue_p95_ms: percentile(&queue, 95),
        service_p95_ms: percentile(&service, 95),
        throughput_per_minute: (completed as u64)
            .saturating_mul(60_000)
            .saturating_div(elapsed_ms),
    }
}

fn rate_basis_points(count: usize, samples: usize) -> Option<u16> {
    (samples > 0).then(|| {
        count
            .saturating_mul(10_000)
            .saturating_div(samples)
            .min(10_000) as u16
    })
}

fn failure_timeout_upper_bound(metrics: &ResourceWindowMetrics) -> Option<u16> {
    let adverse = metrics.failed.saturating_add(metrics.timed_out);
    if adverse == 0 {
        return (metrics.samples > 0).then_some(0);
    }
    wilson_upper_bound_basis_points(adverse, metrics.samples)
}

fn wilson_upper_bound_basis_points(adverse: usize, samples: usize) -> Option<u16> {
    if samples == 0 {
        return None;
    }
    let n = samples as f64;
    let p = adverse.min(samples) as f64 / n;
    let z = 1.96_f64;
    let z2 = z * z;
    let denominator = 1.0 + z2 / n;
    let centre = p + z2 / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt();
    Some(
        (((centre + margin) / denominator) * 10_000.0)
            .round()
            .clamp(0.0, 10_000.0) as u16,
    )
}

fn apply_adaptive_policy(state: &mut ResourceState, observed_at_ms: u64) {
    let latest = state.samples.back().copied();
    let cooldown_ready = state
        .adaptive
        .last_adjustment_at_ms
        .is_none_or(|last| observed_at_ms.saturating_sub(last) >= ADJUSTMENT_COOLDOWN_MS);

    if latest.is_some_and(|sample| sample.result_class == ResourceResultClass::DownstreamOverload)
        && cooldown_ready
        && state.effective_limit > state.quota.minimum
    {
        let reduced = state.effective_limit.div_ceil(2).max(state.quota.minimum);
        apply_limit(
            state,
            reduced,
            observed_at_ms,
            ResourceLimitAdjustment::DecreasedOverload,
        );
        return;
    }

    if state.samples.len() < ADAPTATION_WINDOW {
        state.adaptive.healthy_streak = 0;
        return;
    }
    let recent = state
        .samples
        .iter()
        .rev()
        .take(ADAPTATION_WINDOW)
        .copied()
        .collect::<Vec<_>>();
    let current = window_metrics(recent[..ADAPTATION_HALF_WINDOW].iter().copied());
    let previous = window_metrics(recent[ADAPTATION_HALF_WINDOW..].iter().copied());
    let whole = window_metrics(recent.iter().copied());
    let failure_ucb = failure_timeout_upper_bound(&whole).unwrap_or_default();

    if failure_ucb >= FAILURE_TIMEOUT_UCB_THRESHOLD_BP
        && whole.failed.saturating_add(whole.timed_out) > 0
        && cooldown_ready
        && state.effective_limit > state.quota.minimum
    {
        apply_limit(
            state,
            state.effective_limit.saturating_sub(1),
            observed_at_ms,
            ResourceLimitAdjustment::DecreasedFailureUpperBound,
        );
        return;
    }

    if let Some(baseline) = state.adaptive.increase_baseline {
        let enough_post_increase_samples = state
            .adaptive
            .total_samples
            .saturating_sub(baseline.sample_sequence)
            >= ADAPTATION_HALF_WINDOW as u64;
        let service_regressed = baseline.service_p95_ms > 0
            && current.service_p95_ms > baseline.service_p95_ms.saturating_mul(3) / 2;
        let throughput_stalled = current.throughput_per_minute <= baseline.throughput_per_minute;
        if enough_post_increase_samples
            && service_regressed
            && throughput_stalled
            && cooldown_ready
            && state.effective_limit > state.quota.minimum
        {
            apply_limit(
                state,
                state.effective_limit.saturating_sub(1),
                observed_at_ms,
                ResourceLimitAdjustment::DecreasedServiceRegression,
            );
            return;
        }
    }

    let queue_high = current.queue_p95_ms >= HIGH_QUEUE_P95_MS;
    let service_healthy = previous.service_p95_ms == 0
        || current.service_p95_ms <= previous.service_p95_ms.saturating_mul(6) / 5;
    let throughput_growing =
        current.throughput_per_minute > previous.throughput_per_minute.saturating_mul(21) / 20;
    let no_adverse = current.failed == 0 && current.timed_out == 0 && current.overloaded == 0;
    if queue_high && service_healthy && throughput_growing && no_adverse {
        state.adaptive.healthy_streak = state.adaptive.healthy_streak.saturating_add(1);
    } else {
        state.adaptive.healthy_streak = 0;
    }
    if state.adaptive.healthy_streak >= 3
        && cooldown_ready
        && state.effective_limit < state.quota.maximum
    {
        state.adaptive.healthy_streak = 0;
        state.adaptive.increase_baseline = Some(IncreaseBaseline {
            sample_sequence: state.adaptive.total_samples,
            service_p95_ms: current.service_p95_ms,
            throughput_per_minute: current.throughput_per_minute,
        });
        apply_limit(
            state,
            state.effective_limit.saturating_add(1),
            observed_at_ms,
            ResourceLimitAdjustment::Increased,
        );
    }
}

fn apply_limit(
    state: &mut ResourceState,
    desired: usize,
    observed_at_ms: u64,
    adjustment: ResourceLimitAdjustment,
) {
    state.effective_limit = desired.clamp(state.quota.minimum, state.quota.maximum);
    state.adaptive.last_adjustment_at_ms = Some(observed_at_ms);
    state.adaptive.last_adjustment = adjustment;
    if adjustment != ResourceLimitAdjustment::Increased {
        state.adaptive.increase_baseline = None;
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
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

    fn observation(
        at: u64,
        queue_ms: u64,
        service_ms: u64,
        result_class: ResourceResultClass,
    ) -> ResourceObservation {
        ResourceObservation::at(
            at,
            Duration::from_millis(queue_ms),
            Duration::from_millis(service_ms),
            result_class,
        )
    }

    #[test]
    fn queue_only_pressure_never_decreases_capacity() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let base = now_ms();
        for index in 0..24 {
            let snapshot = manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + index * 1_000,
                        500,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
            assert_eq!(snapshot.effective_limit, 4);
        }
    }

    #[test]
    fn explicit_downstream_overload_decreases_capacity_immediately() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let snapshot = manager
            .record_observation(
                &ExecutionResourceKind::Provider,
                observation(now_ms(), 50, 10, ResourceResultClass::DownstreamOverload),
            )
            .unwrap();
        assert_eq!(snapshot.effective_limit, 2);
        assert_eq!(
            snapshot.last_adjustment,
            ResourceLimitAdjustment::DecreasedOverload
        );
    }

    #[test]
    fn failure_timeout_upper_bound_requires_a_real_adverse_sample() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let base = now_ms();
        for index in 0..16 {
            let result = if index >= 14 {
                ResourceResultClass::TimedOut
            } else {
                ResourceResultClass::Completed
            };
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(base + index * 1_000, 0, 100, result),
                )
                .unwrap();
        }
        let snapshot = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
        assert_eq!(snapshot.effective_limit, 3);
        assert!(snapshot
            .failure_timeout_upper_bound_basis_points
            .is_some_and(|value| value >= FAILURE_TIMEOUT_UCB_THRESHOLD_BP));
    }

    #[test]
    fn slow_service_without_capacity_pressure_does_not_reduce_limit() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let base = now_ms();
        for index in 0..24 {
            let snapshot = manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + index * 61_000,
                        0,
                        60_000,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
            assert_eq!(snapshot.effective_limit, 4);
        }
    }

    #[test]
    fn high_queue_healthy_service_and_positive_throughput_increase_capacity() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let base = now_ms();
        for index in 0..8 {
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + index * 1_000,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        let current_base = base + 8_000;
        for index in 0..11 {
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        current_base + index * 250,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        let snapshot = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
        assert_eq!(snapshot.effective_limit, 5);
    }

    #[test]
    fn post_increase_service_regression_without_throughput_gain_rolls_back() {
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 4, 8).unwrap(),
        )]);
        let base = now_ms();
        for index in 0..8 {
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + index * 1_000,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        for index in 0..11 {
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + 8_000 + index * 250,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .effective_limit,
            5
        );
        for index in 0..8 {
            manager
                .record_observation(
                    &ExecutionResourceKind::Provider,
                    observation(
                        base + 16_000 + index * 1_000,
                        200,
                        1_000,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        let snapshot = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
        assert_eq!(snapshot.effective_limit, 4);
        assert_eq!(
            snapshot.last_adjustment,
            ResourceLimitAdjustment::DecreasedServiceRegression
        );
    }

    #[test]
    fn provider_tool_and_agent_feedback_are_isolated() {
        let manager = ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Provider,
                ResourceQuota::new(1, 4, 8).unwrap(),
            ),
            (
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 4, 8).unwrap(),
            ),
            (
                ExecutionResourceKind::Agent,
                ResourceQuota::new(1, 4, 8).unwrap(),
            ),
        ]);
        manager
            .record_observation(
                &ExecutionResourceKind::Provider,
                observation(now_ms(), 0, 1, ResourceResultClass::DownstreamOverload),
            )
            .unwrap();
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .effective_limit,
            2
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .effective_limit,
            4
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Agent)
                .unwrap()
                .effective_limit,
            4
        );
    }

    #[tokio::test]
    async fn increased_limit_admits_new_work_and_later_shrink_preserves_leases() {
        let kind = ExecutionResourceKind::Provider;
        let manager =
            ExecutionResourceManager::new([(kind.clone(), ResourceQuota::new(1, 4, 8).unwrap())]);
        let base = now_ms();
        for index in 0..8 {
            manager
                .record_observation(
                    &kind,
                    observation(
                        base + index * 1_000,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        for index in 0..11 {
            manager
                .record_observation(
                    &kind,
                    observation(
                        base + 8_000 + index * 250,
                        200,
                        100,
                        ResourceResultClass::Completed,
                    ),
                )
                .unwrap();
        }
        let leases = futures::future::try_join_all(
            (0..5).map(|_| manager.acquire(kind.clone(), Some(Duration::from_millis(50)))),
        )
        .await
        .expect("increased limit must admit five provider operations");
        let overloaded = manager
            .record_observation(
                &kind,
                observation(
                    base + 20_000,
                    200,
                    10,
                    ResourceResultClass::DownstreamOverload,
                ),
            )
            .unwrap();
        assert_eq!(overloaded.active_leases, 5);
        assert_eq!(overloaded.effective_limit, 3);
        assert!(matches!(
            manager.acquire(kind, Some(Duration::from_millis(5))).await,
            Err(ResourceAcquireError::TimedOut { .. })
        ));
        drop(leases);
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
    async fn overload_shrinks_without_revoking_active_leases() {
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
            .record_observation(
                &ExecutionResourceKind::Provider,
                observation(now_ms(), 500, 10, ResourceResultClass::DownstreamOverload),
            )
            .unwrap();
        assert_eq!(snapshot.effective_limit, 2);
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
    async fn snapshots_report_bounded_typed_samples_and_rates() {
        let manager = manager(1);
        let base = now_ms();
        for index in 0..(RESOURCE_SAMPLE_CAPACITY + 20) {
            let result = if index % 11 == 0 {
                ResourceResultClass::Failed
            } else {
                ResourceResultClass::Completed
            };
            manager
                .record_observation(
                    &ExecutionResourceKind::Tool,
                    observation(base + index as u64, 10, 20, result),
                )
                .unwrap();
        }
        let snapshot = manager.snapshot(&ExecutionResourceKind::Tool).unwrap();
        assert_eq!(snapshot.sample_count, RESOURCE_SAMPLE_CAPACITY);
        assert_eq!(snapshot.queue_wait.samples, RESOURCE_SAMPLE_CAPACITY);
        assert_eq!(snapshot.service_time.samples, RESOURCE_SAMPLE_CAPACITY);
        assert!(snapshot.failure_rate_basis_points.is_some());
        assert!(snapshot.throughput_per_minute > 0);
        assert_eq!(snapshot.freshness, ResourceObservationFreshness::Fresh);
    }
}
