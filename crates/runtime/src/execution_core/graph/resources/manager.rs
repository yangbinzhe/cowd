use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

pub use harness_contract::execution_graph::ExecutionServiceClass;

type ResourceAdmissionObserver = Arc<dyn Fn(&ResourceAdmissionObservation) + Send + Sync>;

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

const fn service_class_index(class: ExecutionServiceClass) -> usize {
    match class {
        ExecutionServiceClass::Interactive => 0,
        ExecutionServiceClass::Foreground => 1,
        ExecutionServiceClass::Background => 2,
        ExecutionServiceClass::Maintenance => 3,
    }
}

/// Versioned bounds for the single instance-local admission queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAdmissionPolicy {
    pub revision: u64,
    pub max_pending_instance: usize,
    pub max_pending_per_class: usize,
    pub max_pending_per_key: usize,
    pub aging_interval: Duration,
}

impl Default for ExecutionAdmissionPolicy {
    fn default() -> Self {
        Self {
            revision: 1,
            max_pending_instance: 4_096,
            max_pending_per_class: 2_048,
            max_pending_per_key: 512,
            aging_interval: Duration::from_secs(5),
        }
    }
}

impl ExecutionAdmissionPolicy {
    pub fn validate(self) -> Result<Self, ResourceAcquireError> {
        if self.max_pending_instance == 0
            || self.max_pending_per_class == 0
            || self.max_pending_per_key == 0
            || self.max_pending_per_class > self.max_pending_instance
            || self.max_pending_per_key > self.max_pending_per_class
            || self.aging_interval.is_zero()
        {
            return Err(ResourceAcquireError::InvalidAdmissionPolicy);
        }
        Ok(self)
    }
}

/// One atomic admission request. Scope feasibility is supplied by the scope
/// owner; this manager records and enforces the result without duplicating
/// scope-lock ownership.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAdmissionRequest {
    pub request_id: Uuid,
    pub requested_priority: Option<u8>,
    pub deadline_at_ms: Option<u64>,
    pub requested_service_class: ExecutionServiceClass,
    pub parent_class_ceiling: Option<ExecutionServiceClass>,
    pub demands: Vec<(ExecutionResourceKind, usize)>,
    pub normalized_scope: Option<String>,
    pub scope_feasible: bool,
    pub fairness_key: String,
}

impl ResourceAdmissionRequest {
    #[must_use]
    pub fn new(
        requested_service_class: ExecutionServiceClass,
        demands: impl IntoIterator<Item = (ExecutionResourceKind, usize)>,
    ) -> Self {
        let demands = normalize_demands(demands);
        Self {
            request_id: Uuid::new_v4(),
            requested_priority: None,
            deadline_at_ms: None,
            requested_service_class,
            parent_class_ceiling: None,
            fairness_key: default_fairness_key(&demands),
            demands,
            normalized_scope: None,
            scope_feasible: true,
        }
    }

    #[must_use]
    pub const fn resolved_service_class(&self) -> ExecutionServiceClass {
        self.requested_service_class
            .bounded_by(self.parent_class_ceiling)
    }

    #[must_use]
    pub fn with_parent_class_ceiling(mut self, ceiling: ExecutionServiceClass) -> Self {
        self.parent_class_ceiling = Some(ceiling);
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.requested_priority = Some(priority);
        self
    }

    #[must_use]
    pub fn with_deadline_at_ms(mut self, deadline_at_ms: u64) -> Self {
        self.deadline_at_ms = Some(deadline_at_ms);
        self
    }

    #[must_use]
    pub fn with_scope(mut self, normalized_scope: impl Into<String>, feasible: bool) -> Self {
        let normalized_scope = normalized_scope.into();
        let normalized_scope = normalized_scope.trim();
        self.normalized_scope =
            (!normalized_scope.is_empty()).then(|| normalized_scope.to_string());
        self.scope_feasible = feasible;
        self
    }

    #[must_use]
    pub fn with_fairness_key(mut self, fairness_key: impl Into<String>) -> Self {
        let fairness_key = fairness_key.into();
        let fairness_key = fairness_key.trim();
        if !fairness_key.is_empty() {
            self.fairness_key = fairness_key.to_string();
        }
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceWaitReason {
    Capacity,
    ClassFifo,
    HigherServiceClass,
    ScopeInfeasible,
    DeadlineExpired,
    InstancePendingLimit,
    ClassPendingLimit,
    KeyPendingLimit,
}

/// Auditable evidence emitted for every successful atomic grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceGrantReceipt {
    pub request_id: Uuid,
    pub requested_priority: Option<u8>,
    pub deadline_at_ms: Option<u64>,
    pub requested_service_class: ExecutionServiceClass,
    pub resolved_service_class: ExecutionServiceClass,
    pub parent_class_ceiling: Option<ExecutionServiceClass>,
    pub demands: Vec<(ExecutionResourceKind, usize)>,
    pub normalized_scope: Option<String>,
    pub fairness_key: String,
    pub enqueue_sequence: u64,
    pub enqueued_at_ms: u64,
    pub granted_at_ms: u64,
    pub queue_age_ms: u64,
    pub wait_reason: Option<ResourceWaitReason>,
    pub blocker: Option<Uuid>,
    pub policy_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceAdmissionObservationStatus {
    Queued,
    Waiting,
    Granted,
    Deferred,
    Overloaded,
}

/// Durable, transport-neutral observation of one admission state transition.
///
/// The resource manager remains the sole queue owner. Observers receive facts
/// only when a request enters the queue, changes wait reason, or reaches a
/// terminal decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAdmissionObservation {
    pub request_id: Uuid,
    pub status: ResourceAdmissionObservationStatus,
    pub requested_priority: Option<u8>,
    pub deadline_at_ms: Option<u64>,
    pub requested_service_class: ExecutionServiceClass,
    pub resolved_service_class: ExecutionServiceClass,
    pub parent_class_ceiling: Option<ExecutionServiceClass>,
    pub demands: Vec<(ExecutionResourceKind, usize)>,
    pub normalized_scope: Option<String>,
    pub fairness_key: String,
    pub enqueue_sequence: Option<u64>,
    pub enqueued_at_ms: Option<u64>,
    pub observed_at_ms: u64,
    pub queue_age_ms: u64,
    pub wait_reason: Option<ResourceWaitReason>,
    pub blocker: Option<Uuid>,
    pub policy_revision: u64,
    pub pending: usize,
}

impl ResourceAdmissionObservation {
    #[allow(clippy::too_many_arguments)]
    fn from_request(
        request: &ResourceAdmissionRequest,
        status: ResourceAdmissionObservationStatus,
        resolved_service_class: ExecutionServiceClass,
        enqueue_sequence: Option<u64>,
        enqueued_at_ms: Option<u64>,
        observed_at_ms: u64,
        queue_age_ms: u64,
        wait_reason: Option<ResourceWaitReason>,
        blocker: Option<Uuid>,
        policy_revision: u64,
        pending: usize,
    ) -> Self {
        Self {
            request_id: request.request_id,
            status,
            requested_priority: request.requested_priority,
            deadline_at_ms: request.deadline_at_ms,
            requested_service_class: request.requested_service_class,
            resolved_service_class,
            parent_class_ceiling: request.parent_class_ceiling,
            demands: request.demands.clone(),
            normalized_scope: request.normalized_scope.clone(),
            fairness_key: request.fairness_key.clone(),
            enqueue_sequence,
            enqueued_at_ms,
            observed_at_ms,
            queue_age_ms,
            wait_reason,
            blocker,
            policy_revision,
            pending,
        }
    }

    fn from_receipt(
        receipt: &ResourceGrantReceipt,
        status: ResourceAdmissionObservationStatus,
        pending: usize,
    ) -> Self {
        Self {
            request_id: receipt.request_id,
            status,
            requested_priority: receipt.requested_priority,
            deadline_at_ms: receipt.deadline_at_ms,
            requested_service_class: receipt.requested_service_class,
            resolved_service_class: receipt.resolved_service_class,
            parent_class_ceiling: receipt.parent_class_ceiling,
            demands: receipt.demands.clone(),
            normalized_scope: receipt.normalized_scope.clone(),
            fairness_key: receipt.fairness_key.clone(),
            enqueue_sequence: Some(receipt.enqueue_sequence),
            enqueued_at_ms: Some(receipt.enqueued_at_ms),
            observed_at_ms: receipt.granted_at_ms,
            queue_age_ms: receipt.queue_age_ms,
            wait_reason: receipt.wait_reason,
            blocker: receipt.blocker,
            policy_revision: receipt.policy_revision,
            pending,
        }
    }
}

/// Terminal result of the single fair admission API.
// Keep the hot-path grant inline; boxing its receipt would add an allocation to every admission.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ResourceAdmissionDecision {
    Granted {
        lease: ExecutionResourceLease,
        receipt: ResourceGrantReceipt,
    },
    Deferred {
        request_id: Uuid,
        wait_reason: ResourceWaitReason,
        policy_revision: u64,
    },
    Overloaded {
        request_id: Uuid,
        wait_reason: ResourceWaitReason,
        policy_revision: u64,
        pending: usize,
    },
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
    #[error("invalid resource admission policy")]
    InvalidAdmissionPolicy,
    #[error("resource admission request id is already pending or active: {0}")]
    DuplicateRequest(Uuid),
    #[error("resource admission queue is overloaded: {reason:?}")]
    AdmissionOverloaded { reason: ResourceWaitReason },
    #[error("resource admission was deferred: {reason:?}")]
    AdmissionDeferred { reason: ResourceWaitReason },
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
    requested_priority: Option<u8>,
    deadline_at_ms: Option<u64>,
    requested_service_class: ExecutionServiceClass,
    resolved_service_class: ExecutionServiceClass,
    parent_class_ceiling: Option<ExecutionServiceClass>,
    normalized_scope: Option<String>,
    scope_feasible: bool,
    fairness_key: String,
    enqueue_sequence: u64,
    enqueued_at_ms: u64,
    enqueued_at: Instant,
    policy_revision: u64,
    last_wait_reason: Option<ResourceWaitReason>,
    last_blocker: Option<Uuid>,
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
    admission_policy: ExecutionAdmissionPolicy,
    next_enqueue_sequence: u64,
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
#[derive(Clone, Default)]
pub struct ExecutionResourceManager {
    shared: Arc<Shared>,
    admission_observer: Arc<OnceLock<ResourceAdmissionObserver>>,
}

impl std::fmt::Debug for ExecutionResourceManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionResourceManager")
            .field(
                "admission_observer_installed",
                &self.admission_observer.get().is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl ExecutionResourceManager {
    pub fn new(quotas: impl IntoIterator<Item = (ExecutionResourceKind, ResourceQuota)>) -> Self {
        Self::from_validated_policy(quotas, ExecutionAdmissionPolicy::default())
    }

    pub fn with_admission_policy(
        quotas: impl IntoIterator<Item = (ExecutionResourceKind, ResourceQuota)>,
        admission_policy: ExecutionAdmissionPolicy,
    ) -> Result<Self, ResourceAcquireError> {
        let admission_policy = admission_policy.validate()?;
        Ok(Self::from_validated_policy(quotas, admission_policy))
    }

    fn from_validated_policy(
        quotas: impl IntoIterator<Item = (ExecutionResourceKind, ResourceQuota)>,
        admission_policy: ExecutionAdmissionPolicy,
    ) -> Self {
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
                    admission_policy,
                    next_enqueue_sequence: 0,
                }),
                changed: Notify::new(),
            }),
            admission_observer: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn install_admission_observer(
        &self,
        observer: impl Fn(&ResourceAdmissionObservation) + Send + Sync + 'static,
    ) -> Result<(), &'static str> {
        self.admission_observer
            .set(Arc::new(observer))
            .map_err(|_| "resource admission observer is already installed")
    }

    fn observe_admission(&self, observation: ResourceAdmissionObservation) {
        if let Some(observer) = self.admission_observer.get() {
            observer(&observation);
        }
    }

    fn pending_count(&self) -> usize {
        self.shared
            .state
            .lock()
            .map_or(0, |guard| guard.waiters.len())
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
    /// Existing callers enter the canonical admission queue as foreground
    /// work. Typed callers should use [`Self::admit`] to provide service class,
    /// parent ceiling, deadline, scope evidence, and a fairness key.
    pub async fn acquire_bundle(
        &self,
        demands: impl IntoIterator<Item = (ExecutionResourceKind, usize)>,
        timeout: Option<Duration>,
    ) -> Result<ExecutionResourceLease, ResourceAcquireError> {
        let request = ResourceAdmissionRequest::new(ExecutionServiceClass::Foreground, demands);
        let kind = request.demands[0].0.clone();
        match self.admit_with_timeout(request, timeout).await? {
            ResourceAdmissionDecision::Granted { lease, .. } => Ok(lease),
            ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                Err(ResourceAcquireError::AdmissionOverloaded {
                    reason: wait_reason,
                })
            }
            ResourceAdmissionDecision::Deferred {
                wait_reason: ResourceWaitReason::DeadlineExpired,
                ..
            } => Err(ResourceAcquireError::TimedOut {
                kind,
                waited_ms: timeout.map_or(0, duration_millis),
            }),
            ResourceAdmissionDecision::Deferred { wait_reason, .. } => {
                Err(ResourceAcquireError::AdmissionDeferred {
                    reason: wait_reason,
                })
            }
        }
    }

    /// The sole fair admission API. It resolves the child ceiling, registers a
    /// bounded waiter, and grants the complete resource bundle atomically.
    pub async fn admit(
        &self,
        request: ResourceAdmissionRequest,
    ) -> Result<ResourceAdmissionDecision, ResourceAcquireError> {
        self.admit_with_timeout(request, None).await
    }

    /// Wait for one admission-state transition without exposing queue
    /// ownership. Durable graph pumps use this only after typed overload; the
    /// bounded fallback closes the `notify_waiters` registration race.
    pub(crate) async fn wait_for_change(&self) {
        let notified = self.shared.changed.notified();
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }

    async fn admit_with_timeout(
        &self,
        mut request: ResourceAdmissionRequest,
        timeout: Option<Duration>,
    ) -> Result<ResourceAdmissionDecision, ResourceAcquireError> {
        request.demands = normalize_demands(request.demands);
        if request.fairness_key.trim().is_empty() {
            request.fairness_key = default_fairness_key(&request.demands);
        }
        let resolved_service_class = request.resolved_service_class();
        let started = Instant::now();
        let registration = {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ResourceAcquireError::Poisoned)?;
            validate_request_resources(&guard, &request)?;
            if request
                .deadline_at_ms
                .is_some_and(|deadline| deadline <= now_ms())
            {
                AdmissionRegistration::Deferred {
                    wait_reason: ResourceWaitReason::DeadlineExpired,
                    policy_revision: guard.admission_policy.revision,
                    pending: guard.waiters.len(),
                }
            } else if !request.scope_feasible {
                AdmissionRegistration::Deferred {
                    wait_reason: ResourceWaitReason::ScopeInfeasible,
                    policy_revision: guard.admission_policy.revision,
                    pending: guard.waiters.len(),
                }
            } else if request_id_exists(&guard, request.request_id) {
                return Err(ResourceAcquireError::DuplicateRequest(request.request_id));
            } else if let Some(wait_reason) =
                pending_limit_reason(&guard, resolved_service_class, &request.fairness_key)
            {
                AdmissionRegistration::Overloaded {
                    wait_reason,
                    policy_revision: guard.admission_policy.revision,
                    pending: guard.waiters.len(),
                }
            } else {
                let enqueue_sequence = guard.next_enqueue_sequence;
                guard.next_enqueue_sequence = guard.next_enqueue_sequence.wrapping_add(1);
                let enqueued_at_ms = now_ms();
                let policy_revision = guard.admission_policy.revision;
                guard.waiters.push_back(PendingResourceDemand {
                    id: request.request_id,
                    demands: request.demands.clone(),
                    requested_priority: request.requested_priority,
                    deadline_at_ms: request.deadline_at_ms,
                    requested_service_class: request.requested_service_class,
                    resolved_service_class,
                    parent_class_ceiling: request.parent_class_ceiling,
                    normalized_scope: request.normalized_scope.clone(),
                    scope_feasible: request.scope_feasible,
                    fairness_key: request.fairness_key.clone(),
                    enqueue_sequence,
                    enqueued_at_ms,
                    enqueued_at: started,
                    policy_revision,
                    last_wait_reason: None,
                    last_blocker: None,
                });
                AdmissionRegistration::Queued {
                    enqueue_sequence,
                    enqueued_at_ms,
                    policy_revision,
                    pending: guard.waiters.len(),
                }
            }
        };
        let (enqueue_sequence, enqueued_at_ms, policy_revision) = match registration {
            AdmissionRegistration::Queued {
                enqueue_sequence,
                enqueued_at_ms,
                policy_revision,
                pending,
            } => {
                self.observe_admission(ResourceAdmissionObservation::from_request(
                    &request,
                    ResourceAdmissionObservationStatus::Queued,
                    resolved_service_class,
                    Some(enqueue_sequence),
                    Some(enqueued_at_ms),
                    enqueued_at_ms,
                    0,
                    None,
                    None,
                    policy_revision,
                    pending,
                ));
                (enqueue_sequence, enqueued_at_ms, policy_revision)
            }
            AdmissionRegistration::Deferred {
                wait_reason,
                policy_revision,
                pending,
            } => {
                self.observe_admission(ResourceAdmissionObservation::from_request(
                    &request,
                    ResourceAdmissionObservationStatus::Deferred,
                    resolved_service_class,
                    None,
                    None,
                    now_ms(),
                    0,
                    Some(wait_reason),
                    None,
                    policy_revision,
                    pending,
                ));
                return Ok(ResourceAdmissionDecision::Deferred {
                    request_id: request.request_id,
                    wait_reason,
                    policy_revision,
                });
            }
            AdmissionRegistration::Overloaded {
                wait_reason,
                policy_revision,
                pending,
            } => {
                self.observe_admission(ResourceAdmissionObservation::from_request(
                    &request,
                    ResourceAdmissionObservationStatus::Overloaded,
                    resolved_service_class,
                    None,
                    None,
                    now_ms(),
                    0,
                    Some(wait_reason),
                    None,
                    policy_revision,
                    pending,
                ));
                return Ok(ResourceAdmissionDecision::Overloaded {
                    request_id: request.request_id,
                    wait_reason,
                    policy_revision,
                    pending,
                });
            }
        };
        self.shared.changed.notify_waiters();

        let mut registration = WaiterRegistration {
            shared: Arc::clone(&self.shared),
            waiter_id: request.request_id,
            demands: request.demands.clone(),
            started,
            active: true,
        };

        loop {
            let notified = self.shared.changed.notified();
            let outcome = {
                let mut guard = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| ResourceAcquireError::Poisoned)?;
                evaluate_and_grant(&mut guard, request.request_id, Instant::now(), now_ms())?
            };

            match outcome {
                AdmissionAttempt::Granted { receipt } => {
                    registration.active = false;
                    self.shared.changed.notify_waiters();
                    self.observe_admission(ResourceAdmissionObservation::from_receipt(
                        &receipt,
                        ResourceAdmissionObservationStatus::Granted,
                        self.pending_count(),
                    ));
                    return Ok(ResourceAdmissionDecision::Granted {
                        lease: ExecutionResourceLease {
                            shared: Arc::clone(&self.shared),
                            demands: request.demands.clone(),
                            lease_id: request.request_id,
                            queue_wait: started.elapsed(),
                            released: false,
                        },
                        receipt,
                    });
                }
                AdmissionAttempt::Deferred {
                    wait_reason: ResourceWaitReason::DeadlineExpired,
                    blocker,
                    ..
                } => {
                    registration.finish(ResourceResultClass::TimedOut);
                    self.observe_admission(ResourceAdmissionObservation::from_request(
                        &request,
                        ResourceAdmissionObservationStatus::Deferred,
                        resolved_service_class,
                        Some(enqueue_sequence),
                        Some(enqueued_at_ms),
                        now_ms(),
                        duration_millis(started.elapsed()),
                        Some(ResourceWaitReason::DeadlineExpired),
                        blocker,
                        policy_revision,
                        self.pending_count(),
                    ));
                    return Ok(ResourceAdmissionDecision::Deferred {
                        request_id: request.request_id,
                        wait_reason: ResourceWaitReason::DeadlineExpired,
                        policy_revision,
                    });
                }
                AdmissionAttempt::Deferred {
                    wait_reason,
                    blocker,
                    changed: true,
                } => {
                    self.observe_admission(ResourceAdmissionObservation::from_request(
                        &request,
                        ResourceAdmissionObservationStatus::Waiting,
                        resolved_service_class,
                        Some(enqueue_sequence),
                        Some(enqueued_at_ms),
                        now_ms(),
                        duration_millis(started.elapsed()),
                        Some(wait_reason),
                        blocker,
                        policy_revision,
                        self.pending_count(),
                    ));
                }
                AdmissionAttempt::Deferred { .. } => {}
            }

            let recheck_after = {
                let guard = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| ResourceAcquireError::Poisoned)?;
                scheduler_recheck_after(&guard, request.request_id, Instant::now(), now_ms())
            };
            let remaining_timeout = timeout.and_then(|limit| limit.checked_sub(started.elapsed()));
            if timeout.is_some() && remaining_timeout.is_none() {
                registration.finish(ResourceResultClass::TimedOut);
                self.observe_admission(ResourceAdmissionObservation::from_request(
                    &request,
                    ResourceAdmissionObservationStatus::Deferred,
                    resolved_service_class,
                    Some(enqueue_sequence),
                    Some(enqueued_at_ms),
                    now_ms(),
                    duration_millis(started.elapsed()),
                    Some(ResourceWaitReason::DeadlineExpired),
                    None,
                    policy_revision,
                    self.pending_count(),
                ));
                return Ok(ResourceAdmissionDecision::Deferred {
                    request_id: request.request_id,
                    wait_reason: ResourceWaitReason::DeadlineExpired,
                    policy_revision,
                });
            }
            let sleep_for = match (recheck_after, remaining_timeout) {
                (Some(recheck), Some(remaining)) => Some(recheck.min(remaining)),
                (Some(recheck), None) => Some(recheck),
                (None, Some(remaining)) => Some(remaining),
                (None, None) => None,
            };
            if let Some(sleep_for) = sleep_for {
                tokio::select! {
                    () = notified => {}
                    () = tokio::time::sleep(sleep_for) => {}
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

#[derive(Debug)]
enum AdmissionAttempt {
    Granted {
        receipt: ResourceGrantReceipt,
    },
    Deferred {
        wait_reason: ResourceWaitReason,
        blocker: Option<Uuid>,
        changed: bool,
    },
}

#[derive(Clone, Copy, Debug)]
enum AdmissionRegistration {
    Queued {
        enqueue_sequence: u64,
        enqueued_at_ms: u64,
        policy_revision: u64,
        pending: usize,
    },
    Deferred {
        wait_reason: ResourceWaitReason,
        policy_revision: u64,
        pending: usize,
    },
    Overloaded {
        wait_reason: ResourceWaitReason,
        policy_revision: u64,
        pending: usize,
    },
}

#[derive(Clone, Copy, Debug)]
enum AdmissionEvaluation {
    Grantable {
        wait_reason: Option<ResourceWaitReason>,
        blocker: Option<Uuid>,
    },
    Deferred {
        wait_reason: ResourceWaitReason,
        blocker: Option<Uuid>,
    },
}

fn validate_request_resources(
    state: &ManagerState,
    request: &ResourceAdmissionRequest,
) -> Result<(), ResourceAcquireError> {
    for (kind, weight) in &request.demands {
        let resource = state
            .resources
            .get(kind)
            .ok_or_else(|| ResourceAcquireError::UnknownResource(kind.clone()))?;
        if *weight > resource.quota.maximum {
            return Err(ResourceAcquireError::DemandExceedsQuota {
                kind: kind.clone(),
                demand: *weight,
                maximum: resource.quota.maximum,
            });
        }
    }
    Ok(())
}

fn request_id_exists(state: &ManagerState, request_id: Uuid) -> bool {
    state.waiters.iter().any(|waiter| waiter.id == request_id)
        || state
            .resources
            .values()
            .any(|resource| resource.active.contains_key(&request_id))
}

fn pending_limit_reason(
    state: &ManagerState,
    service_class: ExecutionServiceClass,
    fairness_key: &str,
) -> Option<ResourceWaitReason> {
    if state.waiters.len() >= state.admission_policy.max_pending_instance {
        return Some(ResourceWaitReason::InstancePendingLimit);
    }
    let class_pending = state
        .waiters
        .iter()
        .filter(|waiter| waiter.resolved_service_class == service_class)
        .count();
    if class_pending >= state.admission_policy.max_pending_per_class {
        return Some(ResourceWaitReason::ClassPendingLimit);
    }
    let key_pending = state
        .waiters
        .iter()
        .filter(|waiter| waiter.fairness_key == fairness_key)
        .count();
    (key_pending >= state.admission_policy.max_pending_per_key)
        .then_some(ResourceWaitReason::KeyPendingLimit)
}

fn evaluate_and_grant(
    state: &mut ManagerState,
    request_id: Uuid,
    now: Instant,
    wall_now_ms: u64,
) -> Result<AdmissionAttempt, ResourceAcquireError> {
    let position = state
        .waiters
        .iter()
        .position(|waiter| waiter.id == request_id)
        .ok_or(ResourceAcquireError::RegistrationLost)?;
    let evaluation = evaluate_waiter(state, position, now, wall_now_ms);
    match evaluation {
        AdmissionEvaluation::Grantable {
            wait_reason,
            blocker,
        } => {
            let waiter = state
                .waiters
                .remove(position)
                .ok_or(ResourceAcquireError::RegistrationLost)?;
            for (kind, weight) in &waiter.demands {
                let resource = state
                    .resources
                    .get_mut(kind)
                    .ok_or(ResourceAcquireError::RegistrationLost)?;
                resource
                    .active
                    .insert(waiter.id, ActiveResourceDemand { weight: *weight });
            }
            Ok(AdmissionAttempt::Granted {
                receipt: ResourceGrantReceipt {
                    request_id: waiter.id,
                    requested_priority: waiter.requested_priority,
                    deadline_at_ms: waiter.deadline_at_ms,
                    requested_service_class: waiter.requested_service_class,
                    resolved_service_class: waiter.resolved_service_class,
                    parent_class_ceiling: waiter.parent_class_ceiling,
                    demands: waiter.demands,
                    normalized_scope: waiter.normalized_scope,
                    fairness_key: waiter.fairness_key,
                    enqueue_sequence: waiter.enqueue_sequence,
                    enqueued_at_ms: waiter.enqueued_at_ms,
                    granted_at_ms: wall_now_ms,
                    queue_age_ms: duration_millis(
                        now.saturating_duration_since(waiter.enqueued_at),
                    ),
                    wait_reason,
                    blocker,
                    policy_revision: waiter.policy_revision,
                },
            })
        }
        AdmissionEvaluation::Deferred {
            wait_reason,
            blocker,
        } => {
            let waiter = state
                .waiters
                .get_mut(position)
                .ok_or(ResourceAcquireError::RegistrationLost)?;
            let changed =
                waiter.last_wait_reason != Some(wait_reason) || waiter.last_blocker != blocker;
            waiter.last_wait_reason = Some(wait_reason);
            waiter.last_blocker = blocker;
            Ok(AdmissionAttempt::Deferred {
                wait_reason,
                blocker,
                changed,
            })
        }
    }
}

fn evaluate_waiter(
    state: &ManagerState,
    position: usize,
    now: Instant,
    wall_now_ms: u64,
) -> AdmissionEvaluation {
    let waiter = &state.waiters[position];
    if waiter
        .deadline_at_ms
        .is_some_and(|deadline| deadline <= wall_now_ms)
    {
        return AdmissionEvaluation::Deferred {
            wait_reason: ResourceWaitReason::DeadlineExpired,
            blocker: None,
        };
    }
    if !waiter.scope_feasible {
        return AdmissionEvaluation::Deferred {
            wait_reason: ResourceWaitReason::ScopeInfeasible,
            blocker: None,
        };
    }
    if let Some(blocker) = same_class_fifo_blocker(state, position) {
        return AdmissionEvaluation::Deferred {
            wait_reason: ResourceWaitReason::ClassFifo,
            blocker: Some(blocker),
        };
    }
    if let Some(blocker) = capacity_blocker(state, &waiter.demands) {
        return AdmissionEvaluation::Deferred {
            wait_reason: ResourceWaitReason::Capacity,
            blocker,
        };
    }

    let precedence = scheduling_precedence(waiter, now, state.admission_policy.aging_interval);
    let mut first_demand_owner_by_class =
        std::array::from_fn::<_, 4, _>(|_| HashMap::<ExecutionResourceKind, Uuid>::new());
    let mut higher = None::<((usize, u64), Uuid)>;
    for (other_position, other) in state.waiters.iter().enumerate() {
        let class_demands =
            &mut first_demand_owner_by_class[service_class_index(other.resolved_service_class)];
        let fifo_blocked = other
            .demands
            .iter()
            .find_map(|(kind, _)| class_demands.get(kind).copied())
            .is_some();
        if other_position != position
            && other.resolved_service_class != waiter.resolved_service_class
            && demand_sets_overlap(&waiter.demands, &other.demands)
            && other.scope_feasible
            && other
                .deadline_at_ms
                .is_none_or(|deadline| deadline > wall_now_ms)
            && !fifo_blocked
            && capacity_blocker(state, &other.demands).is_none()
        {
            let other_precedence =
                scheduling_precedence(other, now, state.admission_policy.aging_interval);
            if other_precedence < precedence
                && higher.is_none_or(|(selected, _)| other_precedence < selected)
            {
                higher = Some((other_precedence, other.id));
            }
        }
        for (kind, _) in &other.demands {
            class_demands.entry(kind.clone()).or_insert(other.id);
        }
    }
    let higher = higher.map(|(_, request_id)| request_id);
    if let Some(blocker) = higher {
        return AdmissionEvaluation::Deferred {
            wait_reason: ResourceWaitReason::HigherServiceClass,
            blocker: Some(blocker),
        };
    }

    AdmissionEvaluation::Grantable {
        wait_reason: waiter.last_wait_reason,
        blocker: waiter.last_blocker,
    }
}

fn same_class_fifo_blocker(state: &ManagerState, position: usize) -> Option<Uuid> {
    let waiter = &state.waiters[position];
    state
        .waiters
        .iter()
        .take(position)
        .find(|earlier| {
            earlier.resolved_service_class == waiter.resolved_service_class
                && demand_sets_overlap(&earlier.demands, &waiter.demands)
        })
        .map(|earlier| earlier.id)
}

fn capacity_blocker(
    state: &ManagerState,
    demands: &[(ExecutionResourceKind, usize)],
) -> Option<Option<Uuid>> {
    demands.iter().find_map(|(kind, weight)| {
        let resource = state.resources.get(kind)?;
        (active_weight(resource).saturating_add(*weight) > resource.effective_limit)
            .then(|| resource.active.keys().copied().min())
    })
}

fn scheduling_precedence(
    waiter: &PendingResourceDemand,
    now: Instant,
    aging_interval: Duration,
) -> (usize, u64) {
    let age_steps = duration_millis(now.saturating_duration_since(waiter.enqueued_at))
        / duration_millis(aging_interval).max(1);
    (
        service_class_index(waiter.resolved_service_class).saturating_sub(age_steps as usize),
        waiter.enqueue_sequence,
    )
}

fn scheduler_recheck_after(
    state: &ManagerState,
    request_id: Uuid,
    now: Instant,
    wall_now_ms: u64,
) -> Option<Duration> {
    let aging_interval = state.admission_policy.aging_interval;
    let mut next = state
        .waiters
        .iter()
        .filter_map(|waiter| {
            let rank = service_class_index(waiter.resolved_service_class);
            if rank == 0 {
                return None;
            }
            let age = now.saturating_duration_since(waiter.enqueued_at);
            let elapsed_steps = duration_millis(age) / duration_millis(aging_interval).max(1);
            if elapsed_steps >= rank as u64 {
                return None;
            }
            let next_boundary = aging_interval.saturating_mul((elapsed_steps + 1) as u32);
            Some(next_boundary.saturating_sub(age))
        })
        .min();
    if let Some(deadline) = state
        .waiters
        .iter()
        .find(|waiter| waiter.id == request_id)
        .and_then(|waiter| waiter.deadline_at_ms)
    {
        let until_deadline = Duration::from_millis(deadline.saturating_sub(wall_now_ms));
        next = Some(next.map_or(until_deadline, |current| current.min(until_deadline)));
    }
    next
}

#[derive(Debug)]
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

fn default_fairness_key(demands: &[(ExecutionResourceKind, usize)]) -> String {
    let kinds = demands
        .iter()
        .map(|(kind, _)| format!("{kind:?}"))
        .collect::<Vec<_>>()
        .join("+");
    format!("resource:{kinds}")
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

    fn admission_policy(
        max_pending_instance: usize,
        max_pending_per_class: usize,
        max_pending_per_key: usize,
    ) -> ExecutionAdmissionPolicy {
        ExecutionAdmissionPolicy {
            revision: 7,
            max_pending_instance,
            max_pending_per_class,
            max_pending_per_key,
            aging_interval: Duration::from_millis(10),
        }
    }

    fn fair_manager(limit: usize, policy: ExecutionAdmissionPolicy) -> ExecutionResourceManager {
        ExecutionResourceManager::with_admission_policy(
            [
                (
                    ExecutionResourceKind::Tool,
                    ResourceQuota::new(1, limit, limit).unwrap(),
                ),
                (
                    ExecutionResourceKind::Provider,
                    ResourceQuota::new(1, limit, limit).unwrap(),
                ),
            ],
            policy,
        )
        .unwrap()
    }

    async fn wait_for_pending(
        manager: &ExecutionResourceManager,
        kind: &ExecutionResourceKind,
        expected: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.snapshot(kind).unwrap().queued_waiters == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiter must register");
    }

    fn granted(
        decision: ResourceAdmissionDecision,
    ) -> (ExecutionResourceLease, ResourceGrantReceipt) {
        match decision {
            ResourceAdmissionDecision::Granted { lease, receipt } => (lease, receipt),
            decision => panic!("expected grant, got {decision:?}"),
        }
    }

    #[test]
    fn service_classes_are_fixed_and_child_ceiling_cannot_promote() {
        let classes = [
            ExecutionServiceClass::Interactive,
            ExecutionServiceClass::Foreground,
            ExecutionServiceClass::Background,
            ExecutionServiceClass::Maintenance,
        ];
        assert_eq!(classes.len(), 4);
        assert_eq!(
            ExecutionServiceClass::Interactive.bounded_by(Some(ExecutionServiceClass::Background)),
            ExecutionServiceClass::Background
        );
        assert_eq!(
            ExecutionServiceClass::Maintenance.bounded_by(Some(ExecutionServiceClass::Foreground)),
            ExecutionServiceClass::Maintenance
        );
    }

    #[test]
    fn invalid_pending_bounds_fail_closed() {
        let invalid = admission_policy(2, 3, 1);
        assert_eq!(
            ExecutionResourceManager::with_admission_policy([], invalid).unwrap_err(),
            ResourceAcquireError::InvalidAdmissionPolicy
        );
    }

    #[tokio::test]
    async fn grant_receipt_records_resolved_class_wait_and_policy_evidence() {
        let manager = fair_manager(1, admission_policy(8, 4, 2));
        let occupied = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let observations = Arc::new(Mutex::new(Vec::<ResourceAdmissionObservation>::new()));
        let observed = Arc::clone(&observations);
        manager
            .install_admission_observer(move |observation| {
                observed.lock().unwrap().push(observation.clone());
            })
            .unwrap();
        let request = ResourceAdmissionRequest::new(
            ExecutionServiceClass::Interactive,
            [(ExecutionResourceKind::Tool, 1)],
        )
        .with_parent_class_ceiling(ExecutionServiceClass::Background)
        .with_priority(91)
        .with_scope("workspace:/tmp/project", true)
        .with_fairness_key("session:receipt");
        let request_id = request.request_id;
        let waiter = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.admit(request).await.unwrap() })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
        drop(occupied);
        let (lease, receipt) = granted(waiter.await.unwrap());
        assert_eq!(receipt.request_id, request_id);
        assert_eq!(
            receipt.requested_service_class,
            ExecutionServiceClass::Interactive
        );
        assert_eq!(
            receipt.resolved_service_class,
            ExecutionServiceClass::Background
        );
        assert_eq!(receipt.requested_priority, Some(91));
        assert_eq!(receipt.policy_revision, 7);
        assert_eq!(receipt.wait_reason, Some(ResourceWaitReason::Capacity));
        assert!(receipt.blocker.is_some());
        assert_eq!(
            receipt.normalized_scope.as_deref(),
            Some("workspace:/tmp/project")
        );
        let observations = observations.lock().unwrap();
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.status)
                .collect::<Vec<_>>(),
            vec![
                ResourceAdmissionObservationStatus::Queued,
                ResourceAdmissionObservationStatus::Waiting,
                ResourceAdmissionObservationStatus::Granted,
            ]
        );
        assert!(observations
            .iter()
            .all(|observation| observation.request_id == request_id));
        assert_eq!(
            observations[1].wait_reason,
            Some(ResourceWaitReason::Capacity)
        );
        assert!(observations[1].blocker.is_some());
        drop(lease);
    }

    #[tokio::test]
    async fn same_class_contended_requests_are_stable_fifo() {
        let manager = fair_manager(1, admission_policy(8, 4, 4));
        let occupied = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(
                        ResourceAdmissionRequest::new(
                            ExecutionServiceClass::Foreground,
                            [(ExecutionResourceKind::Tool, 1)],
                        )
                        .with_fairness_key("session:first"),
                    )
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
        let second = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(
                        ResourceAdmissionRequest::new(
                            ExecutionServiceClass::Foreground,
                            [(ExecutionResourceKind::Tool, 1)],
                        )
                        .with_fairness_key("session:second"),
                    )
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 2).await;
        drop(occupied);
        let (first_lease, first_receipt) = granted(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .unwrap()
                .unwrap(),
        );
        assert!(!second.is_finished());
        assert_eq!(first_receipt.enqueue_sequence, 1);
        drop(first_lease);
        let (second_lease, second_receipt) = granted(second.await.unwrap());
        assert_eq!(second_receipt.enqueue_sequence, 2);
        assert!(second_receipt.wait_reason.is_some());
        drop(second_lease);
    }

    #[tokio::test]
    async fn bounded_aging_eventually_serves_old_background_before_new_interactive() {
        let manager = fair_manager(1, admission_policy(8, 4, 4));
        let occupied = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let background = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(
                        ResourceAdmissionRequest::new(
                            ExecutionServiceClass::Background,
                            [(ExecutionResourceKind::Tool, 1)],
                        )
                        .with_fairness_key("mission:old"),
                    )
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
        {
            let mut guard = manager.shared.state.lock().unwrap();
            guard.waiters[0].enqueued_at = Instant::now()
                .checked_sub(Duration::from_millis(30))
                .unwrap();
        }
        let interactive = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(
                        ResourceAdmissionRequest::new(
                            ExecutionServiceClass::Interactive,
                            [(ExecutionResourceKind::Tool, 1)],
                        )
                        .with_fairness_key("session:new"),
                    )
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 2).await;
        drop(occupied);
        let (background_lease, receipt) = granted(
            tokio::time::timeout(Duration::from_secs(1), background)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            receipt.resolved_service_class,
            ExecutionServiceClass::Background
        );
        assert!(!interactive.is_finished());
        drop(background_lease);
        let (interactive_lease, _) = granted(interactive.await.unwrap());
        drop(interactive_lease);
    }

    #[tokio::test]
    async fn infeasible_high_class_does_not_idle_disjoint_capacity() {
        let manager = fair_manager(1, admission_policy(8, 4, 4));
        let occupied = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let blocked_interactive = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(ResourceAdmissionRequest::new(
                        ExecutionServiceClass::Interactive,
                        [(ExecutionResourceKind::Tool, 1)],
                    ))
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
        let background = manager
            .admit(ResourceAdmissionRequest::new(
                ExecutionServiceClass::Background,
                [(ExecutionResourceKind::Provider, 1)],
            ))
            .await
            .unwrap();
        let (background_lease, _) = granted(background);
        assert!(!blocked_interactive.is_finished());
        drop(background_lease);
        drop(occupied);
        let (interactive_lease, _) = granted(blocked_interactive.await.unwrap());
        drop(interactive_lease);
    }

    #[tokio::test]
    async fn lower_class_borrows_capacity_when_high_class_bundle_is_not_feasible() {
        let manager = fair_manager(2, admission_policy(8, 4, 4));
        let occupied = manager
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let blocked_interactive = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .admit(ResourceAdmissionRequest::new(
                        ExecutionServiceClass::Interactive,
                        [(ExecutionResourceKind::Tool, 2)],
                    ))
                    .await
                    .unwrap()
            })
        };
        wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
        let background = manager
            .admit(ResourceAdmissionRequest::new(
                ExecutionServiceClass::Background,
                [(ExecutionResourceKind::Tool, 1)],
            ))
            .await
            .unwrap();
        let (background_lease, _) = granted(background);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .active_leases,
            2
        );
        assert!(!blocked_interactive.is_finished());
        drop(background_lease);
        drop(occupied);
        let (interactive_lease, _) = granted(blocked_interactive.await.unwrap());
        assert_eq!(interactive_lease.demands()[0].1, 2);
        drop(interactive_lease);
    }

    #[tokio::test]
    async fn scope_infeasibility_is_typed_and_never_enters_pending() {
        let manager = fair_manager(1, admission_policy(8, 4, 4));
        let decision = manager
            .admit(
                ResourceAdmissionRequest::new(
                    ExecutionServiceClass::Foreground,
                    [(ExecutionResourceKind::Tool, 1)],
                )
                .with_scope("workspace:/locked", false),
            )
            .await
            .unwrap();
        assert!(matches!(
            decision,
            ResourceAdmissionDecision::Deferred {
                wait_reason: ResourceWaitReason::ScopeInfeasible,
                ..
            }
        ));
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .queued_waiters,
            0
        );
    }

    #[tokio::test]
    async fn expired_deadline_is_typed_and_never_enters_pending() {
        let manager = fair_manager(1, admission_policy(8, 4, 4));
        let decision = manager
            .admit(
                ResourceAdmissionRequest::new(
                    ExecutionServiceClass::Foreground,
                    [(ExecutionResourceKind::Tool, 1)],
                )
                .with_deadline_at_ms(now_ms().saturating_sub(1)),
            )
            .await
            .unwrap();
        assert!(matches!(
            decision,
            ResourceAdmissionDecision::Deferred {
                wait_reason: ResourceWaitReason::DeadlineExpired,
                ..
            }
        ));
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap()
                .queued_waiters,
            0
        );
    }

    #[tokio::test]
    async fn pending_queue_is_bounded_by_instance_class_and_key() {
        async fn overload_reason(
            policy: ExecutionAdmissionPolicy,
            second_class: ExecutionServiceClass,
            second_key: &'static str,
        ) -> ResourceWaitReason {
            let manager = fair_manager(1, policy);
            let occupied = manager
                .acquire(ExecutionResourceKind::Tool, None)
                .await
                .unwrap();
            let first = {
                let manager = manager.clone();
                tokio::spawn(async move {
                    manager
                        .admit(
                            ResourceAdmissionRequest::new(
                                ExecutionServiceClass::Foreground,
                                [(ExecutionResourceKind::Tool, 1)],
                            )
                            .with_fairness_key("shared"),
                        )
                        .await
                })
            };
            wait_for_pending(&manager, &ExecutionResourceKind::Tool, 1).await;
            let decision = manager
                .admit(
                    ResourceAdmissionRequest::new(second_class, [(ExecutionResourceKind::Tool, 1)])
                        .with_fairness_key(second_key),
                )
                .await
                .unwrap();
            first.abort();
            let _ = first.await;
            drop(occupied);
            match decision {
                ResourceAdmissionDecision::Overloaded { wait_reason, .. } => wait_reason,
                decision => panic!("expected overload, got {decision:?}"),
            }
        }

        assert_eq!(
            overload_reason(
                admission_policy(1, 1, 1),
                ExecutionServiceClass::Background,
                "other"
            )
            .await,
            ResourceWaitReason::InstancePendingLimit
        );
        assert_eq!(
            overload_reason(
                admission_policy(2, 1, 1),
                ExecutionServiceClass::Foreground,
                "other"
            )
            .await,
            ResourceWaitReason::ClassPendingLimit
        );
        assert_eq!(
            overload_reason(
                admission_policy(2, 2, 1),
                ExecutionServiceClass::Background,
                "shared"
            )
            .await,
            ResourceWaitReason::KeyPendingLimit
        );
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
