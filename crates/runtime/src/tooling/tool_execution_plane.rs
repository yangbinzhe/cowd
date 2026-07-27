use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use harness_contract::tool::{ResourceAccess, ResourceDemand};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution_core::graph::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager, ExecutionServiceClass,
    ResourceAdmissionDecision, ResourceAdmissionRequest, ResourceObservation, ResourceResultClass,
    ScopeLockLease, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPlaneStats {
    pub active: usize,
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub timed_out_waiters: u64,
    pub panicked: u64,
}

#[derive(Debug, Error)]
pub enum ToolExecutionPlaneError {
    #[error("tool resource admission failed: {0}")]
    Admission(String),
    #[error("tool execution panicked")]
    Panicked,
    #[error("tool execution worker failed: {0}")]
    Worker(String),
    #[error("tool execution exceeded the waiter timeout of {0:?}; the started operation continues under its resource lease")]
    TimedOut(Duration),
}

#[derive(Debug, Default)]
struct PlaneCounters {
    active: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    timed_out_waiters: AtomicU64,
    panicked: AtomicU64,
}

/// Resource ownership retained by the governed executor until its durable
/// terminal receipt has been committed.
pub struct ToolExecutionAdmission {
    _scope_lease: ScopeLockLease,
    _resource_lease: ExecutionResourceLease,
    resources: Arc<ExecutionResourceManager>,
    queue_wait: Duration,
    service_started: Instant,
    result_class: ResourceResultClass,
}

impl ToolExecutionAdmission {
    fn new(
        scope_lease: ScopeLockLease,
        resource_lease: ExecutionResourceLease,
        resources: Arc<ExecutionResourceManager>,
        queue_wait: Duration,
    ) -> Self {
        Self {
            _scope_lease: scope_lease,
            _resource_lease: resource_lease,
            resources,
            queue_wait,
            service_started: Instant::now(),
            result_class: ResourceResultClass::Failed,
        }
    }

    fn set_result_class(&mut self, result_class: ResourceResultClass) {
        self.result_class = result_class;
    }
}

impl Drop for ToolExecutionAdmission {
    fn drop(&mut self) {
        let _ = self.resources.record_observation(
            &ExecutionResourceKind::Tool,
            ResourceObservation::terminal(
                self.queue_wait,
                self.service_started.elapsed(),
                self.result_class,
            ),
        );
    }
}

/// Runtime-owned bounded execution boundary for every synchronous Tool.
///
/// The generic Tool quota is acquired once per invocation. Semantic resource
/// conflicts are already compiled into the governed plan; `ResourceDemand`
/// travels with the operation so future process/network/custom quota adapters
/// can be added without changing Tool callers.
#[derive(Clone)]
pub struct ToolExecutionPlane {
    resources: Arc<ExecutionResourceManager>,
    scopes: Arc<ScopeLockManager>,
    counters: Arc<PlaneCounters>,
    supervisor: Arc<OnceLock<Weak<crate::RuntimeExecutionSupervisor>>>,
}

impl ToolExecutionPlane {
    #[must_use]
    pub fn new(resources: Arc<ExecutionResourceManager>, scopes: Arc<ScopeLockManager>) -> Self {
        Self {
            resources,
            scopes,
            counters: Arc::new(PlaneCounters::default()),
            supervisor: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn bind_supervisor(&self, supervisor: &Arc<crate::RuntimeExecutionSupervisor>) {
        let _ = self.supervisor.set(Arc::downgrade(supervisor));
    }

    pub async fn execute<T, F>(
        &self,
        demand: &ResourceDemand,
        timeout: Option<Duration>,
        operation: F,
    ) -> Result<T, ToolExecutionPlaneError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.execute_classified(
            demand,
            timeout,
            ExecutionServiceClass::Foreground,
            None,
            operation,
        )
        .await
    }

    pub async fn execute_classified<T, F>(
        &self,
        demand: &ResourceDemand,
        timeout: Option<Duration>,
        service_class: ExecutionServiceClass,
        parent_class_ceiling: Option<ExecutionServiceClass>,
        operation: F,
    ) -> Result<T, ToolExecutionPlaneError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (execution, admission) = self
            .execute_classified_retained(
                demand,
                timeout,
                service_class,
                parent_class_ceiling,
                operation,
            )
            .await;
        drop(admission);
        execution
    }

    /// Execute one synchronous Tool while retaining its scope and capacity
    /// leases for the caller. Governed callers must keep the returned
    /// admission alive through durable terminal commit.
    pub async fn execute_classified_retained<T, F>(
        &self,
        demand: &ResourceDemand,
        timeout: Option<Duration>,
        service_class: ExecutionServiceClass,
        parent_class_ceiling: Option<ExecutionServiceClass>,
        operation: F,
    ) -> (
        Result<T, ToolExecutionPlaneError>,
        Option<ToolExecutionAdmission>,
    )
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        let admission_started = Instant::now();
        let scope_requests = match scope_requests(demand) {
            Ok(requests) => requests,
            Err(error) => return (Err(error), None),
        };
        let normalized_scope = format!("{scope_requests:?}");
        let scope_lease = match self.scopes.acquire(scope_requests, timeout).await {
            Ok(lease) => lease,
            Err(error) => {
                return (
                    Err(ToolExecutionPlaneError::Admission(error.to_string())),
                    None,
                );
            }
        };
        let resource_demands = execution_resource_demands(demand);
        let mut request = ResourceAdmissionRequest::new(service_class, resource_demands)
            .with_scope(normalized_scope, true)
            .with_fairness_key(format!("tool:{service_class:?}"));
        if let Some(parent_class_ceiling) = parent_class_ceiling {
            request = request.with_parent_class_ceiling(parent_class_ceiling);
        }
        if let Some(remaining) = remaining_timeout(timeout, admission_started) {
            request = request
                .with_deadline_at_ms(wall_now_ms().saturating_add(duration_millis(remaining)));
        }
        let decision = match self.resources.admit(request).await {
            Ok(decision) => decision,
            Err(error) => {
                return (
                    Err(ToolExecutionPlaneError::Admission(error.to_string())),
                    None,
                );
            }
        };
        let lease = match decision {
            ResourceAdmissionDecision::Granted { lease, .. } => lease,
            ResourceAdmissionDecision::Deferred { wait_reason, .. }
            | ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                return (
                    Err(ToolExecutionPlaneError::Admission(format!(
                        "resource admission did not grant: {wait_reason:?}"
                    ))),
                    None,
                );
            }
        };
        let queue_wait = admission_started.elapsed();
        let mut admission = ToolExecutionAdmission::new(
            scope_lease,
            lease,
            Arc::clone(&self.resources),
            queue_wait,
        );
        self.counters.active.fetch_add(1, Ordering::AcqRel);
        let counters = Arc::clone(&self.counters);
        let mut worker = tokio::task::spawn_blocking(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(operation));
            counters.active.fetch_sub(1, Ordering::AcqRel);
            match result {
                Ok(value) => {
                    counters.completed.fetch_add(1, Ordering::Relaxed);
                    Ok(value)
                }
                Err(_) => {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                    counters.panicked.fetch_add(1, Ordering::Relaxed);
                    Err(ToolExecutionPlaneError::Panicked)
                }
            }
        });

        let mut soft_timed_out = false;
        let execution = if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, &mut worker).await {
                Ok(joined) => joined.unwrap_or_else(|error| {
                    Err(ToolExecutionPlaneError::Worker(error.to_string()))
                }),
                Err(_) => {
                    soft_timed_out = true;
                    self.counters
                        .timed_out_waiters
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        ?timeout,
                        "synchronous tool crossed its soft timeout; waiting for truthful completion"
                    );
                    worker.await.unwrap_or_else(|error| {
                        Err(ToolExecutionPlaneError::Worker(error.to_string()))
                    })
                }
            }
        } else {
            worker
                .await
                .unwrap_or_else(|error| Err(ToolExecutionPlaneError::Worker(error.to_string())))
        };
        let result_class = if soft_timed_out {
            ResourceResultClass::TimedOut
        } else {
            match &execution {
                Ok(_) => ResourceResultClass::Completed,
                Err(ToolExecutionPlaneError::TimedOut(_)) => ResourceResultClass::TimedOut,
                Err(ToolExecutionPlaneError::Panicked | ToolExecutionPlaneError::Worker(_)) => {
                    ResourceResultClass::Failed
                }
                Err(ToolExecutionPlaneError::Admission(_)) => {
                    ResourceResultClass::DownstreamOverload
                }
            }
        };
        admission.set_result_class(result_class);
        (execution, Some(admission))
    }

    #[must_use]
    pub fn stats(&self) -> ToolExecutionPlaneStats {
        ToolExecutionPlaneStats {
            active: self.counters.active.load(Ordering::Acquire),
            submitted: self.counters.submitted.load(Ordering::Relaxed),
            completed: self.counters.completed.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
            timed_out_waiters: self.counters.timed_out_waiters.load(Ordering::Relaxed),
            panicked: self.counters.panicked.load(Ordering::Relaxed),
        }
    }
}

fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn execution_resource_demands(demand: &ResourceDemand) -> Vec<(ExecutionResourceKind, usize)> {
    let mut demands = vec![(
        ExecutionResourceKind::Tool,
        demand.tool_slots.max(1) as usize,
    )];
    if demand.process_slots > 0 {
        demands.push((
            ExecutionResourceKind::Custom("tool.process".to_string()),
            demand.process_slots as usize,
        ));
    }
    if demand.network_slots > 0 {
        demands.push((
            ExecutionResourceKind::Custom("tool.network".to_string()),
            demand.network_slots as usize,
        ));
    }
    if demand.cpu_weight > 0 {
        demands.push((
            ExecutionResourceKind::Custom("tool.cpu".to_string()),
            demand.cpu_weight as usize,
        ));
    }
    if demand.memory_bytes > 0 {
        demands.push((
            ExecutionResourceKind::Custom("tool.memory_mib".to_string()),
            demand.memory_bytes.div_ceil(1024 * 1024) as usize,
        ));
    }
    demands
}

fn scope_requests(
    demand: &ResourceDemand,
) -> Result<Vec<ScopeLockRequest>, ToolExecutionPlaneError> {
    demand
        .scopes
        .iter()
        .map(|scope| {
            let resource = ScopedResource::resource("tool", scope.key.clone())
                .map_err(|error| ToolExecutionPlaneError::Admission(error.to_string()))?;
            Ok(ScopeLockRequest {
                scope: resource,
                mode: match scope.access {
                    ResourceAccess::Read => ScopeLockMode::Read,
                    ResourceAccess::Write => ScopeLockMode::Write,
                },
            })
        })
        .collect()
}

fn remaining_timeout(timeout: Option<Duration>, started: Instant) -> Option<Duration> {
    timeout.map(|timeout| timeout.saturating_sub(started.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_core::graph::ResourceQuota;
    use harness_contract::tool::ResourceScopeDemand;

    fn plane(limit: usize) -> ToolExecutionPlane {
        ToolExecutionPlane::new(
            Arc::new(ExecutionResourceManager::new([
                (
                    ExecutionResourceKind::Tool,
                    ResourceQuota::new(1, limit, limit).expect("quota"),
                ),
                (
                    ExecutionResourceKind::Custom("tool.cpu".to_string()),
                    ResourceQuota::new(1, limit.max(2), limit.max(2)).expect("quota"),
                ),
            ])),
            Arc::new(ScopeLockManager::new()),
        )
    }

    #[tokio::test]
    async fn panic_is_typed_and_releases_capacity() {
        let plane = plane(1);
        let error = plane
            .execute(&ResourceDemand::default(), None, || -> () {
                panic!("boom")
            })
            .await
            .expect_err("panic");
        assert!(matches!(error, ToolExecutionPlaneError::Panicked));
        assert_eq!(plane.stats().active, 0);
        assert_eq!(
            plane
                .execute(&ResourceDemand::default(), None, || 7)
                .await
                .expect("next execution"),
            7
        );
    }

    #[tokio::test]
    async fn soft_timeout_waits_for_truthful_completion() {
        let plane = plane(1);
        plane
            .execute(
                &ResourceDemand::default(),
                Some(Duration::from_millis(5)),
                || std::thread::sleep(Duration::from_millis(30)),
            )
            .await
            .expect("soft timeout must not invent a terminal result");
        assert_eq!(plane.stats().active, 0);
        assert_eq!(plane.stats().timed_out_waiters, 1);
    }

    #[tokio::test]
    async fn retained_admission_blocks_conflicts_until_terminal_owner_releases_it() {
        let plane = plane(2);
        let demand = ResourceDemand {
            scopes: vec![ResourceScopeDemand {
                key: "file:shared".to_string(),
                access: ResourceAccess::Write,
            }],
            ..ResourceDemand::default()
        };
        let (first, admission) = plane
            .execute_classified_retained(
                &demand,
                None,
                ExecutionServiceClass::Interactive,
                None,
                || 1,
            )
            .await;
        assert_eq!(first.expect("first execution"), 1);
        let admission = admission.expect("retained admission");

        let waiting = {
            let plane = plane.clone();
            let demand = demand.clone();
            tokio::spawn(async move { plane.execute(&demand, None, || 2).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished());

        drop(admission);
        assert_eq!(waiting.await.unwrap().unwrap(), 2);
    }

    #[tokio::test]
    async fn same_resource_write_is_serial_but_disjoint_writes_overlap() {
        let plane = plane(3);
        let demand = |key: &str| ResourceDemand {
            scopes: vec![ResourceScopeDemand {
                key: key.to_string(),
                access: ResourceAccess::Write,
            }],
            ..ResourceDemand::default()
        };
        let first = {
            let plane = plane.clone();
            let demand = demand("file:a");
            tokio::spawn(async move {
                plane
                    .execute(&demand, None, || {
                        std::thread::sleep(Duration::from_millis(40));
                        1
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        let blocked_same = {
            let plane = plane.clone();
            let demand = demand("file:a");
            tokio::spawn(async move { plane.execute(&demand, None, || 2).await })
        };
        let disjoint = {
            let plane = plane.clone();
            let demand = demand("file:b");
            tokio::spawn(async move { plane.execute(&demand, None, || 3).await })
        };
        assert_eq!(disjoint.await.unwrap().unwrap(), 3);
        assert!(!blocked_same.is_finished());
        assert_eq!(first.await.unwrap().unwrap(), 1);
        assert_eq!(blocked_same.await.unwrap().unwrap(), 2);
    }
}
