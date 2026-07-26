use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use harness_contract::tool::{ResourceAccess, ResourceDemand};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution_core::graph::{
    ExecutionResourceKind, ExecutionResourceManager, ResourceObservation, ResourceResultClass,
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
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

/// Runtime-owned bounded execution boundary for every synchronous Tool.
///
/// The generic Tool quota is acquired once per invocation. Semantic resource
/// conflicts are already compiled into the governed plan; `ResourceDemand`
/// travels with the operation so future process/network/custom quota adapters
/// can be added without changing Tool callers.
#[derive(Debug, Clone)]
pub struct ToolExecutionPlane {
    resources: Arc<ExecutionResourceManager>,
    scopes: Arc<ScopeLockManager>,
    counters: Arc<PlaneCounters>,
}

impl ToolExecutionPlane {
    #[must_use]
    pub fn new(resources: Arc<ExecutionResourceManager>, scopes: Arc<ScopeLockManager>) -> Self {
        Self {
            resources,
            scopes,
            counters: Arc::new(PlaneCounters::default()),
        }
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
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        let admission_started = Instant::now();
        let scope_requests = scope_requests(demand)?;
        let scope_lease = self
            .scopes
            .acquire(scope_requests, timeout)
            .await
            .map_err(|error| ToolExecutionPlaneError::Admission(error.to_string()))?;
        let resource_demands = execution_resource_demands(demand);
        let lease = self
            .resources
            .acquire_bundle(
                resource_demands,
                remaining_timeout(timeout, admission_started),
            )
            .await
            .map_err(|error| ToolExecutionPlaneError::Admission(error.to_string()))?;
        let queue_wait = admission_started.elapsed();
        self.counters.active.fetch_add(1, Ordering::AcqRel);
        let counters = Arc::clone(&self.counters);
        let service_started = Instant::now();
        let mut worker = tokio::task::spawn_blocking(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(operation));
            drop(scope_lease);
            drop(lease);
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

        let execution = if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, &mut worker).await {
                Ok(joined) => {
                    joined.map_err(|error| ToolExecutionPlaneError::Worker(error.to_string()))?
                }
                Err(_) => {
                    self.counters
                        .timed_out_waiters
                        .fetch_add(1, Ordering::Relaxed);
                    Err(ToolExecutionPlaneError::TimedOut(timeout))
                }
            }
        } else {
            worker
                .await
                .map_err(|error| ToolExecutionPlaneError::Worker(error.to_string()))?
        };
        let result_class = match &execution {
            Ok(_) => ResourceResultClass::Completed,
            Err(ToolExecutionPlaneError::TimedOut(_)) => ResourceResultClass::TimedOut,
            Err(ToolExecutionPlaneError::Panicked | ToolExecutionPlaneError::Worker(_)) => {
                ResourceResultClass::Failed
            }
            Err(ToolExecutionPlaneError::Admission(_)) => ResourceResultClass::DownstreamOverload,
        };
        let _ = self.resources.record_observation(
            &ExecutionResourceKind::Tool,
            ResourceObservation::terminal(queue_wait, service_started.elapsed(), result_class),
        );
        execution
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
    async fn timed_out_started_work_keeps_lease_until_real_completion() {
        let plane = plane(1);
        let error = plane
            .execute(
                &ResourceDemand::default(),
                Some(Duration::from_millis(5)),
                || std::thread::sleep(Duration::from_millis(30)),
            )
            .await
            .expect_err("waiter timeout");
        assert!(matches!(error, ToolExecutionPlaneError::TimedOut(_)));
        assert_eq!(plane.stats().active, 1);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(plane.stats().active, 0);
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
