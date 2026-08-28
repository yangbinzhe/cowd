//! Public Runtime port for admission and observation of one Session turn.
//!
//! Transport hosts must not coordinate Runtime scheduler resources directly.
//! This port keeps the resource kind, lease, and adaptive observation contract
//! inside Runtime while exposing only the lifecycle that a Session ingress
//! adapter needs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::execution_core::graph::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager, ResourceObservation,
    ResourceResultClass,
};

#[derive(Clone, Debug)]
pub struct SessionTurnAdmissionPort {
    manager: Arc<ExecutionResourceManager>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTurnOutcome {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    DownstreamOverload,
}

#[derive(Debug)]
pub struct SessionTurnAdmissionLease {
    manager: Arc<ExecutionResourceManager>,
    lease: Option<ExecutionResourceLease>,
    service_started: Option<Instant>,
}

impl SessionTurnAdmissionPort {
    pub(crate) fn new(manager: Arc<ExecutionResourceManager>) -> Self {
        Self { manager }
    }

    pub async fn acquire(&self) -> Result<SessionTurnAdmissionLease, String> {
        let lease = self
            .manager
            .acquire(ExecutionResourceKind::SessionTurn, None)
            .await
            .map_err(|error| format!("SessionTurn admission failed: {error}"))?;
        Ok(SessionTurnAdmissionLease {
            manager: Arc::clone(&self.manager),
            lease: Some(lease),
            service_started: None,
        })
    }
}

impl SessionTurnAdmissionLease {
    /// Marks the point where admitted work begins service. Queue time remains
    /// owned by the Runtime resource manager and is never reconstructed by a
    /// transport adapter.
    pub fn begin_service(&mut self) {
        self.service_started.get_or_insert_with(Instant::now);
    }

    pub fn finish(mut self, outcome: SessionTurnOutcome) -> Result<(), String> {
        self.record(outcome)
    }

    fn record(&mut self, outcome: SessionTurnOutcome) -> Result<(), String> {
        let Some(lease) = self.lease.take() else {
            return Ok(());
        };
        let queue_wait = lease.queue_wait();
        drop(lease);
        self.manager
            .record_observation(
                &ExecutionResourceKind::SessionTurn,
                ResourceObservation::terminal(
                    queue_wait,
                    self.service_started
                        .map_or(Duration::ZERO, |started| started.elapsed()),
                    result_class(outcome),
                ),
            )
            .map(|_| ())
            .map_err(|error| format!("SessionTurn observation failed: {error}"))
    }
}

impl Drop for SessionTurnAdmissionLease {
    fn drop(&mut self) {
        if self.lease.is_some() {
            let _ = self.record(SessionTurnOutcome::Cancelled);
        }
    }
}

const fn result_class(outcome: SessionTurnOutcome) -> ResourceResultClass {
    match outcome {
        SessionTurnOutcome::Completed => ResourceResultClass::Completed,
        SessionTurnOutcome::Failed => ResourceResultClass::Failed,
        SessionTurnOutcome::Cancelled => ResourceResultClass::Cancelled,
        SessionTurnOutcome::TimedOut => ResourceResultClass::TimedOut,
        SessionTurnOutcome::DownstreamOverload => ResourceResultClass::DownstreamOverload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_core::graph::ResourceQuota;

    fn port() -> (SessionTurnAdmissionPort, Arc<ExecutionResourceManager>) {
        let manager = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::SessionTurn,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]));
        (SessionTurnAdmissionPort::new(Arc::clone(&manager)), manager)
    }

    #[tokio::test]
    async fn completed_turn_releases_capacity_and_records_one_sample() {
        let (port, manager) = port();
        let mut lease = port.acquire().await.unwrap();
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::SessionTurn)
                .unwrap()
                .active_leases,
            1
        );
        lease.begin_service();
        lease.finish(SessionTurnOutcome::Completed).unwrap();
        let snapshot = manager
            .snapshot(&ExecutionResourceKind::SessionTurn)
            .unwrap();
        assert_eq!(snapshot.active_leases, 0);
        assert_eq!(snapshot.sample_count, 1);
        assert_eq!(snapshot.failure_rate_basis_points, Some(0));
    }

    #[tokio::test]
    async fn dropped_turn_is_fail_closed_as_cancelled() {
        let (port, manager) = port();
        drop(port.acquire().await.unwrap());
        let snapshot = manager
            .snapshot(&ExecutionResourceKind::SessionTurn)
            .unwrap();
        assert_eq!(snapshot.active_leases, 0);
        assert_eq!(snapshot.sample_count, 1);
        assert_eq!(snapshot.cancelled_rate_basis_points, Some(10_000));
    }
}
