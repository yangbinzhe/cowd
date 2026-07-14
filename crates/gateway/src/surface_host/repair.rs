use chrono::Utc;
use surface::{
    SurfaceDescriptor, SurfaceError, SurfaceFailureKind, SurfaceLifecycle, SurfaceRepairPolicy,
    SurfaceRuntimeError, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus, SurfaceSupervisorAction,
    SurfaceSupervisorEvent,
};

use super::SurfaceHost;

impl SurfaceHost {
    pub(super) async fn record_surface_failure(
        &self,
        surface: SurfaceDescriptor,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> SurfaceRuntimeSnapshot {
        let message = message.into();
        let policy = surface.health.repair.clone();
        let mut snapshot = self
            .runtime_snapshot(&surface.id)
            .unwrap_or_else(|| SurfaceRuntimeSnapshot::discovered(&surface.id, surface.lifecycle));
        snapshot.consecutive_failures = snapshot.consecutive_failures.saturating_add(1);
        snapshot.last_health_at = Some(Utc::now());
        snapshot.last_error = Some(SurfaceRuntimeError::new(kind, message.clone()));

        if surface.lifecycle != SurfaceLifecycle::Managed {
            snapshot.status = SurfaceRuntimeStatus::Unavailable;
            snapshot.active = false;
            snapshot.available_actions = vec![SurfaceSupervisorAction::HealthCheck];
            self.set_runtime(snapshot.clone()).await;
            return snapshot;
        }

        if snapshot.restart_count >= policy.restart_limit {
            snapshot.status = SurfaceRuntimeStatus::CircuitOpen;
            snapshot.active = false;
            snapshot.circuit_open = true;
            snapshot.next_retry_at = Some(
                Utc::now()
                    + chrono::Duration::milliseconds(policy.circuit_half_open_after_ms as i64),
            );
            snapshot.available_actions = managed_actions(true);
            self.managed.lock().await.remove(&surface.id);
            self.set_runtime(snapshot.clone()).await;
            self.push_ledger(SurfaceSupervisorEvent::error(
                &surface.id,
                SurfaceRuntimeStatus::CircuitOpen,
                SurfaceRuntimeError::new(kind, message),
            ))
            .await;
            return snapshot;
        }

        if snapshot.consecutive_failures >= policy.failure_threshold {
            snapshot.status = SurfaceRuntimeStatus::Restarting;
            snapshot.active = false;
            snapshot.restart_count = snapshot.restart_count.saturating_add(1);
            snapshot.next_retry_at =
                Some(Utc::now() + backoff_duration(&policy, snapshot.restart_count));
            snapshot.available_actions = managed_actions(false);
            self.managed.lock().await.remove(&surface.id);
        } else {
            snapshot.status = SurfaceRuntimeStatus::Degraded;
            snapshot.available_actions = managed_actions(false);
        }
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::error(
            &surface.id,
            snapshot.status,
            SurfaceRuntimeError::new(kind, message),
        ))
        .await;
        snapshot
    }
}

pub(super) fn managed_actions(circuit_open: bool) -> Vec<SurfaceSupervisorAction> {
    if circuit_open {
        return vec![
            SurfaceSupervisorAction::Repair,
            SurfaceSupervisorAction::HealthCheck,
            SurfaceSupervisorAction::ArchiveDeadLetters,
            SurfaceSupervisorAction::PurgeArchivedEvents,
        ];
    }
    vec![
        SurfaceSupervisorAction::Start,
        SurfaceSupervisorAction::Stop,
        SurfaceSupervisorAction::Restart,
        SurfaceSupervisorAction::Repair,
        SurfaceSupervisorAction::HealthCheck,
        SurfaceSupervisorAction::ArchiveDeadLetters,
        SurfaceSupervisorAction::PurgeArchivedEvents,
    ]
}

pub(super) fn backoff_duration(
    policy: &SurfaceRepairPolicy,
    restart_count: u32,
) -> chrono::Duration {
    let exponent = restart_count.saturating_sub(1).min(10);
    let multiplier = 2u64.saturating_pow(exponent);
    let millis = policy
        .backoff_initial_ms
        .saturating_mul(multiplier)
        .min(policy.backoff_max_ms);
    chrono::Duration::milliseconds(millis as i64)
}

pub(super) fn classify_surface_error(error: &SurfaceError) -> SurfaceFailureKind {
    match error {
        SurfaceError::InvalidManifest { .. } => SurfaceFailureKind::ManifestInvalid,
        SurfaceError::Unavailable(_) => SurfaceFailureKind::EntryMissing,
        SurfaceError::Invocation { reason, .. } if reason.contains("timed out") => {
            SurfaceFailureKind::HealthTimeout
        }
        SurfaceError::Invocation { reason, .. } if reason.contains("launch") => {
            SurfaceFailureKind::SpawnFailed
        }
        SurfaceError::FrameParse(_) => SurfaceFailureKind::ProtocolError,
        SurfaceError::ManifestIo { .. } | SurfaceError::ManifestJson { .. } => {
            SurfaceFailureKind::ManifestInvalid
        }
        SurfaceError::Invocation { .. } => SurfaceFailureKind::Unknown,
    }
}
