use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use chrono::Utc;
use surface::{
    normalize_surface_id, SurfaceFailureKind, SurfaceFrame, SurfaceLifecycle, SurfaceRuntimeError,
    SurfaceRuntimeSnapshot, SurfaceRuntimeStatus, SurfaceSupervisorEvent,
};
use tokio::sync::Mutex as AsyncMutex;

use super::{managed_actions, SurfaceHost};

impl SurfaceHost {
    pub(crate) async fn events(&self, surface: &str) -> Vec<SurfaceFrame> {
        let surface = normalize_surface_id(surface);
        let process = self.managed.lock().await.get(&surface).cloned();
        let Some(process) = process else {
            return Vec::new();
        };
        let events = process.events.lock().await;
        events.iter().cloned().collect()
    }

    pub(crate) async fn supervisor_events(&self, surface: &str) -> Vec<SurfaceSupervisorEvent> {
        let surface = normalize_surface_id(surface);
        let ledger = self.ledger.lock().await;
        ledger
            .get(&surface)
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) async fn set_runtime(&self, snapshot: SurfaceRuntimeSnapshot) {
        self.runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(snapshot.surface.clone(), snapshot);
    }

    pub(super) async fn push_ledger(&self, event: SurfaceSupervisorEvent) {
        push_supervisor_event(&self.ledger, event).await;
    }

    pub(super) async fn mark_runtime_error(
        &self,
        surface: &str,
        status: SurfaceRuntimeStatus,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> SurfaceRuntimeSnapshot {
        let surface = normalize_surface_id(surface);
        let error = SurfaceRuntimeError::new(kind, message);
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = status;
        snapshot.active = false;
        snapshot.last_error = Some(error.clone());
        snapshot.available_actions = managed_actions(snapshot.circuit_open);
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::error(&surface, status, error))
            .await;
        snapshot
    }
}

pub(super) fn mark_surface_seen(
    runtime: &Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    surface: &str,
    pid: Option<u32>,
) {
    let mut runtime = runtime
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let snapshot = runtime
        .entry(surface.to_string())
        .or_insert_with(|| SurfaceRuntimeSnapshot::discovered(surface, SurfaceLifecycle::Managed));
    snapshot.active = true;
    snapshot.pid = pid;
    snapshot.last_seen_at = Some(Utc::now());
    if matches!(
        snapshot.status,
        SurfaceRuntimeStatus::Starting
            | SurfaceRuntimeStatus::Restarting
            | SurfaceRuntimeStatus::Discovered
            | SurfaceRuntimeStatus::Unavailable
    ) {
        snapshot.status = SurfaceRuntimeStatus::Ready;
    }
}

pub(super) async fn push_supervisor_event(
    ledger: &Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    event: SurfaceSupervisorEvent,
) {
    let mut ledger = ledger.lock().await;
    let events = ledger.entry(event.surface.clone()).or_default();
    events.push_back(event);
    while events.len() > 500 {
        events.pop_front();
    }
}
