use std::sync::Arc;

use surface::{
    SurfaceActionRequest, SurfaceOperationResult, SurfaceRegistrySnapshot, SurfaceRuntimeSnapshot,
    SurfaceSendRequest, SurfaceSupervisorEvent,
};

use crate::surface_host::{
    SurfaceHost, SurfaceHostHealth, SurfaceResourceSummary, SurfaceRouteSummary, SurfaceStaticFile,
};

use super::{service_envelope, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct SurfaceService {
    label: &'static str,
    owner: &'static str,
    host: Arc<SurfaceHost>,
}

impl SurfaceService {
    pub(crate) fn new() -> Self {
        Self {
            label: "surface",
            owner: "0.9.380 Surface service boundary",
            host: Arc::new(SurfaceHost::default()),
        }
    }

    pub(crate) fn with_host(host: Arc<SurfaceHost>) -> Self {
        Self {
            host,
            ..Self::new()
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        self.label
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn is_runtime_available(&self) -> bool {
        true
    }

    pub(crate) fn snapshot(&self) -> SurfaceRegistrySnapshot {
        self.host.snapshot()
    }

    pub(crate) fn health(&self) -> SurfaceHostHealth {
        self.host.health()
    }

    pub(crate) fn runtime_snapshots(&self) -> Vec<SurfaceRuntimeSnapshot> {
        self.host.runtime_snapshots()
    }

    pub(crate) fn runtime_snapshot(&self, id: &str) -> Option<SurfaceRuntimeSnapshot> {
        self.host.runtime_snapshot(id)
    }

    pub(crate) fn has_surface(&self, id: &str) -> bool {
        self.host.get(id).is_some()
    }

    pub(crate) fn routes(&self, id: &str) -> Option<SurfaceRouteSummary> {
        self.host.routes(id)
    }

    pub(crate) fn resources(&self, id: &str) -> Option<SurfaceResourceSummary> {
        self.host.resources(id)
    }

    pub(crate) fn resolve_static(
        &self,
        id: &str,
        requested_path: &str,
    ) -> Result<Option<SurfaceStaticFile>, String> {
        self.host
            .resolve_static(id, requested_path)
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn send(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .send(request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn action(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .action(request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn callback(
        &self,
        surface: &str,
        path: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .callback(surface, path, method, payload)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn check_surface_health(
        &self,
        surface: &str,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .check_surface_health(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn events(&self, surface: &str) -> Vec<surface::SurfaceFrame> {
        self.host.events(surface).await
    }

    pub(crate) async fn supervisor_events(&self, surface: &str) -> Vec<SurfaceSupervisorEvent> {
        self.host.supervisor_events(surface).await
    }

    pub(crate) async fn start_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .start_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn stop_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .stop_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn restart_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .restart_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn repair_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .repair_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }
}
