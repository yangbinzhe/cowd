use std::sync::Arc;

use axum::{
    extract::State as AxumState,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use connector::builtin_source_adapter_manifests;
use serde::Serialize;
use surface::{EdgeDomain, SurfaceDescriptor, SurfaceRuntimeSnapshot};

use super::AppState;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/edges", get(edge_registry_handler))
        .route("/api/edges/health", get(edge_health_handler))
        .route("/api/edges/reload", post(edge_reload_handler))
        .route("/api/edges/surfaces", get(edge_surfaces_handler))
        .route("/api/edges/connectors", get(edge_connectors_handler))
        .route(
            "/api/edges/connectors/message",
            get(edge_message_connectors_handler),
        )
        .route(
            "/api/edges/connectors/source",
            get(edge_source_connectors_handler),
        )
}

async fn edge_reload_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let discovery = state.services.surface.reload_manifests().await;
    Json(serde_json::json!({
        "kind": "edge.reload",
        "status": if discovery.failures.is_empty() { "applied" } else { "attention" },
        "discovery": discovery,
        "registry": edge_registry_projection(&state),
    }))
}

#[derive(Debug, Clone, Serialize)]
struct EdgeRegistryProjection {
    kind: &'static str,
    health: EdgeHealthProjection,
    surfaces: Vec<EdgeSurfaceProjection>,
    message_connectors: Vec<EdgeSurfaceProjection>,
    source_connectors: Vec<EdgeSourceConnectorProjection>,
    automation_connectors: Vec<EdgeSurfaceProjection>,
}

#[derive(Debug, Clone, Serialize)]
struct EdgeHealthProjection {
    status: String,
    surface_count: usize,
    message_connector_count: usize,
    source_connector_count: usize,
    automation_connector_count: usize,
    ready_count: usize,
    degraded_count: usize,
    failed_count: usize,
    circuit_open_count: usize,
    roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct EdgeSurfaceProjection {
    id: String,
    name: String,
    version: String,
    domain: EdgeDomain,
    kind: surface::SurfaceKind,
    lifecycle: surface::SurfaceLifecycle,
    status: surface::SurfaceStatus,
    runtime: Option<SurfaceRuntimeSnapshot>,
    runtime_spec: Option<surface::SurfaceRuntimeSpec>,
    transport: Option<surface::SurfaceTransport>,
    capability_count: usize,
    capabilities: Vec<String>,
    route_count: usize,
    resource_count: usize,
    routes: Vec<surface::SurfaceRoute>,
    resources: Vec<surface::SurfaceResource>,
    source: String,
    entry: Option<String>,
    diagnostics: Vec<String>,
    message_descriptor: Option<surface::message::MessageConnectorDescriptor>,
}

#[derive(Debug, Clone, Serialize)]
struct EdgeSourceConnectorProjection {
    id: String,
    name: String,
    domain: EdgeDomain,
    adapter_id: Option<String>,
    family: String,
    access_mode: String,
    refresh_modes: Vec<String>,
    supports_schema_discovery: bool,
    supports_snapshot: bool,
    supports_incremental: bool,
    supports_event_subscription: bool,
    requires_sidecar: bool,
    config_schema_ref: Option<String>,
    notes: Vec<String>,
    runtime: Option<SurfaceRuntimeSnapshot>,
}

async fn edge_registry_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(edge_registry_projection(&state))
}

async fn edge_health_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "edge.health",
        "health": edge_registry_projection(&state).health,
    }))
}

async fn edge_surfaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let registry = edge_registry_projection(&state);
    Json(serde_json::json!({
        "kind": "edge.surfaces",
        "surfaces": registry.surfaces,
    }))
}

async fn edge_connectors_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let registry = edge_registry_projection(&state);
    Json(serde_json::json!({
        "kind": "edge.connectors",
        "message_connectors": registry.message_connectors,
        "source_connectors": registry.source_connectors,
        "automation_connectors": registry.automation_connectors,
    }))
}

async fn edge_message_connectors_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let registry = edge_registry_projection(&state);
    Json(serde_json::json!({
        "kind": "edge.connectors.message",
        "connectors": registry.message_connectors,
    }))
}

async fn edge_source_connectors_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let registry = edge_registry_projection(&state);
    Json(serde_json::json!({
        "kind": "edge.connectors.source",
        "connectors": registry.source_connectors,
    }))
}

fn edge_registry_projection(state: &AppState) -> EdgeRegistryProjection {
    let surface_snapshot = state.services.surface.snapshot();
    let runtime = state.services.surface.runtime_snapshots();
    let host_health = state.services.surface.health();
    let mut surfaces = Vec::new();
    let mut message_connectors = Vec::new();
    let mut source_connectors = source_connector_projections(&runtime);
    let mut automation_connectors = Vec::new();

    for descriptor in surface_snapshot.surfaces {
        let projection = edge_surface_projection(&descriptor, &runtime);
        match projection.domain {
            EdgeDomain::Surface => surfaces.push(projection),
            EdgeDomain::MessageConnector => message_connectors.push(projection),
            EdgeDomain::AutomationConnector => automation_connectors.push(projection),
            EdgeDomain::SourceConnector => {
                source_connectors.push(edge_source_connector_from_surface(projection));
            }
        }
    }

    source_connectors.sort_by(|left, right| left.id.cmp(&right.id));
    EdgeRegistryProjection {
        kind: "edge.registry",
        health: EdgeHealthProjection {
            status: host_health.status,
            surface_count: surfaces.len(),
            message_connector_count: message_connectors.len(),
            source_connector_count: source_connectors.len(),
            automation_connector_count: automation_connectors.len(),
            ready_count: host_health.ready_count,
            degraded_count: host_health.degraded_count,
            failed_count: host_health.failed_count,
            circuit_open_count: host_health.circuit_open_count,
            roots: host_health.roots,
        },
        surfaces,
        message_connectors,
        source_connectors,
        automation_connectors,
    }
}

fn edge_surface_projection(
    descriptor: &SurfaceDescriptor,
    runtime: &[SurfaceRuntimeSnapshot],
) -> EdgeSurfaceProjection {
    let runtime_snapshot = runtime
        .iter()
        .find(|snapshot| snapshot.surface == descriptor.id)
        .cloned();
    EdgeSurfaceProjection {
        id: descriptor.id.clone(),
        name: descriptor.name.clone(),
        version: descriptor.version.clone(),
        domain: descriptor.edge_domain(),
        kind: descriptor.kind,
        lifecycle: descriptor.lifecycle,
        status: descriptor.status.clone(),
        runtime: runtime_snapshot.clone(),
        runtime_spec: descriptor.runtime.clone(),
        transport: descriptor.transport,
        capability_count: descriptor.capabilities.len(),
        capabilities: descriptor
            .capabilities
            .iter()
            .map(|capability| capability.capability.clone())
            .collect(),
        route_count: descriptor.routes.len(),
        resource_count: descriptor.resources.len(),
        routes: descriptor.routes.clone(),
        resources: descriptor.resources.clone(),
        source: descriptor.source.clone(),
        entry: descriptor.entry.clone(),
        diagnostics: descriptor.diagnostics.clone(),
        message_descriptor: message_connector_descriptor(descriptor, runtime_snapshot.as_ref()),
    }
}

fn message_connector_descriptor(
    descriptor: &SurfaceDescriptor,
    runtime: Option<&SurfaceRuntimeSnapshot>,
) -> Option<surface::message::MessageConnectorDescriptor> {
    if descriptor.edge_domain() != EdgeDomain::MessageConnector {
        return None;
    }
    let connector = descriptor
        .id
        .strip_prefix("message:")
        .unwrap_or(&descriptor.id);
    let status = runtime
        .map(|snapshot| surface_runtime_status(snapshot.status))
        .unwrap_or_else(|| surface_status(&descriptor.status));
    let mut message_descriptor =
        surface::message::MessageConnectorDescriptor::for_connector(connector, status);
    message_descriptor.reload_required = runtime
        .map(|snapshot| snapshot.circuit_open || !snapshot.available_actions.is_empty())
        .unwrap_or(false);
    message_descriptor
        .degraded_reasons
        .extend(descriptor.diagnostics.clone());
    if let Some(snapshot) = runtime {
        if let Some(error) = &snapshot.last_error {
            message_descriptor
                .degraded_reasons
                .push(error.message.clone());
        }
        if snapshot.circuit_open {
            message_descriptor
                .degraded_reasons
                .push("surface supervisor circuit is open".to_string());
        }
    }
    message_descriptor.degraded_reasons.sort();
    message_descriptor.degraded_reasons.dedup();
    Some(message_descriptor)
}

fn surface_runtime_status(status: surface::SurfaceRuntimeStatus) -> String {
    match status {
        surface::SurfaceRuntimeStatus::Builtin
        | surface::SurfaceRuntimeStatus::Discovered
        | surface::SurfaceRuntimeStatus::Ready => "ready",
        surface::SurfaceRuntimeStatus::Starting | surface::SurfaceRuntimeStatus::Restarting => {
            "starting"
        }
        surface::SurfaceRuntimeStatus::Disabled => "disabled",
        surface::SurfaceRuntimeStatus::CircuitOpen => "circuit_open",
        surface::SurfaceRuntimeStatus::Degraded
        | surface::SurfaceRuntimeStatus::Unavailable
        | surface::SurfaceRuntimeStatus::Failed => "degraded",
    }
    .to_string()
}

fn surface_status(status: &surface::SurfaceStatus) -> String {
    match status {
        surface::SurfaceStatus::Builtin
        | surface::SurfaceStatus::Discovered
        | surface::SurfaceStatus::Ready => "ready",
        surface::SurfaceStatus::Disabled => "disabled",
        surface::SurfaceStatus::Unavailable | surface::SurfaceStatus::Error => "degraded",
    }
    .to_string()
}

fn source_connector_projections(
    runtime: &[SurfaceRuntimeSnapshot],
) -> Vec<EdgeSourceConnectorProjection> {
    builtin_source_adapter_manifests()
        .into_iter()
        .map(|adapter| EdgeSourceConnectorProjection {
            id: format!("source:{}", adapter.adapter_id),
            name: adapter.display_name,
            domain: EdgeDomain::SourceConnector,
            adapter_id: Some(adapter.adapter_id.clone()),
            family: adapter.family,
            access_mode: adapter.access_mode,
            refresh_modes: adapter.refresh_modes,
            supports_schema_discovery: adapter.supports_schema_discovery,
            supports_snapshot: adapter.supports_snapshot,
            supports_incremental: adapter.supports_incremental,
            supports_event_subscription: adapter.supports_event_subscription,
            requires_sidecar: adapter.requires_sidecar,
            config_schema_ref: adapter.config_schema_ref,
            notes: adapter.notes,
            runtime: runtime
                .iter()
                .find(|snapshot| snapshot.surface == adapter.adapter_id)
                .cloned(),
        })
        .collect::<Vec<_>>()
}

fn edge_source_connector_from_surface(
    surface: EdgeSurfaceProjection,
) -> EdgeSourceConnectorProjection {
    EdgeSourceConnectorProjection {
        id: surface.id,
        name: surface.name,
        domain: EdgeDomain::SourceConnector,
        adapter_id: None,
        family: "source.sidecar".to_string(),
        access_mode: "sidecar".to_string(),
        refresh_modes: Vec::new(),
        supports_schema_discovery: surface
            .capabilities
            .iter()
            .any(|capability| capability == "source.schema_discovery"),
        supports_snapshot: surface
            .capabilities
            .iter()
            .any(|capability| capability == "source.snapshot"),
        supports_incremental: surface
            .capabilities
            .iter()
            .any(|capability| capability == "source.incremental"),
        supports_event_subscription: surface
            .capabilities
            .iter()
            .any(|capability| capability == "source.event"),
        requires_sidecar: true,
        config_schema_ref: None,
        notes: vec!["discovered from Edge source connector manifest".to_string()],
        runtime: surface.runtime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_routes::tests::test_state;

    #[test]
    fn edge_registry_projects_builtin_surfaces_and_source_connectors() {
        let state = test_state();
        let projection = edge_registry_projection(&state);

        assert_eq!(projection.kind, "edge.registry");
        assert!(projection
            .surfaces
            .iter()
            .any(|surface| surface.id == "tui"));
        assert!(projection
            .surfaces
            .iter()
            .any(|surface| surface.id == "webui"));
        assert!(projection
            .source_connectors
            .iter()
            .any(|connector| connector.adapter_id.as_deref() == Some("feishu_bitable")));
        assert!(projection
            .source_connectors
            .iter()
            .any(|connector| connector.adapter_id.as_deref() == Some("csv")
                && !connector.requires_sidecar));
    }

    #[test]
    fn surface_descriptor_projects_message_connector_descriptor() {
        let surface = SurfaceDescriptor {
            id: "message:feishu".to_string(),
            name: "Feishu".to_string(),
            version: "1.0.0".to_string(),
            kind: surface::SurfaceKind::MessageConnector,
            status: surface::SurfaceStatus::Ready,
            source: "test".to_string(),
            runtime: Some(surface::SurfaceRuntimeSpec::Managed {
                artifact: "cowd-edge-open-platform-message".to_string(),
                driver_profile: "feishu-message".to_string(),
                transport: surface::SurfaceTransport::UdsHttp2,
            }),
            entry: None,
            transport: Some(surface::SurfaceTransport::UdsHttp2),
            lifecycle: surface::SurfaceLifecycle::Managed,
            capabilities: vec![surface::SurfaceCapability::new(
                "message:feishu",
                "message.send.text",
            )],
            routes: Vec::new(),
            resources: Vec::new(),
            health: surface::SurfaceHealthSpec::default(),
            default_enabled: false,
            edge_domain: EdgeDomain::MessageConnector,
            diagnostics: Vec::new(),
        };
        let projection = edge_surface_projection(&surface, &[]);
        let descriptor = projection
            .message_descriptor
            .as_ref()
            .expect("message connector descriptor should be projected");

        assert_eq!(descriptor.descriptor_version, 1);
        assert!(descriptor.max_message_length > 0);
        assert!(descriptor
            .supported_actions
            .iter()
            .any(|action| action == "message.send.text"));
        assert!(!descriptor.markdown_dialect.is_empty());
        assert!(projection.entry.is_none());
        assert!(matches!(
            projection.runtime_spec,
            Some(surface::SurfaceRuntimeSpec::Managed { .. })
        ));
        assert_eq!(
            projection.transport,
            Some(surface::SurfaceTransport::UdsHttp2)
        );
    }
}
