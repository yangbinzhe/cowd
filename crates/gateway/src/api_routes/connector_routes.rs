use std::sync::{Arc, OnceLock};

use axum::{
    extract::{Path, Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use connector::{
    builtin_service_connector_registry, builtin_source_adapter_manifests, default_capabilities,
    CapabilityManifest, ConnectorBulkhead, ConnectorBulkheadRejection, ConnectorHealth,
    ConnectorRegistrySnapshot, ExternalResourceRef, ProviderAccount, ServiceConnector,
    ServiceToolRequest, ServiceToolResult,
};
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::MemoryScope;
use serde::{Deserialize, Serialize};

use crate::services::GatewayMemoryManager;

use super::{message_connector_routes, AppState};

mod mcp;
mod resources;
mod tools;

use mcp::*;
use resources::*;
use tools::*;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/connectors/summary", get(connector_summary_handler))
        .route("/api/connectors/accounts", get(connector_accounts_handler))
        .route(
            "/api/connectors/capabilities",
            get(connector_capabilities_handler),
        )
        .route("/api/connectors/mcp/servers", get(mcp_servers_handler))
        .route(
            "/api/connectors/resources",
            get(connector_resources_handler),
        )
        .route("/api/connectors/sources", get(connector_sources_handler))
        .route(
            "/api/connectors/resources/revalidate",
            axum::routing::post(connector_resource_revalidate_handler),
        )
        .route(
            "/api/connectors/resources/promote-memory",
            axum::routing::post(connector_resource_promote_memory_handler),
        )
        .route("/api/connectors/services", get(connector_services_handler))
        .route(
            "/api/connectors/services/:service_id/tools",
            get(connector_service_tools_handler),
        )
        .route(
            "/api/connectors/services/:service_id/execute",
            axum::routing::post(connector_service_execute_handler),
        )
}

async fn connector_sources_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "connector.source_adapters",
        "adapters": builtin_source_adapter_manifests(),
    }))
}

const MAX_CONNECTOR_RESOURCE_PAGE: usize = 200;
const DEFAULT_CONNECTOR_RESOURCE_PAGE: usize = 100;

static CONNECTOR_SERVICE_BULKHEAD: OnceLock<ConnectorBulkhead> = OnceLock::new();

fn connector_service_bulkhead() -> &'static ConnectorBulkhead {
    CONNECTOR_SERVICE_BULKHEAD.get_or_init(ConnectorBulkhead::default_service_gate)
}

pub(super) fn connector_snapshot(state: &AppState) -> ConnectorRegistrySnapshot {
    let config = state.runtime_config_json_snapshot();
    let platforms = message_connector_routes::configured_platforms(config.as_ref());
    let mut accounts = platforms
        .iter()
        .filter(|platform| platform.enabled || platform.configured)
        .map(account_from_platform)
        .collect::<Vec<_>>();
    let mcp_servers = configured_mcp_servers(config.as_ref());
    accounts.extend(mcp_servers.iter().map(account_from_mcp_server));
    let service_registry = builtin_service_connector_registry();
    accounts.extend(
        service_registry
            .connector_refs()
            .into_iter()
            .map(account_from_service_connector),
    );
    let mut capabilities = base_connector_capabilities().to_vec();
    for platform in platforms {
        for operation in platform.capabilities {
            let capability = manifest_from_platform_capability(&platform.platform_type, &operation);
            if !capabilities
                .iter()
                .any(|item| item.capability_id == capability.capability_id)
            {
                capabilities.push(capability);
            }
        }
    }
    for server in &mcp_servers {
        let capability = CapabilityManifest::mcp_server(&server.name);
        if !capabilities
            .iter()
            .any(|item| item.capability_id == capability.capability_id)
        {
            capabilities.push(capability);
        }
    }
    accounts.sort_by(|left, right| {
        left.provider
            .cmp(&right.provider)
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    let (resources, resource_error) = list_durable_resources(state, 100, 0, None);
    let mut snapshot = ConnectorRegistrySnapshot::new(accounts, capabilities, resources);
    if let Some(error) = resource_error {
        snapshot.degraded = true;
        snapshot
            .degraded_reasons
            .push(format!("resource_directory:{error}"));
    }
    snapshot
}

fn base_connector_capabilities() -> &'static [CapabilityManifest] {
    static BASE_CONNECTOR_CAPABILITIES: OnceLock<Vec<CapabilityManifest>> = OnceLock::new();
    BASE_CONNECTOR_CAPABILITIES.get_or_init(|| {
        let mut capabilities = default_capabilities();
        capabilities.extend(builtin_service_connector_registry().capabilities());
        capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
        capabilities.dedup_by(|left, right| left.capability_id == right.capability_id);
        capabilities
    })
}

fn account_from_service_connector(connector: &dyn ServiceConnector) -> ProviderAccount {
    let metadata = connector.metadata();
    let mut account = ProviderAccount::new(
        metadata.provider.clone(),
        metadata.id.clone(),
        if metadata.read_only {
            "local_readonly"
        } else {
            "service"
        },
    );
    account.enabled_bindings = connector
        .capabilities()
        .into_iter()
        .map(|capability| capability.capability_id)
        .collect();
    account.health = ConnectorHealth::ready();
    account
}

fn account_from_platform(
    platform: &message_connector_routes::PlatformReadiness,
) -> ProviderAccount {
    let mut account = ProviderAccount::new(
        platform.platform_type.clone(),
        platform.name.clone(),
        auth_mode_for_platform(&platform.platform_type),
    );
    account.secret_refs = vec![format!("config://gateway/platforms/{}", platform.name)];
    account.scopes = platform.scopes.clone();
    account.enabled_bindings = platform
        .capabilities
        .iter()
        .map(|operation| {
            manifest_from_platform_capability(&platform.platform_type, operation).capability_id
        })
        .collect();
    account.health = match platform.status {
        "ready" => ConnectorHealth::ready(),
        "disabled" => ConnectorHealth::disabled("platform is disabled"),
        "degraded" => ConnectorHealth::degraded(format!(
            "missing required fields: {}",
            platform.missing_required.join(", ")
        )),
        other => ConnectorHealth::degraded(format!("platform status is {other}")),
    };
    account
}

fn manifest_from_platform_capability(platform_type: &str, operation: &str) -> CapabilityManifest {
    CapabilityManifest::channel(platform_type, operation)
}

fn auth_mode_for_platform(platform_type: &str) -> &'static str {
    match platform_type {
        "feishu" | "wecom" => "app_secret",
        "wechat-ilink" | "wechat_ilink" | "wechat" => "qr_session",
        "email" => "smtp_imap",
        _ => "config",
    }
}
