use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use runtime::{
    CapabilityManifest, ConnectorHealth, ConnectorRegistrySnapshot, CrossPlaneAction,
    CrossPlaneExecutionReceipt, ExternalResourceRef, MockDocsServiceConnector, PolicyDecisionKind,
    ProviderAccount, ServiceConnector, ServiceToolRequest, ServiceToolResult,
    SqliteResourceDirectory,
};
use serde::{Deserialize, Serialize};

use super::{channel_routes, cross_plane_routes, AppState};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/connectors/summary", get(connector_summary_handler))
        .route("/api/connectors/accounts", get(connector_accounts_handler))
        .route(
            "/api/connectors/capabilities",
            get(connector_capabilities_handler),
        )
        .route(
            "/api/connectors/resources",
            get(connector_resources_handler),
        )
        .route(
            "/api/connectors/services/mock.docs/tools",
            get(mock_docs_tools_handler),
        )
        .route(
            "/api/connectors/services/mock.docs/execute",
            axum::routing::post(mock_docs_execute_handler),
        )
}

#[derive(Debug, Deserialize)]
struct MockDocsExecuteRequest {
    actor_principal: String,
    tool_id: String,
    resource_id: String,
    title: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ConnectorResourceQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

pub(super) fn connector_snapshot(state: &AppState) -> ConnectorRegistrySnapshot {
    let platforms = channel_routes::configured_platforms(state.config.as_ref());
    let mut accounts = platforms
        .iter()
        .filter(|platform| platform.enabled || platform.configured)
        .map(account_from_platform)
        .collect::<Vec<_>>();
    let mcp_servers = configured_mcp_servers(state.config.as_ref());
    accounts.extend(mcp_servers.iter().map(account_from_mcp_server));
    let mut capabilities = runtime::default_capabilities();
    for platform in platforms {
        for operation in platform.capabilities {
            let capability = manifest_from_platform_capability(&platform.platform_type, operation);
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

fn account_from_platform(platform: &channel_routes::PlatformReadiness) -> ProviderAccount {
    let mut account = ProviderAccount::new(
        platform.platform_type.clone(),
        platform.name.clone(),
        auth_mode_for_platform(&platform.platform_type),
    );
    account.secret_refs = vec![format!("config://gateway/platforms/{}", platform.name)];
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
    if platform_type == "feishu" && operation == "doc_ops" {
        CapabilityManifest::service(platform_type, operation)
    } else {
        CapabilityManifest::channel(platform_type, operation)
    }
}

fn auth_mode_for_platform(platform_type: &str) -> &'static str {
    match platform_type {
        "feishu" | "wecom" => "app_secret",
        "wechat-ilink" | "wechat_ilink" | "wechat" => "qr_session",
        "email" => "smtp_imap",
        _ => "config",
    }
}

#[derive(Debug, Clone, Serialize)]
struct McpServerReadiness {
    name: String,
    transport: String,
    enabled: bool,
    status: &'static str,
    configured: bool,
    missing_required: Vec<String>,
    diagnostics: Vec<String>,
}

fn configured_mcp_servers(config: Option<&serde_json::Value>) -> Vec<McpServerReadiness> {
    let Some(servers) = config
        .and_then(|value| value.get("mcpServers"))
        .and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };

    let mut items = servers
        .iter()
        .map(|(name, value)| mcp_server_readiness_from_value(name, value))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.name.cmp(&right.name));
    items
}

fn mcp_server_readiness_from_value(name: &str, value: &serde_json::Value) -> McpServerReadiness {
    let transport = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| infer_mcp_transport(value).to_string());
    let enabled = value
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let missing_required = missing_mcp_required_fields(&transport, value);
    let configured = missing_required.is_empty();
    let status = if !enabled {
        "disabled"
    } else if configured {
        "ready"
    } else {
        "degraded"
    };
    let diagnostics = if configured {
        vec![
            "MCP server declared; live discovery is evaluated outside control-plane snapshot"
                .to_string(),
        ]
    } else {
        vec![format!(
            "missing required fields: {}",
            missing_required.join(", ")
        )]
    };
    McpServerReadiness {
        name: name.to_string(),
        transport,
        enabled,
        status,
        configured,
        missing_required,
        diagnostics,
    }
}

fn infer_mcp_transport(value: &serde_json::Value) -> &'static str {
    if value.get("command").is_some() {
        "stdio"
    } else if value.get("url").is_some() {
        "http"
    } else if value.get("name").is_some() {
        "sdk"
    } else {
        "unknown"
    }
}

fn missing_mcp_required_fields(transport: &str, value: &serde_json::Value) -> Vec<String> {
    let required: &[&str] = match transport {
        "stdio" => &["command"],
        "http" | "sse" | "ws" | "claudeai-proxy" => &["url"],
        "sdk" => &["name"],
        _ => &["type"],
    };
    required
        .iter()
        .filter(|field| !has_non_empty(value, field))
        .map(|field| (*field).to_string())
        .collect()
}

fn has_non_empty(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .is_some_and(|item| !item.is_empty())
}

fn account_from_mcp_server(server: &McpServerReadiness) -> ProviderAccount {
    let health = match server.status {
        "ready" => ConnectorHealth::ready(),
        "disabled" => ConnectorHealth::disabled("MCP server is disabled"),
        "degraded" => ConnectorHealth::degraded(format!(
            "missing required fields: {}",
            server.missing_required.join(", ")
        )),
        other => ConnectorHealth::degraded(format!("MCP server status is {other}")),
    };
    ProviderAccount::mcp_server(server.name.clone(), server.transport.clone(), health)
}

async fn connector_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    Json(serde_json::json!({
        "kind": "connector_summary",
        "summary": snapshot.summary(),
        "generated_at": snapshot.generated_at,
    }))
}

async fn connector_accounts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    let total = snapshot.accounts.len();
    Json(serde_json::json!({
        "kind": "connector_accounts",
        "accounts": snapshot.accounts,
        "total": total,
    }))
}

async fn connector_capabilities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    let total = snapshot.capabilities.len();
    Json(serde_json::json!({
        "kind": "connector_capabilities",
        "capabilities": snapshot.capabilities,
        "total": total,
    }))
}

async fn connector_resources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ConnectorResourceQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let (resources, error) = list_durable_resources(&state, limit, offset, query.q.as_deref());
    let total = resources.len();
    Json(serde_json::json!({
        "kind": "connector_resources",
        "status": if error.is_some() { "degraded" } else { "available" },
        "degraded_reason": error,
        "limit": limit,
        "offset": offset,
        "resources": resources,
        "total": total,
    }))
}

async fn mock_docs_tools_handler() -> impl IntoResponse {
    let connector = MockDocsServiceConnector::new();
    Json(serde_json::json!({
        "kind": "connector_service_tools",
        "service": connector.metadata(),
        "tools": connector.capabilities(),
    }))
}

async fn mock_docs_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MockDocsExecuteRequest>,
) -> impl IntoResponse {
    cross_plane_routes::ensure_cross_plane_loaded(&state);
    let mode = request.mode.as_deref().unwrap_or("dry_run");
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) =
            cross_plane_routes::cross_plane_control().find_execution_by_idempotency_key(key)
        {
            return Json(serde_json::json!({
                "kind": "connector_service_execution",
                "service": "mock.docs",
                "replayed": true,
                "receipt": receipt,
            }));
        }
    }

    let service_request = ServiceToolRequest {
        tool_id: request.tool_id.clone(),
        resource_id: request.resource_id,
        title: request.title,
        input: serde_json::json!({}),
    };
    let preview_resource = ExternalResourceRef::new(
        "mock.docs",
        "document",
        &service_request.resource_id,
        &service_request.title,
    );
    let mut action = CrossPlaneAction::new(request.actor_principal, request.tool_id);
    action.provider_account = Some("mock.docs".to_string());
    action.resource_ref = Some(preview_resource.reference.clone());

    let (action, decision) = cross_plane_routes::cross_plane_control()
        .decide_and_audit_with_action(action, chrono::Utc::now());
    cross_plane_routes::save_cross_plane_state(&state);

    let allowed = decision.decision == PolicyDecisionKind::Allow;
    let status = if mode == "commit" && allowed {
        "executed"
    } else if allowed {
        "dry_run"
    } else {
        "blocked"
    };
    let dispatch_status = if mode == "commit" && allowed {
        "service_mock_executed"
    } else {
        "not_dispatched"
    };
    let blockers = if allowed {
        Vec::new()
    } else {
        vec![format!("policy:{}", decision.reason)]
    };
    let receipt = CrossPlaneExecutionReceipt::new(
        idempotency_key,
        mode,
        status,
        dispatch_status,
        action,
        decision,
        blockers,
        None,
    );
    cross_plane_routes::cross_plane_control().record_execution(receipt.clone());
    cross_plane_routes::save_cross_plane_state(&state);
    let service_result = if mode == "commit" && allowed {
        MockDocsServiceConnector::new().execute_tool(service_request)
    } else {
        ServiceToolResult {
            status: status.to_string(),
            tool_id: receipt.action.requested_capability.clone(),
            resource: Some(preview_resource.clone()),
            output: serde_json::json!({
                "summary": format!("Mock docs service {} for {}", status, preview_resource.reference),
                "read_only": true,
            }),
        }
    };
    let mut resource_persisted = false;
    let mut resource_degraded_reason = None;
    if let Some(resource) = service_result.resource.clone() {
        match durable_resource_directory(&state).and_then(|directory| directory.upsert(&resource)) {
            Ok(_) => {
                resource_persisted = true;
            }
            Err(error) => {
                resource_degraded_reason = Some(format!("resource directory unavailable: {error}"));
            }
        }
    }

    Json(serde_json::json!({
        "kind": "connector_service_execution",
        "service": "mock.docs",
        "replayed": false,
        "result": service_result,
        "resource_persisted": resource_persisted,
        "resource_degraded_reason": resource_degraded_reason,
        "receipt": receipt,
    }))
}

fn list_durable_resources(
    state: &AppState,
    limit: usize,
    offset: usize,
    query: Option<&str>,
) -> (Vec<ExternalResourceRef>, Option<String>) {
    match durable_resource_directory(state).and_then(|directory| {
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }) {
        Ok(resources) => (resources, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    }
}

pub(super) fn durable_resource_directory(
    state: &AppState,
) -> rusqlite::Result<SqliteResourceDirectory> {
    let path = resource_directory_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                error.kind(),
                format!("failed to create resource directory parent: {error}"),
            )))
        })?;
    }
    SqliteResourceDirectory::open(path)
}

pub(super) fn resource_directory_path(workspace_root: &Path) -> PathBuf {
    workspace_root
        .join(".cowd")
        .join("resource-directory.sqlite")
}
