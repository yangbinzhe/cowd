use std::sync::{Arc, OnceLock};

use axum::{
    extract::{Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::MemoryScope;
use runtime::{
    CapabilityManifest, ConnectorBulkhead, ConnectorBulkheadRejection, ConnectorHealth,
    ConnectorRegistrySnapshot, ExternalResourceRef, FeishuReadOnlyServiceConnector,
    MockDocsServiceConnector, ProviderAccount, ServiceConnector, ServiceToolRequest,
    ServiceToolResult,
};
use serde::{Deserialize, Serialize};

use crate::services::GatewayMemoryManager;

use super::{channel_routes, AppState};

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
        .route(
            "/api/connectors/resources/revalidate",
            axum::routing::post(connector_resource_revalidate_handler),
        )
        .route(
            "/api/connectors/resources/promote-memory",
            axum::routing::post(connector_resource_promote_memory_handler),
        )
        .route(
            "/api/connectors/services/mock.docs/tools",
            get(mock_docs_tools_handler),
        )
        .route(
            "/api/connectors/services/mock.docs/execute",
            axum::routing::post(mock_docs_execute_handler),
        )
        .route(
            "/api/connectors/services/feishu.readonly/tools",
            get(feishu_readonly_tools_handler),
        )
        .route(
            "/api/connectors/services/feishu.readonly/execute",
            axum::routing::post(feishu_readonly_execute_handler),
        )
}

const MAX_CONNECTOR_RESOURCE_PAGE: usize = 200;
const DEFAULT_CONNECTOR_RESOURCE_PAGE: usize = 100;

static CONNECTOR_SERVICE_BULKHEAD: OnceLock<ConnectorBulkhead> = OnceLock::new();

fn connector_service_bulkhead() -> &'static ConnectorBulkhead {
    CONNECTOR_SERVICE_BULKHEAD.get_or_init(ConnectorBulkhead::default_service_gate)
}

#[derive(Debug, Deserialize)]
struct MockDocsExecuteRequest {
    actor_principal: String,
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
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

#[derive(Debug, Deserialize)]
struct ConnectorResourceRevalidateRequest {
    reference: String,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConnectorResourcePromoteMemoryRequest {
    reference: String,
    #[serde(default)]
    session_id: Option<String>,
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
    let mock_docs = MockDocsServiceConnector::new();
    accounts.push(account_from_service_connector(&mock_docs));
    let mut capabilities = base_connector_capabilities().to_vec();
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

fn base_connector_capabilities() -> &'static [CapabilityManifest] {
    static BASE_CONNECTOR_CAPABILITIES: OnceLock<Vec<CapabilityManifest>> = OnceLock::new();
    BASE_CONNECTOR_CAPABILITIES.get_or_init(|| {
        let mut capabilities = runtime::default_capabilities();
        capabilities.extend(MockDocsServiceConnector::new().capabilities());
        capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
        capabilities.dedup_by(|left, right| left.capability_id == right.capability_id);
        capabilities
    })
}

fn account_from_service_connector(connector: &impl ServiceConnector) -> ProviderAccount {
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

fn account_from_platform(platform: &channel_routes::PlatformReadiness) -> ProviderAccount {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    probe: Option<McpServerProbe>,
}

#[derive(Debug, Clone, Serialize)]
struct McpServerProbe {
    requested: bool,
    mode: &'static str,
    status: &'static str,
    timeout_ms: u64,
    diagnostics: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct McpServerQuery {
    #[serde(default)]
    probe: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
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
        probe: None,
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

async fn mcp_servers_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<McpServerQuery>,
) -> impl IntoResponse {
    let mut servers = configured_mcp_servers(state.config.as_ref());
    let timeout_ms = query.timeout_ms.unwrap_or(300).clamp(50, 2_000);
    if query.probe {
        apply_mcp_probe_results(&mut servers, state.config.as_ref(), timeout_ms).await;
    }
    let ready = servers
        .iter()
        .filter(|server| server.status == "ready")
        .count();
    let degraded = servers
        .iter()
        .filter(|server| server.status == "degraded")
        .count();
    let disabled = servers
        .iter()
        .filter(|server| server.status == "disabled")
        .count();
    Json(serde_json::json!({
        "kind": "connector_mcp_servers",
        "probe": {
            "requested": query.probe,
            "timeout_ms": timeout_ms,
            "policy": "bounded_http_only",
        },
        "summary": {
            "total": servers.len(),
            "ready": ready,
            "degraded": degraded,
            "disabled": disabled,
        },
        "servers": servers,
    }))
}

async fn apply_mcp_probe_results(
    servers: &mut [McpServerReadiness],
    config: Option<&serde_json::Value>,
    timeout_ms: u64,
) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            for server in servers {
                server.probe = Some(McpServerProbe {
                    requested: true,
                    mode: "client_init",
                    status: "error",
                    timeout_ms,
                    diagnostics: vec![error.to_string()],
                });
            }
            return;
        }
    };

    for server in servers {
        server.probe = Some(
            probe_mcp_server(&client, config, server, timeout_ms)
                .await
                .unwrap_or_else(|diagnostic| McpServerProbe {
                    requested: true,
                    mode: "bounded",
                    status: "error",
                    timeout_ms,
                    diagnostics: vec![diagnostic],
                }),
        );
    }
}

async fn probe_mcp_server(
    client: &reqwest::Client,
    config: Option<&serde_json::Value>,
    server: &McpServerReadiness,
    timeout_ms: u64,
) -> Result<McpServerProbe, String> {
    if !server.enabled {
        return Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "disabled",
            timeout_ms,
            diagnostics: vec!["server disabled".to_string()],
        });
    }
    if !server.configured {
        return Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "degraded",
            timeout_ms,
            diagnostics: vec![format!(
                "missing required fields: {}",
                server.missing_required.join(", ")
            )],
        });
    }

    match server.transport.as_str() {
        "http" | "sse" | "ws" | "claudeai-proxy" => {
            let Some(url) = mcp_server_config_value(config, &server.name)
                .and_then(|value| value.get("url"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|url| !url.is_empty())
            else {
                return Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "degraded",
                    timeout_ms,
                    diagnostics: vec!["url missing".to_string()],
                });
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                client.get(url).send(),
            )
            .await;
            match result {
                Ok(Ok(response)) => {
                    let status = response.status();
                    Ok(McpServerProbe {
                        requested: true,
                        mode: "bounded_http",
                        status: if status.is_server_error() {
                            "degraded"
                        } else {
                            "reachable"
                        },
                        timeout_ms,
                        diagnostics: vec![format!("http_status: {}", status.as_u16())],
                    })
                }
                Ok(Err(error)) => Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "unreachable",
                    timeout_ms,
                    diagnostics: vec![error.to_string()],
                }),
                Err(_) => Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "timeout",
                    timeout_ms,
                    diagnostics: vec!["probe timed out".to_string()],
                }),
            }
        }
        "stdio" | "sdk" => Ok(McpServerProbe {
            requested: true,
            mode: "config_only",
            status: "declared",
            timeout_ms,
            diagnostics: vec![
                "live process discovery is intentionally not started from control-plane probe"
                    .to_string(),
            ],
        }),
        other => Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "unsupported",
            timeout_ms,
            diagnostics: vec![format!("unsupported transport: {other}")],
        }),
    }
}

fn mcp_server_config_value<'a>(
    config: Option<&'a serde_json::Value>,
    name: &str,
) -> Option<&'a serde_json::Value> {
    config
        .and_then(|value| value.get("mcpServers"))
        .and_then(serde_json::Value::as_object)
        .and_then(|servers| servers.get(name))
}

async fn connector_resources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ConnectorResourceQuery>,
) -> impl IntoResponse {
    Json(connector_resources_snapshot(
        &state,
        query.limit,
        query.offset,
        query.q.as_deref(),
    ))
}

pub(crate) fn connector_resources_snapshot(
    state: &AppState,
    limit: Option<usize>,
    offset: Option<usize>,
    query: Option<&str>,
) -> serde_json::Value {
    let limit = limit
        .unwrap_or(DEFAULT_CONNECTOR_RESOURCE_PAGE)
        .clamp(1, MAX_CONNECTOR_RESOURCE_PAGE);
    let offset = offset.unwrap_or(0);
    let (resources, error) = list_durable_resources(state, limit, offset, query);
    let total = resources.len();
    serde_json::json!({
        "kind": "connector_resources",
        "ok": error.is_none(),
        "status": if error.is_some() { "degraded" } else { "available" },
        "degraded_reason": error,
        "limit": limit,
        "offset": offset,
        "resources": resources,
        "total": total,
    })
}

async fn connector_resource_revalidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourceRevalidateRequest>,
) -> impl IntoResponse {
    Json(connector_resource_revalidate_snapshot(
        &state,
        &request.reference,
        request.state.as_deref(),
    ))
}

pub(crate) fn connector_resource_revalidate_snapshot(
    state: &AppState,
    reference: &str,
    state_value: Option<&str>,
) -> serde_json::Value {
    let reference = reference.trim();
    if reference.is_empty() {
        return serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "reason": "reference is required",
        });
    }
    let desired_state = state_value.unwrap_or("indexed");
    let result = state.services.connector.mark_resource_state(
        &state.workspace_root,
        reference,
        desired_state,
    );
    match result {
        Ok((changed, resource, reason)) => serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": changed && reason.is_none(),
            "state": desired_state,
            "changed": changed,
            "resource": resource,
            "reason": reason,
        }),
        Err(error) => serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "state": desired_state,
            "changed": false,
            "resource": null,
            "reason": error.to_string(),
        }),
    }
}

async fn connector_resource_promote_memory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourcePromoteMemoryRequest>,
) -> impl IntoResponse {
    Json(
        connector_resource_promote_memory_snapshot(&state, &request.reference, request.session_id)
            .await,
    )
}

pub(crate) async fn connector_resource_promote_memory_snapshot(
    state: &AppState,
    reference: &str,
    session_id: Option<String>,
) -> serde_json::Value {
    let Some(memory_manager) = state.services.memory.manager() else {
        return serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "memory not configured",
        });
    };
    let reference = reference.trim();
    if reference.is_empty() {
        return serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "reference is required",
        });
    }
    let resource = match state
        .services
        .connector
        .get_resource(&state.workspace_root, reference)
    {
        Ok(Some(resource)) => resource,
        Ok(None) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reason": "resource ref not found",
            });
        }
        Err(error) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reason": error.to_string(),
            });
        }
    };
    let content = connector_resource_memory_content(&resource);
    match find_existing_connector_resource_memory(&memory_manager, reference).await {
        Ok(Some(existing_id)) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": true,
                "replayed": true,
                "memory_id": existing_id,
                "layer": "L3",
                "reference": reference,
                "reason": "resource memory already exists",
            });
        }
        Ok(None) => {}
        Err(error) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reference": reference,
                "reason": format!("memory dedup failed: {error}"),
            });
        }
    }

    let id = MemoryId::new_v4();
    let entry = MemoryEntry {
        id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: format!("Connector resource: {}", resource.title),
        content,
        embedding: None,
        tags: vec![
            "connector_resource".to_string(),
            resource.provider.clone(),
            resource.resource_type.clone(),
        ],
        relations: vec![],
        confidence: 0.86,
        access_count: 0,
        staleness: if resource.indexed_state == "stale" {
            0.35
        } else {
            0.0
        },
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: session_id
            .clone()
            .map(MemoryScope::Session)
            .unwrap_or_else(|| MemoryScope::Project("connector-resource".to_string())),
        session_id,
        source_agent: Some("connector-resource-bridge".to_string()),
        visibility: AgentVisibility::Shared,
    };
    match state
        .services
        .memory
        .remember_entry_with_context(entry, "connector-resource-bridge", "api")
        .await
    {
        Ok(()) => serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": true,
            "memory_id": id,
            "layer": "L3",
            "reference": reference,
        }),
        Err(error) => serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": error.to_string(),
        }),
    }
}

async fn find_existing_connector_resource_memory(
    memory_manager: &Arc<GatewayMemoryManager>,
    reference: &str,
) -> Result<Option<MemoryId>, String> {
    let ref_line = format!("ref: {reference}");
    let entries = memory_manager
        .list_all_entries()
        .await
        .map_err(|error| error.to_string())?;
    Ok(entries
        .into_iter()
        .find(|entry| {
            entry.layer == MemoryLayer::L3
                && entry.tags.iter().any(|tag| tag == "connector_resource")
                && entry.source_agent.as_deref() == Some("connector-resource-bridge")
                && entry.content.lines().any(|line| line.trim() == ref_line)
        })
        .map(|entry| entry.id))
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
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let mode = request.mode.as_deref().unwrap_or("dry_run");
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
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
    let action = state.services.connector.service_action(
        request.actor_principal,
        request.tool_id,
        request.actor_identity_ref,
        request.source_channel,
        request.session_id,
        "mock.docs",
        Some(preview_resource.reference.clone()),
    );

    let snapshot = connector_snapshot(&state);
    let (action, decision, mut evidence) = state.services.cross_plane.decide_connector_action(
        &snapshot,
        action,
        mode,
        chrono::Utc::now(),
    );
    state.services.cross_plane.save_state(&state.config_home);

    let policy_allowed = state.services.connector.policy_allows(&decision);
    let mut allowed = policy_allowed;
    let mut bulkhead_guard = None;
    let mut bulkhead_blocker = None;
    if mode == "commit" && allowed {
        match connector_service_bulkhead().try_acquire("mock.docs") {
            Ok(guard) => {
                bulkhead_guard = Some(guard);
            }
            Err(error) => {
                allowed = false;
                bulkhead_blocker = Some(connector_bulkhead_blocker(error));
            }
        }
    }
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
    let mut blockers = Vec::new();
    if !policy_allowed {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if let Some(blocker) = bulkhead_blocker {
        blockers.push(blocker);
    }
    if mode == "commit" && allowed {
        if let Some((grant_id, remaining)) = state
            .services
            .cross_plane
            .consume_matched_grant_for_decision(&decision)
        {
            evidence.consumed_grant_id = Some(grant_id);
            evidence.remaining_uses_after = Some(remaining);
        }
    }
    let audit_summary = if blockers.is_empty() {
        format!("mock.docs {status}")
    } else {
        blockers.join("; ")
    };
    let receipt = state.services.connector.record_service_execution_receipt(
        &state.services.cross_plane,
        idempotency_key,
        mode,
        status,
        dispatch_status,
        action,
        decision,
        blockers,
        evidence,
        audit_summary,
    );
    state.services.cross_plane.save_state(&state.config_home);
    let service_result = if mode == "commit" && allowed {
        let result = MockDocsServiceConnector::new().execute_tool(service_request);
        connector_service_bulkhead().record_success("mock.docs");
        drop(bulkhead_guard);
        result
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
        match state
            .services
            .connector
            .upsert_resource(&state.workspace_root, &resource)
        {
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

async fn feishu_readonly_tools_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let connector = FeishuReadOnlyServiceConnector::new();
    let snapshot = connector_snapshot(&state);
    Json(serde_json::json!({
        "kind": "connector_service_tools",
        "service": connector.metadata(),
        "health": connector.probe(&snapshot.accounts),
        "tools": connector.capabilities(),
    }))
}

async fn feishu_readonly_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MockDocsExecuteRequest>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let connector = FeishuReadOnlyServiceConnector::new();
    let snapshot = connector_snapshot(&state);
    let health = connector.probe(&snapshot.accounts);
    let account_ready = matches!(health.status, runtime::ConnectorHealthStatus::Ready);
    let mode = request.mode.as_deref().unwrap_or("dry_run");
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            return Json(serde_json::json!({
                "kind": "connector_service_execution",
                "service": "feishu.readonly",
                "replayed": true,
                "receipt": receipt,
            }));
        }
    }

    let service_request = ServiceToolRequest {
        tool_id: request.tool_id.clone(),
        resource_id: request.resource_id,
        title: request.title,
        input: serde_json::json!({ "source": "feishu.readonly" }),
    };
    let preview_result = connector.execute_tool(service_request.clone());
    let preview_resource = preview_result.resource.clone();
    let action = state.services.connector.service_action(
        request.actor_principal,
        request.tool_id,
        request.actor_identity_ref,
        request.source_channel,
        request.session_id,
        "feishu",
        preview_resource
            .as_ref()
            .map(|resource| resource.reference.clone()),
    );

    let (action, decision, mut evidence) = state.services.cross_plane.decide_connector_action(
        &snapshot,
        action,
        mode,
        chrono::Utc::now(),
    );
    state.services.cross_plane.save_state(&state.config_home);

    let policy_allowed = state.services.connector.policy_allows(&decision);
    let mut allowed = account_ready && policy_allowed;
    let mut bulkhead_guard = None;
    let mut bulkhead_blocker = None;
    if mode == "commit" && allowed {
        match connector_service_bulkhead().try_acquire("feishu") {
            Ok(guard) => {
                bulkhead_guard = Some(guard);
            }
            Err(error) => {
                allowed = false;
                bulkhead_blocker = Some(connector_bulkhead_blocker(error));
            }
        }
    }
    let status = if mode == "commit" && allowed {
        "executed"
    } else if allowed {
        "dry_run"
    } else {
        "blocked"
    };
    let dispatch_status = if mode == "commit" && allowed {
        "service_feishu_readonly_resolved"
    } else {
        "not_dispatched"
    };
    let mut blockers = Vec::new();
    if !policy_allowed {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if !account_ready {
        blockers.push(format!(
            "connector:{}",
            health
                .reason
                .clone()
                .unwrap_or_else(|| "no ready feishu account".to_string())
        ));
    }
    if let Some(blocker) = bulkhead_blocker {
        blockers.push(blocker);
    }
    if mode == "commit" && allowed {
        if let Some((grant_id, remaining)) = state
            .services
            .cross_plane
            .consume_matched_grant_for_decision(&decision)
        {
            evidence.consumed_grant_id = Some(grant_id);
            evidence.remaining_uses_after = Some(remaining);
        }
    }
    let audit_summary = if blockers.is_empty() {
        format!("feishu.readonly {status}")
    } else {
        blockers.join("; ")
    };
    let receipt = state.services.connector.record_service_execution_receipt(
        &state.services.cross_plane,
        idempotency_key,
        mode,
        status,
        dispatch_status,
        action,
        decision,
        blockers,
        evidence,
        audit_summary,
    );
    state.services.cross_plane.save_state(&state.config_home);
    let service_result = if allowed {
        let result = connector.execute_tool(service_request);
        connector_service_bulkhead().record_success("feishu");
        drop(bulkhead_guard);
        result
    } else {
        ServiceToolResult {
            status: status.to_string(),
            tool_id: receipt.action.requested_capability.clone(),
            resource: preview_resource,
            output: serde_json::json!({
                "summary": "Feishu read-only request was blocked before external access",
                "read_only": true,
                "body_included": false,
            }),
        }
    };
    let mut resource_persisted = false;
    let mut resource_degraded_reason = None;
    if allowed {
        if let Some(resource) = service_result.resource.clone() {
            match state
                .services
                .connector
                .upsert_resource(&state.workspace_root, &resource)
            {
                Ok(_) => {
                    resource_persisted = true;
                }
                Err(error) => {
                    resource_degraded_reason =
                        Some(format!("resource directory unavailable: {error}"));
                }
            }
        }
    }

    Json(serde_json::json!({
        "kind": "connector_service_execution",
        "service": "feishu.readonly",
        "replayed": false,
        "health": health,
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
    match state
        .services
        .connector
        .list_resources(&state.workspace_root, limit, offset, query)
    {
        Ok(resources) => (resources, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    }
}

fn connector_bulkhead_blocker(error: ConnectorBulkheadRejection) -> String {
    match error {
        ConnectorBulkheadRejection::Busy {
            provider,
            in_flight,
            max_in_flight,
        } => format!("connector.bulkhead:{provider}:busy:{in_flight}/{max_in_flight}"),
        ConnectorBulkheadRejection::CoolingDown { provider } => {
            format!("connector.bulkhead:{provider}:cooling_down")
        }
    }
}

fn connector_resource_memory_content(resource: &ExternalResourceRef) -> String {
    let mut lines = vec![
        format!("resource: {}", resource.title),
        format!("ref: {}", resource.reference),
        format!("provider: {}", resource.provider),
        format!("type: {}", resource.resource_type),
        format!("indexed_state: {}", resource.indexed_state),
        "body_policy: metadata_only".to_string(),
        "evidence: resolve resource ref before relying on external body content".to_string(),
    ];
    if let Some(source) = &resource.source {
        lines.push(format!("source: {source}"));
    }
    if let Some(permissions) = &resource.permissions_summary {
        lines.push(format!("permissions: {permissions}"));
    }
    lines.join("\n")
}
