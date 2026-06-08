use std::fs;
use std::path::{Path, PathBuf};
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
use memory::{CognitiveContextManager, MemoryKernel, MemoryScope, MemoryTurnContext};
use runtime::{
    CapabilityManifest, ConnectorBulkhead, ConnectorBulkheadRejection, ConnectorHealth,
    ConnectorRegistrySnapshot, CrossPlaneAction, CrossPlaneExecutionReceipt, ExternalResourceRef,
    FeishuReadOnlyServiceConnector, MockDocsServiceConnector, PolicyDecisionKind, ProviderAccount,
    ServiceConnector, ServiceToolRequest, ServiceToolResult, SqliteResourceDirectory,
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

async fn mcp_servers_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let servers = configured_mcp_servers(state.config.as_ref());
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
        "summary": {
            "total": servers.len(),
            "ready": ready,
            "degraded": degraded,
            "disabled": disabled,
        },
        "servers": servers,
    }))
}

async fn connector_resources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ConnectorResourceQuery>,
) -> impl IntoResponse {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_CONNECTOR_RESOURCE_PAGE)
        .clamp(1, MAX_CONNECTOR_RESOURCE_PAGE);
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

async fn connector_resource_revalidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourceRevalidateRequest>,
) -> impl IntoResponse {
    let reference = request.reference.trim();
    if reference.is_empty() {
        return Json(serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "reason": "reference is required",
        }));
    }
    let desired_state = request.state.as_deref().unwrap_or("indexed");
    let result = durable_resource_directory(&state).and_then(|directory| {
        let changed = match desired_state {
            "indexed" => directory.mark_indexed(reference)?,
            "stale" => directory.mark_stale(reference)?,
            other => {
                return Ok((false, None, Some(format!("unsupported state: {other}"))));
            }
        };
        let resource = directory.get(reference)?;
        Ok((changed, resource, None))
    });
    match result {
        Ok((changed, resource, reason)) => Json(serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": changed && reason.is_none(),
            "state": desired_state,
            "changed": changed,
            "resource": resource,
            "reason": reason,
        })),
        Err(error) => Json(serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "state": desired_state,
            "changed": false,
            "resource": null,
            "reason": error.to_string(),
        })),
    }
}

async fn connector_resource_promote_memory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourcePromoteMemoryRequest>,
) -> impl IntoResponse {
    let Some(memory_manager) = state.memory_manager.as_ref() else {
        return Json(serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "memory not configured",
        }));
    };
    let reference = request.reference.trim();
    if reference.is_empty() {
        return Json(serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "reference is required",
        }));
    }
    let resource =
        match durable_resource_directory(&state).and_then(|directory| directory.get(reference)) {
            Ok(Some(resource)) => resource,
            Ok(None) => {
                return Json(serde_json::json!({
                    "kind": "connector_resource_memory_promotion",
                    "ok": false,
                    "reason": "resource ref not found",
                }));
            }
            Err(error) => {
                return Json(serde_json::json!({
                    "kind": "connector_resource_memory_promotion",
                    "ok": false,
                    "reason": error.to_string(),
                }));
            }
        };
    let content = connector_resource_memory_content(&resource);
    match find_existing_connector_resource_memory(memory_manager, reference).await {
        Ok(Some(existing_id)) => {
            return Json(serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": true,
                "replayed": true,
                "memory_id": existing_id,
                "layer": "L3",
                "reference": reference,
                "reason": "resource memory already exists",
            }));
        }
        Ok(None) => {}
        Err(error) => {
            return Json(serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reference": reference,
                "reason": format!("memory dedup failed: {error}"),
            }));
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
        scope: request
            .session_id
            .clone()
            .map(MemoryScope::Session)
            .unwrap_or_else(|| MemoryScope::Project("connector-resource".to_string())),
        session_id: request.session_id,
        source_agent: Some("connector-resource-bridge".to_string()),
        visibility: AgentVisibility::Shared,
    };
    let kernel = MemoryKernel::new(Arc::clone(memory_manager));
    let memory_ctx = MemoryTurnContext::new("connector-resource-bridge", "api");
    match kernel.remember(&memory_ctx, entry).await {
        Ok(()) => Json(serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": true,
            "memory_id": id,
            "layer": "L3",
            "reference": reference,
        })),
        Err(error) => Json(serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": error.to_string(),
        })),
    }
}

async fn find_existing_connector_resource_memory(
    memory_manager: &Arc<CognitiveContextManager>,
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
    action.actor_identity_ref = request.actor_identity_ref;
    action.source_channel = request.source_channel;
    action.session_id = request.session_id;
    action.provider_account = Some("mock.docs".to_string());
    action.resource_ref = Some(preview_resource.reference.clone());

    let (action, decision, _evidence) = cross_plane_routes::decide_connector_action_and_audit(
        &state,
        action,
        mode,
        chrono::Utc::now(),
    );
    cross_plane_routes::save_cross_plane_state(&state);

    let policy_allowed = decision.decision == PolicyDecisionKind::Allow;
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
    cross_plane_routes::ensure_cross_plane_loaded(&state);
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
        if let Some(receipt) =
            cross_plane_routes::cross_plane_control().find_execution_by_idempotency_key(key)
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
    let mut action = CrossPlaneAction::new(request.actor_principal, request.tool_id);
    action.actor_identity_ref = request.actor_identity_ref;
    action.source_channel = request.source_channel;
    action.session_id = request.session_id;
    action.provider_account = Some("feishu".to_string());
    action.resource_ref = preview_resource
        .as_ref()
        .map(|resource| resource.reference.clone());

    let (action, decision, _evidence) = cross_plane_routes::decide_connector_action_and_audit(
        &state,
        action,
        mode,
        chrono::Utc::now(),
    );
    cross_plane_routes::save_cross_plane_state(&state);

    let policy_allowed = decision.decision == PolicyDecisionKind::Allow;
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
            match durable_resource_directory(&state)
                .and_then(|directory| directory.upsert(&resource))
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
    match durable_resource_directory(state).and_then(|directory| {
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }) {
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
