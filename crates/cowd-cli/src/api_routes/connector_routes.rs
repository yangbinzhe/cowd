use std::sync::{Arc, OnceLock};

use axum::{extract::State as AxumState, response::IntoResponse, routing::get, Json, Router};
use runtime::{
    CapabilityManifest, ConnectorHealth, ConnectorRegistrySnapshot, CrossPlaneAction,
    CrossPlaneExecutionReceipt, ExternalResourceRef, MockDocsServiceConnector, PolicyDecisionKind,
    ProviderAccount, ResourceDirectory, ServiceConnector, ServiceToolRequest, ServiceToolResult,
};
use serde::Deserialize;

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

static RESOURCE_DIRECTORY: OnceLock<ResourceDirectory> = OnceLock::new();

fn resource_directory() -> &'static ResourceDirectory {
    RESOURCE_DIRECTORY.get_or_init(ResourceDirectory::new)
}

pub(super) fn connector_snapshot(state: &AppState) -> ConnectorRegistrySnapshot {
    let platforms = channel_routes::configured_platforms(state.config.as_ref());
    let accounts = platforms
        .iter()
        .filter(|platform| platform.enabled || platform.configured)
        .map(account_from_platform)
        .collect::<Vec<_>>();
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
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    ConnectorRegistrySnapshot::new(
        accounts,
        capabilities,
        resource_directory().list_recent(100),
    )
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
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    let total = snapshot.resources.len();
    Json(serde_json::json!({
        "kind": "connector_resources",
        "resources": snapshot.resources,
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
    if let Some(resource) = service_result.resource.clone() {
        resource_directory().upsert(resource);
    }

    Json(serde_json::json!({
        "kind": "connector_service_execution",
        "service": "mock.docs",
        "replayed": false,
        "result": service_result,
        "receipt": receipt,
    }))
}
