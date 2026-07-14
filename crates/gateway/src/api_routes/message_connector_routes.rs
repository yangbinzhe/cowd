use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde::Serialize;
use surface::message::{message_connector_required_fields, MessageConnectorContract};
use surface::SurfaceActionRequest;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/platforms", get(list_platforms_handler))
        .route("/api/platforms/:name", get(get_platform_handler))
        .route(
            "/api/message-connectors",
            get(list_message_connectors_handler),
        )
        .route(
            "/api/message-connectors/:name/status",
            get(get_message_connector_status_handler),
        )
        .route(
            "/api/message-connectors/:name/repair",
            post(repair_message_connector_handler),
        )
        .route(
            "/api/message-connectors/wechat-ilink/accounts",
            get(wechat_ilink_accounts_handler),
        )
        .route(
            "/api/message-connectors/wechat-ilink/actions/account.login_qr.start",
            post(wechat_ilink_qr_start_handler),
        )
        .route(
            "/api/message-connectors/wechat-ilink/actions/account.login_qr.poll",
            post(wechat_ilink_qr_poll_handler),
        )
        .route(
            "/api/message-endpoints",
            get(list_message_endpoints_handler),
        )
        .route("/api/message-routes", get(list_message_routes_handler))
        .route("/api/message-bindings", get(list_message_bindings_handler))
}

#[derive(Debug, Deserialize)]
struct WechatQrStartRequest {
    #[serde(default = "default_wechat_bot_type")]
    bot_type: String,
}

#[derive(Debug, Deserialize)]
struct WechatQrPollRequest {
    qrcode: String,
    base_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PlatformReadiness {
    pub(super) name: String,
    pub(super) platform_type: String,
    pub(super) enabled: bool,
    pub(super) status: &'static str,
    pub(super) configured: bool,
    pub(super) credential_present: bool,
    pub(super) missing_required: Vec<String>,
    #[serde(default)]
    pub(super) scopes: Vec<String>,
    pub(super) capabilities: Vec<String>,
    pub(super) diagnostics: Vec<String>,
}

fn default_wechat_bot_type() -> String {
    "3".to_string()
}

async fn list_platforms_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    Json(serde_json::json!(platforms))
}

async fn get_platform_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    let matched = platforms
        .into_iter()
        .find(|platform| platform.name == name || platform.platform_type == name);
    Json(serde_json::json!({
        "name": name,
        "readiness": matched,
        "sessions": []
    }))
}

async fn list_message_connectors_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    let runtimes = state.services.surface.runtime_snapshots();
    let connectors = platforms
        .into_iter()
        .map(|platform| {
            let runtime = runtimes
                .iter()
                .find(|runtime| {
                    runtime.surface == platform.platform_type || runtime.surface == platform.name
                })
                .cloned();
            serde_json::json!({
                "connector": platform.platform_type,
                "name": platform.name,
                "configuration_status": platform.status,
                "configured": platform.configured,
                "enabled": platform.enabled,
                "credential_present": platform.credential_present,
                "missing_required": platform.missing_required,
                "capabilities": platform.capabilities,
                "runtime": runtime,
            })
        })
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "kind": "message.connector.registry",
        "connectors": connectors,
        "runtime": runtimes,
    }))
}

async fn get_message_connector_status_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let connector = surface::message::normalize_message_connector(&name);
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    let platform = platforms
        .into_iter()
        .find(|platform| platform.platform_type == connector || platform.name == connector);
    let runtime = state.services.surface.runtime_snapshot(&connector);
    if platform.is_none() && runtime.is_none() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("message connector `{name}` not found"),
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "message.connector.status",
        "connector": connector,
        "configuration": platform,
        "runtime": runtime,
    })))
}

async fn repair_message_connector_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let connector = surface::message::normalize_message_connector(&name);
    let runtime = state
        .services
        .surface
        .repair_surface(&connector)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "message.connector.repair",
        "connector": connector,
        "runtime": runtime,
    })))
}

pub(super) fn configured_platforms(config: Option<&serde_json::Value>) -> Vec<PlatformReadiness> {
    let Some(config) = config else {
        return vec![
            disabled_platform("feishu"),
            disabled_platform("wechat-ilink"),
            disabled_platform("wecom"),
        ];
    };
    let platform_values = config
        .get("gateway")
        .and_then(|value| value.get("platforms"))
        .or_else(|| {
            config
                .get("platform")
                .and_then(|value| value.get("platforms"))
        })
        .or_else(|| config.get("platforms"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    if platform_values.is_empty() {
        return vec![
            disabled_platform("feishu"),
            disabled_platform("wechat-ilink"),
            disabled_platform("wecom"),
        ];
    }

    platform_values
        .iter()
        .filter_map(platform_readiness_from_value)
        .collect()
}

fn platform_readiness_from_value(value: &serde_json::Value) -> Option<PlatformReadiness> {
    let platform_type = value
        .get("platformType")
        .or_else(|| value.get("platform_type"))
        .and_then(|value| value.as_str())?
        .to_ascii_lowercase();
    if platform_type == "api_server" {
        return None;
    }
    let name = value
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or(&platform_type)
        .to_string();
    let enabled = value
        .get("enabled")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);
    let contract = MessageConnectorContract::for_connector(&platform_type);
    let required = contract
        .required_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let missing_required = required
        .iter()
        .filter(|field| !has_non_empty(value, field))
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let credential_present = required
        .iter()
        .any(|field| credential_field(field) && has_non_empty(value, field));
    let configured = missing_required.is_empty();
    let status = if !enabled {
        "disabled"
    } else if configured {
        "configured"
    } else {
        "degraded"
    };
    let diagnostics = if configured {
        vec!["required configuration present; secrets are redacted".to_string()]
    } else {
        vec![format!(
            "missing required fields: {}",
            missing_required.join(", ")
        )]
    };

    Some(PlatformReadiness {
        name,
        platform_type: platform_type.clone(),
        enabled,
        status,
        configured,
        credential_present,
        missing_required,
        scopes: platform_scopes_from_value(value),
        capabilities: contract.capability_names(),
        diagnostics,
    })
}

fn disabled_platform(platform_type: &str) -> PlatformReadiness {
    let contract = MessageConnectorContract::for_connector(platform_type);
    PlatformReadiness {
        name: platform_type.to_string(),
        platform_type: platform_type.to_string(),
        enabled: false,
        status: "disabled",
        configured: false,
        credential_present: false,
        missing_required: contract
            .required_fields
            .iter()
            .map(|field| field.to_string())
            .collect(),
        scopes: Vec::new(),
        capabilities: contract.capability_names(),
        diagnostics: vec!["platform is not configured".to_string()],
    }
}

fn platform_scopes_from_value(value: &serde_json::Value) -> Vec<String> {
    let mut scopes = value
        .get("scopes")
        .or_else(|| value.get("scope"))
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    scopes.sort();
    scopes.dedup();
    scopes
}

fn required_fields(platform_type: &str) -> Vec<&'static str> {
    message_connector_required_fields(platform_type)
}

fn credential_field(field: &str) -> bool {
    field.contains("secret") || field.contains("password") || field.contains("token")
}

fn has_non_empty(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .map(|value| !value.is_empty())
        .unwrap_or_else(|| value.get(field).is_some_and(|value| !value.is_null()))
}

async fn wechat_ilink_accounts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let surface_available = state.services.surface.has_surface("wechat-ilink");
    Json(serde_json::json!({
        "kind": "wechat_ilink_accounts",
        "surface_available": surface_available,
        "usable": false,
        "accounts": [],
        "diagnostics": ["wechat-ilink account listing is provided by the wechat-ilink Edge message connector"]
    }))
}

async fn wechat_ilink_qr_start_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<WechatQrStartRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .action(SurfaceActionRequest {
            surface: "wechat-ilink".to_string(),
            action: "account.login_qr.start".to_string(),
            payload: serde_json::json!({ "bot_type": body.bot_type }),
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

async fn list_message_endpoints_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    let endpoints = platforms
        .iter()
        .flat_map(message_endpoint_projection)
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "kind": "message.endpoint.directory",
        "endpoints": endpoints,
    }))
}

async fn list_message_routes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = configured_platforms(config.as_ref());
    let runtimes = state.services.surface.runtime_snapshots();
    let routes = platforms
        .iter()
        .map(|platform| {
            let runtime = runtimes
                .iter()
                .find(|runtime| {
                    runtime.surface == platform.platform_type || runtime.surface == platform.name
                })
                .cloned();
            serde_json::json!({
                "route_id": format!("message:{}:default", platform.platform_type),
                "connector": platform.platform_type,
                "policy": "origin",
                "status": platform.status,
                "configured": platform.configured,
                "runtime": runtime,
                "capabilities": platform.capabilities,
            })
        })
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "kind": "message.delivery.routes",
        "routes": routes,
    }))
}

async fn list_message_bindings_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let mut bindings = BTreeMap::<String, serde_json::Value>::new();
    for inbox in state.services.surface.all_inbox() {
        let endpoint = inbox
            .sender_id
            .clone()
            .unwrap_or_else(|| inbox.message_id.clone());
        let thread = inbox.thread_id.clone().unwrap_or_default();
        let key = format!("{}:{}:{}", inbox.surface, endpoint, thread);
        bindings.insert(
            key.clone(),
            serde_json::json!({
                "binding_id": format!("message:{key}"),
                "connector": inbox.surface,
                "endpoint": endpoint,
                "thread": inbox.thread_id,
                "message_id": inbox.message_id,
                "runtime_session_id": inbox.runtime_session_id,
                "runtime_turn_id": inbox.runtime_turn_id,
                "resource_count": payload_media_count(&inbox.payload_json),
                "status": inbox.status,
                "last_seen_at_ms": inbox.updated_at_ms,
                "direction": "inbound",
            }),
        );
    }
    for outbox in state.services.surface.all_outbox() {
        let endpoint = outbox.recipient.clone();
        let thread = outbox.thread_id.clone().unwrap_or_default();
        let key = format!("{}:{}:{}", outbox.surface, endpoint, thread);
        bindings
            .entry(key.clone())
            .and_modify(|binding| {
                binding["outbound_status"] = serde_json::json!(outbox.status.clone());
                binding["source_session_id"] = serde_json::json!(outbox.source_session_id.clone());
                binding["delivery_id"] = serde_json::json!(outbox.delivery_id.clone());
                binding["last_seen_at_ms"] = serde_json::json!(outbox.updated_at_ms);
            })
            .or_insert_with(|| {
                serde_json::json!({
                    "binding_id": format!("message:{key}"),
                    "connector": outbox.surface,
                    "endpoint": endpoint,
                    "thread": outbox.thread_id,
                    "message_id": outbox.reply_to_message_id,
                    "runtime_session_id": outbox.source_session_id,
                    "runtime_turn_id": null,
                    "status": outbox.status,
                    "delivery_id": outbox.delivery_id,
                    "last_seen_at_ms": outbox.updated_at_ms,
                    "direction": "outbound",
                })
            });
    }
    let bindings = bindings.into_values().collect::<Vec<_>>();
    Json(serde_json::json!({
        "kind": "message.conversation.bindings",
        "bindings": bindings,
    }))
}

fn message_endpoint_projection(platform: &PlatformReadiness) -> Vec<serde_json::Value> {
    let contract = MessageConnectorContract::for_connector(&platform.platform_type);
    contract
        .endpoint_kinds
        .into_iter()
        .map(|kind| {
            serde_json::json!({
                "endpoint_id": format!("message:{}:{kind:?}", platform.platform_type).to_ascii_lowercase(),
                "connector": platform.platform_type,
                "name": platform.name,
                "kind": kind,
                "configured": platform.configured,
                "status": platform.status,
                "capabilities": platform.capabilities,
            })
        })
        .collect()
}

fn payload_media_count(payload: &serde_json::Value) -> usize {
    payload
        .get("media_urls")
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

async fn wechat_ilink_qr_poll_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<WechatQrPollRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .action(SurfaceActionRequest {
            surface: "wechat-ilink".to_string(),
            action: "account.login_qr.poll".to_string(),
            payload: serde_json::json!({
                "qrcode": body.qrcode,
                "base_url": body.base_url,
            }),
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}
