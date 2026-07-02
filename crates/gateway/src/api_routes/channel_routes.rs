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
use surface::channel::{channel_required_fields, ChannelContract};
use surface::SurfaceActionRequest;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/platforms", get(list_platforms_handler))
        .route("/api/platforms/:name", get(get_platform_handler))
        .route("/api/channels", get(list_channels_handler))
        .route(
            "/api/channels/:name/status",
            get(get_channel_status_handler),
        )
        .route("/api/channels/:name/repair", post(repair_channel_handler))
        .route(
            "/api/channels/wechat-ilink/accounts",
            get(wechat_ilink_accounts_handler),
        )
        .route(
            "/api/channels/wechat-ilink/qr",
            post(wechat_ilink_qr_start_handler),
        )
        .route(
            "/api/channels/wechat-ilink/qr/poll",
            post(wechat_ilink_qr_poll_handler),
        )
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
    let platforms = configured_platforms(state.config.as_ref());
    Json(serde_json::json!(platforms))
}

async fn get_platform_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let platforms = configured_platforms(state.config.as_ref());
    let matched = platforms
        .into_iter()
        .find(|platform| platform.name == name || platform.platform_type == name);
    Json(serde_json::json!({
        "name": name,
        "readiness": matched,
        "sessions": []
    }))
}

async fn list_channels_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let platforms = configured_platforms(state.config.as_ref());
    let runtimes = state.services.surface.runtime_snapshots();
    let channels = platforms
        .into_iter()
        .map(|platform| {
            let runtime = runtimes
                .iter()
                .find(|runtime| {
                    runtime.surface == platform.platform_type || runtime.surface == platform.name
                })
                .cloned();
            serde_json::json!({
                "channel": platform.platform_type,
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
        "kind": "channel.registry",
        "channels": channels,
        "runtime": runtimes,
    }))
}

async fn get_channel_status_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let channel = surface::channel::normalize_channel(&name);
    let platforms = configured_platforms(state.config.as_ref());
    let platform = platforms
        .into_iter()
        .find(|platform| platform.platform_type == channel || platform.name == channel);
    let runtime = state.services.surface.runtime_snapshot(&channel);
    if platform.is_none() && runtime.is_none() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("channel `{name}` not found"),
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "channel.status",
        "channel": channel,
        "configuration": platform,
        "runtime": runtime,
    })))
}

async fn repair_channel_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let channel = surface::channel::normalize_channel(&name);
    let runtime = state
        .services
        .surface
        .repair_surface(&channel)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "channel.repair",
        "channel": channel,
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
    let contract = ChannelContract::for_channel(&platform_type);
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
    let contract = ChannelContract::for_channel(platform_type);
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
    channel_required_fields(platform_type)
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
