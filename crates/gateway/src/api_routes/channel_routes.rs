use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use channel::{channel_required_fields, ChannelContract};
use serde::Deserialize;
use serde::Serialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/platforms", get(list_platforms_handler))
        .route("/api/platforms/:name", get(get_platform_handler))
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
        "ready"
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

fn qr_svg(scan_data: &str) -> Option<String> {
    qrcode::QrCode::new(scan_data.as_bytes()).ok().map(|code| {
        code.render::<qrcode::render::svg::Color>()
            .min_dimensions(220, 220)
            .quiet_zone(true)
            .build()
    })
}

async fn wechat_ilink_accounts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let runtime_bound = state
        .services
        .channel
        .has_bound_adapter("wechat_ilink")
        .await;
    let bound_adapters = state.services.channel.list_bound_adapters().await;
    let accounts = channel_adapters::platform::wechat_ilink::list_wechat_qr_accounts(None)
        .unwrap_or_default()
        .into_iter()
        .map(|account| {
            serde_json::json!({
                "account_id": account.account_id,
                "base_url": account.base_url,
                "user_id": account.user_id,
                "saved_at": account.saved_at,
            })
        })
        .collect::<Vec<_>>();
    let usable = runtime_bound && !accounts.is_empty();
    Json(serde_json::json!({
        "kind": "wechat_ilink_accounts",
        "runtime_bound": runtime_bound,
        "usable": usable,
        "bound_adapters": bound_adapters,
        "accounts": accounts
    }))
}

async fn wechat_ilink_qr_start_handler(
    Json(body): Json<WechatQrStartRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let qr = channel_adapters::platform::wechat_ilink::request_wechat_qr_login(&body.bot_type)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "wechat_ilink_qr",
        "qrcode": qr.qrcode,
        "scan_data": qr.scan_data,
        "qrcode_img_content": qr.qrcode_img_content,
        "qrcode_svg": qr_svg(&qr.scan_data),
        "base_url": qr.base_url,
        "expires_in_seconds": 480
    })))
}

async fn wechat_ilink_qr_poll_handler(
    Json(body): Json<WechatQrPollRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let status = channel_adapters::platform::wechat_ilink::poll_wechat_qr_login(
        &body.qrcode,
        body.base_url.as_deref(),
    )
    .await
    .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;

    let account = if let Some(credentials) = status.credentials.as_ref() {
        channel_adapters::platform::wechat_ilink::save_wechat_qr_account(credentials, None)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        Some(serde_json::json!({
            "account_id": credentials.account_id,
            "base_url": credentials.base_url,
            "user_id": credentials.user_id,
            "saved_at": credentials.saved_at,
        }))
    } else {
        None
    };

    Ok(Json(serde_json::json!({
        "kind": "wechat_ilink_qr_status",
        "status": status.status,
        "redirect_host": status.redirect_host,
        "account": account,
    })))
}
