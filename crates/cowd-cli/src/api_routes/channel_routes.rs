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
    pub(super) capabilities: Vec<&'static str>,
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
    let required = required_fields(&platform_type);
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
        capabilities: platform_capabilities(&platform_type),
        diagnostics,
    })
}

fn disabled_platform(platform_type: &str) -> PlatformReadiness {
    PlatformReadiness {
        name: platform_type.to_string(),
        platform_type: platform_type.to_string(),
        enabled: false,
        status: "disabled",
        configured: false,
        credential_present: false,
        missing_required: required_fields(platform_type)
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        capabilities: platform_capabilities(platform_type),
        diagnostics: vec!["platform is not configured".to_string()],
    }
}

fn required_fields(platform_type: &str) -> Vec<&'static str> {
    match platform_type {
        "feishu" => vec!["app_id", "app_secret"],
        "wecom" => vec!["corp_id", "corp_secret", "agent_id"],
        "wechat-ilink" | "wechat_ilink" | "wechat" => Vec::new(),
        "email" => vec!["smtp_server", "username", "password"],
        _ => Vec::new(),
    }
}

fn platform_capabilities(platform_type: &str) -> Vec<&'static str> {
    match platform_type {
        "feishu" => vec!["send_text", "send_image", "send_file", "doc_ops"],
        "wecom" => vec!["send_text", "callback"],
        "wechat-ilink" | "wechat_ilink" | "wechat" => vec!["qr_login", "send_text"],
        "email" => vec!["send_email"],
        _ => Vec::new(),
    }
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

async fn wechat_ilink_accounts_handler() -> impl IntoResponse {
    let accounts = runtime::platform::wechat_ilink::list_wechat_qr_accounts(None)
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
    Json(serde_json::json!({
        "kind": "wechat_ilink_accounts",
        "accounts": accounts
    }))
}

async fn wechat_ilink_qr_start_handler(
    Json(body): Json<WechatQrStartRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let qr = runtime::platform::wechat_ilink::request_wechat_qr_login(&body.bot_type)
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
    let status = runtime::platform::wechat_ilink::poll_wechat_qr_login(
        &body.qrcode,
        body.base_url.as_deref(),
    )
    .await
    .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;

    let account = if let Some(credentials) = status.credentials.as_ref() {
        runtime::platform::wechat_ilink::save_wechat_qr_account(credentials, None)
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
