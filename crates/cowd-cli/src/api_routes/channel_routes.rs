use std::sync::Arc;

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
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

fn default_wechat_bot_type() -> String {
    "3".to_string()
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
