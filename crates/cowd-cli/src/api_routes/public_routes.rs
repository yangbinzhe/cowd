use std::sync::Arc;

use axum::{
    extract::State as AxumState,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use super::{AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(gateway_health_handler))
        .route("/readyz", get(gateway_ready_handler))
        .route("/api/webui/manifest", get(webui_manifest_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/verify", get(verify_handler))
        .route("/api/auth/logout", post(logout_handler))
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn gateway_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(
        serde_json::to_value(crate::gateway_health::gateway_health_snapshot(&state))
            .unwrap_or_else(|_| serde_json::json!({"status":"error"})),
    )
}

async fn gateway_ready_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let snapshot = crate::gateway_health::gateway_readiness_snapshot(&state);
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(
            serde_json::to_value(snapshot)
                .unwrap_or_else(|_| serde_json::json!({"ready":false,"status":"error"})),
        ),
    )
}

async fn webui_manifest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let health = crate::gateway_health::gateway_health_snapshot(&state);
    Json(
        serde_json::to_value(crate::gateway_service::webui_manifest(health))
            .unwrap_or_else(|_| serde_json::json!({"kind":"cowd.webui.manifest","status":"error"})),
    )
}

#[derive(Deserialize)]
struct LoginRequest {
    token: String,
}

async fn login_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match &state.auth_token {
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "auth not configured".to_string(),
            }),
        )),
        Some(expected) if expected == &body.token => {
            tracing::info!("API login successful");
            Ok(Json(serde_json::json!({
                "success": true,
                "token": body.token,
            })))
        }
        Some(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid token".to_string(),
            }),
        )),
    }
}

async fn verify_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let auth_token = match &state.auth_token {
        None => {
            return Ok(Json(serde_json::json!({
                "valid": true,
                "auth_required": false,
            })));
        }
        Some(token) => token,
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    match auth_header {
        Some(header) if header == format!("Bearer {auth_token}") => Ok(Json(serde_json::json!({
            "valid": true,
            "auth_required": true,
        }))),
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid or missing token".to_string(),
            }),
        )),
    }
}

async fn logout_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "success": true,
        "token_cleared": true,
    }))
}
