use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{permission_enforcer::ApprovalPersistence, ApprovalConfig};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/approval/pending", get(approval_pending_handler))
        .route("/api/approval/respond", post(approval_respond_handler))
        .route(
            "/api/approval/config",
            get(approval_config_handler).put(update_approval_config_handler),
        )
        .route("/api/approval/solo", post(toggle_solo_handler))
        .route("/api/approval/history", get(approval_history_handler))
}

#[derive(Deserialize)]
struct ApprovalRespondRequest {
    id: String,
    approved: bool,
    #[serde(default)]
    persistence: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

async fn approval_pending_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.approval.pending().await)
}

async fn approval_config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!(state.services.approval.config().await))
}

async fn update_approval_config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(config): Json<ApprovalConfig>,
) -> impl IntoResponse {
    Json(serde_json::json!(
        state.services.approval.update_config(config).await
    ))
}

async fn toggle_solo_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!(
        state.services.approval.toggle_solo().await
    ))
}

async fn approval_history_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .min(200);
    let offset = params
        .get("offset")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    Json(state.services.approval.history(limit, offset).await)
}

async fn approval_respond_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ApprovalRespondRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let persistence = match body.persistence.as_deref().unwrap_or("once") {
        "session" => ApprovalPersistence::Session,
        "always" => ApprovalPersistence::Always,
        _ => ApprovalPersistence::Once,
    };
    state
        .services
        .approval
        .respond(&body.id, body.approved, persistence, body.reason)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}
