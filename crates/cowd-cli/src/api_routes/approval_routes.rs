use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{
    permission_enforcer::{ApprovalPersistence, ApprovalVerdict},
    ApprovalConfig,
};
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
    let pending = match &state.approval_gate {
        Some(gate) => gate.get_pending_requests().await,
        None => Vec::new(),
    };
    Json(serde_json::json!(pending))
}

async fn approval_config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let cfg = match &state.approval_gate {
        Some(gate) => gate.config().read().await.clone(),
        None => ApprovalConfig::default(),
    };
    Json(serde_json::json!(cfg))
}

async fn update_approval_config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(config): Json<ApprovalConfig>,
) -> impl IntoResponse {
    if let Some(gate) = &state.approval_gate {
        gate.update_config(config.clone()).await;
    }
    Json(serde_json::json!(config))
}

async fn toggle_solo_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let mut cfg = match &state.approval_gate {
        Some(gate) => gate.config().read().await.clone(),
        None => ApprovalConfig::default(),
    };
    cfg.solo_mode = !cfg.solo_mode;
    if let Some(gate) = &state.approval_gate {
        gate.update_config(cfg.clone()).await;
    }
    Json(serde_json::json!(cfg))
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
    let history = match &state.approval_gate {
        Some(gate) => gate.history().list_history(limit, offset).await.0,
        None => Vec::new(),
    };
    Json(serde_json::json!(history))
}

async fn approval_respond_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ApprovalRespondRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(gate) = &state.approval_gate else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "approval gate not configured",
        ));
    };
    let persistence = match body.persistence.as_deref().unwrap_or("once") {
        "session" => ApprovalPersistence::Session,
        "always" => ApprovalPersistence::Always,
        _ => ApprovalPersistence::Once,
    };
    let verdict = if body.approved {
        ApprovalVerdict::Approved
    } else {
        ApprovalVerdict::Denied {
            reason: body.reason.unwrap_or_else(|| "denied by user".to_string()),
        }
    };
    let Some(request) = gate.resolve_approval(&body.id, verdict, persistence).await else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "approval request not found",
        ));
    };
    Ok(Json(serde_json::json!({
        "id": body.id,
        "resolved": true,
        "approved": body.approved,
        "tool": "bash",
        "action": request.command,
    })))
}
