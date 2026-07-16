use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Extension, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::ApprovalConfig;
use serde::Deserialize;

use super::{api_error, AppState, AuthenticatedPrincipal, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/approval/pending", get(approval_pending_handler))
        .route("/api/approval/respond", post(approval_respond_handler))
        .route("/api/approval/risk-receipt", post(risk_receipt_handler))
        .route(
            "/api/approval/config",
            get(approval_config_handler).put(update_approval_config_handler),
        )
        .route("/api/approval/solo", post(toggle_solo_handler))
        .route("/api/approval/history", get(approval_history_handler))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalRespondRequest {
    id: String,
    approved: bool,
    #[serde(default)]
    persistence: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct RiskReceiptRequest {
    tool_name: String,
    input: serde_json::Value,
    #[serde(default)]
    session_id: Option<String>,
}

async fn approval_pending_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> impl IntoResponse {
    Json(state.services.approval.pending(&principal.0).await)
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
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
    Json(
        state
            .services
            .approval
            .history(limit, offset, &principal.0)
            .await,
    )
}

async fn approval_respond_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalRespondRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let _persistence = body.persistence.as_deref().unwrap_or("once");
    state
        .services
        .approval
        .respond(&body.id, body.approved, body.reason, &principal.0)
        .await
        .map(Json)
        .map_err(|error| {
            let status = match error.as_str() {
                "approval_human_interactive_capability_required" => StatusCode::FORBIDDEN,
                "mfg_review_requires_typed_decision_service" => StatusCode::CONFLICT,
                _ => StatusCode::NOT_FOUND,
            };
            api_error(status, error)
        })
}

async fn risk_receipt_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<RiskReceiptRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let input = match body.input {
        serde_json::Value::String(value) => value,
        value => serde_json::to_string(&value).unwrap_or_default(),
    };
    let receipt = state
        .services
        .approval
        .risk_receipt(&body.tool_name, &input)
        .await
        .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;
    let audit = state.services.audit.risk_gate_projection(&receipt);
    let growth = state
        .services
        .growth
        .ingest_risk_gate_receipt(
            &state.config_home,
            &state.services.memory,
            &state.services.matrix,
            body.session_id
                .unwrap_or_else(|| "approval-risk-receipt".to_string()),
            &receipt,
        )
        .await;
    Ok(Json(serde_json::json!({
        "ok": true,
        "receipt": receipt,
        "audit": audit,
        "growth": growth,
    })))
}
