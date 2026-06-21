use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};

use super::AppState;
use crate::services::reality_service::RealityFlowQuery;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/reality/status", get(reality_status_handler))
        .route("/api/reality/static", get(reality_static_handler))
        .route("/api/reality/flow", get(reality_flow_handler))
        .route("/api/reality/promotions", get(reality_promotions_handler))
        .route("/api/reality/boundaries", get(reality_boundaries_handler))
}

async fn reality_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .status_projection(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                &state.services.growth,
                &state.services.context,
                &state.services.audit,
            )
            .await,
    )
}

async fn reality_static_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .static_projection(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                &state.services.growth,
                &state.services.context,
                &state.services.audit,
            )
            .await,
    )
}

async fn reality_flow_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_id = params
        .get("session_id")
        .filter(|value| !value.trim().is_empty())
        .cloned();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(25)
        .min(200);
    Json(
        state
            .services
            .reality
            .flow_projection(
                &state.config_home,
                &state.services.growth,
                RealityFlowQuery { session_id, limit },
            )
            .await,
    )
}

async fn reality_promotions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_id = params.get("session_id").map(String::as_str);
    let target = params.get("target").map(String::as_str);
    let status = params.get("status").map(String::as_str);
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    Json(state.services.reality.promotions_projection(
        &state.config_home,
        &state.services.growth,
        session_id,
        target,
        status,
        limit,
    ))
}

async fn reality_boundaries_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .boundaries_projection(&state.config_home, &state.services.growth),
    )
}
