use std::sync::Arc;

use axum::{extract::State as AxumState, response::IntoResponse, routing::get, Json, Router};

use super::AppState;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
}

async fn tools_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tools: Vec<serde_json::Value> = state
        .tool_registry
        .definitions(None)
        .iter()
        .map(|tool| {
            serde_json::json!({
                "name": tool.name,
                "description": tool.description,
                "enabled": true,
            })
        })
        .collect();
    Json(serde_json::json!({ "tools": tools, "count": tools.len() }))
}

async fn config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    match &state.config {
        Some(config) => Json(config.clone()),
        None => Json(serde_json::json!({
            "error": "config not loaded",
            "model": "unknown",
            "version": env!("CARGO_PKG_VERSION"),
        })),
    }
}
