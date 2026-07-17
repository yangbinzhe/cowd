use std::sync::Arc;

use crate::command::slash::CommandSurface;
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use super::{AppState, ErrorResponse, api_error};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/slash", get(slash_catalog_handler))
        .route("/api/slash/history", get(slash_history_handler))
        .route("/api/slash/resolve", post(slash_resolve_handler))
        .route("/api/slash/dispatch", post(slash_dispatch_handler))
        .route("/api/slash/:id", get(slash_detail_handler))
}

#[derive(Deserialize)]
struct SlashDispatchRequest {
    command: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SlashCatalogQuery {
    #[serde(default)]
    surface: Option<String>,
}

#[derive(Deserialize)]
struct SlashResolveRequest {
    input: String,
    #[serde(default)]
    surface: Option<String>,
    #[serde(default)]
    context: serde_json::Value,
}

async fn slash_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SlashCatalogQuery>,
) -> Json<serde_json::Value> {
    let surface = CommandSurface::parse(query.surface.as_deref());
    let projection = state.services.slash.projection(surface);
    Json(serde_json::json!({
        "surface": projection.surface,
        "commands": projection.commands,
    }))
}

async fn slash_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let slash = state
        .services
        .slash
        .detail(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("unknown slash `{id}`")))?;
    Ok(Json(serde_json::json!({ "slash": slash })))
}

async fn slash_history_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let entries = state.services.system.command_history(&state.config_home);
    Json(serde_json::json!({ "history": entries, "total": entries.len() }))
}

async fn slash_dispatch_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<SlashDispatchRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = state
        .services
        .slash
        .dispatch(
            &body.command,
            body.args.unwrap_or_else(|| serde_json::json!({})),
        )
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    if !receipt.ok {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            serde_json::to_string(&receipt).unwrap_or_else(|_| receipt.status.clone()),
        ));
    }
    let receipt = serde_json::to_value(receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    state
        .services
        .system
        .append_command_history(&state.config_home, &receipt);
    Ok(Json(receipt))
}

async fn slash_resolve_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<SlashResolveRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = CommandSurface::parse(body.surface.as_deref());
    let resolution = state
        .services
        .slash
        .resolve(&body.input, surface, body.context)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "resolution": resolution,
    })))
}
