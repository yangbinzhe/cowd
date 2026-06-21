use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use surface::{SurfaceActionRequest, SurfaceSendRequest};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/surfaces", get(list_surfaces_handler))
        .route("/api/surfaces/health", get(surface_health_handler))
        .route("/api/surfaces/:id", get(get_surface_handler))
        .route("/api/surfaces/:id/send", post(send_surface_handler))
        .route("/api/surfaces/:id/action", post(action_surface_handler))
}

#[derive(Debug, Deserialize)]
struct SurfaceSendBody {
    recipient: String,
    #[serde(default)]
    thread: Option<String>,
    text: String,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SurfaceActionBody {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
}

async fn list_surfaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "surface.registry",
        "registry": state.services.surface.snapshot(),
    }))
}

async fn surface_health_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let snapshot = state.services.surface.snapshot();
    Json(serde_json::json!({
        "kind": "surface.health",
        "status": "ready",
        "surface_count": snapshot.surfaces.len(),
        "registry": snapshot,
    }))
}

async fn get_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let normalized = surface::normalize_surface_id(&id);
    let surface = state
        .services
        .surface
        .snapshot()
        .surfaces
        .into_iter()
        .find(|surface| surface.id == normalized)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("surface `{id}` not found")))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.detail",
        "surface": surface,
    })))
}

async fn send_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SurfaceSendBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: id,
            recipient: body.recipient,
            thread: body.thread,
            text: body.text,
            metadata: body.metadata,
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

async fn action_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SurfaceActionBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .action(SurfaceActionRequest {
            surface: id,
            action: body.action,
            payload: body.payload,
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}
