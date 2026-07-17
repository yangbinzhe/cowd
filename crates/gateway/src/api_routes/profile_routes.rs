use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use serde::Deserialize;

use super::{AppState, ErrorResponse, api_error};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/profiles",
            get(profiles_handler).post(create_profile_handler),
        )
        .route("/api/profiles/switch", post(switch_profile_handler))
        .route("/api/profiles/:id", delete(delete_profile_handler))
}

#[derive(Deserialize)]
struct CreateProfileRequest {
    name: String,
}

#[derive(Deserialize)]
struct SwitchProfileRequest {
    profile: String,
}

async fn profiles_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let profiles = state.profile_manager.list_profiles();
    Json(serde_json::json!({
        "profiles": profiles,
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
        "profiles_dir": state.profile_manager.profiles_dir().display().to_string(),
    }))
}

async fn create_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "profile name is required",
        ));
    }
    let profile = state
        .profile_manager
        .create_profile(name)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "profile": {
                "id": profile.id,
                "name": profile.name,
                "base_dir": profile.base_dir.display().to_string(),
                "config_path": profile.config_path().display().to_string(),
                "memory_dir": profile.memory_dir().display().to_string(),
                "permissions_path": profile.permissions_path().display().to_string(),
            },
            "active_profile": state.profile_manager.active_id(),
            "runtime_profile": state.profile_id,
            "restart_required": false,
        })),
    ))
}

async fn switch_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<SwitchProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = body.profile.trim();
    if profile.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "profile is required"));
    }
    state
        .profile_manager
        .switch_profile(profile)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
        "restart_required": state.profile_manager.active_id() != state.profile_id,
        "message": "profile switch persisted; restart the gateway to move memory/session roots",
    })))
}

async fn delete_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .profile_manager
        .delete_profile(&id)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "deleted": id,
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
    })))
}
