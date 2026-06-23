use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    AttachMissionAgentHttpRequest, AttachMissionTeamHttpRequest, StartMissionSessionHttpRequest,
};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/mission/projection", get(mission_projection_handler))
        .route(
            "/api/mission/sessions",
            get(mission_projection_handler).post(start_mission_session_handler),
        )
        .route("/api/mission/sessions/:id", get(mission_projection_handler))
        .route(
            "/api/mission/sessions/:id/switch",
            post(switch_mission_session_handler),
        )
        .route(
            "/api/mission/sessions/:id/background",
            post(background_mission_session_handler),
        )
        .route(
            "/api/mission/sessions/:id/pause",
            post(pause_mission_session_handler),
        )
        .route(
            "/api/mission/sessions/:id/close",
            post(close_mission_session_handler),
        )
        .route(
            "/api/mission/sessions/:id/teams",
            post(attach_mission_team_handler),
        )
        .route(
            "/api/mission/sessions/:id/agents",
            post(attach_mission_agent_handler),
        )
        .route("/api/mission/approvals", get(mission_approvals_handler))
        .route("/api/mission/relations", get(mission_relations_handler))
}

async fn mission_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.projection())
}

async fn mission_approvals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.approvals())
}

async fn mission_relations_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.relations())
}

async fn start_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<StartMissionSessionHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .start_session(body)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn switch_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.switch_session(&id))
}

async fn background_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.background_session(&id))
}

async fn pause_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.pause_session(&id))
}

async fn close_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.close_session(&id))
}

async fn attach_mission_team_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<AttachMissionTeamHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.attach_team(&id, body))
}

async fn attach_mission_agent_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<AttachMissionAgentHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.attach_agent(&id, body))
}

fn mission_command_result(
    result: Result<serde_json::Value, String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    result
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}
