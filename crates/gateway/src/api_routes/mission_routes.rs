use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    CreateMissionScheduleHttpRequest, DecideMissionApprovalHttpRequest,
    InterpretMissionCommandHttpRequest, StartMissionSessionHttpRequest,
    SubmitMissionApprovalHttpRequest, UpdateMissionScheduleHttpRequest,
    UpsertMissionProxyHttpRequest,
};
use serde::Deserialize;

use super::{api_error, AppState, AuthenticatedPrincipal, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL.template(),
            get(mission_control_handler).post(execute_mission_control_command_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_SUMMARY.template(),
            get(mission_control_summary_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_DELTA.template(),
            get(mission_control_delta_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_SESSIONS_BRIDGE.template(),
            post(bridge_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_INTERPRET.template(),
            post(interpret_mission_command_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_TEAMS.template(),
            get(collaboration_runs_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_RUN.template(),
            get(collaboration_run_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_CANCEL.template(),
            post(cancel_team_runtime_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_EXECUTION.template(),
            get(team_execution_plan_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_EVIDENCE.template(),
            get(team_mission_evidence_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONTROL_AGENTS_BY_AGENT_ID_EVENTS.template(),
            get(agent_mission_events_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_PROJECTION.template(),
            get(mission_projection_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES.template(),
            get(mission_schedules_handler).post(create_mission_schedule_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_TICK.template(),
            post(tick_mission_schedules_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_BY_ID_PAUSE.template(),
            post(pause_mission_schedule_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_BY_ID_RESUME.template(),
            post(resume_mission_schedule_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_BY_ID_RUN.template(),
            post(run_mission_schedule_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_BY_ID.template(),
            axum::routing::patch(update_mission_schedule_handler)
                .delete(delete_mission_schedule_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS.template(),
            get(mission_projection_handler).post(start_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID.template(),
            get(mission_session_detail_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID_SWITCH.template(),
            post(switch_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID_BACKGROUND.template(),
            post(background_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID_PAUSE.template(),
            post(pause_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID_CLOSE.template(),
            post(close_mission_session_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_APPROVALS.template(),
            get(mission_approvals_handler).post(submit_mission_approval_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_APPROVALS_BY_ID_DECISION.template(),
            post(decide_mission_approval_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_RELATIONS.template(),
            get(mission_relations_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_CONFLICTS.template(),
            get(mission_conflicts_handler),
        )
        .route(
            surface::gateway_api::paths::API_MISSION_PROXIES.template(),
            post(upsert_mission_proxy_handler),
        )
}

async fn mission_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.projection())
}

async fn mission_schedules_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.schedules())
}

async fn create_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateMissionScheduleHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .create_schedule(body)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn tick_mission_schedules_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .tick_schedules()
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn pause_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .pause_schedule(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn resume_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .resume_schedule(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn run_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .run_schedule_now(&id)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn delete_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .delete_schedule(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn update_mission_schedule_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMissionScheduleHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .update_schedule(&id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn mission_control_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MissionControlQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .mission_control(
            query.mission_id.as_deref(),
            query.detail.as_deref().unwrap_or("summary"),
        )
        .await
        .map(Json)
        .map_err(|error| {
            if error.starts_with("unsupported mission detail") {
                api_error(StatusCode::BAD_REQUEST, error)
            } else {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, error)
            }
        })
}

async fn mission_control_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MissionControlQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .mission_control_summary(query.mission_id.as_deref())
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))
}

#[derive(Debug, Default, Deserialize)]
struct MissionControlQuery {
    #[serde(default)]
    mission_id: Option<String>,
    /// `summary` (default) returns bounded mission_graph facts; `graph`
    /// returns the full graph payload (P5).
    #[serde(default)]
    detail: Option<String>,
}

async fn execute_mission_control_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(mut body): Json<harness_contract::mission::MissionCommand>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if body.actor.trim().is_empty() {
        body.actor = principal.0.claims().principal_id.clone();
    }
    state
        .services
        .mission
        .execute_mission_control_command(body)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

#[derive(Debug, Deserialize)]
struct MissionDeltaQuery {
    #[serde(default)]
    cursor: u64,
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    mission_id: Option<String>,
}

async fn mission_control_delta_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MissionDeltaQuery>,
) -> Result<Json<runtime::MissionProjectionDelta>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .materialized_delta_for(query.cursor, query.revision, query.mission_id.as_deref())
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))
}

async fn bridge_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::SessionHandoff>,
) -> impl IntoResponse {
    Json(state.services.mission.bridge_session_handoff(body).await)
}

async fn interpret_mission_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<InterpretMissionCommandHttpRequest>,
) -> impl IntoResponse {
    Json(state.services.mission.interpret_mission_command(body).await)
}

async fn collaboration_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.collaboration_runs())
}

async fn collaboration_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .collaboration_run(&team_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn cancel_team_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .cancel_team_runtime(&team_id)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn team_execution_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .team_execution_plan(&team_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn agent_mission_events_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(agent_id): Path<String>,
) -> impl IntoResponse {
    Json(state.services.mission.agent_mission_events(&agent_id))
}

async fn team_mission_evidence_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> impl IntoResponse {
    Json(state.services.mission.team_mission_evidence(&team_id))
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

async fn mission_conflicts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.conflicts())
}

async fn mission_session_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .session_detail(&id)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn submit_mission_approval_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<SubmitMissionApprovalHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .submit_approval(body)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn decide_mission_approval_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<DecideMissionApprovalHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .decide_approval(&id, body, &principal.0)
        .map(Json)
        .map_err(|error| {
            let status = if error == "approval_human_interactive_capability_required" {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            api_error(status, error)
        })
}

async fn upsert_mission_proxy_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<UpsertMissionProxyHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .upsert_proxy(body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn start_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<StartMissionSessionHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .start_session(body, principal.0.claims().principal_id.clone())
        .await
        .map_err(|error| {
            api_error(
                StatusCode::BAD_REQUEST,
                format!("failed to create Mission session: {error}"),
            )
        })
        .map(|value| (StatusCode::CREATED, Json(value)))
}

async fn switch_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(state.services.mission.switch_session(&id).await))
}

async fn background_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(state.services.mission.background_session(&id).await))
}

async fn pause_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(state.services.mission.pause_session(&id).await))
}

async fn close_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(state.services.mission.close_session(&id).await))
}
