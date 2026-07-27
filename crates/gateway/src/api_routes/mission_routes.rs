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
            "/api/mission/control",
            get(mission_control_handler).post(execute_mission_control_command_handler),
        )
        .route(
            "/api/mission/control/delta",
            get(mission_control_delta_handler),
        )
        .route(
            "/api/mission/control/sessions/bridge",
            post(bridge_mission_session_handler),
        )
        .route(
            "/api/mission/control/interpret",
            post(interpret_mission_command_handler),
        )
        .route(
            "/api/mission/control/teams",
            get(collaboration_runs_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/run",
            get(collaboration_run_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/cancel",
            post(cancel_team_runtime_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/execution",
            get(team_execution_plan_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/evidence",
            get(team_mission_evidence_handler),
        )
        .route(
            "/api/mission/control/agents/:agent_id/events",
            get(agent_mission_events_handler),
        )
        .route("/api/mission/projection", get(mission_projection_handler))
        .route(
            "/api/mission/schedules",
            get(mission_schedules_handler).post(create_mission_schedule_handler),
        )
        .route(
            "/api/mission/schedules/tick",
            post(tick_mission_schedules_handler),
        )
        .route(
            "/api/mission/schedules/:id/pause",
            post(pause_mission_schedule_handler),
        )
        .route(
            "/api/mission/schedules/:id/resume",
            post(resume_mission_schedule_handler),
        )
        .route(
            "/api/mission/schedules/:id",
            axum::routing::patch(update_mission_schedule_handler),
        )
        .route(
            "/api/mission/sessions",
            get(mission_projection_handler).post(start_mission_session_handler),
        )
        .route(
            "/api/mission/sessions/:id",
            get(mission_session_detail_handler),
        )
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
            "/api/mission/approvals",
            get(mission_approvals_handler).post(submit_mission_approval_handler),
        )
        .route(
            "/api/mission/approvals/:id/decision",
            post(decide_mission_approval_handler),
        )
        .route("/api/mission/relations", get(mission_relations_handler))
        .route("/api/mission/conflicts", get(mission_conflicts_handler))
        .route("/api/mission/proxies", post(upsert_mission_proxy_handler))
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
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .mission_control()
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))
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
}

async fn mission_control_delta_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MissionDeltaQuery>,
) -> Result<Json<runtime::MissionProjectionDelta>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .materialized_delta(query.cursor, query.revision)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))
}

async fn bridge_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::SessionHandoff>,
) -> impl IntoResponse {
    use runtime::ExecutionGraphHost;

    let Some(runtime_service) = state.services.runtime.as_ref().cloned() else {
        return Json(serde_json::json!({
            "ok": false,
            "kind": "mission_control.session_bridge_submission",
            "error": "runtime service unavailable"
        }));
    };
    let receipt = runtime::MissionCommandInterpreter::prepare_submission(
        runtime::MissionCommandInterpreter::interpret_session_handoff(body),
    );
    let result = match &receipt.interpretation.command {
        runtime::MissionInterpretedCommand::SubmitExecutionGraph {
            graph,
            graph_command,
        } => runtime_service
            .runtime_services()
            .execution_supervisor()
            .submit_graph(graph.clone(), graph_command.clone())
            .await
            .map(|receipt| serde_json::to_value(receipt).unwrap_or_default())
            .map_err(|error| error.to_string()),
        runtime::MissionInterpretedCommand::Blocked { reason } => Err(reason.clone()),
    };
    Json(serde_json::json!({
        "ok": result.is_ok(),
        "kind": "mission_control.session_bridge_submission",
        "receipt": receipt,
        "result": result
    }))
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
