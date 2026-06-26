use std::{sync::Arc, time::Duration};

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    AddMissionRelationHttpRequest, AttachMissionAgentHttpRequest, AttachMissionTeamHttpRequest,
    ConsumeMissionSessionCommandHttpRequest, DecideMissionApprovalHttpRequest,
    InterruptMissionStewardHttpRequest, MissionSessionCommandConsumeMode,
    MissionTeamHandoffHttpRequest, RouteMissionCommandHttpRequest, StartMissionSessionHttpRequest,
    StartMissionStewardHttpRequest, StartMissionTeamRuntimeHttpRequest,
    SubmitMissionApprovalHttpRequest, TickMissionStewardHttpRequest, UpsertMissionProxyHttpRequest,
};

use super::{api_error, AppState, ErrorResponse};

const MISSION_COMMAND_TURN_TIMEOUT: Duration = Duration::from_secs(300);

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/mission/control",
            get(mission_control_handler).post(execute_mission_control_command_handler),
        )
        .route(
            "/api/mission/control/command",
            post(execute_mission_control_command_handler),
        )
        .route(
            "/api/mission/control/sessions/dispatch",
            post(dispatch_mission_sessions_handler),
        )
        .route(
            "/api/mission/control/sessions/bridge",
            post(bridge_mission_session_handler),
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
            "/api/mission/control/teams/:team_id/handoff",
            post(handoff_team_runtime_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/synthesis",
            post(synthesize_team_runtime_handler),
        )
        .route(
            "/api/mission/control/teams/:team_id/execution",
            get(team_execution_plan_handler).post(tick_team_execution_handler),
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
            "/api/mission/sessions/:id/inbox",
            get(mission_session_inbox_handler),
        )
        .route(
            "/api/mission/sessions/:id/inbox/:command_id",
            get(mission_session_command_detail_handler),
        )
        .route(
            "/api/mission/sessions/:id/inbox/:command_id/consume",
            post(consume_mission_session_command_handler),
        )
        .route(
            "/api/mission/sessions/:id/inbox/:command_id/cancel",
            post(cancel_mission_session_command_handler),
        )
        .route(
            "/api/mission/sessions/:id/inbox/:command_id/retry",
            post(retry_mission_session_command_handler),
        )
        .route(
            "/api/mission/sessions/:id/teams",
            post(attach_mission_team_handler),
        )
        .route(
            "/api/mission/sessions/:id/teams/runtime",
            post(start_mission_team_runtime_handler),
        )
        .route(
            "/api/mission/sessions/:id/agents",
            post(attach_mission_agent_handler),
        )
        .route(
            "/api/mission/approvals",
            get(mission_approvals_handler).post(submit_mission_approval_handler),
        )
        .route(
            "/api/mission/approvals/:id/decision",
            post(decide_mission_approval_handler),
        )
        .route(
            "/api/mission/relations",
            get(mission_relations_handler).post(add_mission_relation_handler),
        )
        .route("/api/mission/proxies", post(upsert_mission_proxy_handler))
        .route(
            "/api/mission/stewards",
            get(mission_stewards_handler).post(start_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/tick-all",
            post(tick_all_mission_stewards_handler),
        )
        .route(
            "/api/mission/control/stewards/scheduler",
            get(mission_steward_scheduler_handler).post(tick_mission_steward_scheduler_handler),
        )
        .route(
            "/api/mission/control/stewards/:id/handoff",
            get(mission_steward_scheduler_handoff_handler),
        )
        .route(
            "/api/mission/stewards/:id",
            get(mission_steward_detail_handler),
        )
        .route(
            "/api/mission/stewards/:id/tick",
            post(tick_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/:id/pause",
            post(pause_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/:id/resume",
            post(resume_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/:id/interrupt",
            post(interrupt_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/:id/takeover",
            post(takeover_mission_steward_handler),
        )
        .route(
            "/api/mission/stewards/:id/report",
            get(mission_steward_report_handler),
        )
        .route("/api/mission/route", post(route_mission_command_handler))
}

async fn mission_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.projection())
}

async fn mission_control_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mission.mission_control())
}

async fn execute_mission_control_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::MissionControlCommand>,
) -> impl IntoResponse {
    Json(state.services.mission.execute_mission_control_command(body))
}

async fn dispatch_mission_sessions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::SessionExecutionPolicy>,
) -> impl IntoResponse {
    Json(state.services.mission.dispatch_mission_sessions(body))
}

async fn bridge_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::CrossSessionMessage>,
) -> impl IntoResponse {
    Json(state.services.mission.bridge_mission_session(body))
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
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn handoff_team_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
    Json(body): Json<MissionTeamHandoffHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .handoff_team_runtime(&team_id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn synthesize_team_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .synthesize_team_runtime(&team_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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

async fn tick_team_execution_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(team_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .tick_team_execution(&team_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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

async fn mission_stewards_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mission.stewards())
}

async fn tick_all_mission_stewards_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.tick_all_stewards())
}

async fn mission_steward_scheduler_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.steward_scheduler())
}

async fn tick_mission_steward_scheduler_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::StewardSchedulerConfig>,
) -> impl IntoResponse {
    Json(state.services.mission.tick_steward_scheduler(body))
}

async fn mission_steward_scheduler_handoff_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    Json(state.services.mission.steward_scheduler_handoff(&id))
}

async fn mission_steward_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .steward_detail(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn mission_session_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .session_detail(&id)
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
    Json(body): Json<DecideMissionApprovalHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .decide_approval(&id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn start_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<StartMissionStewardHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .start_steward(body)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn tick_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TickMissionStewardHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .tick_steward(&id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn pause_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .pause_steward(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn resume_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .resume_steward(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn interrupt_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<InterruptMissionStewardHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .interrupt_steward(&id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn takeover_mission_steward_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .takeover_steward(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn mission_steward_report_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .steward_report(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn add_mission_relation_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<AddMissionRelationHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .add_relation(body)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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

async fn route_mission_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<RouteMissionCommandHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .route_command(body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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

async fn mission_session_inbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .session_inbox(&id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn mission_session_command_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, command_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .session_command_detail(&id, &command_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn consume_mission_session_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, command_id)): Path<(String, String)>,
    Json(body): Json<ConsumeMissionSessionCommandHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if body.mode == MissionSessionCommandConsumeMode::StartTurn {
        return consume_mission_session_command_as_turn(state, id, command_id, body).await;
    }
    state
        .services
        .mission
        .consume_session_command(&id, &command_id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn consume_mission_session_command_as_turn(
    state: Arc<AppState>,
    session_id: String,
    command_id: String,
    body: ConsumeMissionSessionCommandHttpRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let Some(runtime_service) = state.services.runtime.as_ref().cloned() else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        ));
    };
    if !runtime_service.has_active_session(&session_id) {
        let failed = runtime::global_mission_runtime()
            .fail_session_command(
                &session_id,
                &command_id,
                "runtime session is not active for mission command execution",
            )
            .ok();
        return Ok(Json(serde_json::json!({
            "envelope": state.services.mission.session_control_contract(),
            "ok": false,
            "mode": body.mode,
            "actor_id": body.actor_id,
            "reason": body.reason,
            "error": "runtime session is not active for mission command execution",
            "command": failed,
            "mission": runtime::global_mission_runtime().projection(),
        })));
    }
    let current = runtime::global_mission_runtime()
        .get_session_command(&command_id)
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                format!("mission session command not found: {command_id}"),
            )
        })?;
    if current.target_session_id != session_id {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("command {command_id} does not belong to session {session_id}"),
        ));
    }
    if current.status == runtime::MissionSessionCommandStatus::Pending {
        runtime::global_mission_runtime()
            .claim_session_command(&session_id, &command_id)
            .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    }
    let running = runtime::global_mission_runtime()
        .mark_session_command_running(&session_id, &command_id)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let prompt = running.command.clone();
    match runtime_service
        .run_turn_with_timeout(&session_id, None, prompt, MISSION_COMMAND_TURN_TIMEOUT)
        .await
    {
        Ok(execution) => {
            let result_ref = format!("turn:{}", execution.receipt.turn_id);
            let command = runtime::global_mission_runtime()
                .complete_session_command(&session_id, &command_id, Some(result_ref.clone()))
                .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
            Ok(Json(serde_json::json!({
                "envelope": state.services.mission.session_control_contract(),
                "ok": true,
                "mode": body.mode,
                "actor_id": body.actor_id,
                "reason": body.reason,
                "command": command,
                "turn": execution.receipt,
                "summary": {
                    "iterations": execution.summary.iterations,
                    "assistant_message_count": execution.summary.assistant_messages.len(),
                    "tool_result_count": execution.summary.tool_results.len(),
                    "prompt_cache_event_count": execution.summary.prompt_cache_events.len(),
                    "auto_compaction": execution.summary.auto_compaction.map(|event| serde_json::json!({
                        "removed_message_count": event.removed_message_count,
                    })),
                },
                "result_ref": result_ref,
                "mission": runtime::global_mission_runtime().projection(),
            })))
        }
        Err(error) => {
            let message = error.message();
            let command = runtime::global_mission_runtime()
                .fail_session_command(&session_id, &command_id, message.clone())
                .map_err(|failure| api_error(StatusCode::BAD_REQUEST, failure))?;
            Ok(Json(serde_json::json!({
                "envelope": state.services.mission.session_control_contract(),
                "ok": false,
                "mode": body.mode,
                "actor_id": body.actor_id,
                "reason": body.reason,
                "error": message,
                "command": command,
                "mission": runtime::global_mission_runtime().projection(),
            })))
        }
    }
}

async fn cancel_mission_session_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, command_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .cancel_session_command(&id, &command_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn retry_mission_session_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, command_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .retry_session_command(&id, &command_id)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

async fn attach_mission_team_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<AttachMissionTeamHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    mission_command_result(state.services.mission.attach_team(&id, body))
}

async fn start_mission_team_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartMissionTeamRuntimeHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .start_team_runtime(&id, body)
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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
