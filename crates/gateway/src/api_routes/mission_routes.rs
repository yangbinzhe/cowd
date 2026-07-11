use std::sync::Arc;

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
    InterpretMissionCommandHttpRequest, InterruptMissionStewardHttpRequest,
    MissionSessionCommandConsumeMode, MissionTeamHandoffHttpRequest,
    RouteMissionCommandHttpRequest, StartMissionSessionHttpRequest, StartMissionStewardHttpRequest,
    StartMissionTeamRuntimeHttpRequest, SubmitAgentTaskOutcomeHttpRequest,
    SubmitMissionApprovalHttpRequest, TickMissionStewardHttpRequest, UpsertMissionProxyHttpRequest,
};
use memory::SessionRecord;

use super::{api_error, AppState, ErrorResponse};

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
            "/api/mission/control/teams/:team_id/tasks/:task_id/outcome",
            post(submit_agent_task_outcome_handler),
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
        .route("/api/mission/conflicts", get(mission_conflicts_handler))
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
    let Some(runtime_service) = state.services.runtime.as_ref().cloned() else {
        return Json(serde_json::json!({
            "envelope": state.services.mission.session_control_contract(),
            "kind": "mission_control.session_dispatch_submission",
            "ok": false,
            "error": "runtime service unavailable"
        }));
    };
    let result = runtime_service
        .route_pending_session_inputs(body.max_commands)
        .await
        .map_err(|error| error.message());
    Json(serde_json::json!({
        "envelope": state.services.mission.session_control_contract(),
        "kind": "mission_control.session_dispatch_submission",
        "ok": result.is_ok(),
        "policy": body,
        "result": result
    }))
}

async fn bridge_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::CrossSessionMessage>,
) -> impl IntoResponse {
    let Some(runtime_service) = state.services.runtime.as_ref().cloned() else {
        return Json(serde_json::json!({
            "ok": false,
            "kind": "mission_control.session_bridge_submission",
            "error": "runtime service unavailable"
        }));
    };
    let route = runtime_service
        .runtime_services()
        .session_relations()
        .route(runtime::SessionRouteCommand {
            from_session_id: body.from_session_id.clone(),
            target_ref: body.target_ref.clone(),
            command: body.command.clone(),
        });
    let Some(target_session_id) = route.resolved_session_id.clone() else {
        return Json(serde_json::json!({
            "ok": false,
            "kind": "mission_control.session_bridge_submission",
            "route": route,
            "error": "cross-session target did not resolve to a session"
        }));
    };
    let envelope = harness_contract::turn::SessionInputEnvelope::text(
        target_session_id,
        harness_contract::turn::InputSourceKind::Api,
        body.command,
    )
    .with_source_ref(format!("session:{}", body.from_session_id));
    let result = runtime_service
        .admit_session_input_with_materialized(envelope)
        .await
        .map_err(|error| error.message());
    Json(serde_json::json!({
        "ok": result.is_ok(),
        "kind": "mission_control.session_bridge_submission",
        "route": route,
        "result": result.map(|admission| serde_json::json!({
            "input": admission.receipt,
            "materialized": admission.materialized
        }))
    }))
}

async fn interpret_mission_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<InterpretMissionCommandHttpRequest>,
) -> impl IntoResponse {
    Json(state.services.mission.interpret_mission_command(body))
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

async fn submit_agent_task_outcome_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((team_id, task_id)): Path<(String, String)>,
    Json(body): Json<SubmitAgentTaskOutcomeHttpRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .submit_agent_task_outcome(&team_id, &task_id, body)
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

async fn mission_conflicts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(state.services.mission.conflicts())
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
    let session_runtime =
        ensure_active_runtime_session_for_mission_command(&state, &session_id).await?;
    let current = runtime_service
        .runtime_services()
        .mission_runtime()
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
    let envelope = harness_contract::turn::SessionInputEnvelope::text(
        session_id,
        harness_contract::turn::InputSourceKind::Api,
        current.command.clone(),
    )
    .with_source_ref(format!("mission-command:{command_id}"))
    .with_idempotency_key(format!("mission-command:{command_id}"));
    let admission = runtime_service
        .admit_session_input_with_materialized(envelope)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message()))?;
    Ok(Json(serde_json::json!({
        "envelope": state.services.mission.session_control_contract(),
        "ok": true,
        "mode": body.mode,
        "actor_id": body.actor_id,
        "reason": body.reason,
        "command": current,
        "input": admission.receipt,
        "materialized": admission.materialized,
        "session_runtime": session_runtime,
    })))
}

async fn ensure_active_runtime_session_for_mission_command(
    state: &AppState,
    session_id: &str,
) -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    if runtime_service.has_active_session(session_id) {
        return Ok(serde_json::json!({
            "session_id": session_id,
            "active": true,
            "created": false,
            "source": "existing_runtime",
        }));
    }

    let model = default_mission_session_model(state);
    let mut session = runtime::Session::new();
    session.session_id = session_id.to_string();
    session.model = Some(model.clone());
    let runtime = if let Some(store) = state.services.session.unified_store() {
        crate::runtime_factory::create_runtime_entry_with_session_store(
            store,
            runtime_service.runtime_services(),
            runtime_service.provider_registry(),
            runtime_service.tool_host(),
            session,
            session_id,
            model.clone(),
            vec![],
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    } else {
        crate::runtime_factory::create_runtime_entry(
            runtime_service.runtime_services(),
            runtime_service.provider_registry(),
            runtime_service.tool_host(),
            session,
            session_id,
            model.clone(),
            vec![],
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    }
    .map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to build runtime session for mission command: {error}"),
        )
    })?;

    runtime_service
        .register_runtime(session_id.to_string(), runtime)
        .map_err(|error| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to register runtime session for mission command: {error}"),
            )
        })?;

    if state.services.session.has_unified_store()
        && state
            .services
            .session
            .stored_session(session_id)
            .await
            .ok()
            .flatten()
            .is_none()
    {
        let record = mission_runtime_session_record(session_id, Some(model.clone()));
        state
            .services
            .session
            .upsert_stored_session(&record)
            .await
            .map_err(|error| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to persist mission runtime session: {error}"),
                )
            })?;
    }

    let _ = state
        .services
        .session
        .append_timeline_event(
            session_id,
            "MissionSessionRuntimeActivated",
            serde_json::json!({
                "type": "MissionSessionRuntimeActivated",
                "session_id": session_id,
                "model": model,
                "source": "mission_command_start_turn",
            }),
        )
        .await;

    Ok(serde_json::json!({
        "session_id": session_id,
        "active": true,
        "created": true,
        "source": "mission_command_start_turn",
        "model": model,
    }))
}

fn default_mission_session_model(state: &AppState) -> String {
    state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .ok()
        .and_then(|config| config.model().map(str::to_string))
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string())
}

fn mission_runtime_session_record(session_id: &str, model: Option<String>) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "mission_control".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(
            serde_json::json!({
                "title": format!("Mission {}", session_id.chars().take(8).collect::<String>()),
                "source": "mission_command_start_turn",
            })
            .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
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
