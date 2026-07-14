use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    AddMissionRelationHttpRequest, CreateMissionScheduleHttpRequest,
    DecideMissionApprovalHttpRequest, InterpretMissionCommandHttpRequest,
    StartMissionSessionHttpRequest, SubmitMissionApprovalHttpRequest,
    UpdateMissionScheduleHttpRequest, UpsertMissionProxyHttpRequest,
};
use memory::{SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord};

use super::{api_error, AppState, AuthenticatedPrincipal, ErrorResponse};

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
            "/api/mission/sessions/:id/teams/runtime",
            post(start_mission_team_runtime_handler),
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

async fn mission_control_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mission.mission_control())
}

async fn execute_mission_control_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<runtime::MissionControlCommand>,
) -> impl IntoResponse {
    Json(
        state
            .services
            .mission
            .execute_mission_control_command(body)
            .await,
    )
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
            .graph_runner()
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

async fn start_mission_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(mut body): Json<StartMissionSessionHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = body
        .session_id
        .clone()
        .unwrap_or_else(|| format!("mission-{}", uuid::Uuid::new_v4()));
    let model = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .ok()
        .and_then(|config| config.model().map(str::to_string))
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string());
    let now = chrono::Utc::now().to_rfc3339();
    let mut record = SessionRecord {
        session_id: session_id.clone(),
        platform: "mission_control".to_string(),
        chat_id: session_id.clone(),
        user_id: None,
        model: Some(model),
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(
            serde_json::json!({
                "title": body.title.clone(),
                "source": "mission_control",
            })
            .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    };
    let mut metadata = record
        .metadata_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    metadata["workspace_root"] =
        serde_json::Value::String(state.workspace_root.display().to_string());
    record.metadata_json = Some(metadata.to_string());
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service is not configured".to_string(),
        )
    })?;
    let workspace_key = runtime.runtime_services().workspace_key().to_string();
    let request = SessionMissionOutboxRequest {
        request_id: format!(
            "mission:{workspace_key}:start:{}:{}",
            record.session_id, record.created_at
        ),
        session_id: record.session_id.clone(),
        title: body.title.clone(),
        workspace_key,
        operation: SessionMissionOutboxOperation::Start,
        created_at_ms: current_time_ms(),
    };
    state
        .services
        .session
        .upsert_stored_session_with_mission_outbox(&record, &request)
        .await
        .map_err(|error| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist Mission session intent: {error}"),
            )
        })?;
    body.session_id = Some(session_id);
    state
        .services
        .mission
        .start_session(body)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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

async fn start_mission_team_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<harness_contract::team::TeamInstantiationRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .mission
        .start_team_runtime(&id, body)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
}
