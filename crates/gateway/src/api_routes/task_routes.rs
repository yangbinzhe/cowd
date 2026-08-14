use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use harness_contract::{
    mission::MissionOrganizationStatus,
    reality::EvidenceRef,
    task::{TaskMissionAssignment, TaskPhaseSpec, TaskStatus},
};

use super::{
    api_error,
    session_routes::{authorize_session_access, SessionAccess},
    AppState, AuthenticatedPrincipal, ErrorResponse,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tasks", get(tasks_status_handler))
        .route(
            "/api/tasks/mission/preview",
            post(preview_tasks_mission_handler),
        )
        .route(
            "/api/tasks/mission/commit",
            post(commit_tasks_mission_handler),
        )
        .route(
            "/api/tasks/mission/organization",
            get(mission_organization_handler),
        )
        .route("/api/tasks/start", post(start_task_handler))
        .route("/api/tasks/:id", get(task_detail_handler))
        .route("/api/tasks/:id/turns", get(task_turns_handler))
        .route("/api/tasks/:id/focus", post(focus_task_handler))
        .route(
            "/api/tasks/:id/mission/preview",
            post(preview_task_mission_handler),
        )
        .route(
            "/api/tasks/:id/mission/commit",
            post(commit_task_mission_handler),
        )
        .route("/api/tasks/:id/phases", post(start_task_phase_handler))
        .route(
            "/api/tasks/:id/phases/:phase_id/artifacts",
            post(record_task_phase_artifact_handler),
        )
        .route(
            "/api/tasks/:id/phases/:phase_id/review",
            post(review_task_phase_handler),
        )
        .route("/api/tasks/:id/cancel", post(cancel_task_handler))
        .route("/api/tasks/:id/complete", post(complete_task_handler))
        .route("/api/tasks/:id/failure", post(record_task_failure_handler))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskFocusRequest {
    session_id: String,
    expected_revision: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskMissionRequest {
    operation_id: String,
    #[serde(default)]
    task_ids: Vec<String>,
    target_mission_id: String,
    assignment: TaskMissionAssignment,
    expected_task_revisions: BTreeMap<String, u64>,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
    #[serde(default)]
    confirmed: bool,
}

#[derive(Debug, Deserialize)]
struct MissionOrganizationQuery {
    #[serde(default)]
    status: Option<MissionOrganizationStatus>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct TaskDetailResponse {
    task: runtime::TaskAggregate,
    turns: Vec<harness_contract::task::TaskTurnBinding>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartTaskRequest {
    task_id: String,
    mission_id: String,
    origin_session_id: String,
    origin_turn_id: String,
    objective: String,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskFailureRequest {
    expected_revision: u64,
    reason: String,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartTaskPhaseRequest {
    expected_revision: u64,
    name: String,
    objective: String,
    #[serde(default)]
    plan: Vec<String>,
    #[serde(default)]
    acceptance: Vec<String>,
    #[serde(default)]
    test_commands: Vec<String>,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPhaseArtifactRequest {
    expected_revision: u64,
    #[serde(default = "default_task_artifact_kind")]
    kind: String,
    label: String,
    value: String,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPhaseReviewRequest {
    expected_revision: u64,
    result: String,
    #[serde(default)]
    completed: bool,
    #[serde(default)]
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskTransitionRequest {
    expected_revision: u64,
    note: String,
    evidence_refs: Vec<EvidenceRef>,
}

fn default_task_artifact_kind() -> String {
    "note".to_string()
}

async fn tasks_status_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tasks = state
        .services
        .task
        .list_records()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "tasks": tasks,
    })))
}

async fn task_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .get(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("task `{id}` not found")))?;
    let turns = state
        .services
        .task
        .bindings_for_task(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(TaskDetailResponse { task, turns }))
}

async fn task_turns_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let turns = state
        .services
        .task
        .bindings_for_task(&id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({ "task_id": id, "turns": turns })))
}

async fn focus_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<TaskFocusRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &body.session_id, SessionAccess::Write).await?;
    let receipt = state
        .services
        .session
        .set_task_focus(
            &body.session_id,
            &id,
            body.expected_revision,
            &principal.0.claims().principal_id,
        )
        .await
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(receipt))
}

async fn preview_task_mission_handler(
    state: AxumState<Arc<AppState>>,
    principal: Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(mut body): Json<TaskMissionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    body.task_ids = vec![id];
    preview_mission_assignment(state, principal, body).await
}

async fn preview_tasks_mission_handler(
    state: AxumState<Arc<AppState>>,
    principal: Extension<AuthenticatedPrincipal>,
    Json(body): Json<TaskMissionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    preview_mission_assignment(state, principal, body).await
}

async fn preview_mission_assignment(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    body: TaskMissionRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let command = state
        .services
        .task
        .assignment_command(
            body.operation_id,
            body.task_ids,
            body.target_mission_id,
            body.assignment,
            principal.0.claims().principal_id.clone(),
            body.expected_task_revisions,
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let (command, preview) = state
        .services
        .task
        .preview_mission_assignment(command)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(
        serde_json::json!({ "command": command, "preview": preview }),
    ))
}

async fn commit_task_mission_handler(
    state: AxumState<Arc<AppState>>,
    principal: Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(mut body): Json<TaskMissionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    body.task_ids = vec![id];
    commit_mission_assignment(state, principal, body).await
}

async fn commit_tasks_mission_handler(
    state: AxumState<Arc<AppState>>,
    principal: Extension<AuthenticatedPrincipal>,
    Json(body): Json<TaskMissionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    commit_mission_assignment(state, principal, body).await
}

async fn commit_mission_assignment(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    body: TaskMissionRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let confirmed = body.confirmed;
    let command = state
        .services
        .task
        .assignment_command(
            body.operation_id,
            body.task_ids,
            body.target_mission_id,
            body.assignment,
            principal.0.claims().principal_id.clone(),
            body.expected_task_revisions,
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let (command, preview) = state
        .services
        .task
        .preview_mission_assignment(command)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    if preview.items.iter().any(|item| !item.allowed) {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Mission assignment preview was rejected",
        ));
    }
    if preview.confirm_required && !confirmed {
        return Err(api_error(
            StatusCode::PRECONDITION_REQUIRED,
            "batch Mission assignment requires confirmed=true after preview",
        ));
    }
    let receipt = state
        .services
        .task
        .assign_mission_batch(&command)
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(
        serde_json::json!({ "preview": preview, "receipt": receipt }),
    ))
}

async fn mission_organization_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MissionOrganizationQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let decisions = state
        .services
        .task
        .organization_decisions(query.status, query.limit.unwrap_or(100).clamp(1, 500))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({ "decisions": decisions })))
}

async fn start_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<StartTaskRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .create(
            body.task_id,
            body.mission_id,
            body.origin_session_id,
            body.origin_turn_id,
            body.objective,
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn start_task_phase_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartTaskPhaseRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .start_phase(
            &id,
            body.expected_revision,
            TaskPhaseSpec {
                name: body.name,
                objective: body.objective,
                dependency_refs: Vec::new(),
                plan: body.plan,
                acceptance: body.acceptance,
                test_commands: body.test_commands,
            },
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn record_task_phase_artifact_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, phase_id)): Path<(String, String)>,
    Json(body): Json<TaskPhaseArtifactRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .record_phase_artifact(
            &id,
            body.expected_revision,
            &phase_id,
            body.kind,
            body.label,
            body.value,
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(task))
}

async fn review_task_phase_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, phase_id)): Path<(String, String)>,
    Json(body): Json<TaskPhaseReviewRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .review_phase(
            &id,
            body.expected_revision,
            &phase_id,
            body.result,
            body.completed,
            body.evidence_refs,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(task))
}

async fn cancel_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TaskTransitionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .transition(
            &id,
            body.expected_revision,
            TaskStatus::Cancelled,
            body.evidence_refs,
            body.note,
        )
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(task))
}

async fn complete_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TaskTransitionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .transition(
            &id,
            body.expected_revision,
            TaskStatus::Completed,
            body.evidence_refs,
            body.note,
        )
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(task))
}

async fn record_task_failure_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TaskFailureRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .record_failure(&id, body.expected_revision, body.reason, body.evidence_refs)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(task))
}
