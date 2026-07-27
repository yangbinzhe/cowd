use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use harness_contract::{
    reality::EvidenceRef,
    task::{TaskPhaseSpec, TaskStatus},
};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tasks", get(tasks_status_handler))
        .route("/api/tasks/start", post(start_task_handler))
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
struct StartTaskRequest {
    task_id: String,
    mission_id: String,
    source_session_id: String,
    source_turn_id: String,
    objective: String,
    #[serde(default)]
    yolo_mode: bool,
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
    let current = state
        .services
        .task
        .current()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "tasks": tasks,
        "current": current,
    })))
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
            body.source_session_id,
            body.source_turn_id,
            body.objective,
            body.yolo_mode,
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
