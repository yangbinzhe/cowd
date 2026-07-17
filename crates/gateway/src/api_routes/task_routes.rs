use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use crate::task_kernel::TaskStatus;

use super::{AppState, ErrorResponse, api_error};

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
    objective: String,
    #[serde(default)]
    yolo_mode: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskFailureRequest {
    reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartTaskPhaseRequest {
    name: String,
    objective: String,
    #[serde(default)]
    plan: Vec<String>,
    #[serde(default)]
    acceptance: Vec<String>,
    #[serde(default)]
    test_commands: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPhaseArtifactRequest {
    #[serde(default = "default_task_artifact_kind")]
    kind: String,
    label: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPhaseReviewRequest {
    result: String,
    #[serde(default)]
    completed: bool,
}

fn default_task_artifact_kind() -> String {
    "note".to_string()
}

async fn tasks_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tasks = state.services.task.list_records().unwrap_or_default();
    let current = state.services.task.current().unwrap_or_default();
    Json(serde_json::json!({
        "tasks": tasks,
        "current": current,
    }))
}

async fn start_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<StartTaskRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .start_goal(body.objective, body.yolo_mode)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.started")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
            body.name,
            body.objective,
            body.plan,
            body.acceptance,
            body.test_commands,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.phase.started")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .record_phase_artifact(&id, &phase_id, body.kind, body.label, body.value)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.phase.artifact.recorded")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .review_phase(&id, &phase_id, body.result, body.completed)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.phase.reviewed")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(task))
}

async fn cancel_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .transition(&id, TaskStatus::Cancelled, None, "cancelled by user")
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.cancelled")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(task))
}

async fn complete_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .services
        .task
        .transition(&id, TaskStatus::Completed, None, "accepted")
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    state
        .services
        .task
        .record_lifecycle_event(&task, "task.completed")
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .record_failure(&id, body.reason)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    let kind = if task.status == TaskStatus::Blocked {
        "task.blocked"
    } else {
        "task.failure.recorded"
    };
    state
        .services
        .task
        .record_lifecycle_event(&task, kind)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(task))
}
