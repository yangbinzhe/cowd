use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use memory::store::session::SessionRecord;
use serde::Deserialize;

use crate::task_kernel::{TaskRecord, TaskStatus};

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
struct StartTaskRequest {
    objective: String,
    #[serde(default)]
    yolo_mode: bool,
}

#[derive(Deserialize)]
struct TaskFailureRequest {
    reason: String,
}

#[derive(Deserialize)]
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
struct TaskPhaseArtifactRequest {
    #[serde(default = "default_task_artifact_kind")]
    kind: String,
    label: String,
    value: String,
}

#[derive(Deserialize)]
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
    append_task_runtime_event(&state, &task, "task.started")
        .await
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
    append_task_runtime_event(&state, &task, "task.phase.started")
        .await
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
    append_task_runtime_event(&state, &task, "task.phase.artifact.recorded")
        .await
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
    append_task_runtime_event(&state, &task, "task.phase.reviewed")
        .await
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
    append_task_runtime_event(&state, &task, "task.cancelled")
        .await
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
    append_task_runtime_event(&state, &task, "task.completed")
        .await
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
    append_task_runtime_event(&state, &task, kind)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(task))
}

async fn append_task_runtime_event(
    state: &AppState,
    task: &TaskRecord,
    kind: &'static str,
) -> Result<(), String> {
    ensure_task_session_record(state, task)
        .await
        .map_err(|error| format!("failed to prepare task runtime session: {error}"))?;
    let latest_audit = task.audit.last();
    let payload = serde_json::json!({
        "task": task,
        "task_id": task.id,
        "objective": task.objective,
        "status": task.status.as_str(),
        "current_phase": task.current_phase,
        "failure_count": task.failure_count,
        "latest_audit": latest_audit,
    });
    state
        .services
        .session
        .append_runtime_event(&task.id, memory::RuntimeEventScope::Task, kind, payload)
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to append task runtime event: {error}"))
}

async fn ensure_task_session_record(state: &AppState, task: &TaskRecord) -> Result<(), String> {
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    let metadata_json = serde_json::json!({
        "kind": "task",
        "task_id": task.id,
        "objective": task.objective,
        "yolo_mode": task.yolo_mode,
        "current_phase": task.current_phase,
    })
    .to_string();
    let mut record = SessionRecord {
        session_id: task.id.clone(),
        platform: "task".to_string(),
        chat_id: task.id.clone(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: task.audit.len() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: task.status.as_str().to_string(),
    };
    if let Some(existing) = store
        .get_session(&task.id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.created_at = existing.created_at;
        store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    } else {
        store
            .create_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
