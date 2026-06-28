use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillMaintenanceEvaluateRequest,
    SkillProjectionQuery, SkillServiceError,
};
use skill::SkillActionKind;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/skills/catalog", get(skills_catalog_handler))
        .route("/api/skills/projection", get(skills_projection_handler))
        .route("/api/skills/runs", get(skill_runs_handler))
        .route("/api/skills/runs/:id", get(skill_run_detail_handler))
        .route(
            "/api/skills/maintenance/evaluate",
            post(skill_maintenance_evaluate_handler),
        )
        .route(
            "/api/skills/:id/actions/validate",
            post(skill_action_validate_handler),
        )
        .route(
            "/api/skills/:id/actions/plan",
            post(skill_action_plan_handler),
        )
        .route(
            "/api/skills/:id/actions/run",
            post(skill_action_run_handler),
        )
        .route("/api/skills/:id/files", get(skill_files_handler))
        .route("/api/skills/:id/files/raw", get(skill_file_raw_handler))
        .route("/api/skills/:id", get(skill_get_handler))
}

async fn skills_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillCatalogQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .catalog(&state.workspace_root, query)
        .map(Json)
        .map_err(skill_error)
}

async fn skills_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .projection(&state.workspace_root, query)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .runs(&state.config_home)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_run_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .run_detail(&state.config_home, &id)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_maintenance_evaluate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<SkillMaintenanceEvaluateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .maintenance_evaluate(request)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_action_validate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Validate, request)
}

async fn skill_action_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Plan, request)
}

async fn skill_action_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Run, request)
}

fn skill_action_handler(
    state: Arc<AppState>,
    id: String,
    action: SkillActionKind,
    request: SkillActionRequest,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .run_action(
            &state.workspace_root,
            &state.config_home,
            &id,
            action,
            request,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .detail(&state.workspace_root, &id)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_files_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .files(&state.workspace_root, &id)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_file_raw_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<SkillFileQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .raw_file(&state.workspace_root, &id, query)
        .map(Json)
        .map_err(skill_error)
}

fn skill_error(error: SkillServiceError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        SkillServiceError::BadRequest(message) => api_error(StatusCode::BAD_REQUEST, message),
        SkillServiceError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        SkillServiceError::Internal(message) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }
}
