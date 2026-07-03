use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use harness_eval::HarnessEvalRunRequest;

use crate::services::HarnessEvalServiceError;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/harness-eval/reports",
            get(harness_eval_reports_handler),
        )
        .route(
            "/api/harness-eval/reports/latest",
            get(harness_eval_latest_report_handler),
        )
        .route(
            "/api/harness-eval/reports/:id",
            get(harness_eval_report_detail_handler),
        )
        .route(
            "/api/harness-eval/scenarios",
            get(harness_eval_scenarios_handler),
        )
        .route("/api/harness-eval/runs", get(harness_eval_runs_handler))
        .route(
            "/api/harness-eval/runs",
            post(harness_eval_start_run_handler),
        )
        .route(
            "/api/harness-eval/runs/:id",
            get(harness_eval_run_detail_handler),
        )
        .route(
            "/api/harness-eval/runs/:id/cancel",
            post(harness_eval_cancel_run_handler),
        )
}

async fn harness_eval_reports_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .reports(&state.config_home, config.as_ref())
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_latest_report_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .latest_report(&state.config_home, config.as_ref())
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_report_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .report_detail(&state.config_home, config.as_ref(), &id)
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_scenarios_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(state.services.harness_eval.scenarios()))
}

async fn harness_eval_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .runs(&state.config_home, config.as_ref())
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_start_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<HarnessEvalRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .start_run(&state.config_home, config.as_ref(), request)
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_run_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .run_detail(&state.config_home, config.as_ref(), &id)
        .map(Json)
        .map_err(harness_eval_error)
}

async fn harness_eval_cancel_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let config = state.runtime_config_json_snapshot();
    state
        .services
        .harness_eval
        .cancel_run(&state.config_home, config.as_ref(), &id)
        .map(Json)
        .map_err(harness_eval_error)
}

fn harness_eval_error(error: HarnessEvalServiceError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        HarnessEvalServiceError::BadRequest(_) => StatusCode::BAD_REQUEST,
        HarnessEvalServiceError::NotFound(_) => StatusCode::NOT_FOUND,
        HarnessEvalServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    api_error(status, error.message())
}
