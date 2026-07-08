use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    EvolutionCandidateCreateRequest, EvolutionCandidateDecisionRequest,
    EvolutionProposalCreateRequest, EvolutionProposalDecisionRequest, EvolutionSandboxEvalRequest,
    EvolutionServiceError, EvolutionSignalCreateRequest,
};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/evolution/signals", get(evolution_signals_handler))
        .route(
            "/api/evolution/signals",
            post(evolution_signal_create_handler),
        )
        .route("/api/evolution/proposals", get(evolution_proposals_handler))
        .route(
            "/api/evolution/proposals",
            post(evolution_proposal_create_handler),
        )
        .route(
            "/api/evolution/proposals/:id",
            get(evolution_proposal_detail_handler),
        )
        .route(
            "/api/evolution/proposals/:id/decision",
            post(evolution_proposal_decision_handler),
        )
        .route(
            "/api/evolution/proposals/:id/skill-draft",
            get(evolution_skill_draft_handler),
        )
        .route(
            "/api/evolution/proposals/:id/candidates",
            post(evolution_candidate_create_handler),
        )
        .route(
            "/api/evolution/candidates",
            get(evolution_candidates_handler),
        )
        .route(
            "/api/evolution/candidates/:id/decision",
            post(evolution_candidate_decision_handler),
        )
        .route(
            "/api/evolution/proposals/:id/sandbox-eval",
            post(evolution_sandbox_eval_start_handler),
        )
        .route(
            "/api/evolution/sandbox-evals",
            get(evolution_sandbox_evals_handler),
        )
}

async fn evolution_signals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .signals(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_signal_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<EvolutionSignalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .create_signal(&state.config_home, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .proposals(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<EvolutionProposalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .create_proposal(&state.config_home, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .proposal_detail(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<EvolutionProposalDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .decide_proposal(&state.config_home, &id, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_skill_draft_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proposal = state
        .services
        .evolution
        .proposal_model(&state.config_home, &id)
        .map_err(evolution_error)?;
    Ok(Json(state.services.skill.evolution_skill_draft(&proposal)))
}

async fn evolution_candidates_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .candidates(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<EvolutionCandidateCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .create_candidate(&state.config_home, &id, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<EvolutionCandidateDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .decide_candidate(&state.config_home, &id, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_sandbox_eval_start_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<EvolutionSandboxEvalRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .start_sandbox_eval(&state.config_home, &id, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_sandbox_evals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .sandbox_evals(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

fn evolution_error(error: EvolutionServiceError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        EvolutionServiceError::BadRequest(_) => StatusCode::BAD_REQUEST,
        EvolutionServiceError::NotFound(_) => StatusCode::NOT_FOUND,
        EvolutionServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    api_error(status, error.message())
}
