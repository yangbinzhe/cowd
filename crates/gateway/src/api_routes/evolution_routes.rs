use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::services::{
    EvolutionCandidateAdoptionRequest, EvolutionCandidateCreateRequest,
    EvolutionCandidateDecisionRequest, EvolutionProposalCreateRequest,
    EvolutionProposalDecisionRequest, EvolutionServiceError, EvolutionSignalCreateRequest,
};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/evolution/signals", get(evolution_signals_handler))
        .route(
            "/api/evolution/signals",
            post(evolution_signal_create_handler),
        )
        .route("/api/evolution/diagnoses", get(evolution_diagnoses_handler))
        .route(
            "/api/evolution/diagnoses",
            post(evolution_diagnosis_create_handler),
        )
        .route(
            "/api/evolution/missions/summary",
            get(evolution_missions_summary_handler),
        )
        .route(
            "/api/evolution/missions/:id/detail",
            get(evolution_mission_detail_handler),
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
        .route("/api/evolution/chain/:id", get(evolution_chain_handler))
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
            "/api/evolution/candidates/:id",
            get(evolution_candidate_detail_handler),
        )
        .route(
            "/api/evolution/candidates/:id/decision",
            post(evolution_candidate_decision_handler),
        )
        .route(
            "/api/evolution/candidates/:id/run",
            post(evolution_candidate_sandbox_run_handler),
        )
        .route(
            "/api/evolution/candidates/:id/artifacts",
            get(evolution_candidate_artifacts_handler),
        )
        .route(
            "/api/evolution/candidates/:id/evaluate",
            post(evolution_candidate_evaluate_handler),
        )
        .route(
            "/api/evolution/candidates/:id/comparison",
            get(evolution_candidate_comparison_handler),
        )
        .route(
            "/api/evolution/candidates/:id/sandbox-eval",
            get(evolution_candidate_sandbox_eval_handler),
        )
        .route(
            "/api/evolution/candidates/:id/adoption",
            post(evolution_candidate_adoption_handler),
        )
        .route(
            "/api/evolution/candidates/:id/promote",
            post(evolution_candidate_promote_handler),
        )
        .route("/api/evolution/adoptions", get(evolution_adoptions_handler))
        .route(
            "/api/evolution/active-capabilities",
            get(evolution_active_capabilities_handler),
        )
        .route(
            "/api/evolution/versions/:id/rollback",
            post(evolution_version_rollback_handler),
        )
        .route("/api/evolution/memory", get(evolution_memory_handler))
        .route(
            "/api/evolution/memory/activation",
            get(evolution_memory_activation_handler),
        )
        .route(
            "/api/evolution/sandbox-evals",
            get(evolution_sandbox_evals_handler),
        )
}

#[derive(serde::Deserialize)]
struct EvolutionMemoryActivationParams {
    #[serde(default)]
    task: Option<String>,
}

async fn evolution_missions_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .mission_summary(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_mission_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .mission_detail(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
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

async fn evolution_diagnoses_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .diagnoses(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_diagnosis_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<EvolutionProposalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .create_diagnosis(&state.config_home, request)
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

async fn evolution_chain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .chain(&state.config_home, &id)
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

async fn evolution_candidate_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .candidate_detail(&state.config_home, &id)
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

async fn evolution_candidate_sandbox_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .run_candidate_sandbox(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_artifacts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .candidate_artifacts(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_evaluate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .evaluate_candidate(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_comparison_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .candidate_comparison(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_sandbox_eval_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .candidate_sandbox_eval(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_adoption_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<EvolutionCandidateAdoptionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .adopt_candidate(&state.config_home, &id, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_candidate_promote_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .promote_candidate(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_adoptions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .adoptions(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_active_capabilities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .active_capabilities(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_version_rollback_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .rollback_version(&state.config_home, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_memory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .evolution
        .evolution_memory(&state.config_home)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_memory_activation_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<EvolutionMemoryActivationParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = params
        .task
        .as_deref()
        .unwrap_or("inspect self evolution runtime capability");
    state
        .services
        .evolution
        .evolution_memory_activation(&state.config_home, task)
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
