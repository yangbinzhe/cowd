use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Extension, Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use super::{
    api_error, issue_human_decision_lease, AppState, AuthenticatedPrincipal, ErrorResponse,
};
use crate::services::{
    EvolutionProposalCreateRequest, EvolutionProposalDecisionRequest, EvolutionServiceError,
    EvolutionSignalCreateRequest,
};

fn require_evolution_release_principal(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive() && principal.0.has_capability("evolution.release.manage")
    {
        return Ok(());
    }
    Err(api_error(
        StatusCode::FORBIDDEN,
        "evolution_release_human_interactive_capability_required",
    ))
}

fn require_evolution_capability(
    principal: &AuthenticatedPrincipal,
    capability: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.has_capability(capability) {
        return Ok(());
    }
    Err(api_error(
        StatusCode::FORBIDDEN,
        format!("evolution_capability_required:{capability}"),
    ))
}

/// Issue and verify a one-time lease without consuming it. Typed Runtime
/// release decisions consume the lease in their own transaction so a Gateway
/// crash cannot leave an approval approved without the matching release event.
fn issue_evolution_decision_lease(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    review_id: impl Into<String>,
    action: impl Into<String>,
    scope: impl Into<String>,
    evidence_digest: impl Into<String>,
) -> Result<runtime::VerifiedDecisionLease, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_release_principal(principal)?;
    let review_id = review_id.into();
    let action = action.into();
    let scope = scope.into();
    let evidence_digest = evidence_digest.into();
    let expires_at_ms = now_ms().saturating_add(60_000);
    let credential = state
        .auth_token
        .as_deref()
        .unwrap_or("test-only-credential");
    let (lease, public_key) = issue_human_decision_lease(
        &state.config_home,
        credential,
        review_id.clone(),
        action.clone(),
        scope.clone(),
        evidence_digest.clone(),
        expires_at_ms,
    )
    .map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "decision_authority_unavailable",
        )
    })?;
    let verifier = runtime::PrincipalVerifier::from_base64(&lease.key_id, &public_key)
        .map_err(|_| api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))?
        .requiring_credential_epoch(principal.0.credential_epoch());
    let verified = verifier
        .verify_decision_lease(
            &lease,
            &principal.0,
            &runtime::DecisionLeaseExpectation::new(review_id, action, scope, evidence_digest),
        )
        .map_err(|_| api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))?;
    Ok(verified)
}

fn runtime_services(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_evolution_unavailable",
            )
        })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

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
            "/api/evolution/candidates",
            get(evolution_candidates_handler).post(evolution_candidate_create_handler),
        )
        .route(
            "/api/evolution/candidates/:id",
            get(evolution_candidate_detail_handler),
        )
        .route(
            "/api/evolution/candidates/:id/reviews/canary",
            post(evolution_candidate_canary_review_handler),
        )
        .route(
            "/api/evolution/candidates/:id/evaluate",
            post(evolution_candidate_evaluate_handler),
        )
        .route(
            "/api/evolution/candidates/:id/reviews/stable",
            post(evolution_candidate_stable_review_handler),
        )
        .route("/api/evolution/reviews", get(evolution_reviews_handler))
        .route(
            "/api/evolution/reviews",
            post(evolution_release_change_request_handler),
        )
        .route(
            "/api/evolution/reviews/:id",
            get(evolution_review_detail_handler),
        )
        .route(
            "/api/evolution/reviews/:id/decision",
            post(evolution_review_decision_handler),
        )
        .route(
            "/api/evolution/evaluation-policy",
            get(evolution_evaluation_policy_handler),
        )
        .route(
            "/api/evolution/evaluation-policy/reviews",
            get(evolution_evaluation_policy_reviews_handler)
                .post(evolution_evaluation_policy_change_request_handler),
        )
        .route(
            "/api/evolution/evaluation-policy/reviews/:id/decision",
            post(evolution_evaluation_policy_decision_handler),
        )
}

async fn evolution_missions_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .mission_summary(&runtime)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_mission_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .mission_detail(&runtime, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_signals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .signals(&runtime)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_signal_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionSignalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.signal.write")?;
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .create_signal(&runtime, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposals_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .proposals(&runtime)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_diagnoses_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .diagnoses(&runtime)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_diagnosis_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionProposalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.diagnosis.write")?;
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .create_diagnosis(&runtime, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionProposalCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.diagnosis.write")?;
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .create_proposal(&runtime, request)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .proposal_detail(&runtime, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_chain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    state
        .services
        .evolution
        .chain(&runtime, &id)
        .map(Json)
        .map_err(evolution_error)
}

async fn evolution_proposal_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionProposalDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    let decision = request.decision.trim();
    if !matches!(decision, "approved" | "rejected" | "archived") {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "decision must be approved, rejected, or archived",
        ));
    }
    let digest = services
        .evolution_proposal_decision_digest(&id, decision)
        .map_err(runtime_evolution_error)?;
    let lease = issue_evolution_decision_lease(
        &state,
        &principal,
        format!("evolution-proposal:{id}"),
        format!("proposal.decision.{decision}"),
        format!("evolution.proposal:{id}"),
        digest,
    )?;
    services
        .decide_evolution_proposal(&principal.0, &lease, &id, decision)
        .map(|proposal| {
            Json(serde_json::json!({
                "kind": "evolution.proposal_decision",
                "owner": "runtime",
                "proposal": proposal,
                "mainline_modified": false,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_skill_draft_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    let proposal = state
        .services
        .evolution
        .proposal_model(&runtime, &id)
        .map_err(evolution_error)?;
    Ok(Json(state.services.skill.evolution_skill_draft(&proposal)))
}

async fn evolution_candidates_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    services
        .evolution_candidates()
        .map(|candidates| {
            Json(serde_json::json!({
                "kind": "evolution.candidates",
                "owner": "runtime",
                "candidates": candidates,
            }))
        })
        .map_err(runtime_evolution_error)
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionCandidateRegistrationRequest {
    candidate_id: String,
    proposal_id: String,
    subject: runtime::EvolutionCandidateSubject,
    baseline_revision: u64,
    source_evidence_refs: Vec<harness_contract::reality::EvidenceRef>,
    #[serde(default)]
    canary_policy: runtime::CanaryRolloutPolicy,
}

async fn evolution_candidate_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionCandidateRegistrationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.candidate.register")?;
    let services = runtime_services(&state)?;
    services
        .register_evolution_candidate(runtime::EvolutionCandidateIntent {
            candidate_id: request.candidate_id,
            proposal_id: request.proposal_id,
            subject: request.subject,
            baseline_revision: request.baseline_revision,
            source_evidence_refs: request.source_evidence_refs,
            canary_policy: request.canary_policy,
        })
        .map(|candidate| {
            Json(serde_json::json!({
                "kind": "evolution.candidate",
                "owner": "runtime",
                "candidate": candidate,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_candidate_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    services
        .evolution_candidate(&id)
        .map(|candidate| {
            Json(serde_json::json!({
                "kind": "evolution.candidate",
                "owner": "runtime",
                "candidate": candidate,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_candidate_canary_review_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.review.request")?;
    let services = runtime_services(&state)?;
    services
        .request_evolution_canary_review(&id)
        .map(|review| Json(serde_json::json!({"kind": "evolution.release_review", "owner": "runtime", "review": review})))
        .map_err(runtime_evolution_error)
}

async fn evolution_candidate_evaluate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.evaluate.run")?;
    let services = runtime_services(&state)?;
    services
        .evaluate_evolution_candidate(&id)
        .await
        .map(|candidate| {
            Json(serde_json::json!({
                "kind": "evolution.candidate_evaluated",
                "owner": "runtime",
                "candidate": candidate,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_candidate_stable_review_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_capability(&principal, "evolution.review.request")?;
    let services = runtime_services(&state)?;
    services
        .request_evolution_stable_review(&id)
        .map(|review| Json(serde_json::json!({"kind": "evolution.release_review", "owner": "runtime", "review": review})))
        .map_err(runtime_evolution_error)
}

async fn evolution_reviews_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    services
        .evolution_release_reviews()
        .map(|reviews| Json(serde_json::json!({"kind": "evolution.release_reviews", "owner": "runtime", "reviews": reviews})))
        .map_err(runtime_evolution_error)
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionReleaseChangeRequest {
    request_id: String,
    subject: runtime::EvolutionCandidateSubject,
    action: runtime::ReleaseChangeAction,
    selector: Option<harness_contract::agent::RevisionSelector>,
    candidate_id: Option<String>,
    evidence_refs: Vec<harness_contract::reality::EvidenceRef>,
}

/// Human-initiated pointer, rollback, and Canary-stop actions
/// all enter Runtime as pending reviews. This endpoint cannot decide or
/// materialize a release; it only queues an auditable request.
async fn evolution_release_change_request_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionReleaseChangeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_release_principal(&principal)?;
    let services = runtime_services(&state)?;
    services
        .request_evolution_release_change(runtime::ReleaseChangeRequest {
            request_id: request.request_id,
            subject: request.subject,
            action: request.action,
            selector: request.selector,
            candidate_id: request.candidate_id,
            evidence_refs: request.evidence_refs,
        })
        .map(|review| {
            Json(serde_json::json!({
                "kind": "evolution.release_review",
                "owner": "runtime",
                "review": review,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_review_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    services
        .evolution_release_review(&id)
        .map(|review| {
            Json(serde_json::json!({
                "kind": "evolution.release_review",
                "owner": "runtime",
                "review": review,
            }))
        })
        .map_err(runtime_evolution_error)
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionReviewDecisionRequest {
    decision: runtime::ReleaseChangeReviewDecision,
    #[serde(default)]
    reason: String,
}

async fn evolution_review_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionReviewDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_release_principal(&principal)?;
    let services = runtime_services(&state)?;
    let review = services
        .evolution_release_review(&id)
        .map_err(runtime_evolution_error)?;
    let lease = issue_evolution_decision_lease(
        &state,
        &principal,
        review.review_id.clone(),
        review.action_key(),
        review.subject.scope_ref(),
        review.evidence_digest(),
    )?;
    services
        .decide_evolution_release_review(
            &principal.0,
            &lease,
            &id,
            request.decision,
            request.reason,
        )
        .map(|assignment| Json(serde_json::json!({"kind": "evolution.release_decision", "owner": "runtime", "assignment": assignment})))
        .map_err(runtime_evolution_error)
}

/// The active evaluation floor is read-only at this route. Its mutation path
/// creates a typed Runtime review and can only be decided with a verified
/// human lease; Gateway never changes candidate policy in-place.
async fn evolution_evaluation_policy_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    Ok(Json(serde_json::json!({
        "kind": "evolution.evaluation_policy",
        "owner": "runtime",
        "policy": services.evolution_evaluation_policy_floor(),
    })))
}

async fn evolution_evaluation_policy_reviews_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let services = runtime_services(&state)?;
    services
        .evolution_evaluation_policy_reviews()
        .map(|reviews| {
            Json(serde_json::json!({
                "kind": "evolution.evaluation_policy_reviews",
                "owner": "runtime",
                "reviews": reviews,
            }))
        })
        .map_err(runtime_evolution_error)
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvaluationPolicyChangeRequest {
    request_id: String,
    next_policy: harness_contract::evaluation::EvaluationPolicyFloor,
    evidence_refs: Vec<harness_contract::reality::EvidenceRef>,
}

async fn evolution_evaluation_policy_change_request_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvaluationPolicyChangeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_release_principal(&principal)?;
    let services = runtime_services(&state)?;
    services
        .request_evolution_evaluation_policy_change(runtime::EvaluationPolicyChangeIntent {
            request_id: request.request_id,
            next_policy: request.next_policy,
            evidence_refs: request.evidence_refs,
        })
        .map(|review| {
            Json(serde_json::json!({
                "kind": "evolution.evaluation_policy_review",
                "owner": "runtime",
                "review": review,
            }))
        })
        .map_err(runtime_evolution_error)
}

async fn evolution_evaluation_policy_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<EvolutionReviewDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_evolution_release_principal(&principal)?;
    let services = runtime_services(&state)?;
    let review = services
        .evolution_evaluation_policy_reviews()
        .map_err(runtime_evolution_error)?
        .into_iter()
        .find(|review| review.review_id == id)
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "evolution_evaluation_policy_review_not_found",
            )
        })?;
    let lease = issue_evolution_decision_lease(
        &state,
        &principal,
        review.review_id.clone(),
        review.action_key(),
        review.scope_ref(),
        review.evidence_digest(),
    )?;
    services
        .decide_evolution_evaluation_policy_change(
            &principal.0,
            &lease,
            &id,
            request.decision,
            request.reason,
        )
        .map(|policy| {
            Json(serde_json::json!({
                "kind": "evolution.evaluation_policy_decision",
                "owner": "runtime",
                "policy": policy,
            }))
        })
        .map_err(runtime_evolution_error)
}

fn evolution_error(error: EvolutionServiceError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        EvolutionServiceError::BadRequest(_) => StatusCode::BAD_REQUEST,
        EvolutionServiceError::NotFound(_) => StatusCode::NOT_FOUND,
        EvolutionServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    api_error(status, error.message())
}

fn runtime_evolution_error(error: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    let message = error.to_string();
    let status = if message.contains("was not found") || message.contains("not found") {
        StatusCode::NOT_FOUND
    } else if message.contains("not eligible")
        || message.contains("requires an approved active canary")
        || message.contains("not pending")
        || message.contains("does not satisfy")
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    api_error(status, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_actions_require_their_exact_capability() {
        let principal = AuthenticatedPrincipal(super::super::test_human_principal());
        for capability in [
            "evolution.signal.write",
            "evolution.diagnosis.write",
            "evolution.candidate.register",
            "evolution.evaluate.run",
            "evolution.review.request",
        ] {
            assert!(
                require_evolution_capability(&principal, capability).is_ok(),
                "core manager must receive {capability}"
            );
        }
        assert!(
            require_evolution_capability(&principal, "evolution.not-granted").is_err(),
            "authentication alone cannot authorize an evolution mutation"
        );
    }
}
