use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Extension, Multipart, Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::services::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillProjectionQuery, SkillServiceError,
};
use skill::SkillActionKind;

use super::{
    api_error, issue_human_decision_lease, AppState, AuthenticatedPrincipal, ErrorResponse,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/skills", post(skill_create_handler))
        .route("/api/skills/install/plan", post(skill_install_plan_handler))
        .route(
            "/api/skills/install/commit",
            post(skill_install_commit_handler),
        )
        .route(
            "/api/skills/install/upload/plan",
            post(skill_upload_plan_handler),
        )
        .route(
            "/api/skills/install/upload/commit",
            post(skill_upload_commit_handler),
        )
        .route("/api/skills/catalog", get(skills_catalog_handler))
        .route("/api/skills/projection", get(skills_projection_handler))
        .route("/api/skills/runs", get(skill_runs_handler))
        .route("/api/skills/runs/:id", get(skill_run_detail_handler))
        .route("/api/skills/maintenance", get(skill_maintenance_handler))
        .route(
            "/api/skills/maintenance/:id",
            get(skill_maintenance_detail_handler),
        )
        .route(
            "/api/skills/maintenance/:id/activation-reviews",
            post(skill_revision_activation_review_handler),
        )
        .route(
            "/api/skills/:id/rollback-reviews",
            post(skill_revision_rollback_review_handler),
        )
        .route(
            "/api/skills/revision-reviews/:id",
            get(skill_revision_review_handler),
        )
        .route(
            "/api/skills/revision-reviews/:id/decision",
            post(skill_revision_review_decision_handler),
        )
        .route(
            "/api/skills/:id/active-pointer",
            get(skill_active_pointer_handler),
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
        .route("/api/skills/:id/translate", post(skill_translate_handler))
        .route("/api/skills/:id/files", get(skill_files_handler))
        .route("/api/skills/:id/files/raw", get(skill_file_raw_handler))
        .route(
            "/api/skills/:id",
            get(skill_get_handler).delete(skill_delete_handler),
        )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillInstallPlanRequest {
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillInstallCommitRequest {
    source: String,
    expected_digest: String,
    #[serde(default)]
    allow_warnings: bool,
}

async fn skill_install_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(input): Json<SkillInstallPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let service = state.services.skill.clone();
    let workspace_root = state.workspace_root.clone();
    let source = input.source;
    run_skill_blocking(move || service.plan_install(&workspace_root, &source))
        .await
        .map(Json)
}

async fn skill_install_commit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(input): Json<SkillInstallCommitRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let service = state.services.skill.clone();
    let workspace_root = state.workspace_root.clone();
    let actor = format!("gateway:{}", principal.0.claims().principal_id);
    let result = run_skill_blocking(move || {
        service.commit_install(
            &workspace_root,
            &input.source,
            &input.expected_digest,
            input.allow_warnings,
            &actor,
        )
    })
    .await?;
    crate::services::invalidate_workspace_skill_snapshot(&state.workspace_root);
    Ok(Json(result))
}

async fn skill_upload_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let upload = read_skill_upload(multipart).await?;
    if upload.expected_digest.is_some() || upload.allow_warnings.is_some() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "upload plan accepts only the package field",
        ));
    }
    let service = state.services.skill.clone();
    let result = run_skill_blocking(move || {
        service.plan_uploaded_tar(&upload.archive_name, &upload.archive_bytes)
    })
    .await?;
    Ok(Json(result))
}

async fn skill_upload_commit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let upload = read_skill_upload(multipart).await?;
    let expected_digest = upload.expected_digest.ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "expected_digest is required for upload commit",
        )
    })?;
    let actor = format!("gateway:{}", principal.0.claims().principal_id);
    let service = state.services.skill.clone();
    let result = run_skill_blocking(move || {
        service.commit_uploaded_tar(
            &upload.archive_name,
            &upload.archive_bytes,
            &expected_digest,
            upload.allow_warnings.unwrap_or(false),
            &actor,
        )
    })
    .await?;
    crate::services::invalidate_workspace_skill_snapshot(&state.workspace_root);
    Ok((StatusCode::CREATED, Json(result)))
}

struct SkillUploadRequest {
    archive_name: String,
    archive_bytes: Vec<u8>,
    expected_digest: Option<String>,
    allow_warnings: Option<bool>,
}

async fn read_skill_upload(
    mut multipart: Multipart,
) -> Result<SkillUploadRequest, (StatusCode, Json<ErrorResponse>)> {
    let mut archive_name = None;
    let mut archive_bytes = None;
    let mut expected_digest = None;
    let mut allow_warnings = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
    {
        match field.name() {
            Some("package") if archive_bytes.is_none() => {
                archive_name = Some(field.file_name().unwrap_or("skill.tar").to_string());
                archive_bytes = Some(
                    read_bounded_multipart_field(field, skill::MAX_SKILL_ARCHIVE_BYTES).await?,
                );
            }
            Some("expected_digest") if expected_digest.is_none() => {
                let bytes = read_bounded_multipart_field(field, 256).await?;
                expected_digest = Some(
                    String::from_utf8(bytes)
                        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
                        .trim()
                        .to_string(),
                );
            }
            Some("allow_warnings") if allow_warnings.is_none() => {
                let bytes = read_bounded_multipart_field(field, 16).await?;
                let value = String::from_utf8(bytes)
                    .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
                allow_warnings = Some(value.trim().parse::<bool>().map_err(|_| {
                    api_error(
                        StatusCode::BAD_REQUEST,
                        "allow_warnings must be true or false",
                    )
                })?);
            }
            _ => {
                return Err(api_error(
                    StatusCode::BAD_REQUEST,
                    "upload contains an unknown or duplicate field",
                ))
            }
        }
    }
    Ok(SkillUploadRequest {
        archive_name: archive_name.unwrap_or_else(|| "skill.tar".to_string()),
        archive_bytes: archive_bytes
            .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "package is required"))?,
        expected_digest,
        allow_warnings,
    })
}

async fn read_bounded_multipart_field(
    mut field: axum::extract::multipart::Field<'_>,
    max_bytes: usize,
) -> Result<Vec<u8>, (StatusCode, Json<ErrorResponse>)> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("multipart field exceeds {max_bytes} bytes"),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn require_skill_manager(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive() && principal.0.has_capability("definition.manage") {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "skill_human_definition_manage_capability_required",
        ))
    }
}

async fn skill_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(input): Json<skill::SkillCreateInput>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let service = state.services.skill.clone();
    let actor = format!("gateway:{}", principal.0.claims().principal_id);
    let result = run_skill_blocking(move || service.create_managed(input, &actor)).await?;
    crate::services::invalidate_workspace_skill_snapshot(&state.workspace_root);
    Ok((StatusCode::CREATED, Json(result)))
}

async fn skill_delete_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    let service = state.services.skill.clone();
    let workspace_root = state.workspace_root.clone();
    let actor = format!("gateway:{}", principal.0.claims().principal_id);
    let result =
        run_skill_blocking(move || service.delete_managed(&workspace_root, &id, &actor)).await?;
    crate::services::invalidate_workspace_skill_snapshot(&state.workspace_root);
    Ok(Json(result))
}

async fn run_skill_blocking<T, F>(operation: F) -> Result<T, (StatusCode, Json<ErrorResponse>)>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SkillServiceError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            skill_error(SkillServiceError::Internal(format!(
                "Skill worker failed: {error}"
            )))
        })?
        .map_err(skill_error)
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

async fn skill_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| api_error(StatusCode::SERVICE_UNAVAILABLE, "runtime_skill_unavailable"))?;
    let drafts = runtime
        .skill_maintenance_drafts(50)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.maintenance",
        "schema_version": 1,
        "owner": "runtime",
        "health": runtime.skill_maintenance_health(),
        "count": drafts.len(),
        "drafts": drafts,
    })))
}

async fn skill_maintenance_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| api_error(StatusCode::SERVICE_UNAVAILABLE, "runtime_skill_unavailable"))?;
    let draft = runtime
        .skill_maintenance_draft(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "skill_maintenance_draft_not_found"))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.maintenance_draft",
        "schema_version": 1,
        "owner": "runtime",
        "draft": draft,
    })))
}

fn runtime_skill_services(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| api_error(StatusCode::SERVICE_UNAVAILABLE, "runtime_skill_unavailable"))
}

fn require_skill_revision_principal(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive() && principal.0.has_capability("skill.revision.manage") {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "skill_revision_human_interactive_capability_required",
        ))
    }
}

fn issue_skill_revision_decision_lease(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    review: &harness_contract::skill::SkillRevisionReview,
) -> Result<runtime::VerifiedDecisionLease, (StatusCode, Json<ErrorResponse>)> {
    require_skill_revision_principal(principal)?;
    let expires_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let expires_at_ms = expires_at_ms.saturating_add(60_000);
    let credential = state
        .auth_token
        .as_deref()
        .unwrap_or("test-only-credential");
    let action = review.action.action_key();
    let scope = review.scope_ref();
    let (lease, public_key) = issue_human_decision_lease(
        &state.config_home,
        credential,
        review.review_id.clone(),
        action,
        scope.clone(),
        review.evidence_digest.clone(),
        expires_at_ms,
    )
    .map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "decision_authority_unavailable",
        )
    })?;
    runtime::PrincipalVerifier::from_base64(&lease.key_id, &public_key)
        .map_err(|_| api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))?
        .requiring_credential_epoch(principal.0.credential_epoch())
        .verify_decision_lease(
            &lease,
            &principal.0,
            &runtime::DecisionLeaseExpectation::new(
                review.review_id.clone(),
                action,
                scope,
                review.evidence_digest.clone(),
            ),
        )
        .map_err(|_| api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRevisionActivationReviewRequest {
    request_id: String,
    target_revision: String,
}

async fn skill_revision_activation_review_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(draft_id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SkillRevisionActivationReviewRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_revision_principal(&principal)?;
    let runtime = runtime_skill_services(&state)?;
    let draft = runtime
        .skill_maintenance_draft(&draft_id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "skill_maintenance_draft_not_found"))?;
    let validation_digest = crate::services::validate_workspace_skill_revision(
        &state.workspace_root,
        &draft.skill_id,
        &request.target_revision,
    )
    .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let review = runtime
        .request_skill_revision_activation(
            &principal.0,
            &request.request_id,
            &draft_id,
            &request.target_revision,
            &validation_digest,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.revision_review",
        "owner": "runtime",
        "review": review,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRevisionRollbackReviewRequest {
    request_id: String,
    target_revision: String,
    evidence_digest: String,
}

async fn skill_revision_rollback_review_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(skill_id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SkillRevisionRollbackReviewRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_revision_principal(&principal)?;
    let runtime = runtime_skill_services(&state)?;
    let validation_digest = crate::services::validate_workspace_skill_revision(
        &state.workspace_root,
        &skill_id,
        &request.target_revision,
    )
    .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let evidence_digest = format!(
        "reason={};validated={validation_digest}",
        request.evidence_digest
    );
    let review = runtime
        .request_skill_revision_rollback(
            &principal.0,
            &request.request_id,
            &skill_id,
            &request.target_revision,
            &evidence_digest,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.revision_review",
        "owner": "runtime",
        "review": review,
    })))
}

async fn skill_revision_review_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(review_id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let review = runtime_skill_services(&state)?
        .skill_revision_review(&review_id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.revision_review",
        "owner": "runtime",
        "review": review,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRevisionReviewDecisionRequest {
    decision: harness_contract::skill::SkillRevisionReviewDecision,
    #[serde(default)]
    reason: String,
}

async fn skill_revision_review_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(review_id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SkillRevisionReviewDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_revision_principal(&principal)?;
    let runtime = runtime_skill_services(&state)?;
    let review = runtime
        .skill_revision_review(&review_id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error.to_string()))?;
    let lease = issue_skill_revision_decision_lease(&state, &principal, &review)?;
    let pointer = runtime
        .decide_skill_revision_review(
            &principal.0,
            &lease,
            &review_id,
            request.decision,
            &request.reason,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.revision_decision",
        "owner": "runtime",
        "pointer": pointer,
    })))
}

async fn skill_active_pointer_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(skill_id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let pointer = runtime_skill_services(&state)?
        .skill_active_pointer(&skill_id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "skills.active_pointer",
        "owner": "runtime",
        "skill_id": skill_id,
        "pointer": pointer,
    })))
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

#[derive(Deserialize)]
struct SkillTranslateRequest {
    content: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    locale: Option<String>,
}

async fn skill_translate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillTranslateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let content = request.content.trim();
    if content.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "content is required".to_string(),
        ));
    }
    state
        .services
        .skill
        .detail(&state.workspace_root, &id)
        .map_err(skill_error)?;

    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime services are unavailable".to_string(),
        )
    })?;
    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;
    let model = runtime_config
        .resolved_gateway_translation_model()
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "no translation or default model is configured for skill translation".to_string(),
            )
        })?;
    let cache_entries = runtime_config.gateway().translation.cache_entries;
    let runtime_services = runtime_service.runtime_services();
    let client = runtime::ProviderRuntimeClient::new_with_transport_and_template_cache(
        runtime_service.provider_registry(),
        Arc::clone(runtime_services.provider_transport_pool()),
        Arc::clone(runtime_services.provider_template_cache()),
        model.clone(),
        Vec::new(),
    )
    .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;

    let char_limit = 24_000usize;
    let truncated = content.chars().count() > char_limit;
    let source = content.chars().take(char_limit).collect::<String>();
    let locale = request
        .locale
        .as_deref()
        .map(str::trim)
        .filter(|locale| !locale.is_empty())
        .unwrap_or("zh-CN")
        .to_string();
    let path = request.path.as_deref().unwrap_or("SKILL.md");
    let cache_material = format!("skill-translation-v2\0{id}\0{path}\0{locale}\0{model}\0{source}");
    let source_digest = format!(
        "{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(source.as_bytes())
    );
    let cache_key = format!(
        "{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(cache_material.as_bytes())
    );
    if cache_entries > 0 {
        if let Some(mut cached) = state.services.skill.cached_translation(&cache_key) {
            if let Some(object) = cached.as_object_mut() {
                object.insert("cached".to_string(), serde_json::Value::Bool(true));
            }
            return Ok(Json(cached));
        }
    }
    let prompt = format!(
        "请把下面的 Skill Markdown 翻译为 {locale}。\n\
         要求：保留 Markdown 结构、代码块、YAML front matter、命令和路径；只翻译自然语言说明；不要添加额外解释。\n\n\
         ## Source metadata\n\
         - Skill: `{id}`\n\
         - Path: `{path}`\n\n\
         ## Markdown to translate\n\n\
         {source}"
    );
    let response = client
        .complete_control_analysis(
            &model,
            "你是 Skill 文档翻译器，输出必须是可直接预览的 Markdown。",
            prompt,
            4096,
        )
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;
    let translated_markdown = response.text.trim().to_string();

    let result = serde_json::json!({
        "ok": true,
        "kind": "skills.translation",
        "skill_id": id,
        "path": request.path,
        "locale": locale,
        "model": response.model,
        "translated_markdown": translated_markdown,
        "truncated": truncated,
        "cached": false,
        "source_digest": source_digest,
        "usage": {
            "input_tokens": response.input_tokens,
            "output_tokens": response.output_tokens,
        },
    });
    state
        .services
        .skill
        .cache_translation(cache_key, result.clone(), cache_entries);
    Ok(Json(result))
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
