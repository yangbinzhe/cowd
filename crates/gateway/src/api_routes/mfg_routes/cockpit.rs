use axum::extract::Extension;
use axum::http::{header, HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};

use crate::api_routes::{principal_actor_id, AuthenticatedPrincipal};

use super::*;

type MfgCockpitApiError = (StatusCode, Json<serde_json::Value>);

fn cockpit_mfg_api_error(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    details: serde_json::Value,
    actions: &[&str],
) -> MfgCockpitApiError {
    let error_code = match status {
        StatusCode::UNAUTHORIZED => app_mfg_contract::MfgErrorCode::AuthenticationRequired,
        StatusCode::FORBIDDEN => app_mfg_contract::MfgErrorCode::CapabilityDenied,
        StatusCode::NOT_FOUND => app_mfg_contract::MfgErrorCode::ScopeNotFound,
        StatusCode::CONFLICT => app_mfg_contract::MfgErrorCode::RevisionConflict,
        StatusCode::TOO_MANY_REQUESTS => app_mfg_contract::MfgErrorCode::RateLimited,
        status if status.is_client_error() => app_mfg_contract::MfgErrorCode::ValidationFailed,
        _ => app_mfg_contract::MfgErrorCode::Internal,
    };
    let recovery_actions = actions
        .iter()
        .filter_map(|action| {
            let kind = match *action {
                "reload" | "retry" | "edit" => app_mfg_contract::MfgRecoveryActionKind::Reload,
                "compare" => app_mfg_contract::MfgRecoveryActionKind::Compare,
                "save_as" => app_mfg_contract::MfgRecoveryActionKind::SaveAs,
                "request_access" => app_mfg_contract::MfgRecoveryActionKind::RequestAccess,
                "retry_same_intent" => app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                _ => return None,
            };
            Some(app_mfg_contract::MfgRecoveryAction {
                kind,
                label: (*action).replace('_', " "),
                target: None,
                enabled: true,
            })
        })
        .collect::<Vec<_>>();
    let mut details = details;
    if let Some(object) = details.as_object_mut() {
        object.insert(
            "legacy_code".to_string(),
            serde_json::Value::String(code.to_string()),
        );
    } else {
        details = serde_json::json!({"legacy_code": code, "detail": details});
    }
    let error = app_mfg_contract::MfgApiErrorV1 {
        code: error_code,
        message: message.into(),
        http_status: status.as_u16(),
        details,
        retryable: status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions,
        request_id: None,
        receipt_ref: None,
    };
    (
        status,
        Json(serde_json::to_value(error).unwrap_or_else(|_| {
            serde_json::json!({
                "code": "internal",
                "message": "failed to serialize MFG cockpit error",
                "http_status": 500
            })
        })),
    )
}

fn cockpit_internal_error(error: impl ToString) -> MfgCockpitApiError {
    cockpit_mfg_api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "mfg_cockpit_internal",
        error.to_string(),
        serde_json::Value::Null,
        &["retry"],
    )
}

fn cockpit_not_found(resource: &str, id: &str) -> MfgCockpitApiError {
    cockpit_mfg_api_error(
        StatusCode::NOT_FOUND,
        "mfg_cockpit_not_found",
        format!("{resource} not found"),
        serde_json::json!({ "resource": resource, "id": id }),
        &["reload"],
    )
}

fn cockpit_capability_denied(resource: &str, id: &str, required: &str) -> MfgCockpitApiError {
    cockpit_mfg_api_error(
        StatusCode::FORBIDDEN,
        "mfg_cockpit_forbidden",
        format!("{resource} is not accessible by this principal"),
        serde_json::json!({ "resource": resource, "id": id, "required": required }),
        &["request_access", "reload"],
    )
}

fn cockpit_scope_not_found(resource: &str, id: &str) -> MfgCockpitApiError {
    cockpit_mfg_api_error(
        StatusCode::NOT_FOUND,
        "mfg_scope_not_found",
        format!("{resource} was not found in the verified principal scope"),
        serde_json::json!({ "resource": resource, "id": id }),
        &["reload"],
    )
}

fn cockpit_mutation_error(error: MfgRepositoryError) -> MfgCockpitApiError {
    match error {
        MfgRepositoryError::NotFound(id) => cockpit_not_found("cockpit_profile", &id),
        MfgRepositoryError::RevisionConflict {
            domain,
            subject_id,
            expected,
            actual,
        } => cockpit_mfg_api_error(
            StatusCode::CONFLICT,
            "mfg_revision_conflict",
            "cockpit profile changed since it was loaded",
            serde_json::json!({
                "domain": domain,
                "subject_id": subject_id,
                "expected_revision": expected,
                "actual_revision": actual,
            }),
            &["reload", "compare", "save_as"],
        ),
        MfgRepositoryError::CommandRejected(message) => cockpit_mfg_api_error(
            StatusCode::CONFLICT,
            "mfg_cockpit_validation_failed",
            message,
            serde_json::Value::Null,
            &["edit", "save_as"],
        ),
        other => cockpit_internal_error(other),
    }
}

fn cockpit_profile_response(
    value: serde_json::Value,
    revision: u64,
) -> (HeaderMap, Json<serde_json::Value>) {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&format!("\"{revision}\"")) {
        headers.insert(header::ETAG, value);
    }
    (headers, Json(value))
}

fn cockpit_profile_scope_visible_to(
    profile: &MfgCockpitProfile,
    principal: &AuthenticatedPrincipal,
) -> bool {
    if profile.sharing_policy.visibility != "team" {
        return false;
    }
    let Some(scope_ref) = profile
        .scope
        .scope_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    let qualified = format!("{}:{scope_ref}", profile.scope.kind);
    principal
        .0
        .claims()
        .scopes
        .iter()
        .any(|scope| scope == scope_ref || scope == &qualified)
}

fn cockpit_widget_allowed(
    definition: &app_mfg::MfgWidgetDefinition,
    principal: &AuthenticatedPrincipal,
) -> bool {
    definition.required_capability.trim().is_empty()
        || principal.0.has_capability(&definition.required_capability)
}

fn cockpit_profile_cropped_for(
    mut profile: MfgCockpitProfile,
    principal: &AuthenticatedPrincipal,
) -> MfgCockpitProfile {
    let allowed = mfg_widget_catalog()
        .into_iter()
        .filter(|definition| cockpit_widget_allowed(definition, principal))
        .map(|definition| definition.definition_id)
        .collect::<std::collections::BTreeSet<_>>();
    profile
        .widget_instances
        .retain(|instance| allowed.contains(&instance.definition_id));
    profile
}

fn cockpit_profile_visible_to(
    profile: &MfgCockpitProfile,
    principal: &AuthenticatedPrincipal,
) -> bool {
    let actor = principal_actor_id(principal);
    profile.owner_ref == actor
        || profile
            .sharing_policy
            .editor_refs
            .iter()
            .any(|value| value == &actor)
        || profile
            .sharing_policy
            .viewer_refs
            .iter()
            .any(|value| value == &actor)
        || cockpit_profile_scope_visible_to(profile, principal)
        || profile.sharing_policy.visibility == "public"
}

fn cockpit_profile_editable_by(
    profile: &MfgCockpitProfile,
    principal: &AuthenticatedPrincipal,
) -> bool {
    let actor = principal_actor_id(principal);
    profile.owner_ref == actor
        || profile
            .sharing_policy
            .editor_refs
            .iter()
            .any(|value| value == &actor)
}

fn cockpit_profile_report_allowed(
    profile: &MfgCockpitProfile,
    principal: &AuthenticatedPrincipal,
) -> bool {
    if !cockpit_profile_visible_to(profile, principal) {
        return false;
    }
    let definitions = mfg_widget_catalog()
        .into_iter()
        .map(|definition| (definition.definition_id.clone(), definition))
        .collect::<std::collections::BTreeMap<_, _>>();
    profile.widget_instances.iter().all(|instance| {
        definitions
            .get(&instance.definition_id)
            .is_some_and(|definition| cockpit_widget_allowed(definition, principal))
    })
}

pub(super) fn cockpit_report_accessible_to(
    state: &AppState,
    report: &MfgCockpitReportSnapshot,
    principal: &AuthenticatedPrincipal,
) -> Result<bool, MfgRepositoryError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &report.profile_id)?;
    Ok(profile
        .as_ref()
        .is_some_and(|profile| cockpit_profile_report_allowed(profile, principal)))
}

fn cockpit_report_mutable_by(
    state: &AppState,
    report: &MfgCockpitReportSnapshot,
    principal: &AuthenticatedPrincipal,
) -> Result<bool, MfgRepositoryError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &report.profile_id)?;
    Ok(profile.as_ref().is_some_and(|profile| {
        cockpit_profile_editable_by(profile, principal)
            && cockpit_profile_report_allowed(profile, principal)
    }))
}

pub(super) async fn mfg_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(intent): Json<MfgCockpitReportDeliveryIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mode = normalize_mfg_action_mode(&intent.mode)
        .map_err(|error| mfg_api_error(StatusCode::UNPROCESSABLE_ENTITY, error))?;
    let dry_run = mode == "dry_run";
    let expected_revision = intent.expected_revision;
    let body_idempotency_key = intent.idempotency_key.clone();
    let capability = if dry_run {
        "mfg.read"
    } else {
        "mfg.report.deliver"
    };
    require_mfg_capability(&principal, capability)?;
    let mut request = intent.into_request(principal_actor_id(&principal));
    request.mode = mode.to_string();
    request.idempotency_key =
        if dry_run && body_idempotency_key.is_none() && headers.get("idempotency-key").is_none() {
            None
        } else {
            Some(
                mfg_idempotency_key(&headers, body_idempotency_key)
                    .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?,
            )
        };
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let owner_replay = request.idempotency_key.as_deref().is_some_and(|key| {
        state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
            .is_some()
    });
    if !dry_run && !owner_replay && expected_revision != Some(report.revision) {
        return Err(mfg_api_error(
            StatusCode::CONFLICT,
            format!(
                "report revision conflict: expected {expected_revision:?}, actual {}",
                report.revision
            ),
        ));
    }
    if !cockpit_report_accessible_to(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG cockpit report was not found in the verified principal scope",
        ));
    }
    if !cockpit_report_mutable_by(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::FORBIDDEN,
            "MFG cockpit report is not deliverable by this principal",
        ));
    }
    let outcome = deliver_mfg_cockpit_report(&state, report, request).await?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery",
        "mode": outcome.mode,
        "status": outcome.status,
        "dispatch_status": outcome.dispatch_status,
        "report": outcome.report,
        "delivery_payload": outcome.delivery_payload,
        "cross_plane_execution_receipt": outcome.cross_plane_execution_receipt,
        "idempotent_replay": outcome.idempotent_replay,
    })))
}

pub(super) async fn mfg_cockpit_report_delivery_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    reconcile_mfg_report_review_saga(&state, Some(&id), 8).await?;
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    if !cockpit_report_accessible_to(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG cockpit report was not found in the verified principal scope",
        ));
    }
    let mut delivery_state = MfgCockpitReportDeliveryState::from_report(&report);
    delivery_state.review = state
        .services
        .mfg
        .list_report_delivery_reviews(&state.config_home, Some(&id), 1)
        .map_err(mfg_mutation_error)?
        .first()
        .map(app_mfg_contract::MfgReportDeliveryReviewSummary::from);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_state",
        "report_id": report.report_id,
        "delivery_state": delivery_state,
    })))
}

pub(super) async fn mfg_cockpit_report_review_request_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<app_mfg_contract::MfgReportDeliveryReviewCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_capability(&principal, "mfg.report.deliver")?;
    let actor = principal_actor_id(&principal);
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(mfg_mutation_error)?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    if !cockpit_report_accessible_to(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG cockpit report was not found in the verified principal scope",
        ));
    }
    if !cockpit_report_mutable_by(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::FORBIDDEN,
            "MFG cockpit report review can only be requested by an authorized report operator",
        ));
    }
    let key = canonical_review_idempotency_key(&headers, request.idempotency_key.as_deref())?;
    let review = state
        .services
        .mfg
        .create_report_delivery_review(
            &state.config_home,
            &report,
            request.expected_report_revision,
            &actor,
            &request.reason,
            request.evidence_refs,
            &key,
        )
        .map_err(mfg_mutation_error)?;
    let review = reconcile_review_approval_submission(&state, review)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.report_delivery_review.requested",
        "review": review,
        "approvals_backlink": review.approval_id.as_ref().map(|id| format!("/api/approval/pending#{}", id)),
    })))
}

pub(super) async fn mfg_cockpit_report_review_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgReportReviewListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_reviewer(&principal)?;
    reconcile_mfg_report_review_saga(&state, query.report_id.as_deref(), 16).await?;
    let items = state
        .services
        .mfg
        .list_report_delivery_reviews(
            &state.config_home,
            query.report_id.as_deref(),
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .map_err(mfg_mutation_error)?
        .into_iter()
        .filter(|review| review_visible_to(&state, review, &principal))
        .collect::<Vec<_>>();
    Ok(Json(app_mfg_contract::MfgReportDeliveryReviewCollection {
        items,
        next_cursor: None,
    }))
}

pub(super) async fn mfg_cockpit_report_review_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_reviewer(&principal)?;
    reconcile_mfg_report_review_saga(&state, None, 8).await?;
    let review = state
        .services
        .mfg
        .get_report_delivery_review(&state.config_home, &id)
        .map_err(mfg_mutation_error)?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG report review not found"))?;
    if !review_visible_to(&state, &review, &principal) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG report review was not found in the verified principal scope",
        ));
    }
    Ok(Json(review))
}

pub(super) async fn mfg_cockpit_report_review_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<app_mfg_contract::MfgReportDeliveryReviewDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_reviewer(&principal)?;
    validate_review_decision_request(&request)?;
    let actor = principal_actor_id(&principal);
    let key = canonical_review_idempotency_key(&headers, request.idempotency_key.as_deref())?;
    let initial = state
        .services
        .mfg
        .get_report_delivery_review(&state.config_home, &id)
        .map_err(mfg_mutation_error)?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG report review not found"))?;
    let review = reconcile_review_approval_submission(&state, initial)?;
    if !review_visible_to(&state, &review, &principal) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG report review was not found in the verified principal scope",
        ));
    }
    if let Some(existing) = state
        .services
        .mfg
        .report_delivery_review_by_transition_key(&state.config_home, &id, &key)
        .map_err(mfg_mutation_error)?
    {
        if existing.decision != Some(request.decision) {
            return Err(mfg_api_error(
                StatusCode::CONFLICT,
                "idempotency key is bound to another report review decision",
            ));
        }
        let approval_id = existing.approval_id.clone().ok_or_else(|| {
            mfg_api_error(
                StatusCode::CONFLICT,
                "MFG report review has no correlated approval",
            )
        })?;
        let decision_lease_ref = existing.decision_lease_ref.clone().ok_or_else(|| {
            mfg_api_error(
                StatusCode::CONFLICT,
                "MFG report review has no bound decision lease",
            )
        })?;
        if existing.status == app_mfg_contract::MfgReportDeliveryReviewStatus::DecisionPendingEffect
        {
            let runtime = mfg_review_runtime_services(&state)?;
            runtime
                .approval_queue()
                .record_mfg_decision_fact(
                    &approval_id,
                    &existing.review_id,
                    &actor,
                    request.decision != app_mfg_contract::MfgReportDeliveryReviewDecision::Reject,
                    &review_decision_string(request.decision),
                    &existing.reason,
                    &decision_lease_ref,
                )
                .map_err(|error| mfg_api_error(StatusCode::CONFLICT, error))?;
            state
                .services
                .mfg
                .activate_report_delivery_review_decision(
                    &state.config_home,
                    &existing.review_id,
                    existing.revision,
                    &actor,
                    &format!("{key}:activate"),
                )
                .map_err(mfg_mutation_error)?;
        }
        reconcile_mfg_report_review_effects(&state, 8).await?;
        let review = state
            .services
            .mfg
            .get_report_delivery_review(&state.config_home, &id)
            .map_err(mfg_mutation_error)?
            .unwrap_or(existing);
        return Ok(Json(serde_json::json!({
            "kind": "mfg.report_delivery_review.decision",
            "review": review,
            "decision_lease_ref": decision_lease_ref,
            "approval_id": approval_id,
        })));
    }
    let action_id = review_decision_action_id(request.decision);
    let evidence_digest = review_decision_evidence_digest(&review, &request)?;
    let lease = issue_mfg_review_decision_lease(
        &state,
        &principal,
        &review.review_id,
        action_id,
        &format!("mfg:cockpit-report:{}", review.report_id),
        &evidence_digest,
    )?;
    let runtime = mfg_review_runtime_services(&state)?;
    runtime
        .consume_verified_decision_lease(lease.clone())
        .map_err(|error| {
            if error.contains("already been consumed") {
                mfg_api_error(StatusCode::CONFLICT, "decision_lease_already_consumed")
            } else {
                mfg_api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "runtime_decision_lease_store_unavailable",
                )
            }
        })?;
    let prepared = state
        .services
        .mfg
        .prepare_report_delivery_review_decision(
            &state.config_home,
            &review.review_id,
            request.expected_revision,
            request.decision,
            &actor,
            &request.reason,
            request.evidence_refs,
            request.reroute,
            lease.lease_id(),
            &key,
        )
        .map_err(mfg_mutation_error)?;
    let approval_id = prepared.approval_id.clone().ok_or_else(|| {
        mfg_api_error(
            StatusCode::CONFLICT,
            "MFG report review has no correlated approval",
        )
    })?;
    let decision_lease_ref = prepared.decision_lease_ref.clone().ok_or_else(|| {
        mfg_api_error(
            StatusCode::CONFLICT,
            "MFG report review has no bound decision lease",
        )
    })?;
    runtime
        .approval_queue()
        .record_mfg_decision_fact(
            &approval_id,
            &prepared.review_id,
            &actor,
            request.decision != app_mfg_contract::MfgReportDeliveryReviewDecision::Reject,
            &review_decision_string(request.decision),
            &request.reason,
            &decision_lease_ref,
        )
        .map_err(|error| mfg_api_error(StatusCode::CONFLICT, error))?;
    let activated = state
        .services
        .mfg
        .activate_report_delivery_review_decision(
            &state.config_home,
            &prepared.review_id,
            prepared.revision,
            &actor,
            &format!("{key}:activate"),
        )
        .map_err(mfg_mutation_error)?;
    reconcile_mfg_report_review_effects(&state, 8).await?;
    let review = state
        .services
        .mfg
        .get_report_delivery_review(&state.config_home, &activated.review_id)
        .map_err(mfg_mutation_error)?
        .unwrap_or(activated);
    Ok(Json(serde_json::json!({
        "kind": "mfg.report_delivery_review.decision",
        "review": review,
        "decision_lease_ref": decision_lease_ref,
        "approval_id": approval_id,
    })))
}

fn require_mfg_reviewer(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    require_mfg_capability(principal, "mfg.report.review")?;
    if principal.0.is_human_interactive() && principal.0.has_capability("approval.respond") {
        Ok(())
    } else {
        Err(mfg_api_error(
            StatusCode::FORBIDDEN,
            "MFG report review requires human-interactive approval.respond",
        ))
    }
}

fn canonical_review_idempotency_key(
    headers: &HeaderMap,
    body_key: Option<&str>,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let header_key = headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let (Some(header), Some(body)) = (header_key, body_key.map(str::trim)) {
        if !body.is_empty() && header != body {
            return Err(mfg_api_error(
                StatusCode::BAD_REQUEST,
                "Idempotency-Key conflicts with body idempotency_key",
            ));
        }
    }
    header_key
        .or_else(|| body_key.map(str::trim).filter(|value| !value.is_empty()))
        .map(str::to_string)
        .ok_or_else(|| {
            mfg_api_error(
                StatusCode::BAD_REQUEST,
                "Idempotency-Key is required for MFG review mutations",
            )
        })
}

fn reconcile_review_approval_submission(
    state: &AppState,
    review: app_mfg_contract::MfgReportDeliveryReview,
) -> Result<app_mfg_contract::MfgReportDeliveryReview, (StatusCode, Json<ErrorResponse>)> {
    if review.status != app_mfg_contract::MfgReportDeliveryReviewStatus::ApprovalSubmissionPending {
        return Ok(review);
    }
    let runtime = mfg_review_runtime_services(state)?;
    let approval_id = format!("mfg-approval:{}", review.review_id);
    let approval = runtime
        .approval_queue()
        .submit_scoped(
            approval_id,
            runtime::SubmitGlobalApprovalRequest {
                source: runtime::ApprovalSource {
                    kind: runtime::ApprovalSourceKind::Mfg,
                    session_id: None,
                    agent_id: None,
                    team_id: None,
                    mission_id: None,
                    resource_ref: Some(format!("mfg:cockpit-report:{}", review.report_id)),
                    review_ref: Some(review.review_id.clone()),
                },
                action: "mfg.report.review.typed_decision".to_string(),
                summary: format!(
                    "Review dead-letter delivery for MFG report {}",
                    review.report_id
                ),
                risk: harness_contract::core::TaskRisk::High,
                evidence_refs: review
                    .evidence_refs
                    .iter()
                    .cloned()
                    .chain([format!("digest:{}", review.dead_letter_digest)])
                    .collect(),
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            },
        )
        .map_err(|error| mfg_api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;
    state
        .services
        .mfg
        .bind_report_delivery_review_approval(
            &state.config_home,
            &review.review_id,
            review.revision,
            &approval.approval_id,
            &review.requester_principal,
            &format!("{}:approval-submitted", review.correlation_id),
        )
        .map_err(mfg_mutation_error)
}

pub(super) async fn reconcile_mfg_report_review_saga(
    state: &AppState,
    report_id: Option<&str>,
    limit: usize,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let mut mutated = false;
    let reviews = state
        .services
        .mfg
        .list_report_delivery_reviews(&state.config_home, report_id, limit)
        .map_err(mfg_mutation_error)?;
    for review in reviews {
        let original_revision = review.revision;
        let review = reconcile_review_approval_submission(state, review)?;
        let review = reconcile_prepared_review_decision(state, review)?;
        mutated |= review.revision != original_revision;
    }
    mutated |= reconcile_mfg_report_review_effects(state, limit).await?;
    if mutated {
        state.services.mfg.notify_live_mutation();
    }
    Ok(())
}

fn reconcile_prepared_review_decision(
    state: &AppState,
    review: app_mfg_contract::MfgReportDeliveryReview,
) -> Result<app_mfg_contract::MfgReportDeliveryReview, (StatusCode, Json<ErrorResponse>)> {
    if review.status != app_mfg_contract::MfgReportDeliveryReviewStatus::DecisionPendingEffect {
        return Ok(review);
    }
    let (Some(decision), Some(approval_id), Some(reviewer), Some(lease_ref)) = (
        review.decision,
        review.approval_id.as_deref(),
        review.reviewer_principal.as_deref(),
        review.decision_lease_ref.as_deref(),
    ) else {
        return Err(mfg_api_error(
            StatusCode::CONFLICT,
            "prepared MFG review decision is missing correlation fields",
        ));
    };
    let runtime = mfg_review_runtime_services(state)?;
    runtime
        .approval_queue()
        .record_mfg_decision_fact(
            approval_id,
            &review.review_id,
            reviewer,
            decision != app_mfg_contract::MfgReportDeliveryReviewDecision::Reject,
            &review_decision_string(decision),
            &review.reason,
            lease_ref,
        )
        .map_err(|error| mfg_api_error(StatusCode::CONFLICT, error))?;
    state
        .services
        .mfg
        .activate_report_delivery_review_decision(
            &state.config_home,
            &review.review_id,
            review.revision,
            reviewer,
            &format!("{}:reconcile-activate", review.correlation_id),
        )
        .map_err(mfg_mutation_error)
}

async fn reconcile_mfg_report_review_effects(
    state: &AppState,
    limit: usize,
) -> Result<bool, (StatusCode, Json<ErrorResponse>)> {
    let effects = state
        .services
        .mfg
        .claim_report_delivery_review_effects(&state.config_home, limit)
        .map_err(mfg_mutation_error)?;
    let mutated = !effects.is_empty();
    for effect in effects {
        let review = match state
            .services
            .mfg
            .get_report_delivery_review(&state.config_home, &effect.review_id)
            .map_err(mfg_mutation_error)?
        {
            Some(review) => review,
            None => {
                state
                    .services
                    .mfg
                    .fail_report_delivery_review_effect(
                        &state.config_home,
                        &effect.effect_key,
                        "review disappeared before effect execution",
                        "principal:mfg-review-reconciler",
                    )
                    .map_err(mfg_mutation_error)?;
                continue;
            }
        };
        let report = match state
            .services
            .mfg
            .get_cockpit_report(&state.config_home, &review.report_id)
            .map_err(mfg_mutation_error)?
        {
            Some(report) => report,
            None => {
                state
                    .services
                    .mfg
                    .fail_report_delivery_review_effect(
                        &state.config_home,
                        &effect.effect_key,
                        "report disappeared before effect execution",
                        "principal:mfg-review-reconciler",
                    )
                    .map_err(mfg_mutation_error)?;
                continue;
            }
        };
        let request = review_effect_delivery_request(&review, &effect, &report)?;
        match deliver_mfg_cockpit_report(state, report, request).await {
            Ok(outcome) => {
                state
                    .services
                    .mfg
                    .complete_report_delivery_review_effect(
                        &state.config_home,
                        &effect.effect_key,
                        &outcome.cross_plane_execution_receipt.id,
                        review
                            .reviewer_principal
                            .as_deref()
                            .unwrap_or("principal:mfg-review-reconciler"),
                    )
                    .map_err(mfg_mutation_error)?;
            }
            Err(error) => {
                let message = format!("{}:{:?}", error.0, error.1 .0);
                state
                    .services
                    .mfg
                    .fail_report_delivery_review_effect(
                        &state.config_home,
                        &effect.effect_key,
                        &message,
                        "principal:mfg-review-reconciler",
                    )
                    .map_err(mfg_mutation_error)?;
            }
        }
    }
    Ok(mutated)
}

fn review_effect_delivery_request(
    review: &app_mfg_contract::MfgReportDeliveryReview,
    effect: &app_mfg_contract::MfgReportDeliveryReviewEffect,
    report: &MfgCockpitReportSnapshot,
) -> Result<MfgCockpitReportDeliveryRequest, (StatusCode, Json<ErrorResponse>)> {
    let actor = review
        .reviewer_principal
        .clone()
        .unwrap_or_else(|| "principal:mfg-review-reconciler".to_string());
    match effect.action {
        app_mfg_contract::MfgReportDeliveryReviewDecision::ForceRetry => {
            Ok(MfgCockpitReportDeliveryRequest {
                mode: "commit".to_string(),
                idempotency_key: Some(effect.effect_key.clone()),
                actor_principal: actor,
                actor_identity_ref: None,
                source_channel: Some("mfg.report.review.force_retry".to_string()),
                requested_capability: None,
                provider_account: None,
                target_ref: report.delivery_ref.clone(),
                resource_ref: None,
                channel: None,
                template_id: None,
            })
        }
        app_mfg_contract::MfgReportDeliveryReviewDecision::Reroute => {
            let target = serde_json::from_value::<
                app_mfg_contract::MfgReportDeliveryReviewRerouteTarget,
            >(effect.payload.clone())
            .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
            Ok(MfgCockpitReportDeliveryRequest {
                mode: "commit".to_string(),
                idempotency_key: Some(effect.effect_key.clone()),
                actor_principal: actor,
                actor_identity_ref: None,
                source_channel: Some("mfg.report.review.reroute".to_string()),
                requested_capability: Some(target.requested_capability),
                provider_account: Some(target.provider_account),
                target_ref: Some(target.target_ref),
                resource_ref: None,
                channel: Some(target.channel),
                template_id: None,
            })
        }
        _ => Err(mfg_api_error(
            StatusCode::CONFLICT,
            "non-delivery review action reached effect outbox",
        )),
    }
}

fn review_visible_to(
    state: &AppState,
    review: &app_mfg_contract::MfgReportDeliveryReview,
    principal: &AuthenticatedPrincipal,
) -> bool {
    state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &review.report_id)
        .ok()
        .flatten()
        .is_some_and(|report| {
            cockpit_report_accessible_to(state, &report, principal).unwrap_or(false)
        })
}

fn validate_review_decision_request(
    request: &app_mfg_contract::MfgReportDeliveryReviewDecisionRequest,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    use app_mfg_contract::MfgReportDeliveryReviewDecision as D;
    match request.decision {
        D::Reroute => {
            let target = request.reroute.as_ref().ok_or_else(|| {
                mfg_api_error(StatusCode::BAD_REQUEST, "reroute target is required")
            })?;
            if !(target.target_ref.starts_with("channel://")
                || target.target_ref.starts_with("surface://"))
                || target.provider_account.trim().is_empty()
                || target.channel.trim().is_empty()
                || target.requested_capability.trim().is_empty()
            {
                return Err(mfg_api_error(
                    StatusCode::BAD_REQUEST,
                    "reroute target/provider/channel/capability is invalid",
                ));
            }
        }
        D::Resolve if request.reason.trim().is_empty() || request.evidence_refs.is_empty() => {
            return Err(mfg_api_error(
                StatusCode::BAD_REQUEST,
                "resolve requires external disposition and evidence",
            ));
        }
        D::Abandon if request.reason.trim().is_empty() => {
            return Err(mfg_api_error(
                StatusCode::BAD_REQUEST,
                "abandon requires an irreversible decision reason",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn review_decision_action_id(
    decision: app_mfg_contract::MfgReportDeliveryReviewDecision,
) -> &'static str {
    use app_mfg_contract::MfgReportDeliveryReviewDecision as D;
    match decision {
        D::ForceRetry => "mfg.report.review.force_retry",
        D::Reroute => "mfg.report.review.reroute",
        D::Abandon => "mfg.report.review.abandon",
        D::Resolve => "mfg.report.review.resolve",
        D::Reject => "mfg.report.review.reject",
    }
}

fn review_decision_string(decision: app_mfg_contract::MfgReportDeliveryReviewDecision) -> String {
    serde_json::to_value(decision)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn review_decision_evidence_digest(
    review: &app_mfg_contract::MfgReportDeliveryReview,
    request: &app_mfg_contract::MfgReportDeliveryReviewDecisionRequest,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "review_id": review.review_id,
        "report_id": review.report_id,
        "report_revision": review.report_revision,
        "dead_letter_digest": review.dead_letter_digest,
        "decision": request.decision,
        "reason": request.reason,
        "evidence_refs": request.evidence_refs,
        "reroute": request.reroute,
    }))
    .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn issue_mfg_review_decision_lease(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    review_id: &str,
    action: &str,
    scope: &str,
    evidence_digest: &str,
) -> Result<runtime::VerifiedDecisionLease, (StatusCode, Json<ErrorResponse>)> {
    let credential = state
        .auth_token
        .as_deref()
        .unwrap_or("test-only-credential");
    let (lease, public_key) = crate::api_routes::issue_human_decision_lease(
        &state.config_home,
        credential,
        review_id,
        action,
        scope,
        evidence_digest,
        crate::api_routes::current_time_ms().saturating_add(60_000),
    )
    .map_err(|_| {
        mfg_api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "decision_authority_unavailable",
        )
    })?;
    runtime::PrincipalVerifier::from_base64(&lease.key_id, &public_key)
        .map_err(|_| mfg_api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))?
        .requiring_credential_epoch(principal.0.credential_epoch())
        .verify_decision_lease(
            &lease,
            &principal.0,
            &runtime::DecisionLeaseExpectation::new(review_id, action, scope, evidence_digest),
        )
        .map_err(|_| mfg_api_error(StatusCode::FORBIDDEN, "decision_lease_verification_failed"))
}

fn mfg_review_runtime_services(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| {
            mfg_api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_approval_service_unavailable",
            )
        })
}

pub(super) async fn mfg_cockpit_report_delivery_retry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let normalized_mode = normalize_mfg_action_mode(&request.mode)
        .map_err(|error| mfg_api_error(StatusCode::UNPROCESSABLE_ENTITY, error))?;
    let expected_revision = request.expected_revision;
    let body_idempotency_key = request.idempotency_key.clone();
    let canonical_idempotency_key = if normalized_mode == "dry_run"
        && body_idempotency_key.is_none()
        && headers.get("idempotency-key").is_none()
    {
        None
    } else {
        Some(
            mfg_idempotency_key(&headers, body_idempotency_key)
                .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?,
        )
    };
    let capability = if normalized_mode == "dry_run" {
        "mfg.read"
    } else {
        "mfg.report.deliver"
    };
    require_mfg_capability(&principal, capability)?;
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let owner_replay = canonical_idempotency_key.as_deref().is_some_and(|key| {
        state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
            .is_some()
    });
    if normalized_mode != "dry_run" && !owner_replay && expected_revision != Some(report.revision) {
        return Err(mfg_api_error(
            StatusCode::CONFLICT,
            format!(
                "report revision conflict: expected {expected_revision:?}, actual {}",
                report.revision
            ),
        ));
    }
    if !cockpit_report_accessible_to(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "MFG cockpit report was not found in the verified principal scope",
        ));
    }
    if !cockpit_report_mutable_by(&state, &report, &principal)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        return Err(mfg_api_error(
            StatusCode::FORBIDDEN,
            "MFG cockpit report delivery is not retryable by this principal",
        ));
    }
    let before_state = MfgCockpitReportDeliveryState::from_report(&report);
    if normalized_mode != "dry_run" && !owner_replay && !before_state.retryable {
        return Err(mfg_api_error(
            StatusCode::CONFLICT,
            format!(
                "MFG cockpit report delivery is not retryable; request typed manual review: {}",
                before_state.classification
            ),
        ));
    }
    let mut delivery_request = mfg_retry_delivery_request(
        &report,
        &before_state,
        request,
        principal_actor_id(&principal),
    );
    delivery_request.idempotency_key = canonical_idempotency_key;
    let outcome = deliver_mfg_cockpit_report(&state, report, delivery_request).await?;
    let after_state = MfgCockpitReportDeliveryState::from_report(&outcome.report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_retry",
        "before_state": before_state,
        "after_state": after_state,
        "delivery": outcome,
    })))
}

pub(super) async fn mfg_cockpit_report_schedule_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MfgCockpitReportScheduleRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_capability(&principal, "mfg.report.generate")?;
    if request.deliver {
        require_mfg_capability(&principal, "mfg.report.deliver")?;
    }
    let limit = request.limit.unwrap_or(50).clamp(1, 100);
    let profiles = state
        .services
        .mfg
        .list_cockpit_profiles(&state.config_home, request.cadence.as_deref(), limit)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|profile| {
            cockpit_profile_editable_by(profile, &principal)
                && cockpit_profile_report_allowed(profile, &principal)
        })
        .collect::<Vec<_>>();
    let mut items = Vec::new();
    let mut delivery_count = 0usize;

    for profile in profiles {
        let report_id = request.report_id_prefix.as_ref().map(|prefix| {
            format!(
                "{}-{}",
                prefix.trim().trim_end_matches('-'),
                profile.profile_id
            )
        });
        let report = state
            .services
            .mfg
            .generate_cockpit_report(
                &state.config_home,
                &profile.profile_id,
                MfgCockpitReportRequest {
                    report_id,
                    cadence: request
                        .cadence
                        .clone()
                        .or_else(|| Some(profile.cadence.clone())),
                    delivery_ref: request
                        .delivery_ref
                        .clone()
                        .or_else(|| default_mfg_schedule_delivery_ref(&profile, &request)),
                    note: Some("scheduled cockpit report".to_string()),
                },
            )
            .map_err(|error| match error {
                MfgRepositoryError::NotFound(message) => {
                    mfg_api_error(StatusCode::NOT_FOUND, message)
                }
                other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;

        if request.deliver {
            let delivery_request = mfg_schedule_delivery_request(
                &report,
                &request,
                delivery_count,
                principal_actor_id(&principal),
            );
            let outcome = deliver_mfg_cockpit_report(&state, report, delivery_request).await?;
            delivery_count += 1;
            items.push(serde_json::json!({
                "profile_id": profile.profile_id,
                "owner_ref": profile.owner_ref,
                "cadence": profile.cadence,
                "report": outcome.report,
                "delivery": outcome,
            }));
        } else {
            items.push(serde_json::json!({
                "profile_id": profile.profile_id,
                "owner_ref": profile.owner_ref,
                "cadence": profile.cadence,
                "report": report,
                "delivery": null,
            }));
        }
    }

    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_schedule_run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "cadence": request.cadence,
        "matched_profile_count": items.len(),
        "generated_report_count": items.len(),
        "delivery_count": delivery_count,
        "items": items,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::security::{PrincipalAssurance, PrincipalClaims, PrincipalKind};

    fn principal(id: &str, scopes: &[&str], capabilities: &[&str]) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal(runtime::VerifiedPrincipal::from_test_claims(
            PrincipalClaims {
                principal_id: id.to_string(),
                kind: PrincipalKind::Human,
                scopes: scopes.iter().map(|value| (*value).to_string()).collect(),
                capabilities: capabilities
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
                assurance: PrincipalAssurance::HumanInteractive,
                issuer: "gateway-cockpit-test".to_string(),
                issued_at_ms: 1,
                expires_at_ms: None,
                credential_fingerprint: format!("fixture:{id}"),
                credential_epoch: 1,
                profile_revision: 1,
            },
        ))
    }

    fn profile(
        owner_id: &str,
        scope_kind: &str,
        scope_ref: Option<&str>,
        visibility: &str,
    ) -> MfgCockpitProfile {
        MfgCockpitProfile::from_input(app_mfg::MfgCockpitProfileInput {
            profile_id: Some(format!("profile-{scope_kind}")),
            owner_ref: format!("principal:{owner_id}"),
            display_name: Some(format!("{scope_kind} cockpit")),
            focus_refs: Vec::new(),
            focus_metric_ids: Vec::new(),
            thresholds: serde_json::Value::Null,
            template_id: None,
            cadence: None,
            expected_revision: None,
            scope: Some(app_mfg::MfgDashboardScope {
                kind: scope_kind.to_string(),
                scope_ref: scope_ref.map(str::to_string),
            }),
            layout: None,
            global_filters: serde_json::Value::Null,
            widget_instances: Vec::new(),
            sharing_policy: Some(app_mfg::MfgDashboardSharingPolicy {
                visibility: visibility.to_string(),
                viewer_refs: Vec::new(),
                editor_refs: Vec::new(),
            }),
        })
    }

    #[test]
    fn cockpit_scope_visibility_covers_team_role_organization_and_public_profiles() {
        let owner = principal("owner", &["gateway"], &["mfg.read"]);
        let unrelated = principal("unrelated", &["gateway"], &["mfg.read"]);
        assert!(cockpit_profile_visible_to(
            &profile("owner", "personal", None, "private"),
            &owner
        ));
        assert!(!cockpit_profile_visible_to(
            &profile("owner", "personal", None, "private"),
            &unrelated
        ));

        for (kind, scope_ref) in [
            ("team", "operations"),
            ("role", "planner"),
            ("organization", "factory-a"),
        ] {
            let shared = profile("owner", kind, Some(scope_ref), "team");
            let exact = principal("viewer-exact", &[scope_ref], &["mfg.read"]);
            let qualified = principal(
                "viewer-qualified",
                &[&format!("{kind}:{scope_ref}")],
                &["mfg.read"],
            );
            assert!(cockpit_profile_visible_to(&shared, &exact));
            assert!(cockpit_profile_visible_to(&shared, &qualified));
            assert!(!cockpit_profile_visible_to(&shared, &unrelated));
        }

        assert!(cockpit_profile_visible_to(
            &profile("owner", "organization", Some("factory-a"), "public"),
            &unrelated
        ));
    }

    #[test]
    fn cockpit_widgets_and_reports_are_cropped_by_server_capabilities() {
        let allowed = principal("reader", &["team:operations"], &["mfg.read"]);
        let denied = principal("observer", &["team:operations"], &[]);
        let shared = profile("owner", "team", Some("operations"), "team");

        assert_eq!(
            cockpit_profile_cropped_for(shared.clone(), &allowed)
                .widget_instances
                .len(),
            shared.widget_instances.len()
        );
        assert!(cockpit_profile_cropped_for(shared.clone(), &denied)
            .widget_instances
            .is_empty());
        assert!(cockpit_profile_report_allowed(&shared, &allowed));
        assert!(!cockpit_profile_report_allowed(&shared, &denied));
    }

    #[test]
    fn direct_viewers_can_read_but_only_editors_can_mutate_profiles() {
        let viewer = principal("viewer", &["gateway"], &["mfg.read"]);
        let editor = principal("editor", &["gateway"], &["mfg.read"]);
        let mut shared = profile("owner", "personal", None, "private");
        shared
            .sharing_policy
            .viewer_refs
            .push("principal:viewer".to_string());
        shared
            .sharing_policy
            .editor_refs
            .push("principal:editor".to_string());

        assert!(cockpit_profile_visible_to(&shared, &viewer));
        assert!(!cockpit_profile_editable_by(&shared, &viewer));
        assert!(cockpit_profile_visible_to(&shared, &editor));
        assert!(cockpit_profile_editable_by(&shared, &editor));
    }
}

pub(super) async fn deliver_mfg_cockpit_report(
    state: &AppState,
    report: MfgCockpitReportSnapshot,
    request: MfgCockpitReportDeliveryRequest,
) -> Result<MfgCockpitReportDeliveryOutcome, (StatusCode, Json<ErrorResponse>)> {
    let mode = normalize_mfg_action_mode(&request.mode)
        .map_err(|error| mfg_api_error(StatusCode::UNPROCESSABLE_ENTITY, error))?;
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    let delivery_payload = state
        .services
        .mfg
        .report_delivery_payload(&report, &request);
    let requested_action =
        state
            .services
            .mfg
            .report_delivery_action(&report, &request, &delivery_payload);
    let now = chrono::Utc::now();
    let snapshot = crate::api_routes::connector_routes::connector_snapshot(state);
    let (action, decision, evidence) =
        state
            .services
            .cross_plane
            .decide_connector_action(&snapshot, requested_action, mode, now);

    if mode == "dry_run" {
        let receipt = state.services.cross_plane.preview_action(
            idempotency_key,
            mode.to_string(),
            action,
            decision,
        );
        return Ok(MfgCockpitReportDeliveryOutcome {
            mode: receipt.mode.clone(),
            status: receipt.status.clone(),
            dispatch_status: receipt.dispatch_status.clone(),
            report,
            delivery_payload,
            cross_plane_execution_receipt: receipt,
            idempotent_replay: false,
        });
    }

    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            if !state
                .services
                .mfg
                .report_delivery_receipt_matches(&receipt, &action)
            {
                return Err(mfg_api_error(
                    StatusCode::CONFLICT,
                    "MFG cockpit report delivery idempotency key belongs to another cross-plane action",
                ));
            }
            let report = state
                .services
                .mfg
                .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
                .map_err(|error| {
                    mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                })?;
            return Ok(MfgCockpitReportDeliveryOutcome {
                mode: receipt.mode.clone(),
                status: receipt.status.clone(),
                dispatch_status: receipt.dispatch_status.clone(),
                report,
                delivery_payload,
                cross_plane_execution_receipt: receipt,
                idempotent_replay: true,
            });
        }
    }

    let receipt = if mode == "commit" && decision.decision == runtime::PolicyDecisionKind::Allow {
        let graph_key = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("mfg-report-{}", uuid::Uuid::new_v4()));
        let target = runtime::CrossPlaneDispatchTarget::from_action(
            &action,
            Some("feishu"),
            Some("send_text"),
        )
        .unwrap_or_default();
        let executor = std::sync::Arc::new(crate::services::GatewayCrossPlaneExecutor::new(
            state.services.surface.clone(),
            target.clone(),
            state.services.cross_plane.runtime_control(),
        ));
        let projection = state
            .services
            .cross_plane
            .execute_commit_graph(&action, &decision, &graph_key, Some(&target), executor)
            .await
            .map_err(mfg_cross_plane_graph_error)?;
        state
            .services
            .cross_plane
            .record_message_dispatch_graph(
                graph_key,
                action,
                decision,
                evidence,
                target,
                &projection,
            )
            .map_err(mfg_cross_plane_error)?
    } else {
        state
            .services
            .cross_plane
            .record_non_commit_action(
                idempotency_key,
                mode.to_string(),
                action,
                decision,
                evidence,
            )
            .map_err(mfg_cross_plane_error)?
    };
    let report = state
        .services
        .mfg
        .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(MfgCockpitReportDeliveryOutcome {
        mode: receipt.mode.clone(),
        status: receipt.status.clone(),
        dispatch_status: receipt.dispatch_status.clone(),
        report,
        delivery_payload,
        cross_plane_execution_receipt: receipt,
        idempotent_replay: false,
    })
}

fn default_mfg_schedule_delivery_ref(
    profile: &MfgCockpitProfile,
    request: &MfgCockpitReportScheduleRunRequest,
) -> Option<String> {
    let channel = request
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("feishu");
    profile
        .owner_ref
        .strip_prefix("user:")
        .filter(|user| !user.trim().is_empty())
        .map(|user| format!("channel://{channel}/user/{}", user.trim()))
}

fn mfg_schedule_delivery_request(
    report: &MfgCockpitReportSnapshot,
    request: &MfgCockpitReportScheduleRunRequest,
    delivery_index: usize,
    actor_principal: String,
) -> MfgCockpitReportDeliveryRequest {
    MfgCockpitReportDeliveryRequest {
        mode: request.mode.clone(),
        idempotency_key: Some(format!(
            "mfg-schedule:{}:{}:{}",
            request
                .request_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("report-run"),
            report.report_id,
            delivery_index
        )),
        actor_principal,
        actor_identity_ref: request.actor_identity_ref.clone(),
        source_channel: request
            .source_channel
            .clone()
            .or_else(|| Some("mfg.report.schedule".to_string())),
        requested_capability: request.requested_capability.clone(),
        provider_account: request.provider_account.clone(),
        target_ref: report.delivery_ref.clone(),
        resource_ref: None,
        channel: request.channel.clone(),
        template_id: request.template_id.clone(),
    }
}

fn mfg_retry_delivery_request(
    report: &MfgCockpitReportSnapshot,
    state: &MfgCockpitReportDeliveryState,
    request: MfgCockpitReportDeliveryRetryRequest,
    actor_principal: String,
) -> MfgCockpitReportDeliveryRequest {
    let latest_receipt_id = state
        .latest_receipt
        .as_ref()
        .map(|receipt| receipt.cross_plane_receipt_id.as_str())
        .unwrap_or("no-receipt");
    MfgCockpitReportDeliveryRequest {
        mode: request.mode,
        idempotency_key: request.idempotency_key.or_else(|| {
            Some(format!(
                "mfg-retry:{}:{}:{}",
                report.report_id,
                latest_receipt_id,
                state.attempt_count + 1
            ))
        }),
        actor_principal,
        actor_identity_ref: request.actor_identity_ref,
        source_channel: request
            .source_channel
            .or_else(|| Some("mfg.report.retry".to_string())),
        requested_capability: request.requested_capability,
        provider_account: request.provider_account,
        target_ref: request.target_ref.or_else(|| report.delivery_ref.clone()),
        resource_ref: request.resource_ref,
        channel: request.channel,
        template_id: request.template_id,
    }
}
