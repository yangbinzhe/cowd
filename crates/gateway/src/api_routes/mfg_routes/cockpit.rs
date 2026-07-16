use axum::extract::Extension;
use axum::http::{header, HeaderMap, HeaderValue};

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

pub(super) async fn mfg_cockpit_profile_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgCockpitProfileListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profiles = state
        .services
        .mfg
        .list_cockpit_profiles(
            &state.config_home,
            query.cadence.as_deref(),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|profile| cockpit_profile_visible_to(profile, &principal))
        .map(|profile| cockpit_profile_cropped_for(profile, &principal))
        .collect::<Vec<_>>();
    Ok(Json(
        serde_json::json!({ "kind": "mfg.cockpit.profile_list", "items": profiles }),
    ))
}

pub(super) async fn mfg_cockpit_widget_catalog_handler(
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "mfg.cockpit.widget_catalog",
        "items": mfg_widget_catalog().into_iter().filter(|definition| cockpit_widget_allowed(definition, &principal)).collect::<Vec<_>>(),
        "global_filter_schema": mfg_cockpit_global_filter_schema(),
        "filter_merge_policy": mfg_cockpit_filter_merge_policy(),
    }))
}

pub(super) async fn mfg_cockpit_profile_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(mut request): Json<MfgCockpitProfileUpsertRequest>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let expected_revision = request.profile.expected_revision;
    let actor = principal_actor_id(&principal);
    let idempotency_key =
        mfg_idempotency_key(&headers, request.idempotency_key.take()).map_err(|error| {
            cockpit_mfg_api_error(
                StatusCode::BAD_REQUEST,
                "mfg_idempotency_key_invalid",
                error.message,
                serde_json::Value::Null,
                &["retry_same_intent"],
            )
        })?;
    request
        .profile
        .profile_id
        .get_or_insert_with(|| stable_mfg_resource_id("cockpit-profile", &idempotency_key));
    let mut effective_owner = actor.clone();
    if let Some(profile_id) = request
        .profile
        .profile_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if let Some(existing) = state
            .services
            .mfg
            .get_cockpit_profile(&state.config_home, profile_id)
            .map_err(cockpit_internal_error)?
        {
            if existing.owner_ref != actor && !existing.sharing_policy.editor_refs.contains(&actor)
            {
                return Err(cockpit_scope_not_found("cockpit_profile", profile_id));
            }
            effective_owner = existing.owner_ref;
        }
    }
    request.profile.owner_ref = effective_owner;
    for instance in &request.profile.widget_instances {
        let definition = mfg_widget_catalog()
            .into_iter()
            .find(|definition| definition.definition_id == instance.definition_id)
            .ok_or_else(|| {
                cockpit_not_found("cockpit_widget_definition", &instance.definition_id)
            })?;
        if !cockpit_widget_allowed(&definition, &principal) {
            return Err(cockpit_capability_denied(
                "cockpit_widget_definition",
                &instance.definition_id,
                &definition.required_capability,
            ));
        }
    }
    let (profile, receipt) = state
        .services
        .mfg
        .upsert_cockpit_profile_receipted(
            &state.config_home,
            &MfgCockpitProfile::from_input(request.profile),
            expected_revision,
            "profile.upsert",
            &principal_actor_id(&principal),
            &idempotency_key,
        )
        .map_err(cockpit_mutation_error)?;
    let revision = profile.revision;
    let receipt = receipt
        .canonical_receipt()
        .map_err(cockpit_internal_error)?;
    let profile = cockpit_profile_cropped_for(profile, &principal);
    Ok(cockpit_profile_response(
        serde_json::json!({
            "kind": "mfg.cockpit.profile",
            "request_id": request.request_id,
            "session_id": request.session_id,
            "profile": profile,
            "receipt": receipt,
        }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_profile_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if !cockpit_profile_visible_to(&profile, &principal) {
        return Err(cockpit_scope_not_found("cockpit_profile", &id));
    }
    let revision = profile.revision;
    let profile = cockpit_profile_cropped_for(profile, &principal);
    Ok(cockpit_profile_response(
        serde_json::json!({
            "kind": "mfg.cockpit.profile",
            "profile": profile,
        }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_profile_delete_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Query(query): Query<MfgCockpitProfileDeleteQuery>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let existing = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?;
    let actor = principal_actor_id(&principal);
    if let Some(existing) = existing {
        if existing.owner_ref != actor && !existing.sharing_policy.editor_refs.contains(&actor) {
            return Err(cockpit_scope_not_found("cockpit_profile", &id));
        }
    }
    let idempotency_key =
        mfg_idempotency_key(&headers, query.idempotency_key).map_err(|error| {
            cockpit_mfg_api_error(
                StatusCode::BAD_REQUEST,
                "mfg_idempotency_key_invalid",
                error.message,
                serde_json::Value::Null,
                &["retry_same_intent"],
            )
        })?;
    let (profile, receipt) = state
        .services
        .mfg
        .delete_cockpit_profile_receipted(
            &state.config_home,
            &id,
            query.expected_revision,
            &actor,
            &idempotency_key,
        )
        .map_err(cockpit_mutation_error)?;
    let revision = receipt.current_revision;
    let receipt = receipt
        .canonical_receipt()
        .map_err(cockpit_internal_error)?;
    Ok(cockpit_profile_response(
        serde_json::json!({ "kind": "mfg.cockpit.profile_deleted", "profile": profile, "receipt": receipt }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_profile_clone_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgCockpitProfileCloneRequest>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let idempotency_key =
        mfg_idempotency_key(&headers, request.idempotency_key.clone()).map_err(|error| {
            cockpit_mfg_api_error(
                StatusCode::BAD_REQUEST,
                "mfg_idempotency_key_invalid",
                error.message,
                serde_json::Value::Null,
                &["retry_same_intent"],
            )
        })?;
    let source = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if !cockpit_profile_visible_to(&source, &principal) {
        return Err(cockpit_scope_not_found("cockpit_profile", &id));
    }
    let mut clone = cockpit_profile_cropped_for(source, &principal);
    clone.profile_id = request
        .profile_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| stable_mfg_resource_id("cockpit-profile", &idempotency_key));
    clone.owner_ref = principal_actor_id(&principal);
    clone.display_name = request
        .display_name
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{} copy", clone.display_name));
    clone.revision = 1;
    clone.created_at = chrono::Utc::now();
    clone.updated_at = clone.created_at;
    clone.sharing_policy = Default::default();
    if let Some(existing) = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &clone.profile_id)
        .map_err(cockpit_internal_error)?
    {
        let actor = principal_actor_id(&principal);
        if existing.owner_ref != actor && !existing.sharing_policy.editor_refs.contains(&actor) {
            return Err(cockpit_scope_not_found(
                "cockpit_profile",
                &clone.profile_id,
            ));
        }
    }
    let (clone, receipt) = state
        .services
        .mfg
        .upsert_cockpit_profile_receipted(
            &state.config_home,
            &clone,
            None,
            "profile.clone",
            &principal_actor_id(&principal),
            &idempotency_key,
        )
        .map_err(cockpit_mutation_error)?;
    let revision = clone.revision;
    let receipt = receipt
        .canonical_receipt()
        .map_err(cockpit_internal_error)?;
    let clone = cockpit_profile_cropped_for(clone, &principal);
    Ok(cockpit_profile_response(
        serde_json::json!({ "kind": "mfg.cockpit.profile_cloned", "source_profile_id": id, "profile": clone, "receipt": receipt }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_profile_share_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgCockpitProfileShareRequest>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let idempotency_key =
        mfg_idempotency_key(&headers, request.idempotency_key.clone()).map_err(|error| {
            cockpit_mfg_api_error(
                StatusCode::BAD_REQUEST,
                "mfg_idempotency_key_invalid",
                error.message,
                serde_json::Value::Null,
                &["retry_same_intent"],
            )
        })?;
    let mut profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if profile.owner_ref != principal_actor_id(&principal)
        && !profile
            .sharing_policy
            .editor_refs
            .contains(&principal_actor_id(&principal))
    {
        return Err(cockpit_scope_not_found("cockpit_profile", &id));
    }
    profile.sharing_policy = request.sharing_policy;
    let (profile, receipt) = state
        .services
        .mfg
        .upsert_cockpit_profile_receipted(
            &state.config_home,
            &profile,
            Some(request.expected_revision),
            "profile.share",
            &principal_actor_id(&principal),
            &idempotency_key,
        )
        .map_err(cockpit_mutation_error)?;
    let revision = profile.revision;
    let receipt = receipt
        .canonical_receipt()
        .map_err(cockpit_internal_error)?;
    let profile = cockpit_profile_cropped_for(profile, &principal);
    Ok(cockpit_profile_response(
        serde_json::json!({ "kind": "mfg.cockpit.profile_shared", "profile": profile, "receipt": receipt }),
        revision,
    ))
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

fn cockpit_projection_filters(
    profile: &MfgCockpitProfile,
    query: MfgCockpitProjectionQuery,
) -> serde_json::Value {
    let mut filters = profile
        .global_filters
        .as_object()
        .cloned()
        .unwrap_or_default();
    for (key, value) in [
        ("entity_refs", query.entity),
        ("metric_ids", query.metric),
        ("severities", query.severity),
        ("statuses", query.status),
    ] {
        if let Some(value) = value {
            let values = value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            if values.is_empty() {
                filters.remove(key);
            } else {
                filters.insert(key.to_string(), serde_json::json!(values));
            }
        }
    }
    for (key, value) in [("from", query.from), ("to", query.to)] {
        if let Some(value) = value {
            if value.trim().is_empty() {
                filters.remove(key);
            } else {
                filters.insert(key.to_string(), serde_json::json!(value.trim()));
            }
        }
    }
    serde_json::Value::Object(filters)
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

pub(super) async fn mfg_cockpit_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgCockpitProjectionQuery>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if !cockpit_profile_visible_to(&profile, &principal) {
        return Err(cockpit_scope_not_found("cockpit_profile", &id));
    }
    let filters = cockpit_projection_filters(&profile, query);
    let mut projection = state
        .services
        .mfg
        .cockpit_projection_with_filters(&state.config_home, &id, filters)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => cockpit_not_found("cockpit_profile", &message),
            other => cockpit_internal_error(other),
        })?;
    let original_widget_count = projection.widgets.len();
    projection.profile = cockpit_profile_cropped_for(projection.profile, &principal);
    let visible_instances = projection
        .profile
        .widget_instances
        .iter()
        .map(|instance| instance.instance_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    projection
        .widgets
        .retain(|widget| visible_instances.contains(widget.instance_id.as_str()));
    if projection.widgets.len() != original_widget_count {
        projection.summary = format!(
            "{} permission_cropped={}",
            projection.summary,
            original_widget_count.saturating_sub(projection.widgets.len())
        );
    }
    let revision = projection.profile.revision;
    Ok(cockpit_profile_response(
        serde_json::json!({
            "kind": "mfg.cockpit.projection", "projection": projection,
        }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_widget_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((id, instance_id)): AxumPath<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgCockpitProjectionQuery>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if !cockpit_profile_visible_to(&profile, &principal) {
        return Err(cockpit_scope_not_found("cockpit_profile", &id));
    }
    let instance = profile
        .widget_instances
        .iter()
        .find(|instance| instance.instance_id == instance_id)
        .ok_or_else(|| cockpit_not_found("cockpit_widget", &instance_id))?;
    let definition = mfg_widget_catalog()
        .into_iter()
        .find(|definition| definition.definition_id == instance.definition_id)
        .ok_or_else(|| cockpit_not_found("cockpit_widget_definition", &instance.definition_id))?;
    if !cockpit_widget_allowed(&definition, &principal) {
        return Err(cockpit_capability_denied(
            "cockpit_widget",
            &instance_id,
            &definition.required_capability,
        ));
    }
    let filters = cockpit_projection_filters(&profile, query);
    let projection = state
        .services
        .mfg
        .cockpit_widget_projection_with_filters(&state.config_home, &id, &instance_id, filters)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(_) => cockpit_not_found("cockpit_widget", &instance_id),
            other => cockpit_internal_error(other),
        })?;
    let revision = projection.profile_revision;
    Ok(cockpit_profile_response(
        serde_json::json!({
            "kind": "mfg.cockpit.widget_projection", "projection": projection,
        }),
        revision,
    ))
}

pub(super) async fn mfg_cockpit_report_generate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MfgCockpitReportGenerateRequest>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_profile", &id))?;
    if !cockpit_profile_editable_by(&profile, &principal)
        || !cockpit_profile_report_allowed(&profile, &principal)
    {
        return Err(cockpit_capability_denied(
            "cockpit_profile_report",
            &id,
            "owner_or_editor_and_all_widget_capabilities",
        ));
    }
    let report = state
        .services
        .mfg
        .generate_cockpit_report(&state.config_home, &id, request.report)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => cockpit_not_found("cockpit_profile", &message),
            other => cockpit_internal_error(other),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "report": report,
    })))
}

pub(super) async fn mfg_cockpit_report_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgCockpitReportListQuery>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let reports = state
        .services
        .mfg
        .list_cockpit_reports(
            &state.config_home,
            query.profile_id.as_deref(),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(cockpit_internal_error)?;
    let items = reports
        .into_iter()
        .filter_map(
            |report| match cockpit_report_accessible_to(&state, &report, &principal) {
                Ok(true) => Some(Ok(report)),
                Ok(false) => None,
                Err(error) => Some(Err(cockpit_internal_error(error))),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_list",
        "items": items,
    })))
}

pub(super) async fn mfg_cockpit_report_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, MfgCockpitApiError> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(cockpit_internal_error)?
        .ok_or_else(|| cockpit_not_found("cockpit_report", &id))?;
    if !cockpit_report_accessible_to(&state, &report, &principal).map_err(cockpit_internal_error)? {
        return Err(cockpit_scope_not_found("cockpit_report", &id));
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "report": report,
    })))
}

pub(super) async fn mfg_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(intent): Json<MfgCockpitReportDeliveryIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let capability = if intent.mode.trim().eq_ignore_ascii_case("dry_run") {
        "mfg.read"
    } else {
        "mfg.report.deliver"
    };
    require_mfg_capability(&principal, capability)?;
    let request = intent.into_request(principal_actor_id(&principal));
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
    let delivery_state = MfgCockpitReportDeliveryState::from_report(&report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_state",
        "report_id": report.report_id,
        "delivery_state": delivery_state,
    })))
}

pub(super) async fn mfg_cockpit_report_delivery_retry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MfgCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let normalized_mode = state.services.mfg.normalize_bridge_mode(&request.mode);
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
    if !before_state.retryable && !request.force {
        return Err(mfg_api_error(
            StatusCode::CONFLICT,
            format!(
                "MFG cockpit report delivery is not retryable: {}",
                before_state.classification
            ),
        ));
    }
    let delivery_request = mfg_retry_delivery_request(
        &report,
        &before_state,
        request,
        principal_actor_id(&principal),
    );
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
    let mode = state.services.mfg.normalize_bridge_mode(&request.mode);
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

    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            if !state
                .services
                .mfg
                .report_delivery_receipt_matches(&receipt, &report)
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

    let action = state
        .services
        .mfg
        .report_delivery_action(&report, &request, &delivery_payload);
    let now = chrono::Utc::now();
    let snapshot = crate::api_routes::connector_routes::connector_snapshot(state);
    let (action, decision, evidence) = state
        .services
        .cross_plane
        .decide_connector_action(&snapshot, action, &mode, now);
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
            .execute_commit_graph(&action, &decision, &graph_key, executor)
            .await
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        state
            .services
            .cross_plane
            .record_non_commit_action(idempotency_key, mode.clone(), action, decision, evidence)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
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
