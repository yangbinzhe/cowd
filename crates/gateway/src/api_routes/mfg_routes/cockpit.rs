use super::*;

pub(super) async fn mfg_cockpit_profile_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgCockpitProfileUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .mfg
        .upsert_cockpit_profile(
            &state.config_home,
            &MfgCockpitProfile::from_input(request.profile),
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "profile": profile,
    })))
}

pub(super) async fn mfg_cockpit_profile_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "profile": profile,
    })))
}

pub(super) async fn mfg_cockpit_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let projection = state
        .services
        .mfg
        .cockpit_projection(&state.config_home, &id)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.projection",
        "projection": projection,
    })))
}

pub(super) async fn mfg_cockpit_report_generate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportGenerateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .generate_cockpit_report(&state.config_home, &id, request.report)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "report": report,
    })))
}

pub(super) async fn mfg_cockpit_report_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "report": report,
    })))
}

pub(super) async fn mfg_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportDeliveryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
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
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
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
    Json(request): Json<MfgCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let before_state = MfgCockpitReportDeliveryState::from_report(&report);
    if !before_state.retryable && !request.force {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "MFG cockpit report delivery is not retryable: {}",
                before_state.classification
            ),
        ));
    }
    let delivery_request = mfg_retry_delivery_request(&report, &before_state, request);
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
    Json(request): Json<MfgCockpitReportScheduleRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let limit = request.limit.unwrap_or(50).clamp(1, 100);
    let profiles = state
        .services
        .mfg
        .list_cockpit_profiles(&state.config_home, request.cadence.as_deref(), limit)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
                MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;

        if request.deliver {
            let delivery_request =
                mfg_schedule_delivery_request(&profile, &report, &request, delivery_count);
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
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "MFG cockpit report delivery idempotency key belongs to another cross-plane action",
                ));
            }
            let report = state
                .services
                .mfg
                .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
            target,
            state.services.cross_plane.runtime_control(),
        ));
        state
            .services
            .cross_plane
            .execute_commit_graph(&action, &decision, &graph_key, executor)
            .await
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
        state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(&graph_key)
            .ok_or_else(|| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "canonical cross-plane graph receipt is missing",
                )
            })?
    } else {
        state
            .services
            .cross_plane
            .record_non_commit_action(idempotency_key, mode.clone(), action, decision, evidence)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    };
    let report = state
        .services
        .mfg
        .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    profile: &MfgCockpitProfile,
    report: &MfgCockpitReportSnapshot,
    request: &MfgCockpitReportScheduleRunRequest,
    delivery_index: usize,
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
        actor_principal: request
            .actor_principal
            .clone()
            .or_else(|| Some(profile.owner_ref.clone())),
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
        actor_principal: request
            .actor_principal
            .or_else(|| Some(report.owner_ref.clone())),
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
