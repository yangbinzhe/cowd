use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use harness_contract::task::TaskRouteHint;
use harness_contract::turn::{
    InputRoutingDecision, InputSourceKind, SessionInputEnvelope, SessionInputId, TurnId,
};
use runtime::{ContextProfile, ResumeContextPacket, ResumeContextSource, TaskAggregate};
use serde::Deserialize;

use super::{
    session_routes::{authorize_session_access, SessionAccess},
    AppState, AuthenticatedPrincipal, ErrorResponse,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/sessions/:id/messages",
            get(get_session_messages).post(send_message),
        )
        .route(
            "/api/sessions/:id/input-projection",
            get(get_session_input_projection),
        )
        .route(
            "/api/sessions/:id/inputs",
            get(get_session_input_projection),
        )
        .route("/api/sessions/:id/turn-inbox", get(get_turn_inbox))
        .route(
            "/api/sessions/:id/turns/:turn_id/inbox",
            get(get_turn_inbox_by_path),
        )
        .route(
            "/api/sessions/:id/inputs/:input_id/cancel",
            post(cancel_session_input),
        )
        .route(
            "/api/sessions/:id/inputs/:input_id/reclassify",
            post(reclassify_session_input),
        )
}

#[derive(Deserialize)]
struct SendMessageRequest {
    content: String,
    #[serde(default)]
    resource_ids: Vec<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    client_message_id: Option<String>,
    #[serde(default)]
    task_route_hint: Option<TaskRouteHint>,
}

#[derive(Deserialize)]
struct GetMessagesParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    tail: bool,
}

#[derive(Deserialize)]
struct TurnInboxParams {
    #[serde(default)]
    turn_id: Option<String>,
}

#[derive(Deserialize)]
struct SessionInputCancelRequest {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct SessionInputReclassifyRequest {
    decision: InputRoutingDecision,
    #[serde(default)]
    reason: Option<String>,
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(super) fn task_resume_context_packet(
    session_id: &str,
    task: &TaskAggregate,
) -> ResumeContextPacket {
    let current_phase = task.current_phase_id.as_ref().and_then(|phase_ref| {
        task.phases
            .iter()
            .find(|phase| &phase.phase_id == phase_ref || &phase.name == phase_ref)
    });
    let phase_summary = current_phase.map(|phase| {
        format!(
            "phase={} status={} objective={} acceptance=[{}]",
            phase.name,
            phase.status.as_str(),
            phase.objective,
            phase.acceptance.join("; ")
        )
    });
    let active_task = Some(format!(
        "id={} status={} yolo={} objective={}{}",
        task.task_id,
        task.status.as_str(),
        task.execution_policy.yolo_mode,
        task.objective,
        phase_summary
            .as_ref()
            .map(|summary| format!(" current_{summary}"))
            .unwrap_or_default()
    ));
    let recent_decisions = task
        .phases
        .iter()
        .rev()
        .take(5)
        .map(|phase| {
            format!(
                "phase={} status={} revision={}",
                phase.name,
                phase.status.as_str(),
                phase.revision
            )
        })
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if let Some(reason) = task
        .blocker_reason
        .as_ref()
        .filter(|reason| !reason.is_empty())
    {
        blockers.push(reason.clone());
    }
    if task.failure_count > 0 {
        blockers.push(format!("failure_count={}", task.failure_count));
    }

    ResumeContextPacket {
        session_id: session_id.to_string(),
        handoff_summary: None,
        active_task,
        recent_decisions,
        blockers,
        source: ResumeContextSource::ExecutionGraph,
    }
}

pub(super) fn runtime_run_started_payload(
    session_id: &str,
    run_id: &str,
    profile: ContextProfile,
    intent: &str,
    started_at_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "started",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "profile": profile,
        "status": "running",
        "summary": intent.chars().take(120).collect::<String>(),
        "intent_preview": intent.chars().take(240).collect::<String>(),
        "started_at_ms": started_at_ms,
        "refs": [],
    })
}

pub(super) fn runtime_run_completed_payload(
    session_id: &str,
    run_id: &str,
    turn_id: Option<&str>,
    profile: ContextProfile,
    status: &str,
    iterations: Option<usize>,
    context_envelope_id: Option<String>,
    error: Option<String>,
    started_at_ms: u64,
    completed_at_ms: u64,
) -> serde_json::Value {
    let mut refs = context_envelope_id
        .as_ref()
        .map(|id| vec![serde_json::json!({"type": "context_envelope", "id": id})])
        .unwrap_or_default();
    if let Some(turn_id) = turn_id {
        refs.push(serde_json::json!({"type": "turn", "id": turn_id}));
    }
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "completed",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "turn_id": turn_id,
        "profile": profile,
        "status": status,
        "summary": error
            .as_ref()
            .map(|value| value.chars().take(160).collect::<String>())
            .unwrap_or_else(|| format!("turn {status}")),
        "iterations": iterations,
        "context_envelope_id": context_envelope_id,
        "error": error,
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
        "duration_ms": completed_at_ms.saturating_sub(started_at_ms),
        "refs": refs,
    })
}

async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let ingress_started = std::time::Instant::now();
    let session_service = &state.services.session;
    let session_exists = state
        .services
        .session
        .session_exists(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to resolve session ownership: {error}"),
                }),
            )
        })?;
    if session_exists || session_service.has_active_session(&id) {
        authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    }
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
    // The durable record is the authority boundary, not proof that this
    // Gateway process has an active Runtime instance.  A permitted owner must
    // still restore a cold session before ingress; conversely the foreign
    // check above runs before activation so a guessed id cannot warm or
    // mutate another principal's session.
    if !session_service.has_active_session(&id) {
        let mut request = crate::services::EnsureSessionRequest::new(
            &id,
            None,
            crate::services::SessionSource::WebUi,
        );
        request.owner_principal_id = Some(principal.0.claims().principal_id.clone());
        request.allow_legacy_owner_migration = principal.0.is_human_interactive()
            && principal.0.has_capability("runtime.maintenance.manage");
        state
            .services
            .session
            .activate_existing_session(request)
            .await
            .map_err(|error| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("session {id} not found: {error}"),
                    }),
                )
            })?;
    }

    tracing::info!(
        %id,
        content_len = body.content.len(),
        resource_count = body.resource_ids.len(),
        "API message received"
    );
    let runtime_content = render_message_resource_context(
        &state.config_home,
        state.services.artifact_store(),
        &state.services.resource_capability_index(),
        &body.content,
        &body.resource_ids,
    );

    let session_id = id.clone();
    let run_id = uuid::Uuid::new_v4().to_string();
    let active_projection = session_service
        .input_projection(&session_id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    let observer_id = headers
        .get("x-cowd-observer-id")
        .and_then(|value| value.to_str().ok());
    let surface_id = headers
        .get("x-cowd-surface-id")
        .and_then(|value| value.to_str().ok());
    let source_kind = match (surface_id, observer_id) {
        (Some("cowd.tui"), Some(observer)) if observer.starts_with("tui:") => InputSourceKind::Tui,
        (Some("webui"), Some(observer)) if observer.starts_with("webui:") => InputSourceKind::Webui,
        _ => InputSourceKind::Api,
    };
    let mut envelope = SessionInputEnvelope::text(session_id.clone(), source_kind, runtime_content)
        .with_source_ref(format!(
            "api:/api/sessions/{session_id}/messages;surface={};observer={}",
            surface_id.unwrap_or("api"),
            observer_id.unwrap_or("unknown")
        ));
    if let Some(client_message_id) = body
        .client_message_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        envelope = envelope.with_source_message_id(client_message_id.trim().to_string());
    }
    if let Some(idempotency_key) = body
        .idempotency_key
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        envelope = envelope.with_idempotency_key(idempotency_key.trim().to_string());
    }
    if let Some(task_route_hint) = body.task_route_hint.clone() {
        envelope = envelope.with_task_route_hint(task_route_hint);
    }
    let admission = session_service
        .admit_input(envelope)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    let execution_graph_id = admission.execution_graph_id;
    let terminal_id = admission.terminal_id;
    let turn_id = admission.turn_id;
    let message_id = admission.message_id;
    let message_sequence = admission.message_sequence;
    let materialized = admission.materialized;
    let receipt = admission.receipt;
    let projection = session_service.input_projection(&session_id).await.ok();
    let inbox = session_service
        .turn_inbox(&session_id, receipt.active_turn_id.clone())
        .await
        .ok();
    let response = serde_json::json!({
        "session_id": session_id,
        "run_id": run_id,
        "status": "accepted",
        "execution": {
            "graph_id": execution_graph_id,
            "turn_id": turn_id.clone(),
            "terminal_id": terminal_id,
            "status": "accepted_pending_materialization",
            "materialization": {
                "state": "accepted_pending_graph",
                "source": "durable_session_outbox",
            },
        },
        "message": {
            "message_id": message_id,
            "sequence": message_sequence,
            "turn_id": turn_id,
        },
        "mode": if active_projection.active_turn_id.is_some() {
            "attached_to_active_turn"
        } else {
            "queued_new_turn"
        },
        "input": receipt,
        "materialized": materialized,
        "input_projection": projection,
        "turn_inbox": inbox,
    });
    runtime::execution_core::performance::observe_duration(
        "gateway_accept_ms",
        ingress_started.elapsed(),
    );
    Ok(Json(response))
}

async fn get_session_input_projection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let projection = state
        .services
        .session
        .input_projection(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    Ok(Json(projection))
}

async fn get_turn_inbox(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<TurnInboxParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let turn_id = params.turn_id.map(TurnId::from_string);
    let inbox = state
        .services
        .session
        .turn_inbox(&id, turn_id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    Ok(Json(inbox))
}

async fn get_turn_inbox_by_path(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, turn_id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let inbox = state
        .services
        .session
        .turn_inbox(&id, Some(TurnId::from_string(turn_id)))
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    Ok(Json(inbox))
}

async fn cancel_session_input(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, input_id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<SessionInputCancelRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
    let session_service = &state.services.session;
    let reason = body
        .reason
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("cancelled by user");
    let input = session_service
        .cancel_input(&id, SessionInputId::from_string(input_id), reason)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    let mut projection_warnings = Vec::new();
    let projection = match session_service.input_projection(&id).await {
        Ok(projection) => Some(projection),
        Err(error) => {
            projection_warnings.push(serde_json::json!({
                "projection": "input_projection",
                "error": error,
            }));
            None
        }
    };
    let inbox = match session_service.turn_inbox(&id, None).await {
        Ok(inbox) => Some(inbox),
        Err(error) => {
            projection_warnings.push(serde_json::json!({
                "projection": "turn_inbox",
                "error": error,
            }));
            None
        }
    };
    Ok(Json(serde_json::json!({
        "kind": "session_input.cancel",
        "session_id": id,
        "input": input,
        "input_projection": projection,
        "turn_inbox": inbox,
        "projection_status": if projection_warnings.is_empty() { "ready" } else { "degraded" },
        "projection_warnings": projection_warnings,
    })))
}

async fn reclassify_session_input(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, input_id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(body): Json<SessionInputReclassifyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
    let session_service = &state.services.session;
    let reason = body
        .reason
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("manual route override");
    let input = session_service
        .reclassify_input(
            &id,
            SessionInputId::from_string(input_id),
            body.decision,
            reason,
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            )
        })?;
    let mut projection_warnings = Vec::new();
    let projection = match session_service.input_projection(&id).await {
        Ok(projection) => Some(projection),
        Err(error) => {
            projection_warnings.push(serde_json::json!({
                "projection": "input_projection",
                "error": error,
            }));
            None
        }
    };
    let inbox = match session_service.turn_inbox(&id, None).await {
        Ok(inbox) => Some(inbox),
        Err(error) => {
            projection_warnings.push(serde_json::json!({
                "projection": "turn_inbox",
                "error": error,
            }));
            None
        }
    };
    Ok(Json(serde_json::json!({
        "kind": "session_input.reclassify",
        "session_id": id,
        "input": input,
        "input_projection": projection,
        "turn_inbox": inbox,
        "projection_status": if projection_warnings.is_empty() { "ready" } else { "degraded" },
        "projection_warnings": projection_warnings,
    })))
}

fn render_message_resource_context(
    config_home: &std::path::Path,
    artifact_store: Option<Arc<runtime::ArtifactStore>>,
    capabilities: &runtime::ResourceCapabilityIndex,
    content: &str,
    ids: &[String],
) -> String {
    if ids.is_empty() {
        return content.to_string();
    }
    let store = artifact_store.map_or_else(
        || {
            runtime::ResourceStore::for_config_home_with_capabilities(
                config_home,
                capabilities.clone(),
            )
        },
        |artifacts| {
            runtime::ResourceStore::from_artifact_store(
                config_home,
                artifacts,
                capabilities.clone(),
            )
        },
    );
    let mut pairs = Vec::new();
    let mut failures = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for raw_id in ids {
        let id = raw_id.trim().trim_start_matches("resource://");
        if id.is_empty() || !seen.insert(id.to_string()) {
            continue;
        }
        match store.get(id) {
            Ok(envelope) => {
                let hint = runtime::resource_hint(&envelope, &capabilities.snapshot());
                pairs.push(hint.prompt_hint(&envelope));
            }
            Err(error) => failures.push((raw_id.clone(), error)),
        }
    }
    let mut rendered = content.to_string();
    rendered.push_str(&runtime::render_resource_context_markdown(&pairs));
    if !failures.is_empty() {
        rendered.push_str("\n\n## Attached Resource Lookup Failures\n\n");
        for (id, error) in failures {
            rendered.push_str(&format!("- {id}: {error}\n"));
        }
        rendered.push_str("If a resource cannot be loaded, explain the boundary instead of pretending to inspect it.\n");
    }
    rendered
}

async fn get_session_messages(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let requested_offset = params.offset.unwrap_or(0);
    let from_seq = params.from_seq;
    let limit = params.limit.unwrap_or(50).min(500);

    if state.has_unified_store() {
        let total = state
            .services
            .session
            .stored_message_count(&id)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("durable session message count failed for `{id}`: {error}"),
                    }),
                )
            })?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("session `{id}` was not found in durable storage"),
                    }),
                )
            })?;
        let offset = if params.tail && from_seq.is_none() {
            total.saturating_sub(limit)
        } else {
            requested_offset
        };
        let db_messages = if let Some(seq) = from_seq {
            state
                .services
                .session
                .stored_messages_from_sequence(&id, seq, limit)
                .await
                .map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!(
                                "durable session message read failed for `{id}` from sequence {seq}: {error}"
                            ),
                        }),
                    )
                })?
        } else {
            state
                .services
                .session
                .stored_messages(&id, offset, limit)
                .await
                .map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!(
                                "durable session message read failed for `{id}` at offset {offset}: {error}"
                            ),
                        }),
                    )
                })?
        }
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("session `{id}` was not found in durable storage"),
                }),
            )
        })?;
        let messages: Vec<serde_json::Value> = db_messages
            .iter()
            .map(|m| -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
                let blocks: Vec<serde_json::Value> =
                    serde_json::from_str(&m.content_json).map_err(|error| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: format!(
                                    "durable message `{}` at sequence {} has malformed content: {error}",
                                    m.stable_message_id, m.sequence
                                ),
                            }),
                        )
                    })?;
                let blocks = public_session_blocks(blocks);
                let mut val = serde_json::json!({
                    "id": m.stable_message_id,
                    "session_id": m.session_id,
                    "sequence": m.sequence,
                    "role": m.role,
                    "blocks": blocks,
                    "created_at_ms": m.created_at_ms,
                });
                if let Some(ref tu) = m.token_usage_json {
                    val["token_usage"] = serde_json::from_str::<serde_json::Value>(tu).map_err(
                        |error| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: format!(
                                        "durable message `{}` at sequence {} has malformed token usage: {error}",
                                        m.stable_message_id, m.sequence
                                    ),
                                }),
                            )
                        },
                    )?;
                }
                if let Some(ref tid) = m.tool_use_id {
                    val["tool_use_id"] = serde_json::Value::String(tid.clone());
                }
                if let Some(ref tn) = m.tool_name {
                    val["tool_name"] = serde_json::Value::String(tn.clone());
                }
                Ok(val)
            })
            .collect::<Result<_, _>>()?;
        let next_seq = db_messages.last().map(|m| m.sequence + 1);
        let has_more = next_seq
            .map(|seq| seq < total)
            .unwrap_or_else(|| from_seq.unwrap_or(offset) < total);
        return Ok(Json(serde_json::json!({
            "session_id": id,
            "messages": messages,
            "total": total,
            "offset": offset,
            "from_seq": from_seq,
            "next_seq": next_seq,
            "limit": limit,
            "has_more": has_more,
        })));
    }

    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service not configured".to_string(),
            }),
        )
    })?;

    runtime_service
        .active_messages_page(&id, requested_offset, from_seq, limit, params.tail)
        .await
        .map(|page| Json(serde_json::to_value(page).unwrap_or_else(|_| serde_json::json!({}))))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("session {id} not found"),
                }),
            )
        })
}

pub(super) fn public_session_blocks(blocks: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    blocks
        .into_iter()
        .filter(|block| block.get("type").and_then(serde_json::Value::as_str) != Some("thinking"))
        .collect()
}

pub(super) async fn cleanup_revoked_session_stream_authority(
    state: &AppState,
    session_id: &str,
    attachment_actor: Option<&str>,
    lease_owner: &str,
) {
    if let Some(actor_id) = attachment_actor {
        let detached = state
            .services
            .session
            .detach_session_value(session_id, actor_id)
            .await;
        if detached.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            tracing::warn!(
                session_id,
                actor_id,
                result = %detached,
                "revoked session stream attachment cleanup was not confirmed"
            );
        }
    }
    if let Some(registry) = state.session_lease_registry.as_ref() {
        let released = registry.release(session_id, lease_owner).await;
        if released.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            tracing::warn!(
                session_id,
                lease_owner,
                result = %released,
                "revoked session stream writer lease cleanup was not confirmed"
            );
        }
    }
}

pub(super) fn stream_durable_cursor(data: &str) -> Option<u64> {
    stream_durable_cursor_value(&serde_json::from_str::<serde_json::Value>(data).ok()?)
}

pub(super) fn stream_durable_cursor_value(data: &serde_json::Value) -> Option<u64> {
    data.get("runtime_commit_cursor")?.as_u64()
}

pub(super) async fn replay_materialized_terminal_events(
    state: &AppState,
    session_id: &str,
    after_cursor: u64,
    limit: usize,
) -> ReplayTerminalPage {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return ReplayTerminalPage::default();
    };
    let delivery = runtime.runtime_services().session_terminal_delivery();
    let session_id = session_id.to_string();
    let lookup_session_id = session_id.clone();
    let records = tokio::task::spawn_blocking(move || {
        delivery.materialized_after(&lookup_session_id, after_cursor, limit)
    })
    .await
    .ok()
    .and_then(Result::ok)
    .unwrap_or_default();
    let record_count = records.len();
    let mut last_cursor = None;
    let mut events = Vec::with_capacity(records.len());
    let mut requires_resync = false;
    for record in records {
        if let Some(event) =
            terminal_committed_stream_payload(runtime.runtime_services().artifact_store(), &record)
                .await
        {
            last_cursor = Some(record.commit_cursor);
            events.push(event);
        } else {
            tracing::error!(
                session_id,
                terminal_id = %record.terminal_id,
                runtime_commit_cursor = record.commit_cursor,
                "materialized terminal cannot be replayed; refusing to advance Surface cursor"
            );
            events.push(
                serde_json::json!({
                    "type": "session_stream_resync",
                    "session_id": session_id,
                    "reason": "corrupt_materialized_terminal",
                    "terminal_id": record.terminal_id,
                    "runtime_commit_cursor": last_cursor.unwrap_or(after_cursor),
                })
                .to_string(),
            );
            requires_resync = true;
            break;
        }
    }
    ReplayTerminalPage {
        events,
        record_count,
        last_cursor,
        requires_resync,
    }
}

#[derive(Default)]
pub(super) struct ReplayTerminalPage {
    pub(super) events: Vec<String>,
    pub(super) record_count: usize,
    pub(super) last_cursor: Option<u64>,
    pub(super) requires_resync: bool,
}

async fn terminal_committed_stream_payload(
    artifacts: &runtime::ArtifactStore,
    record: &runtime::RuntimeSessionOutboxRecord,
) -> Option<String> {
    let response = crate::session_runtime_bridge::load_terminal_payload(artifacts, record)
        .await
        .ok()?;
    Some(terminal_committed_stream_payload_from_decoded(
        record, response,
    ))
}

fn terminal_committed_stream_payload_from_decoded(
    record: &runtime::RuntimeSessionOutboxRecord,
    response: crate::session_runtime_bridge::DecodedTerminalPayload,
) -> String {
    let token_usage = response
        .token_usage_json
        .as_deref()
        .and_then(|usage| serde_json::from_str(usage).ok());
    let mut payload = serde_json::json!({
        "type": "TerminalCommitted",
        "session_id": record.session_id,
        "terminal_id": record.terminal_id,
        "message_id": record.message_id,
        "part_id": format!("terminal-message:{}", record.message_id),
        "response": response.text,
        "runtime_commit_cursor": record.commit_cursor,
        "replayed": true,
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(token_usage) = token_usage {
            object.insert("token_usage".to_string(), token_usage);
        }
        if let Some(execution_id) = &record.execution_id {
            object.insert(
                "execution_id".to_string(),
                serde_json::Value::String(execution_id.clone()),
            );
        }
        if let Some(turn_id) = &record.turn_id {
            object.insert(
                "turn_id".to_string(),
                serde_json::Value::String(turn_id.clone()),
            );
        }
    }
    payload.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_history_projects_public_summary_and_drops_private_provider_transcript() {
        let blocks = vec![
            serde_json::json!({"type": "reasoning_summary", "text": "checked evidence"}),
            serde_json::json!({
                "type": "thinking",
                "thinking": "cowd-provider-transcript:v1:ciphertext",
                "signature": "cowd-provider-transcript:v1:signature"
            }),
            serde_json::json!({"type": "text", "text": "answer"}),
        ];

        let projected = public_session_blocks(blocks);

        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0]["type"], "reasoning_summary");
        assert_eq!(projected[1]["type"], "text");
        let encoded = serde_json::to_string(&projected).unwrap();
        assert!(!encoded.contains("provider-transcript"));
        assert!(!encoded.contains("signature"));
    }

    #[test]
    fn message_resource_ids_render_runtime_resource_context() {
        let temp = tempfile::tempdir().expect("tempdir");
        let input = temp.path().join("voice.mp3");
        std::fs::write(&input, b"fake mp3").expect("write resource");
        let store = runtime::ResourceStore::default_for_config_home(&temp.path().join("home"));
        let (resource, _) = store
            .register_resource_from_path(
                &input,
                "test",
                None,
                Some("session-1".to_string()),
                Some("application/octet-stream".to_string()),
            )
            .expect("resource registers");

        let rendered = render_message_resource_context(
            &temp.path().join("home"),
            None,
            &runtime::ResourceCapabilityIndex::default(),
            "请分析附件",
            &[resource.id.clone(), resource.id.clone()],
        );

        assert!(rendered.contains("请分析附件"));
        assert!(rendered.contains("## Attached Resources"));
        assert!(rendered.contains("kind: audio"));
        assert!(rendered.contains("Do not claim audio content"));
        assert_eq!(rendered.matches("### resource://").count(), 1);
    }

    #[test]
    fn terminal_commit_stream_payload_uses_durable_cursor_and_terminal_identity() {
        let record = runtime::RuntimeSessionOutboxRecord {
            terminal_id: "terminal-1".to_string(),
            message_id: "message-1".to_string(),
            session_id: "session-1".to_string(),
            commit_cursor: 42,
            payload_ref: "assistant_json:\"completed\"".to_string(),
            execution_id: Some("execution-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            request_id: Some("request-1".to_string()),
            session_generation: Some(1),
            input_sequence: Some(1),
            input_claim_owner: Some("worker-1".to_string()),
            input_claim_token: Some("claim-1".to_string()),
            input_claim_revision: Some(1),
            status: "materialized".to_string(),
            attempts: 1,
            next_attempt_at_ms: None,
            claim_owner: None,
            claim_expires_at_ms: None,
            failure_class: None,
            last_error: None,
            materialized_at_ms: Some(1),
            revision: 2,
        };
        let encoded = terminal_committed_stream_payload_from_decoded(
            &record,
            crate::session_runtime_bridge::DecodedTerminalPayload {
                text: "completed".to_string(),
                token_usage_json: None,
                ingress_message_id: Some("ingress-1".to_string()),
                transcript: None,
                consumed_input_sequence: Some(1),
            },
        );
        let event: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(event["type"], "TerminalCommitted");
        assert_eq!(event["terminal_id"], "terminal-1");
        assert_eq!(event["execution_id"], "execution-1");
        assert_eq!(event["turn_id"], "turn-1");
        assert_eq!(event["runtime_commit_cursor"], 42);
        assert_eq!(stream_durable_cursor(&encoded), Some(42));
    }

    #[tokio::test]
    async fn revoked_stream_cleanup_removes_only_the_exact_attachment_and_lease_owner() {
        let state = crate::api_routes::tests::test_state();
        let session_id = "session-revoked-stream";
        let revoked_actor = "tui:revoked-observer";
        let surviving_actor = "web:surviving-observer";
        let revoked_owner = "principal:tui:revoked-observer";
        let surviving_owner = "principal:web:surviving-observer";

        state
            .services
            .session
            .create_stored_session_for_tests(&crate::api_routes::new_api_session_record(
                session_id, None,
            ))
            .await
            .expect("create test session");
        for (actor, surface) in [(revoked_actor, "tui"), (surviving_actor, "web")] {
            let attached = state
                .services
                .session
                .attach_session_value(session_id, actor, surface, Some("writer"))
                .await;
            assert_eq!(attached["ok"], true);
        }
        let registry = state
            .session_lease_registry
            .as_ref()
            .expect("test lease registry");
        assert_eq!(
            registry
                .acquire(session_id, revoked_owner, "collaborative")
                .await["ok"],
            true
        );
        assert_eq!(
            registry
                .acquire(session_id, surviving_owner, "collaborative")
                .await["ok"],
            true
        );

        cleanup_revoked_session_stream_authority(
            &state,
            session_id,
            Some(revoked_actor),
            revoked_owner,
        )
        .await;

        let lifecycle = state
            .services
            .session
            .lifecycle_snapshot_value(Some(session_id))
            .await;
        let attachment_ids = lifecycle["snapshot"]["attachments"]
            .as_array()
            .expect("attachments")
            .iter()
            .filter_map(|attachment| attachment["actor"]["id"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(attachment_ids, vec![surviving_actor]);
        let leases = registry.list().await;
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].owner, surviving_owner);
    }
}
