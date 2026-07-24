use std::{
    collections::{BTreeSet, VecDeque},
    convert::Infallible,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{stream::Stream, StreamExt};
use harness_contract::turn::{
    InputRoutingDecision, InputSourceKind, SessionInputEnvelope, SessionInputId, TurnId,
};
use runtime::{ContextProfile, ResumeContextPacket, ResumeContextSource};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::event_bus::SessionEventBus;
use crate::task_kernel::TaskRecord;

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
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
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
}

#[derive(Deserialize)]
struct GetMessagesParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
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

#[derive(Debug, Default, Deserialize)]
struct SessionStreamQuery {
    /// Exclusive Runtime commit cursor. `Last-Event-ID` takes precedence so
    /// native EventSource reconnects need no client-specific query mutation.
    #[serde(default)]
    from_cursor: Option<u64>,
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(super) fn task_resume_context_packet(
    session_id: &str,
    task: &TaskRecord,
) -> ResumeContextPacket {
    let current_phase = task.current_phase.as_ref().and_then(|phase_ref| {
        task.phases
            .iter()
            .find(|phase| &phase.id == phase_ref || &phase.name == phase_ref)
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
        task.id,
        task.status.as_str(),
        task.yolo_mode,
        task.objective,
        phase_summary
            .as_ref()
            .map(|summary| format!(" current_{summary}"))
            .unwrap_or_default()
    ));
    let recent_decisions = task
        .audit
        .iter()
        .rev()
        .take(5)
        .map(|event| format!("{}: {}", event.event_type, event.message))
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
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
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
    if session_exists || runtime_service.has_active_session(&id) {
        authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    }
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
    // The durable record is the authority boundary, not proof that this
    // Gateway process has an active Runtime instance.  A permitted owner must
    // still restore a cold session before ingress; conversely the foreign
    // check above runs before activation so a guessed id cannot warm or
    // mutate another principal's session.
    if !runtime_service.has_active_session(&id) {
        let mut request = crate::unified_session_manager::EnsureSessionRequest::new(
            &id,
            None,
            crate::unified_session_manager::SessionSource::WebUi,
        );
        request.owner_principal_id = Some(principal.0.claims().principal_id.clone());
        request.allow_legacy_owner_migration = principal.0.is_human_interactive()
            && principal.0.has_capability("runtime.maintenance.manage");
        state
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: "unified session manager unavailable".to_string(),
                    }),
                )
            })?
            .ensure_session(request)
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

    let runtime_service = runtime_service.clone();

    tracing::info!(
        %id,
        content_len = body.content.len(),
        resource_count = body.resource_ids.len(),
        "API message received"
    );
    let runtime_content = render_message_resource_context(
        &state.config_home,
        &state.services.resource_capability_index(),
        &body.content,
        &body.resource_ids,
    );

    let session_id = id.clone();
    let run_id = uuid::Uuid::new_v4().to_string();
    let active_projection = runtime_service
        .session_input_projection(&session_id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
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
        (None, None) => InputSourceKind::Webui,
        _ => InputSourceKind::Api,
    };
    let mut envelope = SessionInputEnvelope::text(session_id.clone(), source_kind, runtime_content)
        .with_source_ref(format!(
            "api:/api/sessions/{session_id}/messages;surface={};observer={}",
            surface_id.unwrap_or("legacy-webui"),
            observer_id.unwrap_or("principal-default")
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
    let admission = runtime_service
        .admit_session_input_with_materialized(envelope)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
            )
        })?;
    let execution_graph_id = admission.execution_graph_id;
    let terminal_id = admission.terminal_id;
    let turn_id = admission.turn_id;
    let materialized = admission.materialized;
    let receipt = admission.receipt;
    let projection = runtime_service
        .session_input_projection(&session_id)
        .await
        .ok();
    let inbox = runtime_service
        .active_turn_inbox(&session_id, receipt.active_turn_id.clone())
        .await
        .ok();
    let response = serde_json::json!({
        "session_id": session_id,
        "run_id": run_id,
        "status": "accepted",
        "execution": {
            "graph_id": execution_graph_id,
            "turn_id": turn_id,
            "terminal_id": terminal_id,
            "status": "accepted_pending_materialization",
            "materialization": {
                "state": "accepted_pending_graph",
                "source": "durable_session_outbox",
            },
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
    Ok(Json(response))
}

async fn get_session_input_projection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let projection = runtime_service
        .session_input_projection(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
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
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let turn_id = params.turn_id.map(TurnId::from_string);
    let inbox = runtime_service
        .active_turn_inbox(&id, turn_id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
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
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let inbox = runtime_service
        .active_turn_inbox(&id, Some(TurnId::from_string(turn_id)))
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
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
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let reason = body
        .reason
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("cancelled by user");
    let input = runtime_service
        .cancel_session_input(&id, SessionInputId::from_string(input_id), reason)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
            )
        })?;
    let projection = runtime_service.session_input_projection(&id).await.ok();
    let inbox = runtime_service.active_turn_inbox(&id, None).await.ok();
    Ok(Json(serde_json::json!({
        "kind": "session_input.cancel",
        "session_id": id,
        "input": input,
        "input_projection": projection,
        "turn_inbox": inbox,
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
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let reason = body
        .reason
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("manual route override");
    let input = runtime_service
        .reclassify_session_input(
            &id,
            SessionInputId::from_string(input_id),
            body.decision,
            reason,
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
            )
        })?;
    let projection = runtime_service.session_input_projection(&id).await.ok();
    let inbox = runtime_service.active_turn_inbox(&id, None).await.ok();
    Ok(Json(serde_json::json!({
        "kind": "session_input.reclassify",
        "session_id": id,
        "input": input,
        "input_projection": projection,
        "turn_inbox": inbox,
    })))
}

fn render_message_resource_context(
    config_home: &std::path::Path,
    capabilities: &runtime::ResourceCapabilityIndex,
    content: &str,
    ids: &[String],
) -> String {
    if ids.is_empty() {
        return content.to_string();
    }
    let store = runtime::ResourceStore::for_config_home_with_capabilities(
        config_home,
        capabilities.clone(),
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
    let offset = params.offset.unwrap_or(0);
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
        .active_messages_page(&id, offset, from_seq, limit)
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

struct SseStream {
    rx: ReceiverStream<String>,
    session_id: String,
    event_bus: Arc<SessionEventBus>,
    subscription_id: u64,
    seen_durable_cursors: BTreeSet<u64>,
    durable_cursor_order: VecDeque<u64>,
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.rx.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(data)) => {
                    let durable_cursor = stream_durable_cursor(&data);
                    if let Some(cursor) = durable_cursor {
                        if !self.seen_durable_cursors.insert(cursor) {
                            continue;
                        }
                        self.durable_cursor_order.push_back(cursor);
                        if self.durable_cursor_order.len() > 1_024 {
                            if let Some(expired) = self.durable_cursor_order.pop_front() {
                                self.seen_durable_cursors.remove(&expired);
                            }
                        }
                    }
                    let mut event = Event::default().data(data);
                    if let Some(cursor) = durable_cursor {
                        event = event.id(cursor.to_string());
                    }
                    return std::task::Poll::Ready(Some(Ok(event)));
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

impl Drop for SseStream {
    fn drop(&mut self) {
        let event_bus = self.event_bus.clone();
        let session_id = self.session_id.clone();
        let subscription_id = self.subscription_id;
        tokio::spawn(async move {
            event_bus.unsubscribe(&session_id, subscription_id).await;
        });
    }
}

async fn sse_stream_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<SessionStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &session_id, SessionAccess::Read).await?;
    // Replay runs after Axum receives the stream so arbitrarily many bounded
    // pages can drain concurrently to the client. Producing replay inside the
    // handler would either deadlock on the output channel or silently stop
    // after one page.
    const SESSION_STREAM_OUTPUT_CAPACITY: usize = 512;
    let (tx, rx) = mpsc::channel(SESSION_STREAM_OUTPUT_CAPACITY);
    let (bus_tx, bus_rx) = mpsc::channel(256);
    let event_bus = state.event_bus();
    let subscription_id = event_bus.subscribe(&session_id, bus_tx).await;
    let resume_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(query.from_cursor)
        .unwrap_or_default();
    let observer_id = headers
        .get("x-cowd-observer-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| super::validated_session_observer_id(Some(value)))
        .map(str::to_string);
    let revoked_attachment_actor = observer_id
        .as_deref()
        .map(|observer_id| super::surface_actor_id(&principal, observer_id));
    let revoked_lease_owner = super::session_lease_owner(&principal, observer_id.as_deref());
    spawn_session_stream_authorization_guard(
        bus_rx,
        tx,
        state.clone(),
        state.config_home.clone(),
        principal,
        session_id.clone(),
        resume_cursor,
        revoked_attachment_actor,
        revoked_lease_owner,
    );

    let stream = SseStream {
        rx: ReceiverStream::new(rx),
        session_id,
        event_bus,
        subscription_id,
        seen_durable_cursors: BTreeSet::new(),
        durable_cursor_order: VecDeque::new(),
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

/// Bus subscriptions outlive one HTTP poll, therefore auth middleware alone
/// cannot protect a Session SSE connection.  Forward through a private
/// channel and revalidate on a bounded cadence without ever blocking Tokio's
/// worker threads on the broker's Unix socket.  Once revoked, emit exactly one
/// typed terminal event and stop forwarding before any later durable event.
fn spawn_session_stream_authorization_guard(
    mut bus_rx: mpsc::Receiver<String>,
    tx: mpsc::Sender<String>,
    state: Arc<AppState>,
    config_home: PathBuf,
    principal: AuthenticatedPrincipal,
    session_id: String,
    resume_cursor: u64,
    revoked_attachment_actor: Option<String>,
    revoked_lease_owner: String,
) {
    tokio::spawn(async move {
        const TERMINAL_REPLAY_PAGE_SIZE: usize = 500;
        const TERMINAL_REPLAY_MAX_PAGES: usize = 100;
        let mut replay_cursor = resume_cursor;
        for page_index in 0..TERMINAL_REPLAY_MAX_PAGES {
            let page = replay_materialized_terminal_events(
                &state,
                &session_id,
                replay_cursor,
                TERMINAL_REPLAY_PAGE_SIZE,
            )
            .await;
            for event in page.events {
                if tx.send(event).await.is_err() {
                    return;
                }
            }
            replay_cursor = replay_cursor.max(page.last_cursor.unwrap_or(replay_cursor));
            if page.requires_resync {
                return;
            }
            if page.record_count < TERMINAL_REPLAY_PAGE_SIZE {
                break;
            }
            if page_index + 1 == TERMINAL_REPLAY_MAX_PAGES {
                // This is a typed recovery boundary, not a false Connected:
                // reconnecting from replay_cursor continues with the next
                // bounded page and cannot strand terminal 50_001 forever.
                let _ = tx
                    .send(
                        serde_json::json!({
                            "type": "session_stream_resync",
                            "session_id": session_id,
                            "reason": "terminal_replay_window",
                            "runtime_commit_cursor": replay_cursor,
                        })
                        .to_string(),
                    )
                    .await;
                return;
            }
        }
        if tx
            .send(
                serde_json::json!({
                    "type": "Connected",
                    "session_id": session_id,
                    "runtime_commit_cursor": replay_cursor,
                })
                .to_string(),
            )
            .await
            .is_err()
        {
            return;
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = interval.tick() => {
                    if tx.is_closed() {
                        break;
                    }
                    let config_home = config_home.clone();
                    let principal = principal.clone();
                    let check = tokio::task::spawn_blocking(move || {
                        super::projection_stream_principal_current(&config_home, &principal)
                    }).await;
                    let reason = match check {
                        Ok(Ok(())) => None,
                        Ok(Err(reason)) => Some(reason),
                        Err(error) => Some(format!("session stream authorization check aborted: {error}")),
                    };
                    if let Some(reason) = reason {
                        // Revocation is a server-side authority boundary, not
                        // just an instruction for the remote renderer. Remove
                        // the exact observer attachment and writer lease before
                        // publishing the terminal event so no subsequent HTTP
                        // mutation can continue under stale Surface state.
                        cleanup_revoked_session_stream_authority(
                            &state,
                            &session_id,
                            revoked_attachment_actor.as_deref(),
                            &revoked_lease_owner,
                        ).await;
                        let _ = tx.send(serde_json::json!({
                            "type": "SessionAuthorizationRevoked",
                            "session_id": session_id,
                            "reason": reason,
                        }).to_string()).await;
                        break;
                    }
                }
                event = bus_rx.recv() => match event {
                    Some(event) => {
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
            }
        }
    });
}

async fn cleanup_revoked_session_stream_authority(
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

fn stream_durable_cursor(data: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(data)
        .ok()?
        .get("runtime_commit_cursor")?
        .as_u64()
}

async fn replay_materialized_terminal_events(
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
        if let Some(event) = terminal_committed_stream_payload(&record) {
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
struct ReplayTerminalPage {
    events: Vec<String>,
    record_count: usize,
    last_cursor: Option<u64>,
    requires_resync: bool,
}

fn terminal_committed_stream_payload(
    record: &runtime::RuntimeSessionOutboxRecord,
) -> Option<String> {
    let response =
        crate::session_runtime_bridge::decode_terminal_payload(&record.payload_ref).ok()?;
    let token_usage = response
        .token_usage_json
        .as_deref()
        .and_then(|usage| serde_json::from_str(usage).ok());
    let mut payload = serde_json::json!({
        "type": "TerminalCommitted",
        "session_id": record.session_id,
        "terminal_id": record.terminal_id,
        "message_id": record.message_id,
        "part_id": "assistant_text",
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
    Some(payload.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let encoded = terminal_committed_stream_payload(&record).expect("valid terminal payload");
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
            .unified_store()
            .expect("test session store")
            .create_session(&crate::api_routes::new_api_session_record(session_id, None))
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
