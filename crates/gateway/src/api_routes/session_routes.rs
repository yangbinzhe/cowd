use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use memory::store::session::{SessionEvent, SessionListOptions, SessionRecord};
use serde::{Deserialize, Serialize};

use super::{surface_actor_id, AppState, AuthenticatedPrincipal, ErrorResponse};
use crate::services::{
    SessionMessageCounts, SessionStatsSnapshot, SessionTokenCounts, SessionUpdateRequest,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route(
            "/api/sessions/executions",
            get(list_running_session_execution_indices),
        )
        .route("/api/sessions/search", get(search_messages_handler))
        .route("/api/sessions/:id/evidence", get(get_session_evidence))
        .route("/api/sessions/:id/ensure", post(ensure_session_handler))
        .route("/api/sessions/:id/branch", post(branch_session_handler))
        .route("/api/sessions/:id/attach", post(attach_session_handler))
        .route("/api/sessions/:id/detach", post(detach_session_handler))
        .route(
            "/api/sessions/:id/lifecycle",
            get(session_lifecycle_handler),
        )
        .route("/api/sessions/:id/replay", get(replay_session_handler))
        .route(
            "/api/sessions/:id/cancel",
            post(cancel_session_turn_handler),
        )
        .route(
            "/api/sessions/:id",
            get(get_session)
                .patch(update_session_handler)
                .delete(delete_session),
        )
        .route("/api/sessions/:id/events", get(get_session_events))
        .route("/api/sessions/:id/runs", get(get_session_runs))
        .route(
            "/api/sessions/:id/execution",
            get(get_session_execution_index),
        )
        .route("/api/sessions/:id/turns", get(get_session_turns))
        .route("/api/sessions/:id/turns/:turn_id", get(get_session_turn))
        .route(
            "/api/sessions/:id/turns/:turn_id/evidence",
            get(get_turn_evidence),
        )
        .route("/api/sessions/:id/projection", get(get_session_projection))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/stats", get(get_session_stats_handler))
}

pub(super) async fn get_session_evidence(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<
    Json<harness_contract::projection::SessionEvidenceProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let projection = session_evidence_projection(&state, &id, None).await?;
    Ok(Json(projection))
}

pub(super) async fn get_turn_evidence(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, turn_id)): Path<(String, String)>,
) -> Result<
    Json<harness_contract::projection::TurnEvidenceProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let projection = session_evidence_projection(&state, &id, Some(turn_id.as_str())).await?;
    projection
        .turns
        .into_iter()
        .next()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!(
                        "turn {turn_id} has no durable execution binding in session {id}"
                    ),
                }),
            )
        })
}

async fn session_evidence_projection(
    state: &AppState,
    session_id: &str,
    turn_id: Option<&str>,
) -> Result<
    harness_contract::projection::SessionEvidenceProjection,
    (StatusCode, Json<ErrorResponse>),
> {
    use harness_contract::projection::{
        EvidenceFreshness, ProjectionDetailScope, ProjectionQueryContext,
        SessionEvidenceProjection, TurnEvidenceProjection,
    };

    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let store = runtime_service
        .session_kernel()
        .unified_store()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "unified session store unavailable".to_string(),
                }),
            )
        })?;
    let mut records = store
        .session_runtime_outbox_for_session(session_id, 100)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable turn bindings: {error}"),
                }),
            )
        })?;
    if let Some(turn_id) = turn_id {
        records.retain(|record| record.turn_id == turn_id);
    }
    records.sort_by_key(|record| (record.sequence, record.request_id.clone()));

    let context = ProjectionQueryContext {
        principal: "gateway-local".to_string(),
        workspace_id: runtime_service
            .runtime_services()
            .workspace_key()
            .to_string(),
        // This query is scoped by the durable record selected above, and the
        // Runtime projection performs the same session-scope verification.
        session_scopes: vec![session_id.to_string()],
        mission_scopes: Vec::new(),
        visibility_grants: vec!["gateway-local".to_string()],
        detail_scope: ProjectionDetailScope::Summary,
        authorization_revision: 1,
    };
    let mut all_refs = BTreeSet::new();
    let mut turns = Vec::with_capacity(records.len());
    for record in records {
        // The relation is deterministic from the durable ingress identity;
        // never approximate it from message text, sequence, or timestamps.
        let execution_id =
            runtime::session_ingress_graph_id(session_id, &record.request_id, &record.turn_id);
        let projection = runtime::execution_projection::snapshot(
            runtime_service.runtime_services().as_ref(),
            &execution_id,
            &context,
        )
        .await
        .ok();
        let mut evidence_refs = projection
            .as_ref()
            .map(|projection| {
                projection
                    .evidence
                    .iter()
                    .flat_map(|entity| entity.evidence_refs.iter().cloned())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        all_refs.extend(evidence_refs.iter().cloned());
        let live = runtime_service.execution_live(&execution_id);
        let freshness = if live.is_some() {
            EvidenceFreshness::Live
        } else if projection.is_some() {
            EvidenceFreshness::Durable
        } else {
            EvidenceFreshness::Unavailable
        };
        let terminal_ref = (record.status == memory::OutboxStatus::Materialized)
            .then(|| format!("turn-terminal:{}", record.request_id));
        let assistant_message_id = terminal_ref
            .as_ref()
            .map(|_| format!("assistant:{}", record.message_id));
        turns.push(TurnEvidenceProjection {
            session_id: session_id.to_string(),
            turn_id: record.turn_id,
            input_message_id: record.message_id,
            execution_id,
            terminal_ref,
            assistant_message_id,
            context_report_id: None,
            evidence_refs: std::mem::take(&mut evidence_refs).into_iter().collect(),
            freshness,
        });
    }
    let freshness = if turns
        .iter()
        .any(|turn| turn.freshness == EvidenceFreshness::Live)
    {
        EvidenceFreshness::Live
    } else if turns
        .iter()
        .any(|turn| turn.freshness == EvidenceFreshness::Durable)
    {
        EvidenceFreshness::Durable
    } else {
        EvidenceFreshness::Unavailable
    };
    Ok(SessionEvidenceProjection {
        session_id: session_id.to_string(),
        evidence_refs: all_refs.into_iter().collect(),
        turns,
        freshness,
    })
}

pub(super) async fn list_running_session_execution_indices(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<
    Json<harness_contract::projection::SessionExecutionIndicesProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    Ok(Json(
        harness_contract::projection::SessionExecutionIndicesProjection {
            items: runtime
                .recoverable_running_session_execution_indices()
                .await,
        },
    ))
}

pub(super) async fn get_session_execution_index(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<
    Json<harness_contract::projection::SessionExecutionIndexProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    Ok(Json(runtime.recoverable_session_execution_index(&id).await))
}

#[derive(Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<i64>,
}

#[derive(Deserialize)]
struct CreateSessionRequest {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct ListSessionsParams {
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default = "default_order")]
    order: String,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

fn default_sort() -> String {
    "updated_at".to_string()
}

fn default_order() -> String {
    "desc".to_string()
}

fn default_session_model(state: &AppState) -> String {
    state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .ok()
        .and_then(|config| config.model().map(str::to_string))
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string())
}

fn required_session_manager(
    state: &AppState,
) -> Result<
    &Arc<crate::unified_session_manager::UnifiedSessionManager>,
    (StatusCode, Json<ErrorResponse>),
> {
    state.services.session_manager.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "unified session manager unavailable".to_string(),
            }),
        )
    })
}

fn session_manager_error(error: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("session operation failed: {error}"),
        }),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetEventsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_payload: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchMessagesParams {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionAttachRequest {
    surface: String,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDetachRequest {
    surface: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionReplayParams {
    #[serde(default)]
    from_sequence: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

fn default_search_limit() -> usize {
    20
}

async fn attach_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SessionAttachRequest>,
) -> Json<serde_json::Value> {
    let actor_id = surface_actor_id(&principal, &body.surface);
    Json(
        state
            .services
            .session
            .attach_session_value(&id, &actor_id, &body.surface, body.role.as_deref())
            .await,
    )
}

async fn detach_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SessionDetachRequest>,
) -> Json<serde_json::Value> {
    let actor_id = surface_actor_id(&principal, &body.surface);
    Json(
        state
            .services
            .session
            .detach_session_value(&id, &actor_id)
            .await,
    )
}

async fn session_lifecycle_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    Json(
        state
            .services
            .session
            .lifecycle_snapshot_value(Some(&id))
            .await,
    )
}

async fn replay_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<SessionReplayParams>,
) -> Json<serde_json::Value> {
    Json(
        state
            .services
            .session
            .replay_session_value(
                &id,
                params.from_sequence.unwrap_or(0),
                params.limit.unwrap_or(100),
            )
            .await,
    )
}

#[derive(Serialize)]
struct SearchMessagesItem {
    session_id: String,
    sequence: usize,
    role: String,
    blocks: Vec<serde_json::Value>,
    content_preview: String,
    tool_use_id: Option<String>,
    tool_name: Option<String>,
    created_at_ms: u64,
}

#[derive(Serialize)]
struct SearchMessagesResponse {
    query: String,
    results: Vec<SearchMessagesItem>,
    total: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelSessionTurnRequest {
    #[serde(default)]
    reason: Option<String>,
}

fn session_title_from_metadata(metadata_json: Option<&str>) -> Option<String> {
    metadata_json
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|value| {
            value
                .get("title")
                .and_then(|title| title.as_str())
                .map(ToString::to_string)
        })
}

fn session_info_from_record(record: SessionRecord) -> SessionInfo {
    SessionInfo {
        id: record.session_id,
        status: record.status,
        title: session_title_from_metadata(record.metadata_json.as_deref()),
        model: record.model,
        created_at: Some(record.created_at),
        updated_at: Some(record.last_activity),
        input_tokens: Some(record.input_tokens),
        output_tokens: Some(record.output_tokens),
    }
}

fn active_session_info(id: String) -> SessionInfo {
    SessionInfo {
        id,
        status: "active".to_string(),
        title: None,
        model: None,
        created_at: None,
        updated_at: None,
        input_tokens: None,
        output_tokens: None,
    }
}

async fn list_sessions(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<ListSessionsParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(200);
    let offset = params.offset.unwrap_or(0);

    if let Ok(Some(page)) = state
        .services
        .session
        .list_stored_sessions_page(&SessionListOptions {
            query: params.q.as_deref(),
            model: params.model.as_deref(),
            status: params.status.as_deref(),
            sort: &params.sort,
            order: &params.order,
            limit,
            offset,
        })
        .await
    {
        let total = page.total;
        let sessions: Vec<SessionInfo> = page
            .records
            .into_iter()
            .map(session_info_from_record)
            .collect();
        return Json(serde_json::json!({
            "sessions": sessions,
            "total": total,
            "offset": offset,
            "limit": limit,
            "sort": params.sort,
            "order": params.order,
        }));
    }

    let mut sessions: Vec<SessionInfo> = state
        .services
        .session
        .list_active_session_ids()
        .into_iter()
        .map(active_session_info)
        .collect();
    if let Some(status) = params.status.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| session.status.eq_ignore_ascii_case(status));
    }
    if let Some(model) = params.model.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| {
            session
                .model
                .as_deref()
                .is_some_and(|session_model| session_model.eq_ignore_ascii_case(model))
        });
    }
    if let Some(query) = params
        .q
        .as_ref()
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty())
    {
        sessions.retain(|session| {
            session.id.to_lowercase().contains(&query)
                || session
                    .title
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query)
        });
    }
    if params.sort == "created_at" {
        sessions.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    } else {
        sessions.sort_by(|left, right| left.updated_at.cmp(&right.updated_at));
    }
    if !params.order.eq_ignore_ascii_case("asc") {
        sessions.reverse();
    }
    let total = sessions.len();
    let sessions: Vec<SessionInfo> = sessions.into_iter().skip(offset).take(limit).collect();
    Json(serde_json::json!({
        "sessions": sessions,
        "total": total,
        "offset": offset,
        "limit": limit,
        "sort": params.sort,
        "order": params.order,
    }))
}

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");
    let model = body
        .model
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| default_session_model(&state));
    let manager = required_session_manager(&state)?;
    let outcome = manager
        .ensure_session(crate::unified_session_manager::EnsureSessionRequest::new(
            &session_id,
            Some(model),
            crate::unified_session_manager::SessionSource::WebUi,
        ))
        .await
        .map_err(session_manager_error)?;
    let info = session_info_from_record(outcome.record);

    Ok((StatusCode::CREATED, Json(info)))
}

async fn branch_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session id is required".to_string(),
            }),
        ));
    }
    if !state
        .services
        .session
        .session_exists(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load source session: {error}"),
                }),
            )
        })?
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    }

    let source_record = state
        .services
        .session
        .stored_session(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load source session: {error}"),
                }),
            )
        })?;
    let source_title = source_record
        .as_ref()
        .and_then(|record| session_title_from_metadata(record.metadata_json.as_deref()))
        .unwrap_or_else(|| id.chars().take(8).collect::<String>());
    let model = source_record
        .as_ref()
        .and_then(|record| record.model.clone())
        .unwrap_or_else(|| default_session_model(&state));
    let branch_id = uuid::Uuid::new_v4().to_string();
    let mut request = crate::unified_session_manager::EnsureSessionRequest::new(
        &branch_id,
        Some(model),
        crate::unified_session_manager::SessionSource::WebUi,
    );
    request.title = Some(format!("{} / branch", source_title));
    request.metadata = serde_json::json!({
        "branched_from": id,
        "branch_source_title": source_title,
    });
    let outcome = required_session_manager(&state)?
        .ensure_session(request)
        .await
        .map_err(session_manager_error)?;
    let mut info = session_info_from_record(outcome.record.clone());
    let mut copied_messages = 0usize;
    if state.services.session.has_unified_store() {
        let mut record = outcome.record;
        copied_messages = state
            .services
            .session
            .copy_stored_messages(&id, &branch_id)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to copy branch messages: {error}"),
                    }),
                )
            })?
            .unwrap_or(0);
        record.message_count = copied_messages as i64;
        state
            .services
            .session
            .update_stored_session(&record)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to update branch session: {error}"),
                    }),
                )
            })?;
        let _ = state
            .services
            .session
            .append_timeline_event(
                &branch_id,
                "BranchCreated",
                serde_json::json!({
                    "source_session_id": id,
                    "branch_session_id": branch_id,
                    "copied_message_count": copied_messages,
                    "status": "created",
                }),
            )
            .await;
        info = session_info_from_record(record);
    }

    let _ = state
        .services
        .session
        .append_timeline_event(
            &id,
            "SessionBranched",
            serde_json::json!({
                "source_session_id": id,
                "branch_session_id": branch_id,
                "copied_message_count": copied_messages,
                "status": "created",
            }),
        )
        .await;

    Ok((StatusCode::CREATED, Json(info)))
}

async fn ensure_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session id is required".to_string(),
            }),
        ));
    }

    let outcome = required_session_manager(&state)?
        .ensure_session(crate::unified_session_manager::EnsureSessionRequest::new(
            &id,
            body.model.filter(|model| !model.trim().is_empty()),
            crate::unified_session_manager::SessionSource::Tui,
        ))
        .await
        .map_err(session_manager_error)?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "created": outcome.created,
        "restored": outcome.restored,
        "source": outcome.record.platform,
        "active_sessions": state.services.session.list_active_session_ids().len(),
    })))
}

async fn get_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.services.session.has_unified_store() {
        match state.services.session.stored_session(&id).await {
            Ok(Some(record)) => return Ok(Json(session_info_from_record(record))),
            Ok(None) => {}
            Err(error) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load session: {error}"),
                    }),
                ));
            }
        }
    }

    if state
        .services
        .runtime
        .as_ref()
        .is_some_and(|runtime_service| runtime_service.has_active_session(&id))
    {
        Ok(Json(active_session_info(id)))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn cancel_session_turn_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<CancelSessionTurnRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session id is required".to_string(),
            }),
        ));
    }

    if !state
        .services
        .session
        .session_exists(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session: {error}"),
                }),
            )
        })?
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    }

    let actor_id = format!("principal:{}", principal.0.claims().principal_id);
    let reason = body
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("user_requested");
    let aborted_run_id: Option<String> = None;
    let event = serde_json::json!({
        "type": "TurnCancelRequested",
        "session_id": id,
        "actor_id": actor_id,
        "reason": reason,
        "status": "accepted",
        "aborted": aborted_run_id.is_some(),
        "run_id": aborted_run_id,
    });
    state.event_bus().broadcast(&id, &event.to_string()).await;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "status": "cancel_requested",
        "actor_id": actor_id,
        "reason": reason,
        "aborted": event["aborted"],
        "run_id": event["run_id"],
    })))
}

async fn delete_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let removed = required_session_manager(&state)?
        .delete_session(&id)
        .await
        .map_err(session_manager_error)?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn get_session_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let include_payload = params.include_payload.unwrap_or(false);
    let Some((total, stored_events)) = state
        .services
        .session
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session events: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };
    let events: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
                .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
            let payload =
                session_event_payload_for_response(&event.event_type, payload, include_payload);
            serde_json::json!({
                "session_id": event.session_id,
                "type": event.event_type,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "payload": payload,
            })
        })
        .collect();
    let has_more = events.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "events": events,
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": has_more,
        "include_payload": include_payload,
    })))
}

fn session_event_payload_for_response(
    event_type: &str,
    payload: serde_json::Value,
    include_payload: bool,
) -> serde_json::Value {
    if include_payload {
        return payload;
    }
    if event_type == "ContextEnvelope" {
        return slim_context_envelope_payload(&payload);
    }
    if event_type == "ContextTurnReport" {
        return slim_context_turn_report_payload(&payload);
    }

    let serialized = payload.to_string();
    if serialized.chars().count() <= 4_000 {
        return payload;
    }

    serde_json::json!({
        "type": payload.get("type").cloned().unwrap_or_else(|| serde_json::Value::String(event_type.to_string())),
        "kind": payload.get("kind").cloned().unwrap_or(serde_json::Value::Null),
        "status": payload.get("status").cloned().unwrap_or(serde_json::Value::Null),
        "summary": payload.get("summary").cloned().or_else(|| payload.get("message").cloned()).unwrap_or(serde_json::Value::Null),
        "run_id": payload.get("run_id").cloned().unwrap_or(serde_json::Value::Null),
        "turn_id": payload.get("turn_id").cloned().unwrap_or(serde_json::Value::Null),
        "payload_truncated": true,
        "payload_size_chars": serialized.chars().count(),
        "payload_preview": take_chars(&serialized, 1_200),
        "full_payload_hint": "repeat the request with include_payload=true to retrieve full event evidence",
    })
}

fn slim_context_envelope_payload(payload: &serde_json::Value) -> serde_json::Value {
    let envelope = payload.get("envelope").unwrap_or(payload);
    serde_json::json!({
        "type": payload.get("type").cloned().unwrap_or_else(|| serde_json::Value::String("ContextEnvelope".to_string())),
        "envelope_id": payload
            .get("envelope_id")
            .cloned()
            .or_else(|| envelope.get("id").cloned())
            .unwrap_or(serde_json::Value::Null),
        "session_id": payload
            .get("session_id")
            .cloned()
            .or_else(|| envelope.pointer("/identity/session_id").cloned())
            .unwrap_or(serde_json::Value::Null),
        "agent_id": payload
            .get("agent_id")
            .cloned()
            .or_else(|| envelope.pointer("/identity/agent_id").cloned())
            .unwrap_or(serde_json::Value::Null),
        "profile": payload
            .get("profile")
            .cloned()
            .or_else(|| envelope.get("profile").cloned())
            .unwrap_or(serde_json::Value::Null),
        "budget": payload
            .get("budget")
            .cloned()
            .or_else(|| envelope.get("budget").cloned())
            .unwrap_or(serde_json::Value::Null),
        "diagnostics": payload
            .get("diagnostics")
            .cloned()
            .or_else(|| envelope.get("diagnostics").cloned())
            .unwrap_or(serde_json::Value::Null),
        "selected_count": envelope.get("selected").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
        "omitted_count": envelope.get("omitted").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
        "payload_truncated": true,
        "full_payload_hint": "repeat the request with include_payload=true to retrieve full ContextEnvelope evidence",
    })
}

fn slim_context_turn_report_payload(payload: &serde_json::Value) -> serde_json::Value {
    let report = payload.get("context_turn_report").unwrap_or(payload);
    let knowledge = report.get("knowledge").unwrap_or(&serde_json::Value::Null);
    serde_json::json!({
        "type": payload.get("type").cloned().unwrap_or_else(|| serde_json::Value::String("ContextTurnReport".to_string())),
        "report_id": report.get("report_id").cloned().unwrap_or(serde_json::Value::Null),
        "session_id": report.get("session_id").cloned().unwrap_or(serde_json::Value::Null),
        "turn_id": report.get("turn_id").cloned().unwrap_or(serde_json::Value::Null),
        "input_token_estimate": report.get("input_token_estimate").cloned().unwrap_or(serde_json::Value::Null),
        "governance_decision": report.get("governance_decision").cloned().unwrap_or(serde_json::Value::Null),
        "knowledge": {
            "activation_plan_id": knowledge.get("activation_plan_id").cloned().unwrap_or(serde_json::Value::Null),
            "active_pack_count": knowledge.get("active_pack_ids").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
            "blocked_namespace_count": knowledge.get("blocked_namespaces").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
        },
        "payload_truncated": true,
        "full_payload_hint": "repeat the request with include_payload=true to retrieve full ContextTurnReport evidence",
    })
}

fn take_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn runtime_run_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "run": payload,
    })
}

fn runtime_run_tree_summary(runs: &[serde_json::Value]) -> serde_json::Value {
    use std::collections::BTreeSet;

    let mut parents: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut latest_status: BTreeMap<String, String> = BTreeMap::new();
    let mut failed_count = 0usize;
    let mut completed_count = 0usize;
    let mut running_count = 0usize;

    for event in runs {
        let run = &event["run"];
        let Some(run_id) = run.get("run_id").and_then(|value| value.as_str()) else {
            continue;
        };
        let parent = run
            .get("parent_run_id")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        parents.entry(run_id.to_string()).or_insert(parent.clone());
        if let Some(parent_id) = parent {
            children
                .entry(parent_id)
                .or_default()
                .insert(run_id.to_string());
        }
        if let Some(status) = run.get("status").and_then(|value| value.as_str()) {
            latest_status.insert(run_id.to_string(), status.to_string());
        }
    }

    for status in latest_status.values() {
        match status.as_str() {
            "completed" => completed_count += 1,
            "failed" | "timeout" | "cancelled" => failed_count += 1,
            "running" => running_count += 1,
            _ => {}
        }
    }

    let roots = parents
        .iter()
        .filter_map(|(run_id, parent)| {
            let is_root = match parent.as_ref() {
                Some(parent_id) => !parents.contains_key(parent_id),
                None => true,
            };
            is_root.then(|| run_id.clone())
        })
        .collect::<Vec<_>>();
    let root_count = roots.len();
    let child_map = children
        .into_iter()
        .map(|(parent, child_ids)| (parent, child_ids.into_iter().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();

    serde_json::json!({
        "roots": roots,
        "children": child_map,
        "summary": {
            "event_count": runs.len(),
            "span_count": parents.len(),
            "root_count": root_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "running_count": running_count,
        }
    })
}

fn session_event_value(event: &SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "payload": payload,
    })
}

fn latest_payload_by_type(
    events: &[serde_json::Value],
    event_type: &str,
) -> Option<serde_json::Value> {
    events
        .iter()
        .rev()
        .find(|event| event["type"].as_str() == Some(event_type))
        .and_then(|event| event.get("payload").cloned())
}

fn collect_payloads_by_types(
    events: &[serde_json::Value],
    event_types: &[&str],
) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter(|event| {
            event["type"]
                .as_str()
                .is_some_and(|event_type| event_types.contains(&event_type))
        })
        .filter_map(|event| event.get("payload").cloned())
        .collect()
}

fn runtime_tool_payload_from_event(event: &serde_json::Value) -> Option<serde_json::Value> {
    let event_type = event.get("type").and_then(serde_json::Value::as_str)?;
    let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
    if matches!(
        event_type,
        "ToolStart" | "ToolProgress" | "ToolComplete" | "ToolFailure"
    ) {
        return Some(payload.clone());
    }
    if event_type != "RuntimeEvent" {
        return None;
    }
    let scope = payload.get("scope").and_then(serde_json::Value::as_str)?;
    if scope != "tool" {
        return None;
    }
    let mut tool_payload = payload
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    if let Some(object) = tool_payload.as_object_mut() {
        object.insert(
            "runtime_event_kind".to_string(),
            payload
                .get("kind")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "runtime_event_status".to_string(),
            payload
                .get("status")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "runtime_event_id".to_string(),
            payload
                .get("event_id")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    Some(tool_payload)
}

fn collect_tool_timeline(events: &[serde_json::Value]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(runtime_tool_payload_from_event)
        .collect()
}

fn payload_type_contains(payload: &serde_json::Value, needles: &[&str]) -> bool {
    payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .or_else(|| payload.get("kind").and_then(serde_json::Value::as_str))
        .or_else(|| payload.get("event").and_then(serde_json::Value::as_str))
        .map(|value| {
            let value = value.to_ascii_lowercase();
            needles.iter().any(|needle| value.contains(needle))
        })
        .unwrap_or(false)
}

#[derive(Default)]
struct TurnProjectionAccumulator {
    turn_id: String,
    status: String,
    submitted_at_ms: Option<u64>,
    started_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    user_preview: Option<String>,
    assistant_preview: Option<String>,
    tool_calls: Vec<serde_json::Value>,
    approvals: Vec<serde_json::Value>,
    context_events: Vec<serde_json::Value>,
    usage: Vec<serde_json::Value>,
    evidence_refs: BTreeSet<String>,
    event_sequences: Vec<usize>,
}

impl TurnProjectionAccumulator {
    fn new(turn_id: impl Into<String>) -> Self {
        Self {
            turn_id: turn_id.into(),
            status: "pending".to_string(),
            ..Self::default()
        }
    }

    fn observe_event(&mut self, event: &serde_json::Value) {
        if let Some(sequence) = event.get("sequence").and_then(serde_json::Value::as_u64) {
            self.event_sequences.push(sequence as usize);
        }
        let event_type = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
        if event_type == "TurnJournal" {
            self.observe_turn_journal(event, payload);
            return;
        }
        if let Some(tool_payload) = runtime_tool_payload_from_event(event) {
            self.tool_calls.push(tool_payload);
        } else {
            match event_type {
                "ApprovalRequested" | "ApprovalResolved" | "RiskApproval" => {
                    self.approvals.push(payload.clone());
                }
                "ContextEnvelope" | "ContextTurnReport" | "ContextRecommendationAction" => {
                    self.context_events.push(payload.clone());
                }
                "TokenUsage" | "RunModelTelemetry" => {
                    self.usage.push(payload.clone());
                }
                "SurfaceMessageProcessed" => {
                    if let Some(preview) = payload
                        .get("response_preview")
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                    {
                        self.assistant_preview = Some(preview.to_string());
                    }
                }
                _ => {}
            }
        }
        collect_projection_evidence_refs(payload, &mut self.evidence_refs);
    }

    fn observe_turn_journal(&mut self, event: &serde_json::Value, payload: &serde_json::Value) {
        let created_at_ms = event
            .get("created_at_ms")
            .and_then(serde_json::Value::as_u64);
        match payload.get("phase").and_then(serde_json::Value::as_str) {
            Some("submitted") => {
                self.status = "pending".to_string();
                self.submitted_at_ms = self.submitted_at_ms.or(created_at_ms);
                if let Some(preview) = payload
                    .get("payload")
                    .and_then(|inner| inner.get("prompt_preview"))
                    .and_then(serde_json::Value::as_str)
                {
                    self.user_preview = Some(preview.to_string());
                }
            }
            Some("running") => {
                self.status = "running".to_string();
                self.started_at_ms = self.started_at_ms.or(created_at_ms);
            }
            Some("completed") => {
                self.status = "completed".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            Some("failed") => {
                self.status = "failed".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            Some("cancelled") => {
                self.status = "cancelled".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            _ => {}
        }
        collect_projection_evidence_refs(payload, &mut self.evidence_refs);
    }

    fn into_value(self) -> serde_json::Value {
        serde_json::json!({
            "turn_id": self.turn_id,
            "status": self.status,
            "submitted_at_ms": self.submitted_at_ms,
            "started_at_ms": self.started_at_ms,
            "completed_at_ms": self.completed_at_ms,
            "user_preview": self.user_preview,
            "assistant_preview": self.assistant_preview,
            "tool_calls": self.tool_calls,
            "approvals": self.approvals,
            "context_events": self.context_events,
            "usage": self.usage,
            "evidence_refs": self.evidence_refs.into_iter().collect::<Vec<_>>(),
            "event_sequences": self.event_sequences,
        })
    }
}

fn collect_projection_evidence_refs(payload: &serde_json::Value, out: &mut BTreeSet<String>) {
    for key in [
        "evidence_ref",
        "evidence_id",
        "raw_ref",
        "full_output_ref",
        "output_ref",
        "context_report_id",
    ] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_str) {
            out.insert(value.to_string());
        }
    }
    if let Some(values) = payload
        .get("evidence_refs")
        .and_then(serde_json::Value::as_array)
    {
        for value in values {
            if let Some(value) = value.as_str() {
                out.insert(value.to_string());
            }
        }
    }
    if let Some(object) = payload.as_object() {
        for value in object.values() {
            if value.is_object() {
                collect_projection_evidence_refs(value, out);
            }
        }
    }
}

fn turn_id_from_event_value(event: &serde_json::Value) -> Option<String> {
    let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
    payload
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            payload
                .get("payload")
                .and_then(|inner| inner.get("turn_id"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            payload
                .get("turn")
                .and_then(|turn| turn.get("turn_id"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            payload
                .get("receipt")
                .and_then(|turn| turn.get("turn_id"))
                .and_then(serde_json::Value::as_str)
        })
        .map(ToString::to_string)
}

fn turn_projection_from_event_values(
    session_id: &str,
    events: &[serde_json::Value],
) -> serde_json::Value {
    let mut turns: BTreeMap<String, TurnProjectionAccumulator> = BTreeMap::new();
    let mut unbound_events = Vec::new();
    for event in events {
        if let Some(turn_id) = turn_id_from_event_value(event) {
            turns
                .entry(turn_id.clone())
                .or_insert_with(|| TurnProjectionAccumulator::new(turn_id))
                .observe_event(event);
        } else {
            unbound_events.push(event.clone());
        }
    }
    let mut turn_accumulators = turns.into_values().collect::<Vec<_>>();
    turn_accumulators
        .sort_by_key(|turn| turn.event_sequences.first().copied().unwrap_or(usize::MAX));
    let turn_values = turn_accumulators
        .into_iter()
        .map(TurnProjectionAccumulator::into_value)
        .collect::<Vec<_>>();
    serde_json::json!({
        "kind": "session.turn_projection",
        "source": "gateway.session_events.turn_journal",
        "session_id": session_id,
        "turn_count": turn_values.len(),
        "turns": turn_values,
        "unbound_event_count": unbound_events.len(),
        "unbound_events": unbound_events.into_iter().rev().take(20).collect::<Vec<_>>(),
    })
}

fn session_run_projection_from_events(
    session_id: &str,
    stored_events: Vec<SessionEvent>,
    stats: Option<serde_json::Value>,
) -> serde_json::Value {
    let events = stored_events
        .iter()
        .map(session_event_value)
        .collect::<Vec<_>>();
    let turn_projection = turn_projection_from_event_values(session_id, &events);
    let runs = stored_events
        .iter()
        .filter(|event| event.event_type == "RuntimeRun")
        .cloned()
        .map(runtime_run_event_json)
        .collect::<Vec<_>>();
    let runtime_run_count = runs.len();
    let run_graph = runtime_run_tree_summary(&runs);
    let mut tool_timeline = collect_tool_timeline(&events);
    tool_timeline.extend(collect_payloads_by_types(&events, &["PartialAnswer"]));
    let token_usage = collect_payloads_by_types(&events, &["TokenUsage"]);
    let latest_model_telemetry = latest_payload_by_type(&events, "RunModelTelemetry")
        .and_then(|payload| payload.get("telemetry").cloned().or(Some(payload)));
    let latest_context_payload = latest_payload_by_type(&events, "ContextEnvelope");
    let latest_context_envelope = latest_context_payload
        .as_ref()
        .and_then(|payload| payload.get("envelope").cloned());
    let latest_context_turn_report = latest_payload_by_type(&events, "ContextTurnReport");
    let selected_count = latest_context_envelope
        .as_ref()
        .and_then(|envelope| envelope.get("selected"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let omitted_count = latest_context_envelope
        .as_ref()
        .and_then(|envelope| envelope.get("omitted"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let evidence_refs = latest_context_envelope
        .as_ref()
        .and_then(|envelope| envelope.get("selected"))
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("id")
                        .or_else(|| item.get("memory_id"))
                        .or_else(|| item.get("source_id"))
                        .cloned()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut tool_counts: BTreeMap<String, usize> = BTreeMap::new();
    for tool in &tool_timeline {
        let name = tool
            .get("name")
            .or_else(|| tool.get("tool_name"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("tool")
            .to_string();
        *tool_counts.entry(name).or_default() += 1;
    }
    let agent_events = events
        .iter()
        .filter(|event| {
            payload_type_contains(
                event.get("payload").unwrap_or(&serde_json::Value::Null),
                &[
                    "agent",
                    "team",
                    "mission",
                    "execution_graph",
                    "collaboration",
                ],
            ) || payload_type_contains(event, &["agent", "team", "mission", "execution_graph"])
        })
        .take(50)
        .cloned()
        .collect::<Vec<_>>();
    let approval_events = events
        .iter()
        .filter(|event| {
            payload_type_contains(
                event.get("payload").unwrap_or(&serde_json::Value::Null),
                &["approval", "risk", "permission"],
            ) || payload_type_contains(event, &["approval", "risk", "permission"])
        })
        .take(50)
        .cloned()
        .collect::<Vec<_>>();

    serde_json::json!({
        "kind": "session.run_projection",
        "source": "gateway.session_events",
        "session_id": session_id,
        "turn_projection": turn_projection,
        "view_modes": {
            "default": "full_evidence",
            "pure_available": true,
            "pure_description": "正文和计数摘要由 surface 自行展示；证据详情来自同一投影。"
        },
        "runs": runs,
        "run_graph": run_graph,
        "tool_timeline": tool_timeline,
        "tool_summary": {
            "count": tool_counts.values().sum::<usize>(),
            "by_name": tool_counts,
        },
        "token_speed": {
            "stats": stats,
            "token_usage": token_usage,
            "model_telemetry": latest_model_telemetry,
        },
        "memory_context": {
            "context_envelope": latest_context_envelope,
            "context_turn_report": latest_context_turn_report,
            "selected_count": selected_count,
            "omitted_count": omitted_count,
            "evidence_refs": evidence_refs,
        },
        "team_session": {
            "runtime_run_count": runtime_run_count,
            "agent_events": agent_events,
            "session_event_count": events.len(),
        },
        "risk_approval": {
            "count": approval_events.len(),
            "approval_events": approval_events,
        },
        "event_digest": {
            "total": events.len(),
            "last_sequence": events.last().and_then(|event| event["sequence"].as_u64()),
            "recent": events.iter().rev().take(20).cloned().collect::<Vec<_>>(),
        }
    })
}

async fn get_session_runs(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let Some((total, stored_events)) = state
        .services
        .session
        .stored_events_by_type_page(&id, "RuntimeRun", from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime runs: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let runs: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(runtime_run_event_json)
        .collect();
    let tree = runtime_run_tree_summary(&runs);
    let next_seq = runs
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = runs.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "runs": runs,
        "tree": tree,
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

async fn get_session_turns(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(2_000).min(10_000);
    let Some((total, stored_events)) = state
        .services
        .session
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session turn events: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let events = stored_events
        .iter()
        .map(session_event_value)
        .collect::<Vec<_>>();
    let next_seq = events
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = next_seq.is_some_and(|next| next < total);
    let mut projection = turn_projection_from_event_values(&id, &events);
    projection["paging"] = serde_json::json!({
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    });
    Ok(Json(projection))
}

async fn get_session_turn(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, turn_id)): Path<(String, String)>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(2_000).min(10_000);
    let Some((_total, stored_events)) = state
        .services
        .session
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session turn events: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let events = stored_events
        .iter()
        .map(session_event_value)
        .collect::<Vec<_>>();
    let projection = turn_projection_from_event_values(&id, &events);
    let turn = projection
        .get("turns")
        .and_then(serde_json::Value::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .find(|turn| {
                    turn.get("turn_id").and_then(serde_json::Value::as_str) == Some(&turn_id)
                })
                .cloned()
        });
    match turn {
        Some(turn) => Ok(Json(serde_json::json!({
            "kind": "session.turn_projection.item",
            "session_id": id,
            "turn_id": turn_id,
            "turn": turn,
        }))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("turn {turn_id} not found in session {id}"),
            }),
        )),
    }
}

async fn get_session_projection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(2_000).min(10_000);
    let Some((total, stored_events)) = state
        .services
        .session
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session projection events: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let active_stats = if let Some(runtime_service) = state.services.runtime.as_ref() {
        runtime_service
            .active_session_stats(&id)
            .await
            .and_then(|stats| serde_json::to_value(stats).ok())
    } else {
        None
    };
    let stats = if active_stats.is_some() {
        active_stats
    } else {
        stored_session_stats_response(&state, &id)
            .await
            .ok()
            .and_then(|Json(stats)| serde_json::to_value(stats).ok())
    };
    let mut projection = session_run_projection_from_events(&id, stored_events, stats);
    projection["paging"] = serde_json::json!({
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": projection["event_digest"]["total"].as_u64().unwrap_or_default() < total as u64,
    });

    Ok(Json(projection))
}

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(db_messages) = state
        .services
        .session
        .search_stored_messages(&params.q, params.limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("search failed: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let results: Vec<SearchMessagesItem> = db_messages
        .into_iter()
        .map(|message| {
            let blocks: Vec<serde_json::Value> =
                serde_json::from_str(&message.content_json).unwrap_or_default();
            let content_preview = blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(|text| text.as_str()))
                .collect::<Vec<_>>()
                .join(" ");
            let preview = if content_preview.len() > 200 {
                format!("{}...", &content_preview[..200])
            } else {
                content_preview
            };
            SearchMessagesItem {
                session_id: message.session_id,
                sequence: message.sequence,
                role: message.role,
                blocks,
                content_preview: preview,
                tool_use_id: message.tool_use_id,
                tool_name: message.tool_name,
                created_at_ms: message.created_at_ms,
            }
        })
        .collect();

    let total = results.len();
    Ok(Json(SearchMessagesResponse {
        query: params.q,
        results,
        total,
    }))
}

async fn compact_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service not configured".to_string(),
            }),
        )
    })?;
    match runtime_service.compact_active_session(&id).await {
        Ok(Some(result)) => {
            tracing::info!(
                session_id = %id,
                removed = result.removed_message_count,
                "API session compacted"
            );
            Ok(Json(result))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )),
        Err(error) => {
            tracing::error!(session_id = %id, error = %error, "failed to sync compacted session to unified store");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to sync compacted session: {error}"),
                }),
            ))
        }
    }
}

async fn get_session_stats_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service not configured".to_string(),
            }),
        )
    })?;
    if let Some(stats) = runtime_service.active_session_stats(&id).await {
        return Ok(Json(stats));
    }
    stored_session_stats_response(&state, &id).await
}

fn stored_session_duration_ms(record: &SessionRecord) -> u64 {
    let Ok(created) = chrono::DateTime::parse_from_rfc3339(&record.created_at) else {
        return 0;
    };
    let Ok(updated) = chrono::DateTime::parse_from_rfc3339(&record.last_activity) else {
        return 0;
    };
    updated
        .signed_duration_since(created)
        .num_milliseconds()
        .max(0) as u64
}

fn token_count_from_usage(message: &memory::store::session::SessionMessage, key: &str) -> u32 {
    message
        .token_usage_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|value| {
            value
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .map(|v| v as u32)
        })
        .unwrap_or(0)
}

async fn stored_session_stats_response(
    state: &AppState,
    id: &str,
) -> Result<Json<SessionStatsSnapshot>, (StatusCode, Json<ErrorResponse>)> {
    let record = state
        .services
        .session
        .stored_session(id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load stored session: {error}"),
                }),
            )
        })?;
    let Some(record) = record else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    };
    let messages = state
        .services
        .session
        .stored_messages(id, 0, 10_000)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load stored session messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let mut counts = SessionMessageCounts {
        user: 0,
        assistant: 0,
        tool: 0,
    };
    let mut tool_usage: HashMap<String, usize> = HashMap::new();
    let mut input_tokens = record.input_tokens.max(0) as u32;
    let mut output_tokens = record.output_tokens.max(0) as u32;
    for message in &messages {
        match message.role.as_str() {
            "user" => counts.user += 1,
            "assistant" => counts.assistant += 1,
            "tool" => counts.tool += 1,
            _ => {}
        }
        if let Some(tool_name) = message.tool_name.as_ref().filter(|name| !name.is_empty()) {
            *tool_usage.entry(tool_name.clone()).or_insert(0) += 1;
        }
        if input_tokens == 0 {
            input_tokens =
                input_tokens.saturating_add(token_count_from_usage(message, "input_tokens"));
        }
        if output_tokens == 0 {
            output_tokens =
                output_tokens.saturating_add(token_count_from_usage(message, "output_tokens"));
        }
    }
    Ok(Json(SessionStatsSnapshot {
        session_id: id.to_string(),
        message_count: messages.len().max(record.message_count.max(0) as usize),
        message_counts: counts,
        tokens: SessionTokenCounts {
            input: input_tokens,
            output: output_tokens,
            total: input_tokens.saturating_add(output_tokens),
        },
        tool_usage,
        duration_ms: stored_session_duration_ms(&record),
    }))
}

async fn update_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SessionUpdateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let active_found = match state.services.runtime.as_ref() {
        Some(runtime_service) => {
            runtime_service
                .update_active_session_model(&id, body.model.as_deref())
                .await
        }
        None => false,
    };
    let stored_found = state
        .services
        .session
        .update_session(&id, body)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to update session: {error}"),
                }),
            )
        })?;

    if active_found || stored_found {
        Ok(Json(serde_json::json!({
            "session_id": id,
            "updated": true,
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_event(
        sequence: usize,
        event_type: &str,
        payload: serde_json::Value,
    ) -> SessionEvent {
        SessionEvent {
            session_id: "session-v31".to_string(),
            event_type: event_type.to_string(),
            event_json: payload.to_string(),
            sequence,
            created_at_ms: 1_000 + sequence as u64,
        }
    }

    #[test]
    fn turn_projection_builds_stable_turns_from_journal() {
        let events = vec![
            session_event(
                0,
                "TurnJournal",
                serde_json::json!({
                    "session_id": "session-v31",
                    "turn_id": "turn-1",
                    "event_id": "evt-1",
                    "sequence": 0,
                    "event_type": "turn.submitted",
                    "phase": "submitted",
                    "source": "gateway.runtime_service",
                    "idempotency_key": "session-v31:turn-1:turn.submitted",
                    "payload": {
                        "prompt_preview": "analyse this",
                        "task_id": "task-1"
                    },
                    "created_at": "2026-07-05T00:00:00Z"
                }),
            ),
            session_event(
                1,
                "TurnJournal",
                serde_json::json!({
                    "session_id": "session-v31",
                    "turn_id": "turn-1",
                    "event_id": "evt-2",
                    "sequence": 1,
                    "event_type": "turn.running",
                    "phase": "running",
                    "source": "gateway.runtime_service",
                    "idempotency_key": "session-v31:turn-1:turn.running",
                    "payload": {},
                    "created_at": "2026-07-05T00:00:01Z"
                }),
            ),
            session_event(
                2,
                "SurfaceMessageProcessed",
                serde_json::json!({
                    "type": "SurfaceMessageProcessed",
                    "turn_id": "turn-1",
                    "response_preview": "done"
                }),
            ),
            session_event(
                3,
                "TurnJournal",
                serde_json::json!({
                    "session_id": "session-v31",
                    "turn_id": "turn-1",
                    "event_id": "evt-3",
                    "sequence": 3,
                    "event_type": "turn.completed",
                    "phase": "completed",
                    "source": "gateway.runtime_service",
                    "idempotency_key": "session-v31:turn-1:turn.completed",
                    "payload": {
                        "context_report_id": "ctx-report-1"
                    },
                    "created_at": "2026-07-05T00:00:02Z"
                }),
            ),
        ];
        let values = events.iter().map(session_event_value).collect::<Vec<_>>();
        let projection = turn_projection_from_event_values("session-v31", &values);

        assert_eq!(projection["kind"], "session.turn_projection");
        assert_eq!(projection["turn_count"], 1);
        assert_eq!(projection["turns"][0]["turn_id"], "turn-1");
        assert_eq!(projection["turns"][0]["status"], "completed");
        assert_eq!(projection["turns"][0]["user_preview"], "analyse this");
        assert_eq!(projection["turns"][0]["assistant_preview"], "done");
        assert_eq!(
            projection["turns"][0]["event_sequences"],
            serde_json::json!([0, 1, 2, 3])
        );
        assert!(projection["turns"][0]["evidence_refs"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("ctx-report-1")));
    }

    #[test]
    fn session_run_projection_includes_tool_contract_runtime_events() {
        let events = vec![session_event(
            0,
            "RuntimeEvent",
            serde_json::json!({
                "event_id": "event-tool-1",
                "session_id": "session-v31",
                "sequence": 0,
                "scope": "tool",
                "kind": "tool.invocation.completed",
                "status": "completed",
                "payload": {
                    "contract_version": 2,
                    "invocation_id": "tool-inv-1",
                    "tool_call_id": "tool-1",
                    "tool_name": "read",
                    "turn_index": 1,
                    "status": "completed",
                    "advertised_registration_id": "tool-reg:v2:read_only:read",
                    "effective_registration_id": "tool-reg:v2:read_only:read",
                    "model_visible_preview": "ok",
                    "full_output_ref": "tool://raw-1",
                    "raw_output_tokens": 100,
                    "preview_tokens": 10,
                    "context_saved_tokens": 90,
                    "context_saved_ratio": 9000,
                    "stale_registration": false
                },
                "created_at_ms": 1000
            }),
        )];

        let projection = session_run_projection_from_events("session-v31", events, None);

        assert_eq!(projection["tool_timeline"].as_array().unwrap().len(), 1);
        assert_eq!(projection["tool_timeline"][0]["contract_version"], 2);
        assert_eq!(
            projection["tool_timeline"][0]["full_output_ref"],
            "tool://raw-1"
        );
        assert_eq!(
            projection["tool_timeline"][0]["runtime_event_kind"],
            "tool.invocation.completed"
        );
    }

    #[test]
    fn session_run_projection_aggregates_runtime_evidence() {
        let events = vec![
            session_event(
                0,
                "RuntimeRun",
                serde_json::json!({
                    "run_id": "run-root",
                    "parent_run_id": null,
                    "status": "running"
                }),
            ),
            session_event(
                1,
                "ToolStart",
                serde_json::json!({
                    "type": "ToolStart",
                    "run_id": "run-root",
                    "id": "tool-1",
                    "name": "read",
                    "preview": "README.md"
                }),
            ),
            session_event(
                2,
                "ToolComplete",
                serde_json::json!({
                    "type": "ToolComplete",
                    "run_id": "run-root",
                    "id": "tool-1",
                    "name": "read",
                    "summary": "ok",
                    "exit_code": 0
                }),
            ),
            session_event(
                3,
                "TokenUsage",
                serde_json::json!({
                    "type": "TokenUsage",
                    "input": 100,
                    "output": 40,
                    "cache_create": 0,
                    "cache_read": 10,
                    "total": 150
                }),
            ),
            session_event(
                4,
                "RunModelTelemetry",
                serde_json::json!({
                    "type": "RunModelTelemetry",
                    "telemetry": {
                        "model": "deepseek-v4-flash",
                        "tokens_per_second": 24.5
                    }
                }),
            ),
            session_event(
                5,
                "ContextEnvelope",
                serde_json::json!({
                    "type": "ContextEnvelope",
                    "envelope_id": "ctx-1",
                    "envelope": {
                        "id": "ctx-1",
                        "selected": [{"id": "mem-1"}, {"id": "mem-2"}],
                        "omitted": [{"id": "mem-old"}],
                        "diagnostics": {
                            "pressure_bp": 1000
                        }
                    }
                }),
            ),
            session_event(
                6,
                "ContextTurnReport",
                serde_json::json!({
                    "type": "ContextTurnReport",
                    "run_id": "run-root",
                    "turn_id": "turn-1",
                    "context_turn_report": {
                        "selected_count": 2,
                        "omitted_count": 1
                    }
                }),
            ),
            session_event(
                7,
                "RuntimeRun",
                serde_json::json!({
                    "run_id": "run-root",
                    "parent_run_id": null,
                    "status": "completed"
                }),
            ),
            session_event(
                8,
                "AgentTeamStatus",
                serde_json::json!({
                    "type": "AgentTeamStatus",
                    "agent_count": 2
                }),
            ),
            session_event(
                9,
                "ApprovalRequested",
                serde_json::json!({
                    "type": "ApprovalRequested",
                    "risk": "workspace_write"
                }),
            ),
        ];

        let projection = session_run_projection_from_events(
            "session-v31",
            events,
            Some(serde_json::json!({
                "tokens": {
                    "input": 100,
                    "output": 40,
                    "total": 140
                }
            })),
        );

        assert_eq!(projection["kind"], "session.run_projection");
        assert_eq!(projection["source"], "gateway.session_events");
        assert_eq!(projection["view_modes"]["default"], "full_evidence");
        assert_eq!(projection["run_graph"]["summary"]["completed_count"], 1);
        assert_eq!(projection["tool_summary"]["by_name"]["read"], 2);
        assert_eq!(projection["token_speed"]["token_usage"][0]["total"], 150);
        assert_eq!(
            projection["token_speed"]["model_telemetry"]["model"],
            "deepseek-v4-flash"
        );
        assert_eq!(projection["memory_context"]["selected_count"], 2);
        assert_eq!(projection["memory_context"]["omitted_count"], 1);
        assert_eq!(projection["memory_context"]["evidence_refs"][0], "mem-1");
        assert_eq!(
            projection["team_session"]["agent_events"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(projection["risk_approval"]["count"], 1);
    }
}
