use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use session::{SessionEvent, SessionMessage, SessionRecord};
use sha2::{Digest, Sha256};

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
        .route(
            "/api/sessions/:id/ensure",
            post(ensure_surface_session_handler),
        )
        .route("/api/sessions/:id/branch", post(branch_session_handler))
        .route("/api/sessions/:id/archive", post(archive_session_handler))
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
        .route(
            "/api/sessions/:id/execution/live",
            get(get_session_execution_live),
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<
    Json<harness_contract::projection::SessionEvidenceProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let projection = session_evidence_projection(&state, &principal, &id, None).await?;
    Ok(Json(projection))
}

pub(super) async fn get_turn_evidence(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path((id, turn_id)): Path<(String, String)>,
) -> Result<
    Json<harness_contract::projection::TurnEvidenceProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let projection =
        session_evidence_projection(&state, &principal, &id, Some(turn_id.as_str())).await?;
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
    principal: &AuthenticatedPrincipal,
    session_id: &str,
    turn_id: Option<&str>,
) -> Result<
    harness_contract::projection::SessionEvidenceProjection,
    (StatusCode, Json<ErrorResponse>),
> {
    use harness_contract::projection::{
        EvidenceFreshness, ProjectionDetailScope, SessionEvidenceProjection, TurnEvidenceProjection,
    };

    // Authorize the durable Session boundary before inspecting its outbox.
    // A pruned execution graph cannot turn an unauthorized request into an
    // "unavailable" projection that still leaks turn, message or receipt
    // identifiers from the outbox.
    authorize_session_evidence_read(state, principal, session_id).await?;
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let mut records = state
        .services
        .session
        .runtime_inputs(session_id, 100)
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

    const EVIDENCE_PROJECTION_CONCURRENCY: usize = 8;
    let runtime_services = runtime_service.runtime_services();
    let mut projected = stream::iter(records.into_iter().enumerate().map(|(index, record)| {
        let runtime_services = Arc::clone(&runtime_services);
        async move {
            // The relation is deterministic from the durable ingress identity;
            // never approximate it from message text, sequence, or timestamps.
            let execution_id =
                runtime::session_ingress_graph_id(session_id, &record.request_id, &record.turn_id);
            let projection = match super::runtime_routes::execution_projection_context(
                state,
                principal,
                &execution_id,
                ProjectionDetailScope::Summary,
            )
            .await
            {
                Ok(context) => {
                    match runtime::execution_projection::snapshot(
                        runtime_services.as_ref(),
                        &execution_id,
                        &context,
                    )
                    .await
                    {
                        Ok(projection) => Some(projection),
                        Err(runtime::RuntimeServicesError::ProjectionAccessDenied) => {
                            return Err((
                                StatusCode::FORBIDDEN,
                                Json(ErrorResponse {
                                    error: format!(
                                        "execution {execution_id} evidence is outside the authenticated principal scope"
                                    ),
                                }),
                            ));
                        }
                        Err(_) => None,
                    }
                }
                Err((StatusCode::NOT_FOUND, _)) => None,
                Err(error) => return Err(error),
            };
            let evidence_refs = projection
                .as_ref()
                .map(|projection| {
                    projection
                        .evidence
                        .iter()
                        .flat_map(|entity| entity.evidence_refs.iter().cloned())
                        .collect::<BTreeSet<_>>()
                })
                .unwrap_or_default();
            let freshness = if runtime_service.execution_live(&execution_id).is_some() {
                EvidenceFreshness::Live
            } else if projection.is_some() {
                EvidenceFreshness::Durable
            } else {
                EvidenceFreshness::Unavailable
            };
            let terminal_ref = (record.status == session::SessionRuntimeInputStatus::Completed)
                .then(|| format!("turn-terminal:{}", record.request_id));
            let assistant_message_id = terminal_ref
                .as_ref()
                .map(|_| format!("assistant:{}", record.message_id));
            Ok((
                index,
                TurnEvidenceProjection {
                    session_id: session_id.to_string(),
                    turn_id: record.turn_id,
                    input_message_id: record.message_id,
                    execution_id,
                    terminal_ref,
                    assistant_message_id,
                    context_report_id: None,
                    evidence_refs: evidence_refs.into_iter().collect(),
                    freshness,
                },
            ))
        }
    }))
    .buffer_unordered(EVIDENCE_PROJECTION_CONCURRENCY)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    projected.sort_by_key(|(index, _)| *index);
    let turns = projected
        .into_iter()
        .map(|(_, projection)| projection)
        .collect::<Vec<_>>();
    let all_refs = turns
        .iter()
        .flat_map(|turn| turn.evidence_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionAccess {
    /// Inspect durable or live Session facts without changing them.
    Read,
    /// Add input, attach a surface, cancel a turn, or update mutable metadata.
    Write,
    /// Irreversibly remove or compact a Session.
    Destructive,
}

async fn authorize_session_evidence_read(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    session_id: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(state, principal, session_id, SessionAccess::Read).await
}

/// Resolve Session authority before a route can inspect, mutate, replay, or
/// stream a session.  Session ids are not bearer credentials: a route must
/// prove owner identity, an explicit delegated scope, or privileged human
/// maintenance authority.  The action is deliberately part of the decision:
/// Mission observers may read their Mission's sessions but cannot alter or
/// destroy them merely because they can follow execution progress.
pub(super) async fn authorize_session_access(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    session_id: &str,
    access: SessionAccess,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let record = state
        .services
        .session
        .stored_session(session_id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to resolve session ownership: {error}"),
                }),
            )
        })?;
    let active = state
        .services
        .runtime
        .as_ref()
        .is_some_and(|runtime| runtime.has_active_session(session_id));
    if record.is_none() && !active {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {session_id} not found"),
            }),
        ));
    }
    let claims = principal.0.claims();
    let owner_matches = record
        .as_ref()
        .and_then(|record| session_owner_from_metadata(record.metadata_json.as_deref()))
        .is_some_and(|owner| owner == claims.principal_id);
    let explicit_session = claims
        .scopes
        .iter()
        .any(|scope| scope == &format!("session:{session_id}"));
    let mission_id = state.services.runtime.as_ref().and_then(|runtime| {
        runtime::MissionRuntimePort::new(runtime.runtime_services())
            .mission_id_for_session(session_id)
    });
    let explicit_mission = mission_id.as_ref().is_some_and(|mission_id| {
        claims
            .scopes
            .iter()
            .any(|scope| scope == &format!("mission:{mission_id}"))
    });
    let manager = principal.0.is_human_interactive()
        && principal.0.has_capability("runtime.maintenance.manage");
    if session_access_authorized(
        access,
        owner_matches,
        explicit_session,
        explicit_mission,
        manager,
    ) {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        Json(ErrorResponse {
            error: format!(
                "session {session_id} is outside the authenticated principal scope for {access:?} access"
            ),
        }),
    ))
}

async fn session_record_access_authorized(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    record: &SessionRecord,
    access: SessionAccess,
) -> bool {
    let claims = principal.0.claims();
    let owner_matches = session_owner_from_metadata(record.metadata_json.as_deref())
        .is_some_and(|owner| owner == claims.principal_id);
    let explicit_session = claims
        .scopes
        .iter()
        .any(|scope| scope == &format!("session:{}", record.session_id));
    let mission_id = state.services.runtime.as_ref().and_then(|runtime| {
        runtime::MissionRuntimePort::new(runtime.runtime_services())
            .mission_id_for_session(&record.session_id)
    });
    let explicit_mission = mission_id.as_ref().is_some_and(|mission_id| {
        claims
            .scopes
            .iter()
            .any(|scope| scope == &format!("mission:{mission_id}"))
    });
    let manager = principal.0.is_human_interactive()
        && principal.0.has_capability("runtime.maintenance.manage");
    session_access_authorized(
        access,
        owner_matches,
        explicit_session,
        explicit_mission,
        manager,
    )
}

/// Keep the authorization decision independent from projection/outbox lookup:
/// when the execution graph has been pruned there is still no authority to
/// disclose the durable turn binding that used to reference it.
fn session_access_authorized(
    access: SessionAccess,
    owner_matches: bool,
    explicit_session: bool,
    explicit_mission: bool,
    manager: bool,
) -> bool {
    match access {
        SessionAccess::Read => owner_matches || explicit_session || explicit_mission || manager,
        SessionAccess::Write => owner_matches || explicit_session || manager,
        SessionAccess::Destructive => owner_matches || manager,
    }
}

pub(super) async fn list_running_session_execution_indices(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
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
    let mut items = Vec::new();
    for index in runtime
        .recoverable_running_session_execution_indices()
        .await
    {
        if authorize_session_access(&state, &principal, &index.session_id, SessionAccess::Read)
            .await
            .is_ok()
        {
            items.push(index);
        }
    }
    Ok(Json(
        harness_contract::projection::SessionExecutionIndicesProjection { items },
    ))
}

pub(super) async fn get_session_execution_index(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
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
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    Ok(Json(runtime.recoverable_session_execution_index(&id).await))
}

pub(super) async fn get_session_execution_live(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<
    Json<harness_contract::projection::ExecutionLiveUpdate>,
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
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let index = runtime.recoverable_session_execution_index(&id).await;
    let execution_id = index.latest_execution_id.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} has no recoverable execution"),
            }),
        )
    })?;
    let live = runtime.execution_live(&execution_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("execution {execution_id} has no recoverable live snapshot"),
            }),
        )
    })?;
    Ok(Json(harness_contract::projection::ExecutionLiveUpdate {
        schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id,
        live,
    }))
}

#[derive(Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    message_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<harness_contract::projection::SessionExecutionIndexProjection>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BranchSessionRequest {
    pub(super) idempotency_key: String,
}

#[derive(Debug, Serialize)]
pub(super) struct BranchSessionReceipt {
    pub(super) id: String,
    pub(super) status: String,
    pub(super) message_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_tokens: Option<i64>,
    pub(super) operation_id: String,
    pub(super) source_session_id: String,
    pub(super) source_message_count: usize,
    pub(super) copied_message_count: usize,
    pub(super) replayed: bool,
}

fn branch_operation_identity(
    source_session_id: &str,
    principal_id: &str,
    idempotency_key: &str,
) -> (String, String) {
    let mut digest = Sha256::new();
    for value in [source_session_id, principal_id, idempotency_key] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    let digest = format!("{:x}", digest.finalize());
    (
        format!("session-branch:v1:{digest}"),
        format!("branch-{}", &digest[..32]),
    )
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
    #[serde(default = "default_include_execution")]
    include_execution: bool,
}

fn default_sort() -> String {
    "updated_at".to_string()
}

fn default_order() -> String {
    "desc".to_string()
}

fn default_include_execution() -> bool {
    true
}

fn required_session_service(
    state: &AppState,
) -> Result<&crate::services::SessionService, (StatusCode, Json<ErrorResponse>)> {
    Ok(&state.services.session)
}

fn session_service_error(error: String) -> (StatusCode, Json<ErrorResponse>) {
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
    headers: HeaderMap,
    Json(body): Json<SessionAttachRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let observer_id = headers
        .get("x-cowd-observer-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| super::validated_session_observer_id(Some(value)))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "session attach requires a valid x-cowd-observer-id".to_string(),
                }),
            )
        })?;
    let role = body.role.as_deref().unwrap_or("reader");
    if !matches!(role, "reader" | "writer") {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session attachment role must be reader or writer".to_string(),
            }),
        ));
    }
    authorize_session_access(
        &state,
        &principal,
        &id,
        if role == "writer" {
            SessionAccess::Write
        } else {
            SessionAccess::Read
        },
    )
    .await?;
    let actor_id = surface_actor_id(&principal, observer_id);
    Ok(Json(
        state
            .services
            .session
            .attach_session_value(&id, &actor_id, &body.surface, Some(role))
            .await,
    ))
}

async fn detach_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(_body): Json<SessionDetachRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Detach only removes the exact authenticated observer attachment derived
    // below. A reader must be able to leave without gaining conversation write
    // authority, otherwise read-only Surfaces leak lifecycle attachments.
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let observer_id = headers
        .get("x-cowd-observer-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| super::validated_session_observer_id(Some(value)))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "session detach requires a valid x-cowd-observer-id".to_string(),
                }),
            )
        })?;
    let actor_id = surface_actor_id(&principal, observer_id);
    Ok(Json(
        state
            .services
            .session
            .detach_session_value(&id, &actor_id)
            .await,
    ))
}

async fn session_lifecycle_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    Ok(Json(
        state
            .services
            .session
            .lifecycle_snapshot_value(Some(&id))
            .await,
    ))
}

async fn replay_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<SessionReplayParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    Ok(Json(
        state
            .services
            .session
            .replay_session_value(
                &id,
                params.from_sequence.unwrap_or(0),
                params.limit.unwrap_or(100),
            )
            .await,
    ))
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

fn session_owner_from_metadata(metadata_json: Option<&str>) -> Option<String> {
    metadata_json
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .and_then(|metadata| {
            metadata
                .get("owner_principal_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn principal_can_migrate_legacy_session(principal: &AuthenticatedPrincipal) -> bool {
    principal.0.is_human_interactive() && principal.0.has_capability("runtime.maintenance.manage")
}

fn session_info_from_record(record: SessionRecord) -> SessionInfo {
    SessionInfo {
        id: record.session_id,
        status: record.status,
        message_count: record.message_count,
        execution: None,
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
        message_count: 0,
        execution: None,
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<ListSessionsParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(200);
    let offset = params.offset.unwrap_or(0);

    if let Ok(Some(records)) = state.services.session.list_stored_sessions().await {
        let mut sessions = Vec::new();
        for record in records {
            if session_record_access_authorized(&state, &principal, &record, SessionAccess::Read)
                .await
            {
                sessions.push(session_info_from_record(record));
            }
        }
        filter_and_sort_session_infos(&mut sessions, &params);
        let total = sessions.len();
        let mut sessions: Vec<SessionInfo> =
            sessions.into_iter().skip(offset).take(limit).collect();
        if params.include_execution {
            enrich_session_execution_indices(&state, &mut sessions).await;
        }
        return Json(serde_json::json!({
            "sessions": sessions,
            "total": total,
            "offset": offset,
            "limit": limit,
            "sort": params.sort,
            "order": params.order,
        }));
    }

    let mut sessions = Vec::new();
    for id in state.services.session.list_active_session_ids() {
        if authorize_session_access(&state, &principal, &id, SessionAccess::Read)
            .await
            .is_ok()
        {
            sessions.push(active_session_info(id));
        }
    }
    filter_and_sort_session_infos(&mut sessions, &params);
    let total = sessions.len();
    let mut sessions: Vec<SessionInfo> = sessions.into_iter().skip(offset).take(limit).collect();
    if params.include_execution {
        enrich_session_execution_indices(&state, &mut sessions).await;
    }
    Json(serde_json::json!({
        "sessions": sessions,
        "total": total,
        "offset": offset,
        "limit": limit,
        "sort": params.sort,
        "order": params.order,
    }))
}

fn filter_and_sort_session_infos(sessions: &mut Vec<SessionInfo>, params: &ListSessionsParams) {
    if let Some(status) = params.status.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| session.status.eq_ignore_ascii_case(status));
    } else {
        sessions.retain(|session| !session.status.eq_ignore_ascii_case("deleted"));
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
}

async fn enrich_session_execution_indices(state: &AppState, sessions: &mut [SessionInfo]) {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return;
    };
    let indices = futures::future::join_all(
        sessions
            .iter()
            .map(|session| runtime.recoverable_session_execution_index(&session.id)),
    )
    .await;
    for (session, index) in sessions.iter_mut().zip(indices) {
        if index.latest_execution_id.is_some()
            || index.latest_status.is_some()
            || !index.active_execution_ids.is_empty()
        {
            session.execution = Some(index);
        }
    }
}

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");
    let model = body.model.filter(|model| !model.trim().is_empty());
    let session_service = required_session_service(&state)?;
    let mut request = crate::services::EnsureSessionRequest::new(
        &session_id,
        model,
        crate::services::SessionSource::WebUi,
    );
    request.owner_principal_id = Some(principal.0.claims().principal_id.clone());
    request.allow_legacy_owner_migration = principal_can_migrate_legacy_session(&principal);
    let outcome = session_service
        .create_user_session(request)
        .await
        .map_err(session_service_error)?;
    let info = session_info_from_record(outcome.record);

    Ok((StatusCode::CREATED, Json(info)))
}

async fn branch_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<BranchSessionRequest>,
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

    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    let idempotency_key = body.idempotency_key.trim();
    if idempotency_key.is_empty()
        || idempotency_key.len() > 256
        || idempotency_key.chars().any(char::is_control)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "idempotency_key must be a non-empty bounded non-control value".to_string(),
            }),
        ));
    }
    let principal_id = principal.0.claims().principal_id.clone();
    let (operation_id, branch_id) = branch_operation_identity(&id, &principal_id, idempotency_key);

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
    let model = source_record.and_then(|record| record.model);
    let outcome = required_session_service(&state)?
        .branch_session(
            &id,
            &branch_id,
            operation_id,
            format!("{} / branch", source_title),
            model,
            principal_id,
        )
        .await
        .map_err(session_service_error)?;
    tracing::info!(
        source_session_id = %id,
        branch_session_id = %branch_id,
        copied_message_count = outcome.copied_message_count,
        source_message_count = outcome.source_message_count,
        "session branch committed atomically"
    );
    let info = session_info_from_record(outcome.session.record);
    Ok((
        StatusCode::OK,
        Json(BranchSessionReceipt {
            id: info.id,
            status: info.status,
            message_count: info.message_count,
            title: info.title,
            model: info.model,
            created_at: info.created_at,
            updated_at: info.updated_at,
            input_tokens: info.input_tokens,
            output_tokens: info.output_tokens,
            operation_id: outcome.operation_id,
            source_session_id: id,
            source_message_count: outcome.source_message_count,
            copied_message_count: outcome.copied_message_count,
            replayed: outcome.replayed,
        }),
    ))
}

async fn ensure_surface_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
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

    if state
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
        })?
    {
        authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    }

    let mut request = crate::services::EnsureSessionRequest::new(
        &id,
        body.model.filter(|model| !model.trim().is_empty()),
        crate::services::SessionSource::Tui,
    );
    request.owner_principal_id = Some(principal.0.claims().principal_id.clone());
    request.allow_legacy_owner_migration = principal_can_migrate_legacy_session(&principal);
    let outcome = required_session_service(&state)?
        .ensure_surface_session(request)
        .await
        .map_err(session_service_error)?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "created": outcome.created,
        "restored": outcome.restored,
        "source": outcome.record.platform,
        "model": outcome.record.model,
        "active_sessions": state.services.session.list_active_session_ids().len(),
    })))
}

async fn get_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
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

    if state.services.session.has_active_session(&id) {
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
    headers: HeaderMap,
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

    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;

    let actor_id = format!("principal:{}", principal.0.claims().principal_id);
    let reason = body
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("user_requested");
    let cancelled_execution_ids = required_session_service(&state)?
        .cancel_active_turns(&id, reason)
        .map_err(session_service_error)?;
    let aborted_run_id = cancelled_execution_ids.first().cloned();
    let event = crate::event_bus::SessionProjectionEvent::TurnCancelRequested {
        session_id: id.clone(),
        actor_id: actor_id.clone(),
        reason: reason.to_string(),
        aborted_run_id: aborted_run_id.clone(),
        execution_ids: cancelled_execution_ids.clone(),
    };
    state.event_bus().publish(&id, event).await;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "status": "cancel_requested",
        "actor_id": actor_id,
        "reason": reason,
        "aborted": aborted_run_id.is_some(),
        "run_id": aborted_run_id,
        "execution_ids": cancelled_execution_ids,
    })))
}

async fn archive_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Destructive).await?;
    let archived = required_session_service(&state)?
        .archive_session(&id)
        .await
        .map_err(session_service_error)?;
    if archived {
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

async fn delete_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Destructive).await?;
    let removed = required_session_service(&state)?
        .delete_session(&id)
        .await
        .map_err(session_service_error)?;
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
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
        .iter()
        .map(|event| canonical_session_event_value(event, include_payload))
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
    canonical_session_event_value(event, true)
}

fn canonical_session_event_value(event: &SessionEvent, include_payload: bool) -> serde_json::Value {
    if event.event_type == session::SESSION_DOMAIN_EVENT_TYPE {
        return match session::SessionDomainEvent::from_session_event(event) {
            Ok(domain) => {
                let payload = session_event_payload_for_response(
                    &domain.kind,
                    domain.payload,
                    include_payload,
                );
                serde_json::json!({
                    "event_id": domain.event_id,
                    "session_id": domain.session_id,
                    "type": domain.kind,
                    "scope": domain.scope,
                    "status": domain.status,
                    "span_id": domain.span_id,
                    "parent_span_id": domain.parent_span_id,
                    "correlation_id": domain.correlation_id,
                    "refs": domain.refs,
                    "sequence": event.sequence,
                    "created_at_ms": domain.created_at_ms,
                    "payload": payload,
                })
            }
            Err(error) => serde_json::json!({
                "session_id": event.session_id,
                "type": session::SESSION_DOMAIN_EVENT_TYPE,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "payload": {
                    "raw": event.event_json,
                    "parse_error": error.to_string(),
                },
            }),
        };
    }
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    let payload = session_event_payload_for_response(&event.event_type, payload, include_payload);
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
        "ToolStart"
            | "ToolProgress"
            | "ToolComplete"
            | "ToolFailure"
            | "tool.invocation.started"
            | "tool.invocation.completed"
            | "tool.invocation.failed"
            | "tool.execution_plan.created"
    ) {
        let mut tool_payload = payload.clone();
        if let Some(object) = tool_payload.as_object_mut() {
            object.insert(
                "runtime_event_kind".to_string(),
                serde_json::Value::String(event_type.to_string()),
            );
            if let Some(status) = event.get("status").cloned() {
                object.insert("runtime_event_status".to_string(), status);
            }
            if let Some(event_id) = event.get("event_id").cloned() {
                object.insert("runtime_event_id".to_string(), event_id);
            }
        }
        return Some(tool_payload);
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

fn tool_instance_identity(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("cowd_tool_instance_id")
        .or_else(|| payload.get("tool_instance_id"))
        .or_else(|| payload.get("invocation_id"))
        .or_else(|| payload.get("id"))
        .or_else(|| payload.get("tool_call_id"))
        .or_else(|| payload.get("tool_use_id"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn merge_tool_projection_item(
    order: &mut Vec<String>,
    tools: &mut BTreeMap<String, serde_json::Value>,
    mut payload: serde_json::Value,
) {
    let Some(identity) = tool_instance_identity(&payload) else {
        return;
    };
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    object.insert(
        "tool_instance_id".to_string(),
        serde_json::Value::String(identity.clone()),
    );
    if !object.contains_key("tool_name") {
        if let Some(name) = object.get("name").cloned() {
            object.insert("tool_name".to_string(), name);
        }
    }
    if !tools.contains_key(&identity) {
        order.push(identity.clone());
        tools.insert(identity, payload);
        return;
    }
    let Some(existing) = tools
        .get_mut(&identity)
        .and_then(serde_json::Value::as_object_mut)
    else {
        tools.insert(identity, payload);
        return;
    };
    for (key, value) in object {
        if !value.is_null() {
            existing.insert(key.clone(), value.clone());
        }
    }
}

fn durable_tool_payloads(messages: &[SessionMessage]) -> Vec<serde_json::Value> {
    let mut payloads = Vec::new();
    for message in messages {
        let Ok(blocks) = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        else {
            continue;
        };
        for block in blocks {
            let Some(block_type) = block
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
            else {
                continue;
            };
            if !matches!(block_type.as_str(), "tool_use" | "tool_result") {
                continue;
            }
            let mut payload = block;
            let Some(object) = payload.as_object_mut() else {
                continue;
            };
            object.insert(
                "source".to_string(),
                serde_json::Value::String("durable_transcript".to_string()),
            );
            object.insert(
                "durable_message_id".to_string(),
                serde_json::Value::String(message.stable_message_id.clone()),
            );
            object.insert(
                "durable_sequence".to_string(),
                serde_json::json!(message.sequence),
            );
            match block_type.as_str() {
                "tool_use" => {
                    object.insert(
                        "status".to_string(),
                        serde_json::Value::String("running".to_string()),
                    );
                    if let Some(input) = object.get("input").cloned() {
                        object.insert("preview".to_string(), input);
                    }
                }
                "tool_result" => {
                    let failed = object
                        .get("is_error")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    object.insert(
                        "status".to_string(),
                        serde_json::Value::String(
                            if failed { "failed" } else { "completed" }.to_string(),
                        ),
                    );
                    if let Some(output) = object.get("output").cloned() {
                        object.insert("summary".to_string(), output);
                    }
                }
                _ => continue,
            }
            payloads.push(payload);
        }
    }
    payloads
}

fn canonical_tool_timeline(
    events: &[serde_json::Value],
    messages: &[SessionMessage],
) -> Vec<serde_json::Value> {
    let mut order = Vec::new();
    let mut tools = BTreeMap::new();
    for payload in collect_tool_timeline(events)
        .into_iter()
        .chain(durable_tool_payloads(messages))
    {
        if payload.is_object() {
            merge_tool_projection_item(&mut order, &mut tools, payload);
        }
    }
    order
        .into_iter()
        .filter_map(|identity| tools.remove(&identity))
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

fn session_input_content_preview(payload: &serde_json::Value) -> Option<String> {
    let direct = payload
        .get("content_preview")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let classified = payload.get("classification").and_then(|classification| {
        if classification.is_object() {
            classification
                .get("content_preview")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        } else {
            let parsed =
                serde_json::from_str::<serde_json::Value>(classification.as_str()?).ok()?;
            parsed
                .get("content_preview")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }
    });
    direct
        .or(classified)
        .map(|preview| preview.trim().to_string())
        .filter(|preview| !preview.is_empty())
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
        let created_at_ms = event
            .get("created_at_ms")
            .and_then(serde_json::Value::as_u64);
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
        match event_type {
            "session.input.accepted.v1" | "session.input.queued.v1" => {
                self.status = "pending".to_string();
                self.submitted_at_ms = self.submitted_at_ms.or(created_at_ms);
                if let Some(preview) = session_input_content_preview(payload) {
                    self.observe_user_preview(preview);
                }
            }
            "SessionInputIngressBound" => {
                self.status = "running".to_string();
                self.started_at_ms = self.started_at_ms.or(created_at_ms);
            }
            "session.input.completed.v1" | "SessionInputIngressSettled" => {
                self.status = "completed".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            "session.input.failed.v1" => {
                self.status = "failed".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            "session.input.cancelled.v1" => {
                self.status = "cancelled".to_string();
                self.completed_at_ms = self.completed_at_ms.or(created_at_ms);
            }
            _ => {}
        }
        if let Some(tool_payload) = runtime_tool_payload_from_event(event) {
            self.tool_calls.push(tool_payload);
        } else {
            match event_type {
                "ApprovalRequested" | "ApprovalResolved" | "RiskApproval" => {
                    self.approvals.push(payload.clone());
                }
                "ContextEnvelope"
                | "ContextTurnReport"
                | "ContextRecommendationAction"
                | "context.turn_report"
                | "context.governance_report"
                | "context.fact_candidate_review"
                | "context.session_compacted"
                | "context.recommendation_action" => {
                    self.context_events.push(payload.clone());
                }
                "TokenUsage" | "RunModelTelemetry" => {
                    self.usage.push(payload.clone());
                }
                "SurfaceMessageProcessed" | "surface.message_replied" => {
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
        collect_projection_evidence_refs(event, &mut self.evidence_refs);
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
                    self.observe_user_preview(preview.to_string());
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

    fn observe_user_preview(&mut self, preview: String) {
        if self.user_preview.is_none() && !preview.trim().is_empty() {
            self.user_preview = Some(preview);
        }
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
    if let Some(references) = payload.get("refs").and_then(serde_json::Value::as_array) {
        for reference in references {
            let ref_type = reference
                .get("type")
                .or_else(|| reference.get("ref_type"))
                .and_then(serde_json::Value::as_str);
            if matches!(ref_type, Some("evidence" | "raw_evidence")) {
                if let Some(id) = reference.get("id").and_then(serde_json::Value::as_str) {
                    out.insert(id.to_string());
                }
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
    let supplemental = event
        .get("supplemental")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        || payload.get("decision").and_then(serde_json::Value::as_str)
            == Some("supplement_current_turn");
    if supplemental {
        let target = event
            .get("target_turn_id")
            .and_then(serde_json::Value::as_str)
            .or_else(|| target_turn_ref_from_refs(event.get("refs")))
            .or_else(|| {
                payload
                    .get("target_turn_id")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| target_turn_ref_from_refs(payload.get("refs")));
        if let Some(target) = target {
            return Some(target.to_string());
        }
    }
    event
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| turn_ref_from_refs(event.get("refs")))
        .or_else(|| payload.get("turn_id").and_then(serde_json::Value::as_str))
        .or_else(|| turn_ref_from_refs(payload.get("refs")))
        .or_else(|| {
            payload
                .get("input")
                .and_then(|input| input.get("active_turn_id"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            payload
                .get("record")
                .and_then(|record| record.get("active_turn_id"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            payload
                .get("active_turn_id")
                .and_then(serde_json::Value::as_str)
        })
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

fn turn_ref_from_refs(refs: Option<&serde_json::Value>) -> Option<&str> {
    typed_ref_from_refs(refs, "turn")
}

fn target_turn_ref_from_refs(refs: Option<&serde_json::Value>) -> Option<&str> {
    typed_ref_from_refs(refs, "target_turn")
}

fn supplement_turn_alias(event: &serde_json::Value) -> Option<(String, String)> {
    let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
    let supplemental = event
        .get("supplemental")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        || payload.get("decision").and_then(serde_json::Value::as_str)
            == Some("supplement_current_turn");
    if !supplemental {
        return None;
    }
    let source = event
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| turn_ref_from_refs(event.get("refs")))
        .or_else(|| payload.get("turn_id").and_then(serde_json::Value::as_str))
        .or_else(|| turn_ref_from_refs(payload.get("refs")))?;
    let target = event
        .get("target_turn_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| target_turn_ref_from_refs(event.get("refs")))
        .or_else(|| {
            payload
                .get("target_turn_id")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| target_turn_ref_from_refs(payload.get("refs")))?;
    (source != target).then(|| (source.to_string(), target.to_string()))
}

fn typed_ref_from_refs<'a>(
    refs: Option<&'a serde_json::Value>,
    expected_type: &str,
) -> Option<&'a str> {
    refs.and_then(serde_json::Value::as_array)?
        .iter()
        .find(|reference| {
            reference
                .get("type")
                .or_else(|| reference.get("ref_type"))
                .and_then(serde_json::Value::as_str)
                == Some(expected_type)
        })
        .and_then(|reference| reference.get("id"))
        .and_then(serde_json::Value::as_str)
}

fn opens_active_turn(event_type: &str) -> bool {
    event_type == "SessionInputIngressBound"
}

fn closes_active_turn(event_type: &str) -> bool {
    matches!(
        event_type,
        "session.input.completed.v1"
            | "session.input.failed.v1"
            | "session.input.cancelled.v1"
            | "SessionInputIngressSettled"
    )
}

fn turn_projection_from_event_values(
    session_id: &str,
    events: &[serde_json::Value],
) -> serde_json::Value {
    let mut turns: BTreeMap<String, TurnProjectionAccumulator> = BTreeMap::new();
    let mut turn_aliases = BTreeMap::<String, String>::new();
    let mut unbound_events = Vec::new();
    let mut active_turn_id = None::<String>;
    for event in events {
        let event_type = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if let Some((source, target)) = supplement_turn_alias(event) {
            turn_aliases.insert(source, target);
        }
        let direct_turn_id = turn_id_from_event_value(event)
            .map(|turn_id| turn_aliases.get(&turn_id).cloned().unwrap_or(turn_id));
        let turn_id = direct_turn_id.clone().or_else(|| active_turn_id.clone());
        if let Some(turn_id) = turn_id {
            turns
                .entry(turn_id.clone())
                .or_insert_with(|| TurnProjectionAccumulator::new(turn_id))
                .observe_event(event);
        } else {
            unbound_events.push(event.clone());
        }
        if opens_active_turn(event_type) {
            active_turn_id = direct_turn_id;
        } else if closes_active_turn(event_type)
            && direct_turn_id
                .as_ref()
                .is_some_and(|turn_id| active_turn_id.as_ref() == Some(turn_id))
        {
            active_turn_id = None;
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

fn runtime_event_ref<'a>(event: &'a runtime::DurableRuntimeEvent, kind: &str) -> Option<&'a str> {
    event
        .refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.id.as_str())
}

fn bounded_activity_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let mut bounded = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

fn projection_turn_for_runtime_event(
    event: &runtime::DurableRuntimeEvent,
    turn_windows: &[(String, u64, Option<u64>)],
    root_turn_ids: &BTreeSet<String>,
    root_execution_turns: &HashMap<String, String>,
) -> Option<String> {
    if let Some(turn_id) =
        runtime_event_ref(event, "turn").filter(|turn_id| root_turn_ids.contains(*turn_id))
    {
        return Some(turn_id.to_string());
    }
    if let Some(turn_id) = runtime_event_ref(event, "execution")
        .and_then(|execution_id| root_execution_turns.get(execution_id))
    {
        return Some(turn_id.clone());
    }
    turn_windows
        .iter()
        .rev()
        .find(|(_, started_at_ms, completed_at_ms)| {
            event.created_at_ms >= *started_at_ms
                && completed_at_ms
                    .is_none_or(|completed_at_ms| event.created_at_ms <= completed_at_ms)
        })
        .map(|(turn_id, _, _)| turn_id.clone())
}

fn runtime_activity_from_event(
    event: &runtime::DurableRuntimeEvent,
    turn_id: &str,
    model_steps_with_tools: &BTreeSet<String>,
    root_execution_turns: &HashMap<String, String>,
) -> Option<serde_json::Value> {
    let execution_id = runtime_event_ref(event, "execution").unwrap_or_default();
    let delegated = !execution_id.is_empty() && !root_execution_turns.contains_key(execution_id);
    let common = serde_json::json!({
        "at": event.created_at_ms,
        "sequence": format!("{}.{}", event.commit_cursor, event.transaction_index),
        "commit_cursor": event.commit_cursor,
        "turn_id": turn_id,
        "execution_id": execution_id,
        "event_kind": event.kind,
        "raw": {
            "event_id": event.event_id,
            "scope": event.scope,
            "actor": event.actor,
            "refs": event.refs,
            "schema_version": event.schema_version,
        },
    });
    let mut activity = common.as_object()?.clone();
    let payload = &event.payload;
    match event.kind.as_str() {
        "model.item_completed" => {
            let item_kind = payload
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let model_step_id = payload
                .get("model_step_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let content = payload
                .get("content")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let id = payload
                .get("segment_id")
                .or_else(|| payload.get("item_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&event.event_id);
            match item_kind {
                "public_reasoning" => {
                    activity.insert("id".to_string(), id.into());
                    activity.insert("kind".to_string(), "think".into());
                    activity.insert("title".to_string(), "思考".into());
                    activity.insert(
                        "detail".to_string(),
                        bounded_activity_text(content, 8_000).into(),
                    );
                    activity.insert("status".to_string(), "complete".into());
                }
                "tool_call" => {
                    let tool_call_id = payload
                        .get("tool_call_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(id);
                    activity.insert("id".to_string(), tool_call_id.into());
                    activity.insert("kind".to_string(), "tool".into());
                    activity.insert(
                        "title".to_string(),
                        payload
                            .get("tool_name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("tool")
                            .into(),
                    );
                    activity.insert("status".to_string(), "queued".into());
                    activity.insert("input".to_string(), content.into());
                    activity.insert("tool_call_id".to_string(), tool_call_id.into());
                }
                "text" if delegated || model_steps_with_tools.contains(model_step_id) => {
                    activity.insert("id".to_string(), id.into());
                    activity.insert("kind".to_string(), "think".into());
                    activity.insert(
                        "title".to_string(),
                        if delegated {
                            "Agent 阶段输出".into()
                        } else {
                            "思考".into()
                        },
                    );
                    activity.insert(
                        "detail".to_string(),
                        bounded_activity_text(content, 8_000).into(),
                    );
                    activity.insert("status".to_string(), "complete".into());
                }
                _ => return None,
            }
            activity.insert("model_step_id".to_string(), model_step_id.into());
            if let Some(item_id) = payload.get("item_id").cloned() {
                activity.insert("item_id".to_string(), item_id);
            }
            if let Some(segment_id) = payload.get("segment_id").cloned() {
                activity.insert("segment_id".to_string(), segment_id);
            }
        }
        "tool.invocation.started" | "tool.invocation.completed" | "tool.invocation.failed" => {
            let tool_call_id = payload
                .get("tool_call_id")
                .and_then(serde_json::Value::as_str)
                .or_else(|| runtime_event_ref(event, "tool_call"))
                .unwrap_or(&event.event_id);
            let failed = event.kind.ends_with("failed")
                || payload
                    .get("is_error")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
            let completed = event.kind != "tool.invocation.started";
            activity.insert("id".to_string(), tool_call_id.into());
            activity.insert(
                "kind".to_string(),
                if failed {
                    "error".into()
                } else {
                    "tool".into()
                },
            );
            activity.insert(
                "title".to_string(),
                payload
                    .get("tool_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("tool")
                    .into(),
            );
            activity.insert(
                "status".to_string(),
                if failed {
                    "error".into()
                } else if completed {
                    "complete".into()
                } else {
                    "running".into()
                },
            );
            activity.insert("tool_call_id".to_string(), tool_call_id.into());
            if let Some(input) = payload.get("input_preview").cloned() {
                activity.insert("input".to_string(), input);
            }
            if let Some(output) = payload
                .get("output_preview")
                .or_else(|| payload.get("model_visible_preview"))
                .cloned()
            {
                activity.insert("output".to_string(), output.clone());
                activity.insert(
                    "detail".to_string(),
                    bounded_activity_text(output.as_str().unwrap_or_default(), 2_000).into(),
                );
            }
            if let Some(duration) = payload.get("duration_ms").cloned() {
                activity.insert("duration_ms".to_string(), duration);
            }
            if let Some(raw) = activity
                .get_mut("raw")
                .and_then(serde_json::Value::as_object_mut)
            {
                for key in ["full_output_ref", "output_ref", "failure_kind"] {
                    if let Some(value) = payload.get(key).cloned() {
                        raw.insert(key.to_string(), value);
                    }
                }
            }
        }
        "runtime.strategy.selected" => {
            activity.insert("id".to_string(), event.event_id.clone().into());
            activity.insert("kind".to_string(), "runtime".into());
            activity.insert("title".to_string(), "执行策略".into());
            activity.insert("status".to_string(), "complete".into());
            let detail = payload
                .get("selected_candidate")
                .or_else(|| payload.get("compile_target"))
                .map(ToString::to_string)
                .unwrap_or_default();
            activity.insert("detail".to_string(), detail.into());
        }
        "tool.execution_plan.created" | "tool.schedule.created" => {
            activity.insert("id".to_string(), event.event_id.clone().into());
            activity.insert("kind".to_string(), "runtime".into());
            activity.insert(
                "title".to_string(),
                if event.kind == "tool.schedule.created" {
                    "工具调度".into()
                } else {
                    "工具计划".into()
                },
            );
            activity.insert("status".to_string(), "complete".into());
            let detail = payload
                .get("task_count")
                .or_else(|| payload.get("tool_count"))
                .map(|count| format!("{count}"))
                .unwrap_or_default();
            activity.insert("detail".to_string(), detail.into());
        }
        _ => return None,
    }
    Some(serde_json::Value::Object(activity))
}

fn enrich_turn_projection_history(
    projection: &mut serde_json::Value,
    runtime_events: &[runtime::DurableRuntimeEvent],
    durable_messages: &[SessionMessage],
) {
    let Some(turns) = projection
        .get_mut("turns")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    let root_turn_ids = turns
        .iter()
        .filter_map(|turn| turn.get("turn_id").and_then(serde_json::Value::as_str))
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let turn_windows = turns
        .iter()
        .filter_map(|turn| {
            let turn_id = turn.get("turn_id")?.as_str()?.to_string();
            let started = turn
                .get("submitted_at_ms")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    turn.get("started_at_ms")
                        .and_then(serde_json::Value::as_u64)
                })?;
            let completed = turn
                .get("completed_at_ms")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value.saturating_add(1_000));
            Some((turn_id, started, completed))
        })
        .collect::<Vec<_>>();
    let root_execution_turns = runtime_events
        .iter()
        .filter(|event| event.kind == "runtime.strategy.selected")
        .filter_map(|event| {
            let turn_id = runtime_event_ref(event, "turn")?;
            if !root_turn_ids.contains(turn_id) {
                return None;
            }
            let execution_id = runtime_event_ref(event, "execution_graph")?;
            Some((execution_id.to_string(), turn_id.to_string()))
        })
        .collect::<HashMap<_, _>>();
    let model_steps_with_tools = runtime_events
        .iter()
        .filter(|event| {
            event.kind == "model.item_completed"
                && event
                    .payload
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("tool_call")
        })
        .filter_map(|event| {
            event
                .payload
                .get("model_step_id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
        .collect::<BTreeSet<_>>();
    let mut activities = BTreeMap::<String, Vec<serde_json::Value>>::new();
    for event in runtime_events {
        let Some(turn_id) = projection_turn_for_runtime_event(
            event,
            &turn_windows,
            &root_turn_ids,
            &root_execution_turns,
        ) else {
            continue;
        };
        if let Some(activity) = runtime_activity_from_event(
            event,
            &turn_id,
            &model_steps_with_tools,
            &root_execution_turns,
        ) {
            activities.entry(turn_id).or_default().push(activity);
        }
    }
    for payload in durable_tool_payloads(durable_messages) {
        let Some(turn_id) = payload
            .get("cowd_turn_id")
            .and_then(serde_json::Value::as_str)
            .filter(|turn_id| root_turn_ids.contains(*turn_id))
        else {
            continue;
        };
        let tool_call_id = payload
            .get("tool_use_id")
            .or_else(|| payload.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("durable-tool");
        let is_result =
            payload.get("type").and_then(serde_json::Value::as_str) == Some("tool_result");
        let failed = payload
            .get("is_error")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        activities
            .entry(turn_id.to_string())
            .or_default()
            .push(serde_json::json!({
                "id": tool_call_id,
                "kind": if failed { "error" } else { "tool" },
                "title": payload.get("tool_name").or_else(|| payload.get("name")).and_then(serde_json::Value::as_str).unwrap_or("tool"),
                "detail": payload.get("output").or_else(|| payload.get("input")).cloned().unwrap_or(serde_json::Value::Null),
                "status": if failed { "error" } else if is_result { "complete" } else { "running" },
                "input": (!is_result).then(|| payload.get("input").cloned()).flatten(),
                "output": is_result.then(|| payload.get("output").cloned()).flatten(),
                "turn_id": turn_id,
                "sequence": payload.get("durable_sequence").cloned(),
                "tool_call_id": tool_call_id,
                "raw": {
                    "source": "durable_transcript",
                    "durable_message_id": payload.get("durable_message_id"),
                },
            }));
    }
    for turn in turns {
        let turn_id = turn
            .get("turn_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        turn["activity_events"] =
            serde_json::Value::Array(activities.remove(turn_id).unwrap_or_default());
    }
}

fn session_run_projection_from_events(
    session_id: &str,
    stored_events: Vec<SessionEvent>,
    durable_messages: &[SessionMessage],
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
    let tool_timeline = canonical_tool_timeline(&events, durable_messages);
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
            .get("tool_name")
            .or_else(|| tool.get("name"))
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let limit = params.limit.unwrap_or(2_000).min(10_000);
    let from_seq = if let Some(from_seq) = params.from_seq {
        from_seq
    } else {
        let Some((total, _)) = state
            .services
            .session
            .stored_events_page(&id, 0, 1)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to probe session projection events: {error}"),
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
        total.saturating_sub(limit)
    };
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
    let message_total = state
        .services
        .session
        .stored_message_count(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to count durable turn messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let durable_messages = state
        .services
        .session
        .stored_messages(&id, message_total.saturating_sub(10_000), 10_000)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable turn messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let mut runtime_events = state
        .services
        .runtime_events
        .session_timeline_events(&id, None, 20_000)
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable runtime turn events: {error}"),
                }),
            )
        })?;
    runtime_events.extend(
        state
            .services
            .runtime_events
            .list_stream(&format!("session:{id}"))
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load legacy runtime turn stream: {error}"),
                    }),
                )
            })?,
    );
    runtime_events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    runtime_events.dedup_by(|left, right| left.event_id == right.event_id);
    enrich_turn_projection_history(&mut projection, &runtime_events, &durable_messages);
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
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
    let mut projection = turn_projection_from_event_values(&id, &events);
    let message_total = state
        .services
        .session
        .stored_message_count(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to count durable turn messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let durable_messages = state
        .services
        .session
        .stored_messages(&id, message_total.saturating_sub(10_000), 10_000)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable turn messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let mut runtime_events = state
        .services
        .runtime_events
        .session_timeline_events(&id, None, 20_000)
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable runtime turn events: {error}"),
                }),
            )
        })?;
    runtime_events.extend(
        state
            .services
            .runtime_events
            .list_stream(&format!("session:{id}"))
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load legacy runtime turn stream: {error}"),
                    }),
                )
            })?,
    );
    runtime_events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    runtime_events.dedup_by(|left, right| left.event_id == right.event_id);
    enrich_turn_projection_history(&mut projection, &runtime_events, &durable_messages);
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let limit = params.limit.unwrap_or(2_000).min(10_000);
    let Some((global_total, _)) = state
        .services
        .session
        .stored_events_page(&id, 0, 1)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to probe session projection events: {error}"),
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
    let from_seq = params
        .from_seq
        .unwrap_or_else(|| global_total.saturating_sub(limit));
    let Some((_remaining_total, stored_events)) = state
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
    let message_total = state
        .services
        .session
        .stored_message_count(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to count durable projection messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let durable_messages = state
        .services
        .session
        .stored_messages(&id, message_total.saturating_sub(10_000), 10_000)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load durable projection messages: {error}"),
                }),
            )
        })?
        .unwrap_or_default();
    let projection_has_more = stored_events
        .last()
        .map(|event| event.sequence.saturating_add(1) < global_total)
        .unwrap_or(false);
    let mut projection =
        session_run_projection_from_events(&id, stored_events, &durable_messages, stats);
    projection["paging"] = serde_json::json!({
        "total": global_total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": projection_has_more,
    });

    Ok(Json(projection))
}

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.clamp(1, 100);
    let Some(stored_sessions) = state
        .services
        .session
        .list_stored_sessions()
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session search authority set: {error}"),
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
    let mut authorized_session_ids = Vec::new();
    for record in stored_sessions {
        if authorize_session_access(&state, &principal, &record.session_id, SessionAccess::Read)
            .await
            .is_ok()
        {
            authorized_session_ids.push(record.session_id);
        }
    }
    let Some(db_messages) = state
        .services
        .session
        .search_stored_messages_in_sessions(&params.q, &authorized_session_ids, limit)
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

    let mut results = Vec::new();
    for message in db_messages {
        let blocks: Vec<serde_json::Value> =
            serde_json::from_str(&message.content_json).unwrap_or_default();
        let blocks = super::message_routes::public_session_blocks(blocks);
        let preview = search_message_preview(&blocks);
        results.push(SearchMessagesItem {
            session_id: message.session_id,
            sequence: message.sequence,
            role: message.role,
            blocks,
            content_preview: preview,
            tool_use_id: message.tool_use_id,
            tool_name: message.tool_name,
            created_at_ms: message.created_at_ms,
        });
        if results.len() >= limit {
            break;
        }
    }

    let total = results.len();
    Ok(Json(SearchMessagesResponse {
        query: params.q,
        results,
        total,
    }))
}

fn search_message_preview(blocks: &[serde_json::Value]) -> String {
    let content_preview = blocks
        .iter()
        .filter_map(|block| block.get("text").and_then(|text| text.as_str()))
        .collect::<Vec<_>>()
        .join(" ");
    if content_preview.chars().count() > 200 {
        format!(
            "{}...",
            content_preview.chars().take(200).collect::<String>()
        )
    } else {
        content_preview
    }
}

async fn compact_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Destructive).await?;
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
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

fn token_count_from_usage(message: &session::SessionMessage, key: &str) -> u32 {
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
    let recorded_input_tokens = record.input_tokens.max(0) as u32;
    let recorded_output_tokens = record.output_tokens.max(0) as u32;
    let mut fallback_input_tokens = 0u32;
    let mut fallback_output_tokens = 0u32;
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
        fallback_input_tokens =
            fallback_input_tokens.saturating_add(token_count_from_usage(message, "input_tokens"));
        fallback_output_tokens =
            fallback_output_tokens.saturating_add(token_count_from_usage(message, "output_tokens"));
    }
    let input_tokens = if recorded_input_tokens == 0 {
        fallback_input_tokens
    } else {
        recorded_input_tokens
    };
    let output_tokens = if recorded_output_tokens == 0 {
        fallback_output_tokens
    } else {
        recorded_output_tokens
    };
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SessionUpdateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    if body
        .metadata
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .is_some_and(|metadata| metadata.contains_key("owner_principal_id"))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "owner_principal_id is immutable after Session creation".to_string(),
            }),
        ));
    }
    let requested_model = body.model.clone();
    let stored_found = state
        .services
        .session
        .update_session(&id, body)
        .await
        .map_err(|error| {
            let status = if matches!(error, session::SessionError::InvalidArgument(_)) {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (
                status,
                Json(ErrorResponse {
                    error: format!("failed to update session: {error}"),
                }),
            )
        })?;
    let active_found = match state.services.runtime.as_ref() {
        Some(runtime_service) => runtime_service
            .update_active_session_model(&id, requested_model.as_deref())
            .await
            .map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("failed to update active session: {error}"),
                    }),
                )
            })?,
        None => false,
    };

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

    #[test]
    fn search_preview_truncates_on_unicode_character_boundaries() {
        let blocks = vec![serde_json::json!({
            "type": "text",
            "text": "你".repeat(201),
        })];
        let preview = search_message_preview(&blocks);
        assert_eq!(preview.chars().count(), 203);
        assert!(preview.starts_with(&"你".repeat(200)));
        assert!(preview.ends_with("..."));
    }

    #[test]
    fn session_evidence_rejects_foreign_principal_before_pruned_graph_metadata() {
        assert!(session_access_authorized(
            SessionAccess::Read,
            true,
            false,
            false,
            false
        ));
        assert!(session_access_authorized(
            SessionAccess::Read,
            false,
            true,
            false,
            false
        ));
        assert!(session_access_authorized(
            SessionAccess::Read,
            false,
            false,
            true,
            false
        ));
        assert!(session_access_authorized(
            SessionAccess::Read,
            false,
            false,
            false,
            true
        ));
        assert!(
            !session_access_authorized(SessionAccess::Read, false, false, false, false),
            "a missing execution graph must not turn a foreign session into evidence access"
        );
    }

    #[test]
    fn session_access_actions_do_not_turn_observer_scope_into_write_or_delete() {
        assert!(session_access_authorized(
            SessionAccess::Read,
            false,
            false,
            true,
            false
        ));
        assert!(!session_access_authorized(
            SessionAccess::Write,
            false,
            false,
            true,
            false
        ));
        assert!(!session_access_authorized(
            SessionAccess::Destructive,
            false,
            true,
            false,
            false
        ));
        assert!(session_access_authorized(
            SessionAccess::Destructive,
            true,
            false,
            false,
            false
        ));
    }

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

    fn durable_runtime_event(
        sequence: u64,
        created_at_ms: u64,
        kind: &str,
        refs: &[(&str, &str)],
        payload: serde_json::Value,
    ) -> runtime::DurableRuntimeEvent {
        runtime::DurableRuntimeEvent {
            event_id: format!("runtime-event-{sequence}"),
            stream_id: "session:session-v31".to_string(),
            sequence,
            scope: runtime::RuntimeEventScope::ExecutionGraph,
            kind: kind.to_string(),
            status: Some("completed".to_string()),
            actor: Some("runtime".to_string()),
            refs: refs
                .iter()
                .map(|(kind, id)| runtime::RuntimeEventRef {
                    kind: (*kind).to_string(),
                    id: (*id).to_string(),
                })
                .collect(),
            payload,
            created_at_ms,
            commit_cursor: sequence,
            transaction_id: format!("transaction-{sequence}"),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }

    #[test]
    fn turn_projection_builds_stable_turns_from_journal() {
        let events = [
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
    fn turn_projection_binds_canonical_ingress_runtime_events() {
        let events = vec![
            serde_json::json!({
                "type": "session.input.accepted.v1",
                "sequence": 0,
                "created_at_ms": 10,
                "status": "accepted",
                "refs": [{"type": "turn", "id": "turn-1"}],
                "payload": {
                    "turn_id": "turn-1",
                    "classification": "{\"decision\":\"start_new_turn\",\"content_preview\":\"inspect the runtime\"}"
                }
            }),
            serde_json::json!({
                "type": "SessionInputIngressBound",
                "sequence": 1,
                "created_at_ms": 20,
                "payload": {"input": {"active_turn_id": "turn-1"}}
            }),
            serde_json::json!({
                "type": "ContextEnvelope",
                "sequence": 2,
                "created_at_ms": 30,
                "payload": {"envelope_id": "ctx-1"}
            }),
            serde_json::json!({
                "type": "tool.invocation.completed",
                "sequence": 3,
                "created_at_ms": 40,
                "status": "completed",
                "payload": {"tool_call_id": "tool-1", "tool_name": "read_file"}
            }),
            serde_json::json!({
                "type": "session.input.completed.v1",
                "sequence": 4,
                "created_at_ms": 50,
                "status": "completed",
                "refs": [{"type": "turn", "id": "turn-1"}],
                "payload": {"turn_id": "turn-1"}
            }),
        ];

        let projection = turn_projection_from_event_values("session-1", &events);

        assert_eq!(projection["turn_count"], 1);
        assert_eq!(projection["turns"][0]["status"], "completed");
        assert_eq!(
            projection["turns"][0]["user_preview"],
            "inspect the runtime"
        );
        assert_eq!(
            projection["turns"][0]["event_sequences"],
            serde_json::json!([0, 1, 2, 3, 4])
        );
        assert_eq!(
            projection["turns"][0]["context_events"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            projection["turns"][0]["tool_calls"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(projection["unbound_event_count"], 0);
    }

    #[test]
    fn turn_projection_history_keeps_child_agent_work_inside_canonical_user_turns() {
        let mut projection = serde_json::json!({
            "kind": "session.turn_projection",
            "session_id": "session-v31",
            "turn_count": 3,
            "turns": [
                {
                    "turn_id": "turn-1",
                    "submitted_at_ms": 1_000,
                    "completed_at_ms": 1_900
                },
                {
                    "turn_id": "turn-2",
                    "submitted_at_ms": 2_000,
                    "completed_at_ms": 2_900
                },
                {
                    "turn_id": "turn-3",
                    "submitted_at_ms": 3_000,
                    "completed_at_ms": 3_900
                }
            ]
        });
        let runtime_events = vec![
            durable_runtime_event(
                1,
                1_010,
                "runtime.strategy.selected",
                &[("turn", "turn-1"), ("execution_graph", "root-execution-1")],
                serde_json::json!({"selected_candidate": "team"}),
            ),
            durable_runtime_event(
                2,
                1_100,
                "model.item_completed",
                &[("execution", "child-agent-execution")],
                serde_json::json!({
                    "kind": "public_reasoning",
                    "model_step_id": "child-step-1",
                    "item_id": "child-reasoning-1",
                    "segment_id": "child-reasoning-1:public_reasoning:0",
                    "content": "inspect delegated evidence"
                }),
            ),
            durable_runtime_event(
                3,
                1_200,
                "tool.invocation.completed",
                &[
                    ("execution", "child-agent-execution"),
                    ("tool_call", "tool-call-1"),
                ],
                serde_json::json!({
                    "tool_call_id": "tool-call-1",
                    "tool_name": "read_file",
                    "output_preview": "evidence",
                    "duration_ms": 23
                }),
            ),
            durable_runtime_event(
                4,
                2_010,
                "runtime.strategy.selected",
                &[("turn", "turn-2"), ("execution_graph", "root-execution-2")],
                serde_json::json!({"selected_candidate": "direct"}),
            ),
            durable_runtime_event(
                5,
                3_010,
                "runtime.strategy.selected",
                &[("turn", "turn-3"), ("execution_graph", "root-execution-3")],
                serde_json::json!({"selected_candidate": "direct"}),
            ),
        ];

        enrich_turn_projection_history(&mut projection, &runtime_events, &[]);

        let turns = projection["turns"].as_array().expect("turn projection");
        assert_eq!(projection["turn_count"], 3);
        assert_eq!(
            turns.len(),
            3,
            "child executions must not create user turns"
        );
        let first_activities = turns[0]["activity_events"]
            .as_array()
            .expect("first turn activities");
        assert!(first_activities.iter().any(|activity| {
            activity["execution_id"] == "child-agent-execution" && activity["kind"] == "think"
        }));
        assert!(first_activities.iter().any(|activity| {
            activity["tool_call_id"] == "tool-call-1" && activity["status"] == "complete"
        }));
        assert_eq!(
            turns[1]["activity_events"]
                .as_array()
                .expect("second turn activities")
                .len(),
            1
        );
        assert_eq!(
            turns[2]["activity_events"]
                .as_array()
                .expect("third turn activities")
                .len(),
            1
        );
    }

    #[test]
    fn turn_projection_attaches_supplements_to_the_target_turn() {
        let events = vec![
            serde_json::json!({
                "type": "session.input.accepted.v1",
                "sequence": 0,
                "created_at_ms": 10,
                "status": "accepted",
                "refs": [{"type": "turn", "id": "turn-main"}],
                "payload": {
                    "decision": "start_new_turn",
                    "turn_id": "turn-main",
                    "classification": {
                        "content_preview": "the original user request"
                    }
                }
            }),
            serde_json::json!({
                "type": "SessionInputIngressBound",
                "sequence": 1,
                "created_at_ms": 20,
                "payload": {"input": {"active_turn_id": "turn-main"}}
            }),
            serde_json::json!({
                "type": "session.input.accepted.v1",
                "sequence": 2,
                "created_at_ms": 30,
                "status": "accepted",
                "refs": [
                    {"type": "turn", "id": "turn-supplement"},
                    {"type": "target_turn", "id": "turn-main"}
                ],
                "payload": {
                    "decision": "supplement_current_turn",
                    "turn_id": "turn-supplement",
                    "target_turn_id": "turn-main",
                    "classification": {
                        "content_preview": "a later supplemental instruction"
                    }
                }
            }),
            serde_json::json!({
                "type": "session.input.completed.v1",
                "sequence": 3,
                "created_at_ms": 40,
                "status": "completed",
                "refs": [{"type": "turn", "id": "turn-supplement"}],
                "payload": {"turn_id": "turn-supplement"}
            }),
            serde_json::json!({
                "type": "session.input.completed.v1",
                "sequence": 4,
                "created_at_ms": 50,
                "status": "completed",
                "refs": [{"type": "turn", "id": "turn-main"}],
                "payload": {"turn_id": "turn-main"}
            }),
        ];

        let projection = turn_projection_from_event_values("session-1", &events);

        assert_eq!(projection["turn_count"], 1);
        assert_eq!(projection["turns"][0]["turn_id"], "turn-main");
        assert_eq!(
            projection["turns"][0]["event_sequences"],
            serde_json::json!([0, 1, 2, 3, 4])
        );
        assert_eq!(
            projection["turns"][0]["user_preview"],
            "the original user request"
        );
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

        let projection = session_run_projection_from_events("session-v31", events, &[], None);

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
    fn session_domain_events_project_logical_kind_refs_and_tool_payload() {
        let mut domain = session::SessionDomainEvent::new(
            "session-v31",
            0,
            session::SessionDomainScope::Tool,
            "tool.invocation.completed",
            serde_json::json!({
                "contract_version": 2,
                "invocation_id": "tool-domain-1",
                "tool_name": "read",
                "turn_id": "turn-domain-1",
                "status": "completed",
                "full_output_ref": "tool://domain-raw-1"
            }),
            1_000,
        );
        domain.event_id = "event-domain-tool-1".to_string();
        domain.status = Some("completed".to_string());
        domain.refs.push(session::SessionDomainRef {
            ref_type: "evidence".to_string(),
            id: "tool://domain-raw-1".to_string(),
            label: None,
        });
        let stored = domain.to_session_event().unwrap();

        let logical = session_event_value(&stored);
        assert_eq!(logical["type"], "tool.invocation.completed");
        assert_eq!(logical["scope"], "tool");
        assert_eq!(logical["event_id"], "event-domain-tool-1");
        assert_eq!(logical["refs"][0]["id"], "tool://domain-raw-1");

        let projection = session_run_projection_from_events("session-v31", vec![stored], &[], None);
        assert_eq!(projection["tool_timeline"].as_array().unwrap().len(), 1);
        assert_eq!(
            projection["tool_timeline"][0]["runtime_event_kind"],
            "tool.invocation.completed"
        );
        assert_eq!(
            projection["turn_projection"]["turns"][0]["evidence_refs"][0],
            "tool://domain-raw-1"
        );
    }

    #[test]
    fn surface_domain_phases_keep_logical_semantics_and_empty_terminal() {
        let phases = [
            (
                session::SessionDomainScope::Message,
                "surface.message_received",
                "received",
                serde_json::json!({
                    "type": "SurfaceMessageReceived",
                    "surface": "feishu",
                    "message_id": "om-1",
                    "content_preview": "inspect incident"
                }),
            ),
            (
                session::SessionDomainScope::Session,
                "surface.runtime_activated",
                "active",
                serde_json::json!({
                    "type": "SurfaceSessionRuntimeActivated",
                    "surface": "feishu",
                    "session_id": "session-v31",
                    "message_id": "om-1"
                }),
            ),
            (
                session::SessionDomainScope::Tool,
                "surface.resources_registered",
                "registered",
                serde_json::json!({
                    "type": "SurfaceMessageResourcesRegistered",
                    "surface": "feishu",
                    "message_id": "om-1",
                    "current": [],
                    "recent": []
                }),
            ),
            (
                session::SessionDomainScope::Turn,
                "surface.message_accepted",
                "accepted",
                serde_json::json!({
                    "type": "SurfaceMessageAccepted",
                    "surface": "feishu",
                    "message_id": "om-1",
                    "turn_id": "turn-surface-1",
                    "execution_id": "execution-surface-1"
                }),
            ),
            (
                session::SessionDomainScope::Message,
                "surface.message_replied",
                "empty_terminal",
                serde_json::json!({
                    "type": "SurfaceMessageReplied",
                    "surface": "feishu",
                    "message_id": "om-1",
                    "turn_id": "turn-surface-1",
                    "execution_id": "execution-surface-1",
                    "terminal_id": "terminal-surface-1",
                    "empty_terminal": true
                }),
            ),
        ];
        let stored = phases
            .into_iter()
            .enumerate()
            .map(|(sequence, (scope, kind, status, payload))| {
                let mut event = session::SessionDomainEvent::new(
                    "session-v31",
                    sequence,
                    scope,
                    kind,
                    payload,
                    1_000 + sequence as u64,
                );
                event.event_id = format!("surface-projection:{kind}:om-1");
                event.correlation_id = Some("feishu:om-1".to_string());
                event.status = Some(status.to_string());
                event.to_session_event().unwrap()
            })
            .collect::<Vec<_>>();
        let logical = stored.iter().map(session_event_value).collect::<Vec<_>>();

        assert_eq!(logical.len(), 5);
        assert_eq!(logical[0]["type"], "surface.message_received");
        assert_eq!(logical[3]["status"], "accepted");
        assert_eq!(logical[4]["status"], "empty_terminal");
        assert_eq!(logical[4]["payload"]["empty_terminal"], true);
        assert!(logical
            .iter()
            .all(|event| event["correlation_id"] == "feishu:om-1"));

        let projection = turn_projection_from_event_values("session-v31", &logical);
        assert_eq!(projection["turn_count"], 1);
        assert_eq!(projection["turns"][0]["turn_id"], "turn-surface-1");
        assert_eq!(
            projection["turns"][0]["event_sequences"],
            serde_json::json!([3, 4])
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
            &[],
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
        assert_eq!(projection["tool_summary"]["by_name"]["read"], 1);
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

    #[test]
    fn session_run_projection_materializes_one_tool_from_durable_transcript() {
        let messages = vec![
            SessionMessage {
                stable_message_id: "assistant-tool".to_string(),
                session_id: "session-v31".to_string(),
                sequence: 10,
                role: "assistant".to_string(),
                content_json: serde_json::json!([{
                    "type": "tool_use",
                    "id": "provider-tool-1",
                    "cowd_tool_instance_id": "provider-tool-1#cowd-0",
                    "name": "runtime_capabilities",
                    "input": "{\"detail\":\"execution_patterns\"}"
                }])
                .to_string(),
                blocks_count: 1,
                tool_use_id: Some("provider-tool-1".to_string()),
                tool_name: Some("runtime_capabilities".to_string()),
                token_usage_json: None,
                created_at_ms: 1_000,
            },
            SessionMessage {
                stable_message_id: "tool-result".to_string(),
                session_id: "session-v31".to_string(),
                sequence: 11,
                role: "tool".to_string(),
                content_json: serde_json::json!([{
                    "type": "tool_result",
                    "tool_use_id": "provider-tool-1",
                    "cowd_tool_instance_id": "provider-tool-1#cowd-0",
                    "tool_name": "runtime_capabilities",
                    "output": "ok",
                    "is_error": false
                }])
                .to_string(),
                blocks_count: 1,
                tool_use_id: Some("provider-tool-1".to_string()),
                tool_name: Some("runtime_capabilities".to_string()),
                token_usage_json: None,
                created_at_ms: 1_001,
            },
        ];

        let projection =
            session_run_projection_from_events("session-v31", Vec::new(), &messages, None);

        assert_eq!(projection["tool_summary"]["count"], 1);
        assert_eq!(
            projection["tool_summary"]["by_name"]["runtime_capabilities"],
            1
        );
        assert_eq!(projection["tool_timeline"][0]["status"], "completed");
        assert_eq!(
            projection["tool_timeline"][0]["tool_instance_id"],
            "provider-tool-1#cowd-0"
        );
        assert_eq!(
            projection["tool_timeline"][0]["durable_sequence"],
            serde_json::json!(11)
        );
    }
}
