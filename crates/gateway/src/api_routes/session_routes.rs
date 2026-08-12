use std::{
    collections::{BTreeSet, HashMap},
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
use session::{SessionEvent, SessionListOptions, SessionRecord};
use sha2::{Digest, Sha256};

use super::{api_error, surface_actor_id, AppState, AuthenticatedPrincipal, ErrorResponse};
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
            "/api/sessions/:id/execution-policy",
            get(get_session_execution_policy).put(put_session_execution_policy),
        )
        .route(
            "/api/sessions/execution-policy-defaults",
            get(get_execution_policy_defaults).put(put_execution_policy_defaults),
        )
        .route(
            "/api/sessions/:id/task-focus",
            get(get_task_focus_handler)
                .put(set_task_focus_handler)
                .delete(clear_task_focus_handler),
        )
        .route(
            "/api/sessions/:id/mission-focus",
            get(get_mission_focus_handler)
                .put(set_mission_focus_handler)
                .delete(clear_mission_focus_handler),
        )
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
        .route(
            "/api/sessions/:id/execution",
            get(get_session_execution_index),
        )
        .route(
            "/api/sessions/:id/execution/live",
            get(get_session_execution_live),
        )
        .route(
            "/api/sessions/:id/history-index",
            get(get_session_history_index),
        )
        .route(
            "/api/sessions/:id/turns/:turn_id/evidence",
            get(get_turn_evidence),
        )
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/stats", get(get_session_stats_handler))
}

async fn get_session_execution_policy(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<
    Json<harness_contract::policy::SessionExecutionPolicyResponse>,
    (StatusCode, Json<ErrorResponse>),
> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    runtime
        .session_execution_policy_value(&id)
        .await
        .map(Json)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn put_session_execution_policy(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<harness_contract::policy::UpdateSessionExecutionPolicyRequest>,
) -> Result<
    Json<harness_contract::policy::SessionExecutionPolicyResponse>,
    (StatusCode, Json<ErrorResponse>),
> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    super::require_session_writer_admission(&state, &principal, &headers, &id).await?;
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    runtime
        .set_session_execution_policy(
            &id,
            body.preset,
            body.expected_revision,
            runtime::SessionExecutionPolicyOrigin::SessionExplicit,
        )
        .await
        .map(Json)
        .map_err(|error| {
            let status = if error.starts_with("session_execution_policy_revision_conflict") {
                StatusCode::CONFLICT
            } else if error.contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            api_error(status, error)
        })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateExecutionPolicyDefaultsRequest {
    permission_mode: runtime::PermissionMode,
    approval_profile: runtime::ApprovalProfile,
}

async fn get_execution_policy_defaults(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let policy = runtime.execution_policy_default_value();
    serde_json::to_value(policy)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn put_execution_policy_defaults(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<UpdateExecutionPolicyDefaultsRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if !principal.0.is_human_interactive()
        || !principal.0.has_capability("runtime.maintenance.manage")
    {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "execution policy defaults update requires runtime.maintenance.manage",
        ));
    }
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    Ok(Json(
        runtime
            .update_execution_policy_defaults(body.permission_mode, body.approval_profile)
            .await,
    ))
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
                        Err(error) => {
                            return Err((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: format!(
                                        "failed to build execution {execution_id} evidence projection: {error}"
                                    ),
                                }),
                            ));
                        }
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
    let explicit_mission = state.services.runtime.as_ref().is_some_and(|runtime| {
        runtime::MissionRuntimePort::new(runtime.runtime_services())
            .mission_ids_for_session(session_id)
            .into_iter()
            .any(|mission_id| {
                claims
                    .scopes
                    .iter()
                    .any(|scope| scope == &format!("mission:{mission_id}"))
            })
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
    /// Optional execution policy preset applied at creation time (P0).
    /// Accepted values match `AutonomyProfileId` snake_case spellings:
    /// cautious | supervised | solo | yolo | stewarded.
    #[serde(default)]
    execution_policy_preset: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionTaskFocusRequest {
    task_id: String,
    expected_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionMissionFocusRequest {
    mission_id: String,
    expected_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionFocusClearRequest {
    expected_revision: u64,
}

async fn get_task_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let focus = state
        .services
        .session
        .routing_focus(&id)
        .await
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({
        "session_id": id,
        "revision": focus.revision,
        "task_focus": focus.task,
    })))
}

async fn set_task_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<SessionTaskFocusRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    let receipt = state
        .services
        .session
        .set_task_focus(
            &id,
            &body.task_id,
            body.expected_revision,
            &principal.0.claims().principal_id,
        )
        .await
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(receipt))
}

async fn clear_task_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<SessionFocusClearRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    let receipt = state
        .services
        .session
        .clear_task_focus(
            &id,
            body.expected_revision,
            &principal.0.claims().principal_id,
        )
        .await
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(receipt))
}

async fn get_mission_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let focus = state
        .services
        .session
        .routing_focus(&id)
        .await
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({
        "session_id": id,
        "revision": focus.revision,
        "mission_focus": focus.mission,
    })))
}

async fn set_mission_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<SessionMissionFocusRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    let receipt = state
        .services
        .session
        .set_mission_focus(
            &id,
            &body.mission_id,
            body.expected_revision,
            &principal.0.claims().principal_id,
        )
        .await
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(receipt))
}

async fn clear_mission_focus_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<SessionFocusClearRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    authorize_session_access(&state, &principal, &id, SessionAccess::Write).await?;
    let receipt = state
        .services
        .session
        .clear_mission_focus(
            &id,
            body.expected_revision,
            &principal.0.claims().principal_id,
        )
        .await
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(receipt))
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
pub(super) struct SessionHistoryIndexQuery {
    #[serde(default = "default_history_metadata_limit")]
    metadata_limit: usize,
    #[serde(default = "default_history_card_limit")]
    card_limit: usize,
}

const fn default_history_metadata_limit() -> usize {
    128
}

const fn default_history_card_limit() -> usize {
    64
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchMessagesParams {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

pub(super) async fn get_session_history_index(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<SessionHistoryIndexQuery>,
) -> Result<
    Json<harness_contract::projection::SessionHistoryIndexProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    use harness_contract::projection::{
        SessionHistoryCardProjection, SessionHistoryIndexProjection,
        SessionHistoryMessageMetadataProjection, SessionHistoryRecoveryState,
        SESSION_HISTORY_INDEX_SCHEMA_VERSION,
    };

    authorize_session_access(&state, &principal, &id, SessionAccess::Read).await?;
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    let history = runtime
        .runtime_services()
        .session_history_reader()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "canonical session history reader unavailable".to_string(),
                }),
            )
        })?;
    let mut rebuilt = false;
    let manifest = match history.activation_manifest(&id).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            rebuilt = true;
            history
                .rebuild_activation_manifest(
                    &id,
                    chrono::Utc::now().timestamp_millis().max(0) as u64,
                )
                .await
                .map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("failed to rebuild session history manifest: {error}"),
                        }),
                    )
                })?
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!("session {id} has no durable history manifest"),
                        }),
                    )
                })?
        }
        Err(error) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to read session history manifest: {error}"),
                }),
            ));
        }
    };
    let total_messages = manifest.recovery.transcript_messages as usize;
    let metadata_limit = query.metadata_limit.clamp(1, 2_048);
    let recent_metadata = history
        .message_metadata_page(
            &id,
            total_messages.saturating_sub(metadata_limit),
            metadata_limit,
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to read session history metadata: {error}"),
                }),
            )
        })?
        .into_iter()
        .map(|item| SessionHistoryMessageMetadataProjection {
            message_id: item.stable_message_id,
            sequence: item.sequence as u64,
            role: item.role,
            blocks_count: item.blocks_count as u64,
            tool_use_id: item.tool_use_id,
            tool_name: item.tool_name,
            created_at_ms: item.created_at_ms,
            content_bytes: item.content_bytes as u64,
        })
        .collect();
    let cards = history
        .context_index_cards(&id, query.card_limit.clamp(1, 512))
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to read session history cards: {error}"),
                }),
            )
        })?
        .into_iter()
        .map(|card| SessionHistoryCardProjection {
            card_id: card.card_id,
            parent_card_id: card.parent_card_id,
            source_start_sequence: card.source_start_sequence as u64,
            source_end_sequence: card.source_end_sequence as u64,
            source_message_count: card.source_message_count as u64,
            source_digest: card.source_digest,
            summary: card.summary,
            scope: card.scope,
            authority: card.authority,
            generation: card.generation,
            updated_at_ms: card.updated_at_ms,
        })
        .collect();
    let checkpoint = history
        .latest_domain_event_by_kind(&id, "memory.semantic_checkpoint.created")
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to read session checkpoint locator: {error}"),
                }),
            )
        })?;
    let recovery_state = if rebuilt {
        SessionHistoryRecoveryState::ManifestRebuilt
    } else if manifest.recovery.latest_checkpoint_sequence.is_some() && checkpoint.is_none() {
        SessionHistoryRecoveryState::CheckpointMissing
    } else if checkpoint
        .as_ref()
        .is_some_and(|event| crate::semantic_checkpoint_from_event(event, &id).is_none())
    {
        SessionHistoryRecoveryState::CheckpointMalformed
    } else if !manifest.index_complete {
        SessionHistoryRecoveryState::IndexPending
    } else {
        SessionHistoryRecoveryState::Ready
    };
    Ok(Json(SessionHistoryIndexProjection {
        schema_version: SESSION_HISTORY_INDEX_SCHEMA_VERSION,
        session_id: id,
        projection_generation: manifest.projection_generation,
        durable_cursor: manifest.recovery.durable_cursor,
        event_cursor: manifest.recovery.event_cursor,
        history_revision: manifest.recovery.history_revision,
        total_messages: manifest.recovery.transcript_messages,
        total_bytes: manifest.recovery.transcript_bytes,
        latest_checkpoint_sequence: manifest.recovery.latest_checkpoint_sequence,
        latest_checkpoint_event_id: manifest.recovery.latest_checkpoint_event_id,
        index_generation: manifest.recovery.index_generation,
        indexed_through_sequence: manifest.recovery.indexed_through_sequence,
        index_card_count: manifest.recovery.index_card_count,
        index_complete: manifest.index_complete,
        recovery_state,
        recent_metadata,
        cards,
    }))
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

    let (owner_principal_id, visible_session_ids, unrestricted) =
        session_catalog_visibility(&state, &principal);
    match state
        .services
        .session
        .list_stored_sessions_page(&SessionListOptions {
            query: params.q.as_deref(),
            model: params.model.as_deref(),
            status: params.status.as_deref(),
            owner_principal_id: Some(&owner_principal_id),
            visible_session_ids: &visible_session_ids,
            unrestricted,
            include_deleted: false,
            sort: &params.sort,
            order: &params.order,
            limit,
            offset,
        })
        .await
    {
        Ok(Some(page)) => {
            let total = page.total;
            let mut sessions = page
                .records
                .into_iter()
                .map(session_info_from_record)
                .collect::<Vec<_>>();
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
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(%error, "database-backed Session catalog query failed");
            return Json(serde_json::json!({
                "sessions": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "sort": params.sort,
                "order": params.order,
                "error": error.to_string(),
            }));
        }
    }

    // Store-less test/runtime fallback. Production deployments use the
    // database-backed path above so filtering, authorization and pagination
    // remain one atomic query.
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

fn session_catalog_visibility(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
) -> (String, Vec<String>, bool) {
    let claims = principal.0.claims();
    let unrestricted = principal.0.is_human_interactive()
        && principal.0.has_capability("runtime.maintenance.manage");
    let mut visible_session_ids = claims
        .scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("session:"))
        .filter(|session_id| !session_id.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    if let Some(runtime) = state.services.runtime.as_ref() {
        let mission_ids = claims
            .scopes
            .iter()
            .filter_map(|scope| scope.strip_prefix("mission:"))
            .filter(|mission_id| !mission_id.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        visible_session_ids.extend(
            runtime::MissionRuntimePort::new(runtime.runtime_services())
                .session_ids_for_missions(&mission_ids),
        );
    }
    (
        claims.principal_id.clone(),
        visible_session_ids.into_iter().collect(),
        unrestricted,
    )
}

async fn enrich_session_execution_indices(state: &AppState, sessions: &mut [SessionInfo]) {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return;
    };
    let session_ids = sessions
        .iter()
        .map(|session| session.id.clone())
        .collect::<Vec<_>>();
    let mut indices = runtime
        .recoverable_session_execution_indices(&session_ids)
        .await;
    for session in sessions {
        let Some(index) = indices.remove(&session.id) else {
            continue;
        };
        if index.latest_execution_id.is_some()
            || index.latest_status.is_some()
            || !index.active_execution_ids.is_empty()
        {
            session.execution = Some(index);
        }
    }
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

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");
    let model = body.model.filter(|model| !model.trim().is_empty());
    let mut metadata = serde_json::json!({});
    if let Some(preset) = body
        .execution_policy_preset
        .as_deref()
        .filter(|preset| !preset.trim().is_empty())
    {
        let profile = runtime::AutonomyProfileId::parse(preset).ok_or_else(|| {
            api_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "unsupported execution_policy_preset `{preset}`; expected cautious, supervised, solo, yolo, or stewarded"
                ),
            )
        })?;
        let policy = runtime::SessionExecutionPolicy::from_profile(
            profile,
            1,
            runtime::SessionExecutionPolicyOrigin::SessionExplicit,
        );
        metadata["execution_policy"] = serde_json::to_value(&policy)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    }
    let session_service = required_session_service(&state)?;
    let mut request = crate::services::EnsureSessionRequest::new(
        &session_id,
        model,
        crate::services::SessionSource::WebUi,
    );
    request.metadata = metadata;
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

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.clamp(1, 100);
    let (owner_principal_id, visible_session_ids, unrestricted) =
        session_catalog_visibility(&state, &principal);
    let Some(db_messages) = state
        .services
        .session
        .search_stored_messages_visible(
            &params.q,
            Some(&owner_principal_id),
            &visible_session_ids,
            unrestricted,
            limit,
        )
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
}
