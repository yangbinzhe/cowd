use std::{sync::Arc, time::Instant};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::{header, HeaderMap, StatusCode},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;
use std::convert::Infallible;

mod control;
use super::{connector_routes, AppState, AuthenticatedPrincipal, ErrorResponse};
pub(super) use control::{
    agent_value_summary, execution_graph_summary, get_runtime_control_plane, health_summary,
    session_lease_projection, value_loop_summary,
};
use memory::store::session::SessionListOptions;
use runtime::{AgentControlPolicy, RuntimeConfig};

#[derive(Clone, serde::Serialize)]
struct RuntimeTimelineRef {
    #[serde(rename = "type")]
    ref_type: String,
    id: String,
    label: Option<String>,
}

#[derive(Clone, serde::Serialize)]
pub(in crate::api_routes) struct RuntimeEvent {
    sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id: Option<String>,
    scope: String,
    kind: String,
    status: Option<String>,
    refs: Vec<RuntimeTimelineRef>,
    payload: Value,
    created_at_ms: u64,
    source: &'static str,
}

impl From<memory::SessionDomainEvent> for RuntimeEvent {
    fn from(event: memory::SessionDomainEvent) -> Self {
        let payload_refs = runtime_timeline_refs_from_payload(&event.payload);
        let refs = if event.refs.is_empty() {
            payload_refs
        } else {
            event
                .refs
                .into_iter()
                .map(|reference| RuntimeTimelineRef {
                    ref_type: reference.ref_type,
                    id: reference.id,
                    label: reference.label,
                })
                .collect()
        };
        let status = event.status.or_else(|| {
            event
                .payload
                .get("status")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        });
        let kind = if matches!(
            event.kind.as_str(),
            "matrix.execution_outcome" | "mfg.execution_outcome"
        ) {
            "execution.outcome".to_string()
        } else {
            event.kind
        };
        Self {
            sequence: event.sequence as u64,
            commit_cursor: None,
            stream_id: None,
            scope: session_domain_scope_label(event.scope).to_string(),
            kind,
            status,
            refs,
            payload: event.payload,
            created_at_ms: event.created_at_ms,
            source: "session_domain",
        }
    }
}

fn session_domain_scope_label(scope: memory::SessionDomainScope) -> &'static str {
    match scope {
        memory::SessionDomainScope::Session => "session",
        memory::SessionDomainScope::Message => "message",
        memory::SessionDomainScope::Turn => "turn",
        memory::SessionDomainScope::Context => "context",
        memory::SessionDomainScope::Tool => "tool",
        memory::SessionDomainScope::Memory => "memory",
        memory::SessionDomainScope::Policy => "policy",
        memory::SessionDomainScope::ApplicationTask => "task",
        memory::SessionDomainScope::Mfg => "mfg",
    }
}

fn runtime_timeline_refs_from_payload(payload: &Value) -> Vec<RuntimeTimelineRef> {
    payload
        .get("refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|reference| {
            let ref_type = reference
                .get("type")
                .or_else(|| reference.get("ref_type"))?
                .as_str()?;
            let id = reference.get("id")?.as_str()?;
            Some(RuntimeTimelineRef {
                ref_type: ref_type.to_string(),
                id: id.to_string(),
                label: reference
                    .get("label")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            })
        })
        .collect()
}

impl From<runtime::DurableRuntimeEvent> for RuntimeEvent {
    fn from(event: runtime::DurableRuntimeEvent) -> Self {
        Self {
            sequence: event.sequence,
            commit_cursor: Some(event.commit_cursor),
            stream_id: Some(event.stream_id),
            scope: event.scope.as_str().to_string(),
            kind: event.kind,
            status: event.status,
            refs: event
                .refs
                .into_iter()
                .map(|reference| RuntimeTimelineRef {
                    ref_type: reference.kind,
                    id: reference.id,
                    label: None,
                })
                .collect(),
            payload: event.payload,
            created_at_ms: event.created_at_ms,
            source: "runtime_lifecycle",
        }
    }
}

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/runtime/timeline", get(get_runtime_timeline))
        .route(
            "/api/runtime/config/effective",
            get(get_runtime_effective_config),
        )
        .route(
            "/api/runtime/providers/reload",
            post(reload_runtime_providers),
        )
        .route("/api/runtime/config/reload", post(reload_runtime_config))
        .route(
            "/api/runtime/upgrade/maintenance",
            get(get_upgrade_maintenance).post(enter_upgrade_maintenance),
        )
        .route(
            "/api/runtime/upgrade/dispositions",
            post(record_upgrade_disposition),
        )
        .route("/api/runtime/upgrade/inventory", get(get_upgrade_inventory))
        .route("/api/runtime/upgrade/export", post(export_upgrade_manifest))
        .route(
            "/api/runtime/config/reload/status",
            get(get_runtime_config_reload_status),
        )
        .route("/api/runtime/status", get(get_runtime_status))
        .route("/api/runtime/outbox", get(get_runtime_outbox_status))
        .route(
            "/api/runtime/outbox/:direction/:id/retry",
            post(retry_runtime_outbox),
        )
        .route("/api/runtime/events", get(get_runtime_events))
        .merge(super::route_registry::register_execution_projection_routes(
            Router::new(),
        ))
        .route(
            "/api/runtime/events/replay-report",
            get(get_runtime_events_replay_report),
        )
        .route("/api/runtime/events/recover", post(recover_runtime_events))
        .route("/api/runtime/snapshot", get(get_runtime_snapshot))
        .route("/api/runtime/source-audit", get(get_runtime_source_audit))
        .route(
            "/api/runtime/source-repair-plan",
            get(get_runtime_source_repair_plan),
        )
        .route("/api/runtime/capabilities", get(get_runtime_capabilities))
        .route("/api/runtime/control-plane", get(get_runtime_control_plane))
        .route(
            "/api/runtime/turns",
            get(get_runtime_turns).post(submit_runtime_turn),
        )
        .route("/api/runtime/turns/:id", get(get_runtime_turn))
        .route("/api/runtime/turns/:id/cancel", post(cancel_runtime_turn))
        .route(
            "/api/runtime/session-leases",
            get(get_runtime_session_leases),
        )
        .route(
            "/api/runtime/session-leases/acquire",
            post(acquire_runtime_session_lease),
        )
        .route(
            "/api/runtime/session-leases/release",
            post(release_runtime_session_lease),
        )
}

#[derive(Debug, Deserialize)]
pub(super) struct ExecutionProjectionQuery {
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default)]
    detail_scope: harness_contract::projection::ProjectionDetailScope,
}

pub(super) async fn execution_projection_context(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    execution_id: &str,
    detail_scope: harness_contract::projection::ProjectionDetailScope,
) -> Result<harness_contract::projection::ProjectionQueryContext, (StatusCode, Json<ErrorResponse>)>
{
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let runtime_services = runtime.runtime_services();
    let workspace_id = runtime_services.workspace_key().to_string();
    let claims = principal.0.claims();
    let scope = runtime::execution_projection::authorization_scope(&runtime_services, execution_id)
        .map_err(projection_error)?;
    let explicit_session = scope.session_id.as_ref().is_some_and(|session_id| {
        claims
            .scopes
            .iter()
            .any(|claim| claim == &format!("session:{session_id}"))
    });
    let explicit_mission = scope.mission_id.as_ref().is_some_and(|mission_id| {
        claims
            .scopes
            .iter()
            .any(|claim| claim == &format!("mission:{mission_id}"))
    });
    let owns_session = if let Some(session_id) = scope.session_id.as_deref() {
        state
            .services
            .session
            .stored_session(session_id)
            .await
            .map_err(|error| {
                runtime_event_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to resolve execution session owner: {error}"),
                )
            })?
            .and_then(|record| {
                record
                    .metadata_json
                    .as_deref()
                    .and_then(|metadata| serde_json::from_str::<Value>(metadata).ok())
                    .and_then(|metadata| {
                        metadata
                            .get("owner_principal_id")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
            })
            .is_some_and(|owner| owner == claims.principal_id)
    } else {
        false
    };
    if !projection_read_authorized(principal, owns_session, explicit_session, explicit_mission) {
        return Err(runtime_event_error(
            StatusCode::FORBIDDEN,
            "execution projection is outside the authenticated principal scope",
        ));
    }
    // Being allowed to inspect an execution is not the same authority as being
    // allowed to see every resource that happened to be granted to its worker
    // graph.  In particular, a shared `session:` or `mission:` observer must
    // not inherit the graph's read/write/worktree grants.  Only the verified
    // principal's own resource claims can make a resource reference visible;
    // Runtime still crops every emitted reference against this list.
    let mut visibility_grants = projection_visibility_grants(&claims.scopes);
    if evaluation_projection_resource_visibility_enabled() {
        // The paired evaluator runs in a dedicated authenticated Gateway and
        // must inspect the safe workspace-relative refs produced by its own
        // frozen resource lease. Production processes never receive this
        // grant, and raw contents remain behind normal evidence authority.
        visibility_grants.push("resource:*".to_string());
    }
    visibility_grants.push(format!("principal:{}", claims.principal_id));
    visibility_grants.sort();
    visibility_grants.dedup();
    Ok(harness_contract::projection::ProjectionQueryContext {
        principal: claims.principal_id.clone(),
        workspace_id,
        session_scopes: scope.session_id.into_iter().collect(),
        mission_scopes: scope.mission_id.into_iter().collect(),
        visibility_grants,
        detail_scope,
        authorization_revision: claims
            .credential_epoch
            .saturating_mul(1_000_000)
            .saturating_add(claims.profile_revision),
    })
}

fn evaluation_projection_resource_visibility_enabled() -> bool {
    let harness = std::env::var("COWD_EVAL_HARNESS").ok();
    let corpus = std::env::var("COWD_EVAL_CORPUS_ID").ok();
    evaluation_projection_resource_visibility(harness.as_deref(), corpus.as_deref())
}

fn evaluation_projection_resource_visibility(harness: Option<&str>, corpus: Option<&str>) -> bool {
    harness == Some("1") && corpus == Some("auto-strategy-v1")
}

fn projection_visibility_grants(claims: &[String]) -> Vec<String> {
    let mut grants = claims
        .iter()
        .filter_map(|claim| {
            if claim == "resource:*" {
                return Some(claim.clone());
            }
            let candidate = claim.strip_prefix("resource:").unwrap_or(claim);
            let (mode, path) = candidate.split_once(':')?;
            if !matches!(mode, "read" | "write" | "worktree") || path.trim().is_empty() {
                return None;
            }
            Some(candidate.to_string())
        })
        .collect::<Vec<_>>();
    grants.sort();
    grants.dedup();
    grants
}

fn projection_read_authorized(
    principal: &AuthenticatedPrincipal,
    owns_session: bool,
    explicit_session: bool,
    explicit_mission: bool,
) -> bool {
    owns_session
        || explicit_session
        || explicit_mission
        || (principal.0.is_human_interactive()
            && principal.0.has_capability("runtime.maintenance.manage"))
}

fn execution_runtime(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| {
            runtime_event_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime service unavailable",
            )
        })
}

fn projection_error(error: runtime::RuntimeServicesError) -> (StatusCode, Json<ErrorResponse>) {
    let status = if matches!(error, runtime::RuntimeServicesError::ProjectionAccessDenied) {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::NOT_FOUND
    };
    runtime_event_error(status, error)
}

pub(super) async fn get_execution_projection(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(execution_id): Path<String>,
    Query(query): Query<ExecutionProjectionQuery>,
) -> Result<
    Json<harness_contract::projection::ExecutionProjection>,
    (StatusCode, Json<ErrorResponse>),
> {
    let context =
        execution_projection_context(&state, &principal, &execution_id, query.detail_scope).await?;
    let runtime = execution_runtime(&state)?;
    let projection = runtime::execution_projection::snapshot(&runtime, &execution_id, &context)
        .await
        .map_err(projection_error)?;
    Ok(Json(projection))
}

pub(super) async fn get_execution_projection_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(execution_id): Path<String>,
    Query(query): Query<ExecutionProjectionQuery>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(query.cursor)
        .unwrap_or_default();
    let context =
        execution_projection_context(&state, &principal, &execution_id, query.detail_scope).await?;
    let runtime = execution_runtime(&state)?;
    let wants_sse = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    if !wants_sse {
        let delta = runtime::execution_projection::delta(&runtime, &execution_id, cursor, &context)
            .map_err(projection_error)?;
        return Ok(Json(delta).into_response());
    }
    let stream = futures::stream::unfold(
        (
            state,
            runtime,
            execution_id,
            principal,
            query.detail_scope,
            cursor,
            false,
            None::<std::time::Instant>,
        ),
        |(
            state,
            runtime,
            execution_id,
            principal,
            detail_scope,
            mut cursor,
            ended,
            mut auth_checked_at,
        )| async move {
            if ended {
                return None;
            }
            loop {
                if auth_checked_at.is_none_or(|checked_at| {
                    checked_at.elapsed() >= std::time::Duration::from_secs(1)
                }) {
                    let config_home = state.config_home.clone();
                    let principal_for_check = principal.clone();
                    let check = tokio::task::spawn_blocking(move || {
                        super::projection_stream_principal_current(
                            &config_home,
                            &principal_for_check,
                        )
                    })
                    .await;
                    auth_checked_at = Some(std::time::Instant::now());
                    let reason = match check {
                        Ok(Ok(())) => None,
                        Ok(Err(reason)) => Some(reason),
                        Err(error) => {
                            Some(format!("projection authorization check aborted: {error}"))
                        }
                    };
                    if let Some(reason) = reason {
                        let event = Event::default()
                            .event("projection_authorization_revoked")
                            .data(
                                serde_json::json!({
                                    "reason": reason,
                                    "execution_id": execution_id.clone(),
                                })
                                .to_string(),
                            );
                        return Some((
                            Ok::<Event, Infallible>(event),
                            (
                                state,
                                runtime,
                                execution_id,
                                principal,
                                detail_scope,
                                cursor,
                                true,
                                auth_checked_at,
                            ),
                        ));
                    }
                }
                let context = match execution_projection_context(
                    &state,
                    &principal,
                    &execution_id,
                    detail_scope,
                )
                .await
                {
                    Ok(context) => context,
                    Err((status, _))
                        if status == StatusCode::UNAUTHORIZED
                            || status == StatusCode::FORBIDDEN =>
                    {
                        let event = Event::default()
                            .event("projection_authorization_revoked")
                            .data(
                                serde_json::json!({
                                    "reason": "projection scope is no longer authorized",
                                    "execution_id": execution_id.clone(),
                                })
                                .to_string(),
                            );
                        return Some((
                            Ok(event),
                            (
                                state,
                                runtime,
                                execution_id,
                                principal,
                                detail_scope,
                                cursor,
                                true,
                                auth_checked_at,
                            ),
                        ));
                    }
                    Err((_, error)) => {
                        let event = Event::default().event("projection_resync").data(
                            serde_json::json!({
                                "reason": error.error,
                                "snapshot_url": format!("/api/runtime/executions/{execution_id}"),
                                "base_cursor": cursor,
                            })
                            .to_string(),
                        );
                        return Some((
                            Ok(event),
                            (
                                state,
                                runtime,
                                execution_id,
                                principal,
                                detail_scope,
                                cursor,
                                true,
                                auth_checked_at,
                            ),
                        ));
                    }
                };
                match runtime::execution_projection::delta(
                    &runtime,
                    &execution_id,
                    cursor,
                    &context,
                ) {
                    Ok(delta) if delta.target_cursor > cursor => {
                        cursor = delta.target_cursor;
                        let event = Event::default()
                            .id(cursor.to_string())
                            .event("projection_delta")
                            .json_data(delta)
                            .unwrap_or_else(|_| Event::default().event("projection_error"));
                        return Some((
                            Ok::<Event, Infallible>(event),
                            (
                                state,
                                runtime,
                                execution_id,
                                principal,
                                detail_scope,
                                cursor,
                                false,
                                auth_checked_at,
                            ),
                        ));
                    }
                    Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
                    Err(error) => {
                        let event = Event::default().event("projection_resync").data(
                            serde_json::json!({
                                "reason": error.to_string(),
                                "snapshot_url": format!("/api/runtime/executions/{execution_id}"),
                                "base_cursor": cursor,
                            })
                            .to_string(),
                        );
                        return Some((
                            Ok(event),
                            (
                                state,
                                runtime,
                                execution_id,
                                principal,
                                detail_scope,
                                cursor,
                                true,
                                auth_checked_at,
                            ),
                        ));
                    }
                }
            }
        },
    );
    Ok(Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response())
}

pub(super) async fn execute_projection_command(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(execution_id): Path<String>,
    Json(request): Json<harness_contract::projection::ExecutionCommandRequest>,
) -> Result<
    Json<harness_contract::projection::ExecutionCommandReceipt>,
    (StatusCode, Json<ErrorResponse>),
> {
    if !projection_command_authorized(&principal) {
        return Err(runtime_event_error(
            StatusCode::FORBIDDEN,
            "runtime.maintenance.manage capability is required for execution commands",
        ));
    }
    let context = execution_projection_context(
        &state,
        &principal,
        &execution_id,
        harness_contract::projection::ProjectionDetailScope::Full,
    )
    .await?;
    let runtime = execution_runtime(&state)?;
    runtime::execution_projection::command(&runtime, &execution_id, &context, request)
        .await
        .map(Json)
        .map_err(projection_error)
}

fn projection_command_authorized(principal: &AuthenticatedPrincipal) -> bool {
    principal.0.is_human_interactive() && principal.0.has_capability("runtime.maintenance.manage")
}

#[derive(Debug, Deserialize)]
struct RuntimeOutboxQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeOutboxRetryRequest {
    reason: String,
    expected_revision: Option<u64>,
}

async fn get_runtime_outbox_status(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeOutboxQuery>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    let session_store = state.services.session.unified_store().ok_or_else(|| {
        runtime_event_error(StatusCode::SERVICE_UNAVAILABLE, "session store unavailable")
    })?;
    let ingress_health = session_store
        .session_runtime_outbox_health()
        .await
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let ingress_poison = session_store
        .blocked_session_runtime_outbox(limit)
        .await
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let runtime = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let delivery = runtime.runtime_services().session_terminal_delivery();
    let (terminal_health, terminal_poison) = tokio::task::spawn_blocking(move || {
        Ok::<_, String>((
            delivery.health().map_err(|error| error.to_string())?,
            delivery.blocked(limit).map_err(|error| error.to_string())?,
        ))
    })
    .await
    .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "runtime.outbox.status",
        "healthy": ingress_health.blocked == 0 && terminal_health.blocked == 0,
        "ingress": { "health": ingress_health, "poison": ingress_poison },
        "terminal": { "health": terminal_health, "poison": terminal_poison },
    })))
}

async fn retry_runtime_outbox(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((direction, id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<RuntimeOutboxRetryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if request.reason.trim().is_empty() {
        return Err(runtime_event_error(
            StatusCode::BAD_REQUEST,
            "reason is required",
        ));
    }
    if !principal.0.is_human_interactive() || !principal.0.has_capability("runtime.outbox.retry") {
        return Err(runtime_event_error(
            StatusCode::FORBIDDEN,
            "runtime_outbox_retry_human_interactive_capability_required",
        ));
    }
    let actor = principal.0.claims().principal_id.clone();
    let record = match direction.as_str() {
        "ingress" => {
            let store = state.services.session.unified_store().ok_or_else(|| {
                runtime_event_error(StatusCode::SERVICE_UNAVAILABLE, "session store unavailable")
            })?;
            let current = store
                .get_session_runtime_outbox(&id)
                .await
                .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
                .ok_or_else(|| {
                    runtime_event_error(StatusCode::NOT_FOUND, "outbox item not found")
                })?;
            let expected_revision = request.expected_revision.unwrap_or(current.revision);
            serde_json::to_value(
                store
                    .retry_blocked_session_runtime_outbox(
                        &id,
                        expected_revision,
                        &actor,
                        request.reason.trim(),
                        now_ms(),
                    )
                    .await
                    .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?,
            )
        }
        "terminal" => {
            let runtime = state.services.runtime.as_ref().ok_or_else(|| {
                runtime_event_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "runtime service unavailable",
                )
            })?;
            let delivery = runtime.runtime_services().session_terminal_delivery();
            let actor = actor.clone();
            let reason = request.reason;
            let terminal_id = id.clone();
            serde_json::to_value(
                tokio::task::spawn_blocking(move || {
                    delivery.retry_blocked(&terminal_id, actor.trim(), reason.trim(), now_ms())
                })
                .await
                .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
                .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?,
            )
        }
        _ => {
            return Err(runtime_event_error(
                StatusCode::BAD_REQUEST,
                "direction must be ingress or terminal",
            ));
        }
    }
    .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "runtime.outbox.manual_retry",
        "direction": direction,
        "id": id,
        "record": record,
    })))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn get_runtime_source_audit(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let report = runtime::RuntimeSourceSelfAudit::audit_repo(&state.workspace_root);
    Json(serde_json::json!({
        "kind": "runtime.source_audit",
        "report": report,
    }))
}

async fn get_runtime_source_repair_plan(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let report = runtime::RuntimeSourceSelfAudit::audit_repo(&state.workspace_root);
    Json(serde_json::json!({
        "kind": "runtime.source_repair_plan",
        "ok": report.ok,
        "repair_plan": report.repair_plan,
    }))
}

#[derive(Deserialize)]
pub(super) struct RuntimeTimelineParams {
    session_id: String,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSessionLeaseAcquireRequest {
    session_id: String,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSessionLeaseReleaseRequest {
    session_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeTurnSubmitRequest {
    prompt: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    task_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpgradeDispositionRequest {
    carrier_kind: String,
    carrier_id: String,
    action: String,
    reason: String,
    #[serde(default)]
    result_refs: Vec<String>,
}

async fn get_upgrade_maintenance(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    register_gateway_upgrade_collectors(&state, runtime_service);
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.maintenance",
        "snapshot": runtime_service.upgrade_coordinator().snapshot(),
    })))
}

async fn enter_upgrade_maintenance(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if !principal.0.is_human_interactive()
        || !principal.0.has_capability("runtime.maintenance.manage")
    {
        return Err(runtime_event_error(
            StatusCode::FORBIDDEN,
            "runtime_maintenance_human_interactive_capability_required",
        ));
    }
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let coordinator = runtime_service.upgrade_coordinator();
    register_gateway_upgrade_collectors(&state, runtime_service);
    coordinator
        .enter_maintenance(principal.0.claims().principal_id.clone())
        .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.maintenance",
        "snapshot": coordinator.snapshot(),
    })))
}

async fn record_upgrade_disposition(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<UpgradeDispositionRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if request.carrier_kind.trim().is_empty()
        || request.carrier_id.trim().is_empty()
        || request.reason.trim().is_empty()
    {
        return Err(runtime_event_error(
            StatusCode::BAD_REQUEST,
            "carrier_kind, carrier_id and reason are required",
        ));
    }
    if !principal.0.is_human_interactive()
        || !principal.0.has_capability("runtime.maintenance.manage")
    {
        return Err(runtime_event_error(
            StatusCode::FORBIDDEN,
            "runtime_maintenance_human_interactive_capability_required",
        ));
    }
    let actor = principal.0.claims().principal_id.clone();
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    if !matches!(request.action.as_str(), "cancel" | "drain") {
        return Err(runtime_event_error(
            StatusCode::BAD_REQUEST,
            "upgrade disposition action must be cancel or drain",
        ));
    }
    if request.action == "cancel" {
        let cancellation = match request.carrier_kind.as_str() {
            "active_turn" => {
                let value = runtime_service.cancel_turn_value(&request.carrier_id).await;
                value
                    .get("ok")
                    .and_then(Value::as_bool)
                    .filter(|ok| *ok)
                    .map(|_| ())
                    .ok_or_else(|| {
                        value
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("turn cancellation failed")
                            .to_string()
                    })
            }
            "agent" => {
                let runtime_services = runtime_service.runtime_services();
                let agent_runtime = runtime_services.agent_runtime();
                match agent_runtime.get(&request.carrier_id) {
                    Some(snapshot) => {
                        let receipt = agent_runtime
                            .command(harness_contract::agent::AgentCommandRequest {
                                command_id: format!(
                                    "gateway-upgrade-cancel-{}",
                                    uuid::Uuid::new_v4()
                                ),
                                agent_id: snapshot.agent_id.clone(),
                                expected_revision: snapshot.revision,
                                command: harness_contract::agent::AgentCommand::Cancel,
                                input: None,
                            })
                            .await;
                        receipt
                            .accepted
                            .then_some(())
                            .ok_or(receipt.message)
                    }
                    None => Err(format!("agent not found: {}", request.carrier_id)),
                }
            }
            "team" => state
                .services
                .mission
                .cancel_team_runtime(&request.carrier_id)
                .await
                .map(|_| ()),
            "mission_session" => {
                // Maintenance is a surface-originated control action too. It
                // must use the same durable Mission command boundary as TUI,
                // WebUI, and API controls instead of mutating the aggregate
                // directly from this route.
                let services = runtime_service.runtime_services();
                let receipt = runtime::execute_mission_command(
                    services.as_ref(),
                    harness_contract::mission::MissionCommand {
                        command_id: format!(
                            "gateway-upgrade-close-session-{}",
                            uuid::Uuid::new_v4()
                        ),
                        action: harness_contract::mission::MissionCommandAction::Close,
                        target: harness_contract::mission::MissionCommandTarget::Session {
                            session_id: request.carrier_id.clone(),
                        },
                        actor: actor.clone(),
                        expected_revision: None,
                        correlation_id: format!("upgrade:{}", request.carrier_id),
                        payload: Value::Null,
                        evidence_refs: request.result_refs.clone(),
                    },
                )
                .await;
                (receipt.status == "accepted")
                    .then_some(())
                    .ok_or_else(|| {
                        receipt
                            .reason
                            .unwrap_or_else(|| "mission session close was not accepted".to_string())
                    })
            }
            "cross_plane_execution" => Err(
                "cross-plane executions have no safe cancellation adapter; wait for terminal state before recording drain"
                    .to_string(),
            ),
            kind => Err(format!("unsupported upgrade carrier kind `{kind}`")),
        };
        cancellation.map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    } else {
        register_gateway_upgrade_collectors(&state, runtime_service);
        let active = runtime_service
            .upgrade_runtime_carriers()
            .into_iter()
            .chain(cross_plane_upgrade_carriers(&state.services.cross_plane))
            .any(|carrier| {
                carrier.carrier_kind == request.carrier_kind
                    && carrier.carrier_id == request.carrier_id
                    && carrier.status.is_active()
            });
        if active {
            return Err(runtime_event_error(
                StatusCode::CONFLICT,
                "carrier is still active; drain disposition can be recorded only after it reaches a terminal state",
            ));
        }
    }
    let receipt = runtime::UpgradeDispositionReceipt {
        carrier_kind: request.carrier_kind,
        carrier_id: request.carrier_id,
        action: request.action,
        actor,
        reason: request.reason,
        result_refs: request.result_refs,
        created_at_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
    };
    runtime_service
        .upgrade_coordinator()
        .record_disposition(receipt.clone())
        .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.disposition",
        "receipt": receipt,
    })))
}

async fn export_upgrade_manifest(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let coordinator = runtime_service.upgrade_coordinator();
    register_gateway_upgrade_collectors(&state, runtime_service);
    let runtime_services = runtime_service.runtime_services();
    let workspace_id = runtime_services.workspace_key().to_string();
    let inventory = coordinator
        .collect_inventory(
            env!("CARGO_PKG_VERSION"),
            workspace_id,
            runtime_services.workspace_root().to_path_buf(),
        )
        .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    let path = state
        .config_home
        .join("migrations")
        .join("v3-active-inventory.json");
    let receipt = coordinator
        .export_clean_shutdown_manifest(&inventory, &path)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let receipt_path = state
        .config_home
        .join("migrations")
        .join("v3-clean-shutdown-receipt.json");
    let receipt_bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let temporary = receipt_path.with_extension("json.tmp");
    std::fs::write(&temporary, receipt_bytes)
        .and_then(|_| std::fs::rename(&temporary, &receipt_path))
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.manifest",
        "inventory": inventory,
        "receipt": receipt,
    })))
}

async fn get_upgrade_inventory(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    register_gateway_upgrade_collectors(&state, runtime_service);
    let mut carriers = runtime_service.upgrade_runtime_carriers();
    carriers.extend(cross_plane_upgrade_carriers(&state.services.cross_plane));
    carriers.sort_by(|left, right| {
        (&left.carrier_kind, &left.carrier_id).cmp(&(&right.carrier_kind, &right.carrier_id))
    });
    let active_count = carriers
        .iter()
        .filter(|carrier| carrier.status.is_active())
        .count();
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.inventory_preview",
        "maintenance": !runtime_service.upgrade_coordinator().accepts_new_work(),
        "carrier_count": carriers.len(),
        "active_count": active_count,
        "carriers": carriers,
    })))
}

fn register_gateway_upgrade_collectors(
    state: &AppState,
    runtime_service: &crate::runtime_service::RuntimeService,
) {
    let cross_plane = state.services.cross_plane.clone();
    runtime_service
        .upgrade_coordinator()
        .register_collector(Arc::new(runtime::ClosureUpgradeInventoryCollector::new(
            "cross_plane_executions",
            move || Ok(cross_plane_upgrade_carriers(&cross_plane)),
        )));
}

fn cross_plane_upgrade_carriers(
    cross_plane: &crate::services::CrossPlaneService,
) -> Vec<runtime::UpgradeCarrierRecord> {
    cross_plane
        .control()
        .snapshot()
        .executions
        .into_iter()
        .map(|receipt| {
            let payload = serde_json::to_vec(&receipt).unwrap_or_default();
            runtime::UpgradeCarrierRecord {
                carrier_kind: "cross_plane_execution".to_string(),
                carrier_id: receipt.id,
                status: upgrade_cross_plane_status(&receipt.status),
                revision: receipt.timestamp.timestamp_millis().max(0) as u64,
                result_ref: receipt
                    .audit_record_id
                    .map(|id| format!("cross-plane://audit/{id}")),
                state_ref: receipt.action.resource_ref.clone().or_else(|| {
                    Some(format!(
                        "cross-plane://capability/{}",
                        receipt.action.requested_capability
                    ))
                }),
                state_hash: format!(
                    "{:016x}",
                    model_protocol::prompt_cache::stable_hash_bytes(&payload)
                ),
            }
        })
        .collect()
}

fn upgrade_cross_plane_status(status: &str) -> runtime::UpgradeCarrierStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "pending" | "ready" | "accepted" => runtime::UpgradeCarrierStatus::Ready,
        "running" | "dispatching" | "executing" => runtime::UpgradeCarrierStatus::Running,
        "waiting" | "approval_required" => runtime::UpgradeCarrierStatus::Waiting,
        "paused" => runtime::UpgradeCarrierStatus::Paused,
        "failed" | "error" | "dispatch_failed" => runtime::UpgradeCarrierStatus::Failed,
        "cancelled" | "canceled" => runtime::UpgradeCarrierStatus::Cancelled,
        _ => runtime::UpgradeCarrierStatus::Completed,
    }
}

#[cfg(test)]
mod v2_upgrade_tests {
    use super::{
        evaluation_projection_resource_visibility, projection_command_authorized,
        projection_read_authorized, projection_visibility_grants, upgrade_cross_plane_status,
    };
    use crate::api_routes::AuthenticatedPrincipal;
    use harness_contract::security::{PrincipalAssurance, PrincipalClaims, PrincipalKind};

    #[test]
    fn cross_plane_inventory_distinguishes_active_and_terminal_executions() {
        assert_eq!(
            upgrade_cross_plane_status("dispatching"),
            runtime::UpgradeCarrierStatus::Running
        );
        assert_eq!(
            upgrade_cross_plane_status("dispatch_failed"),
            runtime::UpgradeCarrierStatus::Failed
        );
        assert_eq!(
            upgrade_cross_plane_status("sent"),
            runtime::UpgradeCarrierStatus::Completed
        );
    }

    #[test]
    fn execution_commands_require_interactive_control_capability() {
        let principal = |assurance, capabilities: Vec<&str>| {
            AuthenticatedPrincipal(runtime::VerifiedPrincipal::from_test_claims(
                PrincipalClaims {
                    principal_id: "route-test".to_string(),
                    kind: PrincipalKind::Human,
                    scopes: vec!["gateway".to_string()],
                    capabilities: capabilities.into_iter().map(str::to_string).collect(),
                    assurance,
                    issuer: "route-test".to_string(),
                    issued_at_ms: 1,
                    expires_at_ms: None,
                    credential_fingerprint: "route-test".to_string(),
                    credential_epoch: 1,
                    profile_revision: 1,
                },
            ))
        };

        assert!(!projection_command_authorized(&principal(
            PrincipalAssurance::HumanInteractive,
            Vec::new(),
        )));
        assert!(!projection_command_authorized(&principal(
            PrincipalAssurance::Normal,
            vec!["runtime.maintenance.manage"],
        )));
        assert!(projection_command_authorized(&principal(
            PrincipalAssurance::HumanInteractive,
            vec!["runtime.maintenance.manage"],
        )));
        let observer = principal(PrincipalAssurance::HumanInteractive, Vec::new());
        let manager = principal(
            PrincipalAssurance::HumanInteractive,
            vec!["runtime.maintenance.manage"],
        );
        assert!(projection_read_authorized(&observer, true, false, false));
        assert!(!projection_read_authorized(&observer, false, false, false));
        assert!(projection_read_authorized(&manager, false, false, false));
        assert!(projection_read_authorized(&observer, false, false, true));
    }

    #[test]
    fn execution_read_scope_does_not_inherit_graph_resource_grants() {
        let grants = projection_visibility_grants(&[
            "gateway".to_string(),
            "session:shared-session".to_string(),
            "mission:shared-mission".to_string(),
            "resource:read:crates/runtime".to_string(),
            "write:surfaces/webui".to_string(),
            "resource:*".to_string(),
            "resource:session:must-not-be-a-resource-grant".to_string(),
        ]);

        assert_eq!(
            grants,
            vec![
                "read:crates/runtime".to_string(),
                "resource:*".to_string(),
                "write:surfaces/webui".to_string(),
            ]
        );
        // A graph's resource scope is intentionally absent: it may be used by
        // the worker, but is never promoted into a viewer's claims.
        assert!(!grants.contains(&"read:private/worker-only".to_string()));
    }

    #[test]
    fn resource_refs_are_visible_only_in_the_dedicated_paired_evaluator() {
        assert!(evaluation_projection_resource_visibility(
            Some("1"),
            Some("auto-strategy-v1")
        ));
        assert!(!evaluation_projection_resource_visibility(
            Some("1"),
            Some("another-corpus")
        ));
        assert!(!evaluation_projection_resource_visibility(
            None,
            Some("auto-strategy-v1")
        ));
    }
}

#[derive(Deserialize)]
struct RuntimeEventsParams {
    #[serde(default)]
    stream_id: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct RuntimeCapabilitiesParams {
    #[serde(default)]
    intent: Option<String>,
    #[serde(default)]
    surface: Option<String>,
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    detail: Option<String>,
}

async fn get_runtime_capabilities(Query(params): Query<RuntimeCapabilitiesParams>) -> Json<Value> {
    let intent = params
        .intent
        .unwrap_or_else(|| "inspect active runtime capability map".to_string());
    Json(runtime::runtime_capabilities_response_with_detail(
        &intent,
        params.surface.as_deref(),
        params.profile.as_deref(),
        params.detail.as_deref(),
    ))
}

#[derive(Deserialize)]
struct RuntimeReplayParams {
    #[serde(default)]
    limit: Option<usize>,
}

async fn get_runtime_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeEventsParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(100).min(500);
    let events = if let Some(stream_id) = params.stream_id {
        state
            .services
            .runtime_events
            .list_stream(&stream_id)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    } else if let Some(scope) = params.scope {
        let scope = parse_runtime_event_scope(&scope)
            .map_err(|error| runtime_event_error(StatusCode::BAD_REQUEST, error))?;
        state
            .services
            .runtime_events
            .list_scope(scope, limit)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    } else {
        state
            .services
            .runtime_events
            .all_events(limit)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    };
    Ok(Json(serde_json::json!({
        "kind": "runtime.events",
        "count": events.len(),
        "events": events,
    })))
}

async fn get_runtime_events_replay_report(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeReplayParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    runtime_replay_report(&state, params.limit.unwrap_or(500).min(2_000)).map(Json)
}

async fn recover_runtime_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeReplayParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let services = state.services.runtime.as_ref().ok_or_else(|| {
        runtime_event_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime service unavailable",
        )
    })?;
    let runtime_services = services.runtime_services();
    let report = runtime::RecoveryExecutor::execute(
        params.limit.unwrap_or(500).min(2_000),
        runtime_services.as_ref(),
    )
    .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "runtime.recovery_result",
        "ok": report.ok,
        "applied": report.applied,
        "skipped": report.skipped,
        "failed": report.failed,
        "report": report,
    })))
}

fn runtime_replay_report(
    state: &AppState,
    limit: usize,
) -> Result<Value, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .runtime_events
        .replay_report(limit)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(serde_json::json!({
        "kind": "runtime.events.replay_report",
        "actions": &report.actions,
        "candidates": &report.candidates,
        "report": report,
    }))
}

fn runtime_event_error(
    status: StatusCode,
    error: impl ToString,
) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

fn parse_runtime_event_scope(scope: &str) -> Result<runtime::RuntimeEventScope, String> {
    runtime::RuntimeEventScope::parse(scope).map_err(|error| error.to_string())
}

async fn submit_runtime_turn(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<RuntimeTurnSubmitRequest>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(
            runtime
                .submit_turn_value(body.session_id, body.task_id, body.prompt)
                .await,
        ),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn get_runtime_turns(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.turns_value()),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn get_runtime_turn(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.turn_value(&id)),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn cancel_runtime_turn(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.cancel_turn_value(&id).await),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

pub(super) async fn get_runtime_status(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.status_value()),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

pub(super) async fn get_runtime_snapshot(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.snapshot_value().await),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

pub(super) async fn get_runtime_timeline(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeTimelineParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let agent_policy = load_agent_control_policy(&state);
    let page = state
        .services
        .session
        .stored_timeline_runtime_page(&params.session_id, from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime timeline: {e}"),
                }),
            )
        })?;

    let session_store_available = page.is_some();
    let (domain_total, domain_next_seq, domain_has_more, mut events) = page
        .map(|page| {
            (
                page.total,
                page.next_seq,
                page.has_more,
                page.events
                    .into_iter()
                    .map(RuntimeEvent::from)
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap_or((0, None, false, Vec::new()));
    let lifecycle_events = state
        .services
        .runtime_events
        .session_timeline_events(&params.session_id, from_seq as u64, limit)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let lifecycle_total = lifecycle_events.len();
    events.extend(lifecycle_events.into_iter().map(RuntimeEvent::from));
    events.sort_by_key(|event| (event.created_at_ms, event.sequence));
    events.dedup_by(|left, right| {
        left.source == right.source
            && left.stream_id == right.stream_id
            && left.sequence == right.sequence
            && left.kind == right.kind
    });
    let combined_total = domain_total + lifecycle_total;
    let combined_has_more = domain_has_more || events.len() > limit;
    events.truncate(limit);

    let execution_graph_summary = execution_graph_summary(&events);
    let degraded_reason = (!session_store_available).then_some("session store not available");
    let degraded = degraded_reason.is_some();
    let health_summary = health_summary(&events, degraded, degraded_reason);
    let value_loop = value_loop_summary(&events, degraded, degraded_reason);
    let agent_value = agent_value_summary(&events, &agent_policy, degraded, degraded_reason);

    Ok(Json(serde_json::json!({
        "session_id": params.session_id,
        "events": events,
        "total": combined_total,
        "from_seq": from_seq,
        "next_seq": domain_next_seq,
        "limit": limit,
        "has_more": combined_has_more,
        "degraded": !session_store_available,
        "degraded_reason": degraded_reason,
        "execution_graph_summary": execution_graph_summary,
        "health_summary": health_summary,
        "value_loop": value_loop,
        "agent_value": agent_value,
    })))
}

fn load_agent_control_policy(state: &AppState) -> AgentControlPolicy {
    state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map(|config| config.runtime_control().policy.agent.clone())
        .unwrap_or_else(|error| {
            tracing::warn!(
                target: "cowd.runtime.agent_value",
                error = %error,
                "failed to load agent control policy; using defaults"
            );
            AgentControlPolicy::default()
        })
}

pub(super) async fn get_runtime_effective_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let (source, runtime_config, warnings) = match state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
    {
        Ok(config) => {
            let source = if config.loaded_entries().is_empty() {
                "default"
            } else {
                "config"
            };
            (source, config, Vec::<String>::new())
        }
        Err(error) => (
            "default",
            RuntimeConfig::empty(),
            vec![format!("failed to load runtime config: {error}")],
        ),
    };
    let control = runtime_config.runtime_control();
    Json(serde_json::json!({
        "source": source,
        "workspace_root": state.workspace_root,
        "profile_id": state.profile_id,
        "scenario": control.scenario.as_str(),
        "control_policy": control.policy,
        "warnings": warnings,
    }))
}

pub(super) async fn reload_runtime_providers(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return Json(serde_json::json!({"ok": false, "error": "runtime service unavailable"}));
    };
    let reload = runtime.config_reload();
    Json(
        crate::runtime_host::config_reload::reload_runtime_providers_from_disk(&reload, &state)
            .await,
    )
}

pub(super) async fn reload_runtime_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return Json(serde_json::json!({"ok": false, "error": "runtime service unavailable"}));
    };
    let reload = runtime.config_reload();
    Json(
        crate::runtime_host::config_reload::force_gateway_config_reload(&reload, &state, "manual")
            .await,
    )
}

pub(super) async fn get_runtime_config_reload_status(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let Some(runtime) = state.services.runtime.as_ref() else {
        return Json(serde_json::json!({"ok": false, "error": "runtime service unavailable"}));
    };
    let reload = runtime.config_reload();
    Json(crate::runtime_host::config_reload::status_value(&reload))
}

async fn get_runtime_session_leases(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    Json(session_lease_projection(&state).await)
}

async fn acquire_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<RuntimeSessionLeaseAcquireRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    let mode = request.mode.as_deref().unwrap_or("collaborative");
    let owner = format!("principal:{}", principal.0.claims().principal_id);
    Json(registry.acquire(&request.session_id, &owner, mode).await)
}

async fn release_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<RuntimeSessionLeaseReleaseRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    let owner = format!("principal:{}", principal.0.claims().principal_id);
    Json(registry.release(&request.session_id, &owner).await)
}
