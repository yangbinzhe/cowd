use std::{sync::Arc, time::Instant};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;

mod control;
use super::{connector_routes, AppState, ErrorResponse};
pub(super) use control::{
    agent_value_summary, degraded_agent_value_summary, degraded_health_summary,
    degraded_value_loop_summary, empty_workgraph_summary, get_runtime_control_plane,
    health_summary, session_lease_projection, value_loop_summary, workgraph_summary,
};
use memory::store::session::SessionListOptions;
use memory::RuntimeEvent;
use runtime::{AgentControlPolicy, RuntimeConfig};

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
            "/api/runtime/config/reload/status",
            get(get_runtime_config_reload_status),
        )
        .route("/api/runtime/status", get(get_runtime_status))
        .route("/api/runtime/events", get(get_runtime_events))
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
struct RuntimeSessionLeaseAcquireRequest {
    session_id: String,
    owner: String,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
struct RuntimeSessionLeaseReleaseRequest {
    session_id: String,
    owner: String,
}

#[derive(Deserialize)]
struct RuntimeTurnSubmitRequest {
    prompt: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    task_id: Option<String>,
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
struct RuntimeReplayParams {
    #[serde(default)]
    limit: Option<usize>,
}

async fn get_runtime_events(
    Query(params): Query<RuntimeEventsParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(100).min(500);
    let store = runtime::global_runtime_event_store();
    let events = if let Some(stream_id) = params.stream_id {
        store
            .list_stream(&stream_id)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    } else if let Some(scope) = params.scope {
        store
            .list_scope(parse_runtime_event_scope(&scope), limit)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    } else {
        store
            .all_events(limit)
            .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
    };
    Ok(Json(serde_json::json!({
        "kind": "runtime.events",
        "store_path": store.path(),
        "count": events.len(),
        "events": events,
    })))
}

async fn get_runtime_events_replay_report(
    Query(params): Query<RuntimeReplayParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    runtime_replay_report(params.limit.unwrap_or(500).min(2_000)).map(Json)
}

async fn recover_runtime_events(
    Query(params): Query<RuntimeReplayParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let report = runtime::RecoveryExecutor::execute(params.limit.unwrap_or(500).min(2_000))
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

fn runtime_replay_report(limit: usize) -> Result<Value, (StatusCode, Json<ErrorResponse>)> {
    let store = runtime::global_runtime_event_store();
    let report = runtime::RuntimeEventReplayer::report(store, limit)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(serde_json::json!({
        "kind": "runtime.events.replay_report",
        "store_path": store.path(),
        "report": report,
    }))
}

fn runtime_event_error(status: StatusCode, error: String) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error }))
}

fn parse_runtime_event_scope(scope: &str) -> runtime::RuntimeEventScope {
    match scope {
        "session" => runtime::RuntimeEventScope::Session,
        "session_command" => runtime::RuntimeEventScope::SessionCommand,
        "team" => runtime::RuntimeEventScope::Team,
        "agent" => runtime::RuntimeEventScope::Agent,
        "approval" => runtime::RuntimeEventScope::Approval,
        "relation" => runtime::RuntimeEventScope::Relation,
        "steward" => runtime::RuntimeEventScope::Steward,
        "task" => runtime::RuntimeEventScope::Task,
        "worker" => runtime::RuntimeEventScope::Worker,
        "schedule" => runtime::RuntimeEventScope::Schedule,
        "tool" => runtime::RuntimeEventScope::Tool,
        "recovery" => runtime::RuntimeEventScope::Recovery,
        _ => runtime::RuntimeEventScope::Mission,
    }
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

    let Some(page) = page else {
        return Ok(Json(serde_json::json!({
            "session_id": params.session_id,
            "events": [],
            "total": 0,
            "from_seq": from_seq,
            "next_seq": null,
            "limit": limit,
            "has_more": false,
            "degraded": true,
            "degraded_reason": "session store not available",
            "workgraph_summary": empty_workgraph_summary(),
            "health_summary": degraded_health_summary("session store not available"),
            "value_loop": degraded_value_loop_summary("session store not available"),
            "agent_value": degraded_agent_value_summary(&agent_policy, "session store not available"),
        })));
    };

    let workgraph_summary = workgraph_summary(&page.events);
    let health_summary = health_summary(&page.events, false, None);
    let value_loop = value_loop_summary(&page.events, false, None);
    let agent_value = agent_value_summary(&page.events, &agent_policy, false, None);

    Ok(Json(serde_json::json!({
        "session_id": params.session_id,
        "events": page.events,
        "total": page.total,
        "from_seq": from_seq,
        "next_seq": page.next_seq,
        "limit": limit,
        "has_more": page.has_more,
        "degraded": false,
        "degraded_reason": null,
        "workgraph_summary": workgraph_summary,
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
    Json(crate::runtime_host::config_reload::reload_runtime_providers_from_disk(&state).await)
}

pub(super) async fn reload_runtime_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    Json(crate::runtime_host::config_reload::force_gateway_config_reload(&state, "manual").await)
}

pub(super) async fn get_runtime_config_reload_status() -> Json<Value> {
    Json(crate::runtime_host::config_reload::status_value())
}

async fn get_runtime_session_leases(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    Json(session_lease_projection(&state).await)
}

async fn acquire_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<RuntimeSessionLeaseAcquireRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    let mode = request.mode.as_deref().unwrap_or("collaborative");
    Json(
        registry
            .acquire(&request.session_id, &request.owner, mode)
            .await,
    )
}

async fn release_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<RuntimeSessionLeaseReleaseRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    Json(registry.release(&request.session_id, &request.owner).await)
}
