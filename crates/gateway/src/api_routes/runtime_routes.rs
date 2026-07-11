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
struct UpgradeMaintenanceRequest {
    actor: String,
}

#[derive(Deserialize)]
struct UpgradeDispositionRequest {
    carrier_kind: String,
    carrier_id: String,
    action: String,
    actor: String,
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
    Json(request): Json<UpgradeMaintenanceRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if request.actor.trim().is_empty() {
        return Err(runtime_event_error(
            StatusCode::BAD_REQUEST,
            "actor is required",
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
        .enter_maintenance(request.actor)
        .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "runtime.upgrade.maintenance",
        "snapshot": coordinator.snapshot(),
    })))
}

async fn record_upgrade_disposition(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<UpgradeDispositionRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if request.carrier_kind.trim().is_empty()
        || request.carrier_id.trim().is_empty()
        || request.actor.trim().is_empty()
        || request.reason.trim().is_empty()
    {
        return Err(runtime_event_error(
            StatusCode::BAD_REQUEST,
            "carrier_kind, carrier_id, actor and reason are required",
        ));
    }
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
            "agent" => runtime::global_agent_lifecycle_service()
                .cancel(&request.carrier_id)
                .map(|_| ()),
            "team" => runtime::global_team_runtime_service()
                .cancel(&request.carrier_id)
                .map(|_| ()),
            "mission_session" => runtime::global_mission_runtime()
                .close_session(&request.carrier_id)
                .map(|_| ()),
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
        actor: request.actor,
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
    let workspace_id = format!(
        "{:016x}",
        model_protocol::prompt_cache::stable_hash_bytes(
            state.workspace_root.to_string_lossy().as_bytes()
        )
    );
    let inventory = coordinator
        .collect_inventory(
            env!("CARGO_PKG_VERSION"),
            workspace_id,
            state.workspace_root.clone(),
        )
        .map_err(|error| runtime_event_error(StatusCode::CONFLICT, error))?;
    let path = state
        .config_home
        .join("migrations")
        .join("v3-active-inventory.json");
    let receipt = coordinator
        .export_clean_shutdown_manifest(&inventory, &path)
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
    use super::upgrade_cross_plane_status;

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
    let active_evolution = crate::current_active_evolution_capability_overlay();
    Json(
        runtime::runtime_capabilities_response_with_detail_and_overlay(
            &intent,
            params.surface.as_deref(),
            params.profile.as_deref(),
            params.detail.as_deref(),
            &active_evolution,
        ),
    )
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
    let plan = runtime::RecoveryPlanner::plan(limit)
        .map_err(|error| runtime_event_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(serde_json::json!({
        "kind": "runtime.events.replay_report",
        "store_path": store.path(),
        "report": plan.report,
        "actions": plan.actions,
        "candidates": plan.candidates,
        "plan": plan,
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
