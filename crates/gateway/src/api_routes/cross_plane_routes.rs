use std::{collections::HashSet, path::Path as FsPath, sync::Arc};

use axum::{
    extract::{Path, State as AxumState},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use runtime::{
    CrossPlaneAction, CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome,
    CrossPlaneDispatchTarget, CrossPlaneGrant, CrossPlaneIdentityBinding, CrossPlanePolicyDecision,
    PolicyDecisionKind,
};
use serde::{Deserialize, Serialize};
use surface::SurfaceSendRequest;

use super::{channel_routes, AppState};
use crate::services::CrossPlaneExecutionRecord;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/cross-plane/summary", get(cross_plane_summary_handler))
        .route(
            "/api/cross-plane/identities",
            get(cross_plane_identities_handler).post(cross_plane_create_identity_handler),
        )
        .route(
            "/api/cross-plane/identities/:id",
            delete(cross_plane_revoke_identity_handler),
        )
        .route(
            "/api/cross-plane/grants",
            get(cross_plane_grants_handler).post(cross_plane_create_grant_handler),
        )
        .route(
            "/api/cross-plane/grants/:id",
            delete(cross_plane_revoke_grant_handler),
        )
        .route("/api/cross-plane/audit", get(cross_plane_audit_handler))
        .route(
            "/api/cross-plane/action/adapters",
            get(cross_plane_action_adapters_handler),
        )
        .route(
            "/api/cross-plane/action/executions",
            get(cross_plane_action_executions_handler),
        )
        .route(
            "/api/cross-plane/policy/simulate",
            post(cross_plane_policy_simulate_handler),
        )
        .route(
            "/api/cross-plane/action/preflight",
            post(cross_plane_action_preflight_handler),
        )
        .route(
            "/api/cross-plane/action/execute",
            post(cross_plane_action_execute_handler),
        )
        .route(
            "/api/cross-plane/identity/resolve",
            post(cross_plane_identity_resolve_handler),
        )
}

#[derive(Debug, Deserialize)]
struct CrossPlaneIdentityResolveRequest {
    identity_ref: String,
}

#[derive(Debug, Deserialize)]
struct CrossPlaneActionExecuteRequest {
    action: CrossPlaneAction,
    #[serde(default = "default_execute_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct CrossPlaneActionReadiness {
    action: CrossPlaneAction,
    decision: CrossPlanePolicyDecision,
    target_platform: Option<String>,
    platform_readiness: Option<channel_routes::PlatformReadiness>,
    adapter_capability: Option<CrossPlaneAdapterCapability>,
    dispatch_target: Option<CrossPlaneDispatchTarget>,
    evidence: CrossPlaneDecisionEvidence,
    executable: bool,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CrossPlaneAdapterCapability {
    platform: String,
    capability: String,
    operation: String,
    live_supported: bool,
    adapter_bound: bool,
}

fn default_execute_mode() -> String {
    "dry_run".to_string()
}

async fn cross_plane_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let summary = state
        .services
        .cross_plane
        .control()
        .summary(chrono::Utc::now());
    Json(serde_json::json!({
        "kind": "cross_plane_summary",
        "providers": [],
        "channels": [],
        "services": [],
        "identity_bindings": {
            "verified": summary.verified_identities,
            "claimed": summary.claimed_identities,
            "observed": summary.observed_identities,
            "unknown": 0
        },
        "grants": {
            "active": summary.active_grants,
            "expiring": 0,
            "expired": 0
        },
        "approvals": {
            "pending": 0
        },
        "interop": {
            "actions_24h": summary.audit_records,
            "allowed_24h": summary.allowed_actions,
            "denied_24h": summary.denied_actions,
            "approval_required_24h": summary.approval_required_actions
        }
    }))
}

async fn cross_plane_grants_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let grants = state.services.cross_plane.control().list_grants();
    Json(serde_json::json!({
        "kind": "cross_plane_grants",
        "grants": grants
    }))
}

async fn cross_plane_identities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let identities = state.services.cross_plane.control().list_identities();
    Json(serde_json::json!({
        "kind": "cross_plane_identities",
        "identities": identities
    }))
}

async fn cross_plane_create_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(binding): Json<CrossPlaneIdentityBinding>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let binding = state
        .services
        .cross_plane
        .control()
        .upsert_identity(binding);
    state.services.cross_plane.save_state(&state.config_home);
    Json(serde_json::json!({
        "kind": "cross_plane_identity",
        "identity": binding
    }))
}

async fn cross_plane_revoke_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let revoked = state.services.cross_plane.control().revoke_identity(&id);
    state.services.cross_plane.save_state(&state.config_home);
    Json(serde_json::json!({
        "kind": "cross_plane_identity_revoked",
        "id": id,
        "revoked": revoked
    }))
}

async fn cross_plane_create_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(grant): Json<CrossPlaneGrant>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let grant = state.services.cross_plane.control().upsert_grant(grant);
    state.services.cross_plane.save_state(&state.config_home);
    Json(serde_json::json!({
        "kind": "cross_plane_grant",
        "grant": grant
    }))
}

async fn cross_plane_revoke_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let revoked = state.services.cross_plane.control().revoke_grant(&id);
    state.services.cross_plane.save_state(&state.config_home);
    Json(serde_json::json!({
        "kind": "cross_plane_grant_revoked",
        "id": id,
        "revoked": revoked
    }))
}

async fn cross_plane_audit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let records = state.services.cross_plane.control().list_audit(100, 0);
    let total = records.len();
    Json(serde_json::json!({
        "kind": "cross_plane_audit",
        "records": records,
        "total": total
    }))
}

async fn cross_plane_action_adapters_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let platforms = channel_routes::configured_platforms(state.config.as_ref());
    let bound_adapters = bound_adapter_snapshot(&state).await;
    let capabilities = platforms
        .iter()
        .flat_map(|platform| adapter_capabilities_for_platform(platform, &bound_adapters))
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "kind": "cross_plane_action_adapters",
        "capabilities": capabilities,
    }))
}

async fn cross_plane_action_executions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let executions = state.services.cross_plane.control().list_executions(100, 0);
    let total = executions.len();
    Json(serde_json::json!({
        "kind": "cross_plane_action_executions",
        "executions": executions,
        "total": total,
    }))
}

async fn cross_plane_policy_simulate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(action): Json<CrossPlaneAction>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let (action, decision, evidence) =
        decide_connector_action(&state, action, "dry_run", chrono::Utc::now());
    state.services.cross_plane.save_state(&state.config_home);
    Json(serde_json::json!({
        "kind": "cross_plane_policy_simulation",
        "action": action,
        "decision": decision,
        "evidence": evidence,
    }))
}

async fn cross_plane_action_preflight_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(action): Json<CrossPlaneAction>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let readiness = evaluate_action_readiness(&state, action, "dry_run", chrono::Utc::now()).await;
    Json(serde_json::json!({
        "kind": "cross_plane_action_preflight",
        "action": readiness.action,
        "decision": readiness.decision,
        "target_platform": readiness.target_platform,
        "platform_readiness": readiness.platform_readiness,
        "adapter_capability": readiness.adapter_capability,
        "dispatch_target": readiness.dispatch_target,
        "evidence": readiness.evidence,
        "executable": readiness.executable,
        "blockers": readiness.blockers,
    }))
}

async fn cross_plane_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<CrossPlaneActionExecuteRequest>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let now = chrono::Utc::now();
    let mode = normalize_execute_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            let target_platform = target_platform_from_action(&receipt.action);
            return Json(serde_json::json!({
                "kind": "cross_plane_action_execution",
                "mode": receipt.mode.clone(),
                "status": receipt.status.clone(),
                "dispatch_status": receipt.dispatch_status.clone(),
                "idempotency_key": receipt.idempotency_key.clone(),
                "action": receipt.action.clone(),
                "decision": receipt.decision.clone(),
                "target_platform": target_platform,
                "platform_readiness": null,
                "adapter_capability": null,
                "dispatch_target": null,
                "executable": false,
                "blockers": receipt.blockers.clone(),
                "dispatched": false,
                "audit_record_id": receipt.audit_record_id.clone(),
                "execution_receipt": receipt,
                "idempotent_replay": true,
            }));
        }
    }
    let mut readiness = evaluate_action_readiness(&state, request.action, &mode, now).await;
    let mut evidence = readiness.evidence.clone();
    let mut status = "blocked";
    let mut dispatch_status = "not_started";
    let mut audit_result = "blocked";
    let mut audit_summary = readiness
        .blockers
        .first()
        .cloned()
        .unwrap_or_else(|| readiness.decision.reason.clone());
    let mut dispatched = false;
    let mut dispatch_outcome = None;

    if readiness.executable {
        if mode == "dry_run" {
            status = "planned";
            dispatch_status = "dry_run";
            audit_result = "dry_run";
            audit_summary = "dry_run_execution_plan".to_string();
        } else if readiness
            .adapter_capability
            .as_ref()
            .is_some_and(|capability| capability.live_supported && !capability.adapter_bound)
        {
            readiness
                .blockers
                .push("dispatch:adapter_not_bound".to_string());
            readiness.executable = false;
            dispatch_status = "adapter_not_bound";
            audit_result = "blocked_dispatch";
            audit_summary = "live_dispatch_adapter_not_bound".to_string();
        } else if readiness
            .adapter_capability
            .as_ref()
            .is_some_and(|capability| capability.live_supported && capability.adapter_bound)
        {
            if readiness
                .dispatch_target
                .as_ref()
                .is_some_and(|target| target.ready)
            {
                match dispatch_ready_target(&state, readiness.dispatch_target.as_ref().unwrap())
                    .await
                {
                    Ok(outcome) => {
                        status = "dispatched";
                        dispatch_status = "sent";
                        audit_result = "dispatched";
                        audit_summary = "live_dispatch_sent".to_string();
                        dispatched = true;
                        dispatch_outcome = Some(outcome);
                        if let Some((grant_id, remaining)) = state
                            .services
                            .cross_plane
                            .consume_matched_grant_for_decision(&readiness.decision)
                        {
                            evidence.consumed_grant_id = Some(grant_id);
                            evidence.remaining_uses_after = Some(remaining);
                        }
                    }
                    Err((blocker, outcome)) => {
                        readiness.blockers.push(blocker);
                        dispatch_outcome = outcome;
                        readiness.executable = false;
                        dispatch_status = "dispatch_failed";
                        audit_result = "blocked_dispatch";
                        audit_summary = "live_dispatch_failed".to_string();
                    }
                }
            } else {
                let target_blockers = readiness
                    .dispatch_target
                    .as_ref()
                    .map(|target| target.blockers.clone())
                    .unwrap_or_else(|| vec!["dispatch:target_unavailable".to_string()]);
                readiness.blockers.extend(target_blockers);
                dispatch_status = "dispatch_target_not_ready";
                audit_summary = "live_dispatch_target_not_ready".to_string();
            }
            if !dispatched {
                readiness.executable = false;
                if audit_result != "blocked_dispatch" {
                    audit_result = "blocked_dispatch";
                }
            }
        } else {
            readiness
                .blockers
                .push("dispatch:capability_not_supported".to_string());
            readiness.executable = false;
            dispatch_status = "capability_not_supported";
            audit_result = "blocked_dispatch";
            audit_summary = "live_dispatch_capability_not_supported".to_string();
        }
    }

    let (audit_record_id, receipt) =
        state
            .services
            .cross_plane
            .record_action_execution(CrossPlaneExecutionRecord {
                idempotency_key: idempotency_key.clone(),
                mode: mode.clone(),
                status: status.to_string(),
                dispatch_status: dispatch_status.to_string(),
                action: readiness.action.clone(),
                decision: readiness.decision.clone(),
                blockers: readiness.blockers.clone(),
                dispatch_target: readiness.dispatch_target.clone(),
                dispatch_outcome: dispatch_outcome.clone(),
                evidence,
                audit_result: audit_result.to_string(),
                audit_summary,
            });
    state.services.cross_plane.save_state(&state.config_home);

    Json(serde_json::json!({
        "kind": "cross_plane_action_execution",
        "mode": mode,
        "status": status,
        "dispatch_status": dispatch_status,
        "idempotency_key": idempotency_key,
        "action": readiness.action,
        "decision": readiness.decision,
        "target_platform": readiness.target_platform,
        "platform_readiness": readiness.platform_readiness,
        "adapter_capability": readiness.adapter_capability,
        "dispatch_target": readiness.dispatch_target,
        "dispatch_outcome": dispatch_outcome,
        "executable": readiness.executable,
        "blockers": readiness.blockers,
        "dispatched": dispatched,
        "audit_record_id": audit_record_id,
        "execution_receipt": receipt,
        "idempotent_replay": false,
    }))
}

async fn cross_plane_identity_resolve_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<CrossPlaneIdentityResolveRequest>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let resolved = state
        .services
        .cross_plane
        .control()
        .resolve_identity(&request.identity_ref, chrono::Utc::now());
    Json(serde_json::json!({
        "kind": "cross_plane_identity_resolution",
        "identity_ref": request.identity_ref,
        "resolved": resolved,
    }))
}

fn target_platform_from_action(action: &CrossPlaneAction) -> Option<String> {
    for value in [
        action.requested_capability.as_str(),
        action.provider_account.as_deref().unwrap_or_default(),
        action.target_ref.as_deref().unwrap_or_default(),
        action.resource_ref.as_deref().unwrap_or_default(),
    ] {
        if let Some(platform) = target_platform_from_ref(value) {
            return Some(platform);
        }
    }
    None
}

async fn evaluate_action_readiness(
    state: &AppState,
    action: CrossPlaneAction,
    mode: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> CrossPlaneActionReadiness {
    let (action, decision, evidence) = decide_connector_action(state, action, mode, now);
    let target_platform = target_platform_from_action(&action);
    let platforms = channel_routes::configured_platforms(state.config.as_ref());
    let platform_readiness = target_platform.as_ref().and_then(|target| {
        platforms
            .into_iter()
            .find(|platform| platform.name == *target || platform.platform_type == *target)
    });
    let bound_adapters = bound_adapter_snapshot(state).await;
    let adapter_capability = platform_readiness
        .as_ref()
        .and_then(|platform| adapter_capability_for_action(platform, &action, &bound_adapters));
    let dispatch_target = build_dispatch_target(
        &action,
        target_platform.as_deref(),
        adapter_capability.as_ref(),
    );
    let mut blockers = Vec::new();
    if decision.decision != PolicyDecisionKind::Allow {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if let Some(readiness) = &platform_readiness {
        if readiness.status != "ready" {
            blockers.push(format!("platform:{}:{}", readiness.name, readiness.status));
        }
    } else if let Some(target) = &target_platform {
        blockers.push(format!("platform:{target}:unconfigured"));
    }
    let executable = blockers.is_empty();
    CrossPlaneActionReadiness {
        action,
        decision,
        target_platform,
        platform_readiness,
        adapter_capability,
        dispatch_target,
        evidence,
        executable,
        blockers,
    }
}

pub(super) fn decide_connector_action(
    state: &AppState,
    action: CrossPlaneAction,
    mode: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> (
    CrossPlaneAction,
    CrossPlanePolicyDecision,
    CrossPlaneDecisionEvidence,
) {
    let snapshot = super::connector_routes::connector_snapshot(state);
    state
        .services
        .cross_plane
        .decide_connector_action(&snapshot, action, mode, now)
}

fn normalize_execute_mode(mode: &str) -> String {
    match mode.trim().to_ascii_lowercase().as_str() {
        "commit" | "live" | "execute" => "commit".to_string(),
        _ => "dry_run".to_string(),
    }
}

fn target_platform_from_ref(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty() {
        return None;
    }
    let mut dotted = value.split('.');
    let first = dotted.next()?;
    if matches!(first, "channel" | "service") {
        return dotted.next().map(str::to_string);
    }
    if let Some(rest) = value
        .strip_prefix("channel://")
        .or_else(|| value.strip_prefix("service://"))
    {
        return rest.split('/').next().map(str::to_string);
    }
    None
}

fn adapter_capability_for_action(
    platform: &channel_routes::PlatformReadiness,
    action: &CrossPlaneAction,
    bound_adapters: &HashSet<String>,
) -> Option<CrossPlaneAdapterCapability> {
    let operation = operation_from_capability(&action.requested_capability)?;
    adapter_capabilities_for_platform(platform, bound_adapters)
        .into_iter()
        .find(|capability| capability.operation == operation)
}

fn adapter_capabilities_for_platform(
    platform: &channel_routes::PlatformReadiness,
    bound_adapters: &HashSet<String>,
) -> Vec<CrossPlaneAdapterCapability> {
    let adapter_bound = platform_binding_keys(platform)
        .iter()
        .any(|key| bound_adapters.contains(key));
    platform_live_dispatch_operations(&platform.platform_type)
        .into_iter()
        .map(|operation| CrossPlaneAdapterCapability {
            platform: platform.platform_type.clone(),
            capability: format!("channel.{}.{}", platform.platform_type, operation),
            operation: operation.to_string(),
            live_supported: true,
            adapter_bound,
        })
        .collect()
}

async fn bound_adapter_snapshot(state: &AppState) -> HashSet<String> {
    state
        .services
        .surface
        .snapshot()
        .surfaces
        .into_iter()
        .map(|surface| surface.id)
        .collect()
}

fn platform_binding_keys(platform: &channel_routes::PlatformReadiness) -> Vec<String> {
    let mut keys = vec![
        platform.name.to_ascii_lowercase(),
        platform.platform_type.to_ascii_lowercase(),
    ];
    keys.extend(keys.clone().into_iter().map(|key| key.replace('-', "_")));
    keys.sort();
    keys.dedup();
    keys
}

fn build_dispatch_target(
    action: &CrossPlaneAction,
    target_platform: Option<&str>,
    adapter_capability: Option<&CrossPlaneAdapterCapability>,
) -> Option<CrossPlaneDispatchTarget> {
    let operation = adapter_capability
        .map(|capability| capability.operation.clone())
        .or_else(|| operation_from_capability(&action.requested_capability));
    CrossPlaneDispatchTarget::from_action(action, target_platform, operation.as_deref())
}

async fn dispatch_ready_target(
    state: &AppState,
    target: &CrossPlaneDispatchTarget,
) -> Result<CrossPlaneDispatchOutcome, (String, Option<CrossPlaneDispatchOutcome>)> {
    if !target.ready {
        return Err(("dispatch:target_not_ready".to_string(), None));
    }
    let platform = target
        .platform
        .as_deref()
        .ok_or_else(|| ("dispatch:target_platform_missing".to_string(), None))?;
    let outbound = target
        .outbound_message
        .as_ref()
        .ok_or_else(|| ("dispatch:outbound_message_missing".to_string(), None))?;
    if !state.services.surface.is_runtime_available() {
        return Err(("dispatch:surface_runtime_unavailable".to_string(), None));
    }
    let operation = target.operation.as_deref().unwrap_or("send_text");
    let payload_kind = match outbound.payload_kind.as_str() {
        "text" | "image" | "file" => outbound.payload_kind.as_str(),
        other => {
            return Err((
                format!("dispatch:payload_kind_unsupported:{other}"),
                Some(CrossPlaneDispatchOutcome::failed(
                    platform,
                    operation,
                    outbound.session_key.clone(),
                    format!("unsupported payload kind: {other}"),
                )),
            ));
        }
    };
    let payload_ref =
        resolve_dispatch_payload_ref(&state.workspace_root, payload_kind, &outbound.payload_ref)
            .map_err(|error| {
                (
                    format!("dispatch:payload_blocked:{error}"),
                    Some(CrossPlaneDispatchOutcome::failed(
                        platform,
                        operation,
                        outbound.session_key.clone(),
                        error,
                    )),
                )
            })?;

    match state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: platform.to_string(),
            recipient: outbound.session_key.clone(),
            thread: outbound.reply_to.clone(),
            text: outbound
                .caption
                .clone()
                .unwrap_or_else(|| payload_ref.clone()),
            metadata: serde_json::json!({
                "payload_kind": payload_kind,
                "payload_ref": payload_ref,
                "file_name": outbound.file_name,
                "operation": operation,
                "source": "cross_plane_dispatch",
                "metadata": outbound.metadata,
            }),
        })
        .await
    {
        Ok(result) if result.status == "sent" || result.status == "ok" => {
            Ok(CrossPlaneDispatchOutcome::sent(
                platform,
                operation,
                outbound.session_key.clone(),
                result.message_id,
            ))
        }
        Ok(result) => {
            let error = result.error.map_or_else(
                || "surface reported unsuccessful send".to_string(),
                |error| format!("{}: {}", error.code, error.message),
            );
            Err((
                format!("dispatch:send_failed:{error}"),
                Some(CrossPlaneDispatchOutcome::failed(
                    platform,
                    operation,
                    outbound.session_key.clone(),
                    error,
                )),
            ))
        }
        Err(err) => {
            let error = err.to_string();
            Err((
                format!("dispatch:send_failed:{error}"),
                Some(CrossPlaneDispatchOutcome::failed(
                    platform,
                    operation,
                    outbound.session_key.clone(),
                    error,
                )),
            ))
        }
    }
}

fn resolve_dispatch_payload_ref(
    workspace_root: &FsPath,
    kind: &str,
    payload_ref: &str,
) -> Result<String, String> {
    let payload_ref = payload_ref.trim();
    if payload_ref.is_empty() {
        return Err("payload_ref_missing".to_string());
    }
    if kind == "text" {
        return Ok(payload_ref.to_string());
    }
    if payload_ref.starts_with("http://") || payload_ref.starts_with("https://") {
        return match kind {
            "image" => Ok(payload_ref.to_string()),
            "file" => Err("file_remote_payload_unsupported".to_string()),
            "text" => Ok(payload_ref.to_string()),
            _ => Err("payload_kind_unsupported".to_string()),
        };
    }

    let relative = payload_ref
        .strip_prefix("workspace://file/")
        .or_else(|| payload_ref.strip_prefix("workspace://changed-file/"));
    let root = workspace_root
        .canonicalize()
        .map_err(|_| "workspace_root_unavailable".to_string())?;
    let candidate = match relative {
        Some(path) => {
            let rel = FsPath::new(path);
            if !path_has_safe_dispatch_components(rel) {
                return Err("workspace_payload_outside_root".to_string());
            }
            root.join(rel)
        }
        None => {
            let path = FsPath::new(payload_ref);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                if !path_has_safe_dispatch_components(path) {
                    return Err("workspace_payload_outside_root".to_string());
                }
                root.join(path)
            }
        }
    };
    let resolved = candidate
        .canonicalize()
        .map_err(|_| "workspace_payload_not_found".to_string())?;
    if !resolved.starts_with(&root) {
        return Err("workspace_payload_outside_root".to_string());
    }
    if !resolved.is_file() {
        return Err("workspace_payload_not_file".to_string());
    }
    resolved
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| "workspace_payload_path_not_utf8".to_string())
}

fn path_has_safe_dispatch_components(path: &FsPath) -> bool {
    !path.is_absolute()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

fn operation_from_capability(capability: &str) -> Option<String> {
    let lower = capability.trim().to_ascii_lowercase();
    for part in lower.split('.') {
        let operation = normalize_operation(part);
        if is_known_cross_plane_operation(operation) {
            return Some(operation.to_string());
        }
    }
    lower
        .rsplit('.')
        .next()
        .map(normalize_operation)
        .filter(|operation| !operation.is_empty())
        .map(str::to_string)
}

fn normalize_operation(operation: &str) -> &str {
    match operation.trim() {
        "send_file" | "send_document" => "send_file",
        other => other,
    }
}

fn platform_supports_live_operation(platform: &str, operation: &str) -> bool {
    platform_live_dispatch_operations(platform).contains(&operation)
}

fn platform_live_dispatch_operations(platform: &str) -> Vec<&'static str> {
    match platform {
        "feishu" => vec!["send_text", "send_image", "send_file"],
        "wechat-ilink" | "wechat_ilink" | "wechat" => vec!["send_text", "send_image"],
        "wecom" => vec!["send_text"],
        _ => Vec::new(),
    }
}

fn is_known_cross_plane_operation(operation: &str) -> bool {
    matches!(
        operation,
        "send_text" | "send_image" | "send_file" | "callback" | "qr_login"
    )
}
