use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use axum::{
    extract::{Path, State as AxumState},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use runtime::{
    CrossPlaneAction, CrossPlaneAuditRecord, CrossPlaneControlPlane, CrossPlaneExecutionReceipt,
    CrossPlaneGrant, CrossPlaneIdentityBinding, CrossPlanePolicyDecision, PolicyDecisionKind,
};
use serde::{Deserialize, Serialize};

use super::{channel_routes, AppState};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CrossPlaneDispatchTarget {
    platform: Option<String>,
    operation: Option<String>,
    target_ref: Option<String>,
    resource_ref: Option<String>,
    session_key: Option<String>,
    outbound_message: Option<CrossPlaneOutboundMessagePlan>,
    ready: bool,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CrossPlaneOutboundMessagePlan {
    session_key: String,
    text: String,
    reply_to: Option<String>,
    metadata: serde_json::Value,
}

fn default_execute_mode() -> String {
    "dry_run".to_string()
}

static CROSS_PLANE_CONTROL: OnceLock<CrossPlaneControlPlane> = OnceLock::new();

fn cross_plane_control() -> &'static CrossPlaneControlPlane {
    CROSS_PLANE_CONTROL.get_or_init(CrossPlaneControlPlane::new)
}

fn cross_plane_state_path(state: &AppState) -> PathBuf {
    state
        .config_home
        .join("cross-plane")
        .join("control-state.json")
}

fn ensure_cross_plane_loaded(state: &AppState) {
    static CROSS_PLANE_LOADED: OnceLock<()> = OnceLock::new();
    let _ = CROSS_PLANE_LOADED.get_or_init(|| {
        let _ = cross_plane_control().load_from_path(&cross_plane_state_path(state));
    });
}

fn save_cross_plane_state(state: &AppState) {
    let _ = cross_plane_control().save_to_path(&cross_plane_state_path(state));
}

async fn cross_plane_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let summary = cross_plane_control().summary(chrono::Utc::now());
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
    ensure_cross_plane_loaded(&state);
    let grants = cross_plane_control().list_grants();
    Json(serde_json::json!({
        "kind": "cross_plane_grants",
        "grants": grants
    }))
}

async fn cross_plane_identities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let identities = cross_plane_control().list_identities();
    Json(serde_json::json!({
        "kind": "cross_plane_identities",
        "identities": identities
    }))
}

async fn cross_plane_create_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(binding): Json<CrossPlaneIdentityBinding>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let binding = cross_plane_control().upsert_identity(binding);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_identity",
        "identity": binding
    }))
}

async fn cross_plane_revoke_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let revoked = cross_plane_control().revoke_identity(&id);
    save_cross_plane_state(&state);
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
    ensure_cross_plane_loaded(&state);
    let grant = cross_plane_control().upsert_grant(grant);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_grant",
        "grant": grant
    }))
}

async fn cross_plane_revoke_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let revoked = cross_plane_control().revoke_grant(&id);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_grant_revoked",
        "id": id,
        "revoked": revoked
    }))
}

async fn cross_plane_audit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let records = cross_plane_control().list_audit(100, 0);
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
    ensure_cross_plane_loaded(&state);
    let executions = cross_plane_control().list_executions(100, 0);
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
    ensure_cross_plane_loaded(&state);
    let (action, decision) =
        cross_plane_control().decide_and_audit_with_action(action, chrono::Utc::now());
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_policy_simulation",
        "action": action,
        "decision": decision,
    }))
}

async fn cross_plane_action_preflight_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(action): Json<CrossPlaneAction>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let readiness = evaluate_action_readiness(&state, action, chrono::Utc::now()).await;
    Json(serde_json::json!({
        "kind": "cross_plane_action_preflight",
        "action": readiness.action,
        "decision": readiness.decision,
        "target_platform": readiness.target_platform,
        "platform_readiness": readiness.platform_readiness,
        "adapter_capability": readiness.adapter_capability,
        "dispatch_target": readiness.dispatch_target,
        "executable": readiness.executable,
        "blockers": readiness.blockers,
    }))
}

async fn cross_plane_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<CrossPlaneActionExecuteRequest>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let now = chrono::Utc::now();
    let mode = normalize_execute_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) = cross_plane_control().find_execution_by_idempotency_key(key) {
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
    let mut readiness = evaluate_action_readiness(&state, request.action, now).await;
    let mut status = "blocked";
    let mut dispatch_status = "not_started";
    let mut audit_result = "blocked";
    let mut audit_summary = readiness
        .blockers
        .first()
        .cloned()
        .unwrap_or_else(|| readiness.decision.reason.clone());

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
                readiness
                    .blockers
                    .push("dispatch:not_implemented".to_string());
                dispatch_status = "dispatch_not_implemented";
                audit_summary = "live_dispatch_not_implemented".to_string();
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
            readiness.executable = false;
            audit_result = "blocked_dispatch";
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

    let audit_record = CrossPlaneAuditRecord::new(
        readiness.action.clone(),
        readiness.decision.clone(),
        audit_result,
        audit_summary,
    );
    let audit_record_id = audit_record.id.clone();
    cross_plane_control().record_audit(audit_record);
    let receipt = CrossPlaneExecutionReceipt::new(
        idempotency_key.clone(),
        mode.clone(),
        status,
        dispatch_status,
        readiness.action.clone(),
        readiness.decision.clone(),
        readiness.blockers.clone(),
        Some(audit_record_id.clone()),
    );
    cross_plane_control().record_execution(receipt.clone());
    save_cross_plane_state(&state);

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
        "executable": readiness.executable,
        "blockers": readiness.blockers,
        "dispatched": false,
        "audit_record_id": audit_record_id,
        "execution_receipt": receipt,
        "idempotent_replay": false,
    }))
}

async fn cross_plane_identity_resolve_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<CrossPlaneIdentityResolveRequest>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let resolved =
        cross_plane_control().resolve_identity(&request.identity_ref, chrono::Utc::now());
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
    now: chrono::DateTime<chrono::Utc>,
) -> CrossPlaneActionReadiness {
    let (action, decision) = cross_plane_control().decide_with_action(action, now);
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
        executable,
        blockers,
    }
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
    platform
        .capabilities
        .iter()
        .map(|capability| {
            let operation = normalize_operation(capability);
            CrossPlaneAdapterCapability {
                platform: platform.platform_type.clone(),
                capability: format!("channel.{}.{}", platform.platform_type, operation),
                operation: operation.to_string(),
                live_supported: platform_supports_live_operation(
                    &platform.platform_type,
                    operation,
                ),
                adapter_bound,
            }
        })
        .collect()
}

async fn bound_adapter_snapshot(state: &AppState) -> HashSet<String> {
    let Some(runtime) = &state.platform_runtime else {
        return HashSet::new();
    };
    runtime.list_bound_adapters().await.into_iter().collect()
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
    if target_platform.is_none() && operation.is_none() {
        return None;
    }

    let platform = target_platform.map(str::to_string);
    let mut blockers = Vec::new();
    if platform.is_none() {
        blockers.push("dispatch:target_platform_missing".to_string());
    }
    let session_key = match (platform.as_deref(), action.target_ref.as_deref()) {
        (_, None) => {
            blockers.push("dispatch:target_ref_missing".to_string());
            None
        }
        (None, Some(_)) => None,
        (Some(platform), Some(target_ref)) => {
            match session_key_from_target_ref(platform, target_ref) {
                Some(session_key) => Some(session_key),
                None => {
                    blockers.push("dispatch:target_ref_invalid".to_string());
                    None
                }
            }
        }
    };

    let text = operation
        .as_deref()
        .and_then(|operation| outbound_text_for_operation(operation, action, &mut blockers));
    let outbound_message =
        session_key
            .as_ref()
            .zip(text)
            .map(|(session_key, text)| CrossPlaneOutboundMessagePlan {
                session_key: session_key.clone(),
                text,
                reply_to: None,
                metadata: serde_json::json!({
                    "cross_plane": true,
                    "operation": operation,
                    "requested_capability": action.requested_capability,
                    "resource_ref": action.resource_ref,
                    "source_channel": action.source_channel,
                    "session_id": action.session_id,
                }),
            });
    let ready = blockers.is_empty() && outbound_message.is_some();

    Some(CrossPlaneDispatchTarget {
        platform,
        operation,
        target_ref: action.target_ref.clone(),
        resource_ref: action.resource_ref.clone(),
        session_key,
        outbound_message,
        ready,
        blockers,
    })
}

fn session_key_from_target_ref(platform: &str, target_ref: &str) -> Option<String> {
    let value = target_ref.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(rest) = value
        .strip_prefix("channel://")
        .or_else(|| value.strip_prefix("service://"))
    {
        let mut parts = rest.split('/').filter(|part| !part.is_empty());
        let target_platform = parts.next()?.to_ascii_lowercase();
        if target_platform != platform.to_ascii_lowercase() {
            return None;
        }
        let remaining = parts.collect::<Vec<_>>();
        return session_key_from_path_parts(platform, &remaining);
    }

    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case(platform) {
        let user_id = parts[1].trim();
        if user_id.is_empty() {
            return None;
        }
        return if parts.get(2).is_some_and(|thread| !thread.trim().is_empty()) {
            Some(format!("{platform}:{user_id}:{}", parts[2].trim()))
        } else {
            Some(format!("{platform}:{user_id}"))
        };
    }

    None
}

fn session_key_from_path_parts(platform: &str, parts: &[&str]) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let user_id = if matches!(parts[0], "user" | "chat" | "session") {
        parts.get(1).copied()
    } else {
        parts.first().copied()
    }?
    .trim();
    if user_id.is_empty() {
        return None;
    }
    let thread_id = parts
        .windows(2)
        .find(|window| matches!(window[0], "thread" | "topic"))
        .map(|window| window[1].trim())
        .filter(|thread| !thread.is_empty());
    Some(match thread_id {
        Some(thread_id) => format!("{platform}:{user_id}:{thread_id}"),
        None => format!("{platform}:{user_id}"),
    })
}

fn outbound_text_for_operation(
    operation: &str,
    action: &CrossPlaneAction,
    blockers: &mut Vec<String>,
) -> Option<String> {
    match operation {
        "send_text" => {
            let text = action
                .resource_ref
                .as_deref()
                .and_then(text_payload_from_resource_ref);
            if text.as_deref().is_none_or(str::is_empty) {
                blockers.push("dispatch:payload_text_missing".to_string());
            }
            text
        }
        "send_image" | "send_file" => {
            let Some(resource_ref) = action.resource_ref.as_deref().map(str::trim) else {
                blockers.push("dispatch:resource_ref_missing".to_string());
                return None;
            };
            if resource_ref.is_empty() {
                blockers.push("dispatch:resource_ref_missing".to_string());
                None
            } else {
                Some(resource_ref.to_string())
            }
        }
        _ => {
            blockers.push("dispatch:operation_not_dispatchable".to_string());
            None
        }
    }
}

fn text_payload_from_resource_ref(resource_ref: &str) -> Option<String> {
    let value = resource_ref.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(text) = value
        .strip_prefix("text://")
        .or_else(|| value.strip_prefix("text:"))
    {
        return Some(text.to_string());
    }
    if value.contains("://") {
        return None;
    }
    Some(value.to_string())
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
        "doc_ops" | "docx" | "docs" => "doc_ops",
        other => other,
    }
}

fn platform_supports_live_operation(platform: &str, operation: &str) -> bool {
    matches!(
        (platform, operation),
        ("feishu", "send_text")
            | ("feishu", "send_image")
            | ("feishu", "send_file")
            | ("wechat-ilink", "send_text")
            | ("wechat_ilink", "send_text")
            | ("wechat", "send_text")
            | ("wechat-ilink", "send_image")
            | ("wechat_ilink", "send_image")
            | ("wechat", "send_image")
            | ("wecom", "send_text")
    )
}

fn is_known_cross_plane_operation(operation: &str) -> bool {
    matches!(
        operation,
        "send_text" | "send_image" | "send_file" | "doc_ops" | "callback" | "qr_login"
    )
}
