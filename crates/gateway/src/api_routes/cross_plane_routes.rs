use std::{collections::HashSet, path::Path as FsPath, sync::Arc};

use axum::{
    extract::{Extension, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use runtime::{
    CrossPlaneAction, CrossPlaneDecisionEvidence, CrossPlaneDecisionKind, CrossPlaneDispatchTarget,
    CrossPlaneGrant, CrossPlaneIdentityBinding, CrossPlanePolicyDecision,
};
use serde::{Deserialize, Serialize};

use super::{message_connector_routes, principal_actor_id, AppState, AuthenticatedPrincipal};
use crate::services::{CrossPlaneExecutionRecord, GatewayCrossPlaneExecutor};

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
            "/api/cross-plane/action/executions/:id",
            get(cross_plane_action_execution_get_handler),
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
#[serde(deny_unknown_fields)]
struct CrossPlaneActionExecuteRequest {
    action: CrossPlaneActionIntent,
    #[serde(default = "default_execute_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
}

/// Untrusted action description supplied by a caller.  The authenticated
/// Gateway principal is deliberately excluded and injected at the boundary.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CrossPlaneActionIntent {
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    requested_capability: String,
    #[serde(default)]
    provider_account: Option<String>,
    #[serde(default)]
    target_ref: Option<String>,
    #[serde(default)]
    resource_ref: Option<String>,
    #[serde(default = "default_cross_plane_risk")]
    risk: harness_contract::policy::CrossPlaneRisk,
    #[serde(default = "default_data_classification")]
    data_classification: harness_contract::policy::DataClassification,
    #[serde(default = "default_identity_trust")]
    identity_trust: runtime::IdentityTrust,
}

impl CrossPlaneActionIntent {
    fn into_action(self, principal: &AuthenticatedPrincipal) -> CrossPlaneAction {
        let mut action =
            CrossPlaneAction::new(principal_actor_id(principal), self.requested_capability);
        action.actor_identity_ref = self.actor_identity_ref;
        action.source_channel = self.source_channel;
        action.session_id = self.session_id;
        action.provider_account = self.provider_account;
        action.target_ref = self.target_ref;
        action.resource_ref = self.resource_ref;
        action.risk = self.risk;
        action.data_classification = self.data_classification;
        action.identity_trust = self.identity_trust;
        action
    }
}

#[derive(Debug, Serialize)]
struct CrossPlaneActionReadiness {
    action: CrossPlaneAction,
    decision: CrossPlanePolicyDecision,
    target_platform: Option<String>,
    platform_readiness: Option<message_connector_routes::PlatformReadiness>,
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

fn default_cross_plane_risk() -> harness_contract::policy::CrossPlaneRisk {
    harness_contract::policy::CrossPlaneRisk::Low
}

fn default_data_classification() -> harness_contract::policy::DataClassification {
    harness_contract::policy::DataClassification::Internal
}

fn default_identity_trust() -> runtime::IdentityTrust {
    runtime::IdentityTrust::Unknown
}

async fn cross_plane_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    match state
        .services
        .cross_plane
        .control()
        .summary(chrono::Utc::now())
    {
        Ok(summary) => Json(serde_json::json!({
            "kind": "cross_plane_summary",
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
        .into_response(),
        Err(error) => cross_plane_commit_error("summary", error),
    }
}

async fn cross_plane_grants_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let grants = state.services.cross_plane.control().list_grants();
    Json(serde_json::json!({
        "kind": "cross_plane_grants",
        "grants": grants
    }))
}

async fn cross_plane_identities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
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
    match state
        .services
        .cross_plane
        .control()
        .upsert_identity(binding)
    {
        Ok(binding) => Json(serde_json::json!({
            "kind": "cross_plane_identity",
            "identity": binding
        }))
        .into_response(),
        Err(error) => cross_plane_commit_error("upsert_identity", error),
    }
}

async fn cross_plane_revoke_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.services.cross_plane.control().revoke_identity(&id) {
        Ok(revoked) => Json(serde_json::json!({
            "kind": "cross_plane_identity_revoked",
            "id": id,
            "revoked": revoked
        }))
        .into_response(),
        Err(error) => cross_plane_commit_error("revoke_identity", error),
    }
}

async fn cross_plane_create_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(grant): Json<CrossPlaneGrant>,
) -> impl IntoResponse {
    match state.services.cross_plane.control().upsert_grant(grant) {
        Ok(grant) => Json(serde_json::json!({
            "kind": "cross_plane_grant",
            "grant": grant
        }))
        .into_response(),
        Err(error) => cross_plane_commit_error("upsert_grant", error),
    }
}

async fn cross_plane_revoke_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.services.cross_plane.control().revoke_grant(&id) {
        Ok(revoked) => Json(serde_json::json!({
            "kind": "cross_plane_grant_revoked",
            "id": id,
            "revoked": revoked
        }))
        .into_response(),
        Err(error) => cross_plane_commit_error("revoke_grant", error),
    }
}

fn cross_plane_commit_error(
    operation: &str,
    error: impl std::fmt::Display,
) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "kind": "cross_plane_commit_error",
            "operation": operation,
            "error": error.to_string(),
        })),
    )
        .into_response()
}

async fn cross_plane_audit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    match state.services.cross_plane.control().list_audit(100, 0) {
        Ok((records, total)) => Json(serde_json::json!({
            "kind": "cross_plane_audit",
            "records": records,
            "total": total
        }))
        .into_response(),
        Err(error) => cross_plane_commit_error("audit", error),
    }
}

async fn cross_plane_action_adapters_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let platforms = message_connector_routes::configured_platforms(config.as_ref());
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
    let executions = state.services.cross_plane.control().list_executions(100, 0);
    let total = executions.len();
    Json(serde_json::json!({
        "kind": "cross_plane_action_executions",
        "executions": executions,
        "total": total,
    }))
}

async fn cross_plane_action_execution_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .services
        .cross_plane
        .find_execution(&id)
        .map(|receipt| {
            Json(serde_json::json!({
                "kind": "cross_plane_action_execution",
                "execution_receipt": receipt,
            }))
        })
        .ok_or(StatusCode::NOT_FOUND)
}

async fn cross_plane_policy_simulate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(intent): Json<CrossPlaneActionIntent>,
) -> impl IntoResponse {
    let (action, decision, evidence) = decide_connector_action(
        &state,
        intent.into_action(&principal),
        "dry_run",
        chrono::Utc::now(),
    );
    Json(serde_json::json!({
        "kind": "cross_plane_policy_simulation",
        "action": action,
        "decision": decision,
        "evidence": evidence,
    }))
}

async fn cross_plane_action_preflight_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(intent): Json<CrossPlaneActionIntent>,
) -> impl IntoResponse {
    let readiness = evaluate_action_readiness(
        &state,
        intent.into_action(&principal),
        "dry_run",
        chrono::Utc::now(),
    )
    .await;
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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CrossPlaneActionExecuteRequest>,
) -> axum::response::Response {
    let now = chrono::Utc::now();
    let mode = normalize_execute_mode(&request.mode);
    let readiness =
        evaluate_action_readiness(&state, request.action.into_action(&principal), &mode, now).await;
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
            if receipt.mode != mode
                || receipt.action != readiness.action
                || receipt.dispatch_target != readiness.dispatch_target
            {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "kind": "cross_plane_action_execution_failed",
                        "error": "idempotency key is bound to another canonical cross-plane action",
                    })),
                )
                    .into_response();
            }
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
            }))
            .into_response();
        }
    }
    let evidence = readiness.evidence.clone();
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
    let mut execution_graph = None;

    if mode == "dry_run" {
        if readiness.decision.decision == CrossPlaneDecisionKind::Allow {
            status = "planned";
            dispatch_status = "dry_run";
            audit_result = "dry_run";
            audit_summary = "dry_run_execution_plan".to_string();
        }
    } else if readiness.executable {
        let graph_key = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("cross-plane-{}", uuid::Uuid::new_v4()));
        let target = readiness.dispatch_target.clone().unwrap_or_default();
        let executor = Arc::new(GatewayCrossPlaneExecutor::new(
            state.services.surface.clone(),
            target.clone(),
            state.services.cross_plane.runtime_control(),
        ));
        match state
            .services
            .cross_plane
            .execute_commit_graph(
                &readiness.action,
                &readiness.decision,
                &graph_key,
                Some(&target),
                executor,
            )
            .await
        {
            Ok(graph) => {
                dispatch_outcome = graph
                    .nodes
                    .iter()
                    .find(|node| {
                        node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                    })
                    .and_then(|node| node.result_ref.as_deref())
                    .and_then(|value| serde_json::from_str(value).ok());
                dispatched = dispatch_outcome.as_ref().is_some_and(
                    |value: &runtime::CrossPlaneDispatchOutcome| value.status == "sent",
                );
                status = if dispatched { "dispatched" } else { "blocked" };
                dispatch_status = if dispatched {
                    "sent"
                } else {
                    "dispatch_failed"
                };
                audit_result = if dispatched {
                    "dispatched"
                } else {
                    "blocked_dispatch"
                };
                audit_summary = if dispatched {
                    "execution_graph_dispatch_sent"
                } else {
                    "execution_graph_dispatch_failed"
                }
                .to_string();
                execution_graph = Some(graph);
            }
            Err(error) => {
                let status = if error.is_idempotency_conflict() {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                let code = if error.is_idempotency_conflict() {
                    "cross_plane_idempotency_conflict"
                } else {
                    "cross_plane_execution_graph_failed"
                };
                return (
                    status,
                    Json(serde_json::json!({
                        "kind": "cross_plane_execution_error",
                        "code": code,
                        "error": error.to_string(),
                        "idempotency_key": graph_key,
                    })),
                )
                    .into_response();
            }
        }
    } else if readiness
        .blockers
        .iter()
        .any(|blocker| blocker.starts_with("dispatch:payload_blocked:"))
    {
        dispatch_status = "payload_rejected";
        audit_result = "blocked_payload_rejected";
        audit_summary = "commit_payload_failed_workspace_validation".to_string();
    } else if readiness
        .adapter_capability
        .as_ref()
        .is_some_and(|capability| capability.live_supported && !capability.adapter_bound)
    {
        dispatch_status = "adapter_unavailable";
        audit_result = "blocked_adapter_unavailable";
        audit_summary = "commit_requires_bound_surface_adapter".to_string();
    }

    let record = CrossPlaneExecutionRecord {
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
        execution_graph_id: execution_graph.as_ref().map(|graph| graph.graph_id.clone()),
    };
    let (audit_record_id, receipt) = match if mode == "commit" && dispatched {
        state
            .services
            .cross_plane
            .record_completed_effect_execution(record)
    } else {
        state.services.cross_plane.record_action_execution(record)
    } {
        Ok(committed) => committed,
        Err(error) => {
            let (status, code) = match &error {
                runtime::CrossPlaneRuntimeError::IdempotencyConflict(_) => {
                    (StatusCode::CONFLICT, "cross_plane_idempotency_conflict")
                }
                runtime::CrossPlaneRuntimeError::GrantUnavailable(_) => {
                    (StatusCode::CONFLICT, "cross_plane_grant_unavailable")
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "cross_plane_action_execution_failed",
                ),
            };
            return (
                status,
                Json(serde_json::json!({
                    "kind": "cross_plane_action_execution_failed",
                    "code": code,
                    "error": error.to_string(),
                })),
            )
                .into_response();
        }
    };

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
        "execution_graph": execution_graph,
        "idempotent_replay": false,
    }))
    .into_response()
}

async fn cross_plane_identity_resolve_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<CrossPlaneIdentityResolveRequest>,
) -> impl IntoResponse {
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
    let config = state.runtime_config_json_snapshot();
    let platforms = message_connector_routes::configured_platforms(config.as_ref());
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
    let mut blockers = dispatch_target
        .as_ref()
        .map(|target| target.blockers.clone())
        .unwrap_or_default();
    if decision.decision != CrossPlaneDecisionKind::Allow {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if let Some(readiness) = &platform_readiness {
        if !matches!(readiness.status, "ready" | "configured") {
            blockers.push(format!("platform:{}:{}", readiness.name, readiness.status));
        }
    } else if let Some(target) = &target_platform {
        blockers.push(format!("platform:{target}:unconfigured"));
    }
    if mode == "commit" && dispatch_target.is_some() {
        match &adapter_capability {
            Some(capability) if capability.live_supported && capability.adapter_bound => {}
            Some(capability) if !capability.live_supported => {
                blockers.push(format!(
                    "adapter:{}:{}:operation_unsupported",
                    capability.platform, capability.operation
                ));
            }
            Some(capability) => {
                blockers.push(format!(
                    "adapter:{}:{}:not_bound",
                    capability.platform, capability.operation
                ));
            }
            None if target_platform.is_some() => {
                blockers.push("adapter:operation_unavailable".to_string());
            }
            None => {}
        }
    }
    if mode == "commit" {
        if let Some(outbound) = dispatch_target
            .as_ref()
            .and_then(|target| target.outbound_message.as_ref())
        {
            if let Err(error) = resolve_dispatch_payload_ref(
                &state.workspace_root,
                &outbound.payload_kind,
                &outbound.payload_ref,
            ) {
                blockers.push(format!("dispatch:payload_blocked:{error}"));
            }
        }
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
    if matches!(first, "message" | "service") {
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
    platform: &message_connector_routes::PlatformReadiness,
    action: &CrossPlaneAction,
    bound_adapters: &HashSet<String>,
) -> Option<CrossPlaneAdapterCapability> {
    let operation = operation_from_capability(&action.requested_capability)?;
    adapter_capabilities_for_platform(platform, bound_adapters)
        .into_iter()
        .find(|capability| capability.operation == operation)
}

fn adapter_capabilities_for_platform(
    platform: &message_connector_routes::PlatformReadiness,
    bound_adapters: &HashSet<String>,
) -> Vec<CrossPlaneAdapterCapability> {
    let adapter_bound = platform_binding_keys(platform)
        .iter()
        .any(|key| bound_adapters.contains(key));
    platform_live_dispatch_operations(&platform.platform_type)
        .into_iter()
        .map(|operation| CrossPlaneAdapterCapability {
            platform: platform.platform_type.clone(),
            capability: format!("message.{}.{}", platform.platform_type, operation),
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

fn platform_binding_keys(platform: &message_connector_routes::PlatformReadiness) -> Vec<String> {
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
    // A connector service action may be cross-plane without being an
    // outbound user message. Only Surface message capabilities own a target,
    // media payload validation, and sidecar delivery requirement.
    if !action
        .requested_capability
        .trim()
        .to_ascii_lowercase()
        .starts_with("message.")
    {
        return None;
    }
    let operation = adapter_capability
        .map(|capability| capability.operation.clone())
        .or_else(|| operation_from_capability(&action.requested_capability));
    CrossPlaneDispatchTarget::from_action(action, target_platform, operation.as_deref())
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

#[cfg(test)]
mod tests {
    use super::cross_plane_commit_error;
    use axum::{body::to_bytes, http::StatusCode};

    #[tokio::test]
    async fn commit_failures_return_a_structured_internal_error() {
        let response = cross_plane_commit_error("upsert_identity", "durable store unavailable");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("cross-plane error response body must remain readable");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("cross-plane error response must be JSON");
        assert_eq!(payload["kind"], "cross_plane_commit_error");
        assert_eq!(payload["operation"], "upsert_identity");
        assert_eq!(payload["error"], "durable store unavailable");
    }
}
