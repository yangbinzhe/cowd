//! Concrete Cowd effect ports exposed to compile-time linked applications.
//!
//! Applications receive only the stable SDK traits.  This module is the
//! product-side adapter that owns Gateway state, policy and dispatch details;
//! no application can import or downcast it.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cowd_app_sdk::{
    AppHostError, AppHostPorts, ApprovalPort, ConnectorPort, CowdAppContext, CrossPlanePort,
    HostIntent, HostReceipt, InvocationContext, RealityPort, RuntimePort, WorkContextPort,
};
use harness_contract::{
    core::TaskRisk,
    policy::{CrossPlaneRisk, DataClassification},
};
use runtime::{
    ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, CrossPlaneAction,
    CrossPlaneDispatchTarget, IdentityTrust, PolicyDecisionKind, SubmitGlobalApprovalRequest,
    VerifiedPrincipal,
};
use serde::Deserialize;

use crate::api_routes::{connector_routes::connector_snapshot, AppState};

use super::GatewayCrossPlaneExecutor;

/// Closed intent name for a policy-gated, durable cross-plane message
/// dispatch.  The payload deliberately uses only transport data, never an
/// application-domain DTO or a Gateway service type.
pub(crate) const CROSS_PLANE_DISPATCH_INTENT_V1: &str = "cowd.cross_plane.dispatch.v1";
/// Closed approval-submit intent used by a compile-time linked APP that owns
/// a typed business review while Cowd owns the durable approval record.
pub(crate) const APPROVAL_SUBMIT_INTENT_V1: &str = "cowd.approval.submit.v1";
/// Closed approval-decision intent.  The Gateway resolves the request's
/// original verified principal before issuing and consuming the lease; an APP
/// cannot select an actor by putting one in this payload.
pub(crate) const APPROVAL_DECIDE_INTENT_V1: &str = "cowd.approval.decide.v1";
/// Closed generic Matrix operation used by an APP that owns the user-facing
/// projection but not the Reality Core storage authority.
pub(crate) const REALITY_RECOMPUTE_METRICS_INTENT_V1: &str = "cowd.reality.recompute_metrics.v1";

const APP_REQUEST_PRINCIPAL_TTL: Duration = Duration::from_secs(300);
const APP_REQUEST_PRINCIPAL_LIMIT: usize = 4096;

#[derive(Clone)]
struct BoundAppPrincipal {
    principal: VerifiedPrincipal,
    workspace_id: String,
    surface: String,
    bound_at: Instant,
}

/// One binding is created during product composition and attached once the
/// fully assembled [`AppState`] exists.  It solves startup ordering without
/// making applications aware of Gateway internals or giving them a mutable
/// service handle.
#[derive(Clone, Default)]
pub(crate) struct GatewayAppHostBinding {
    state: Arc<RwLock<Weak<AppState>>>,
    request_principals: Arc<RwLock<BTreeMap<String, BoundAppPrincipal>>>,
}

impl GatewayAppHostBinding {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub(crate) fn context(&self) -> CowdAppContext {
        CowdAppContext::new(Arc::new(self.clone()))
    }

    /// Bind exactly the live product state after construction and before the
    /// router starts accepting traffic. Rebinding is intentionally supported
    /// for isolated test routers that replace their immutable APP registry.
    pub(crate) fn bind(&self, state: &Arc<AppState>) {
        let mut slot = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Arc::downgrade(state);
    }

    /// Bind verified request identity immediately after Gateway authentication
    /// and before handing the stable request context to an APP.  Effect ports
    /// resolve this opaque binding instead of trusting an APP-supplied actor.
    pub(crate) fn bind_request_principal(
        &self,
        principal: &VerifiedPrincipal,
        context: &InvocationContext,
    ) {
        let now = Instant::now();
        let mut principals = self
            .request_principals
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        principals
            .retain(|_, bound| now.duration_since(bound.bound_at) <= APP_REQUEST_PRINCIPAL_TTL);
        if principals.len() >= APP_REQUEST_PRINCIPAL_LIMIT {
            let overflow = principals.len() + 1 - APP_REQUEST_PRINCIPAL_LIMIT;
            let stale = principals
                .iter()
                .take(overflow)
                .map(|(request_id, _)| request_id.clone())
                .collect::<Vec<_>>();
            for request_id in stale {
                principals.remove(&request_id);
            }
        }
        principals.insert(
            context.request_id.clone(),
            BoundAppPrincipal {
                principal: principal.clone(),
                workspace_id: context.workspace_id.clone(),
                surface: context.surface.clone(),
                bound_at: now,
            },
        );
    }

    fn state(&self) -> Result<Arc<AppState>, AppHostError> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade()
            .ok_or_else(|| {
                AppHostError::Unavailable("Gateway application host is not active".into())
            })
    }

    fn verified_principal(
        &self,
        context: &InvocationContext,
    ) -> Result<VerifiedPrincipal, AppHostError> {
        let now = Instant::now();
        let mut principals = self
            .request_principals
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        principals
            .retain(|_, bound| now.duration_since(bound.bound_at) <= APP_REQUEST_PRINCIPAL_TTL);
        let bound = principals.get(&context.request_id).ok_or_else(|| {
            AppHostError::Denied(
                "application effect request is not bound to a verified Gateway request".into(),
            )
        })?;
        if bound.workspace_id != context.workspace_id
            || bound.surface != context.surface
            || bound.principal.claims().principal_id != context.principal_id
        {
            return Err(AppHostError::Denied(
                "application effect invocation does not match its verified Gateway request".into(),
            ));
        }
        Ok(bound.principal.clone())
    }

    fn unsupported(port: &str, kind: &str) -> AppHostError {
        AppHostError::Unavailable(format!(
            "Gateway {port} port does not accept intent kind {kind} in this product revision"
        ))
    }
}

impl AppHostPorts for GatewayAppHostBinding {
    fn runtime(&self) -> &dyn RuntimePort {
        self
    }

    fn approval(&self) -> &dyn ApprovalPort {
        self
    }

    fn cross_plane(&self) -> &dyn CrossPlanePort {
        self
    }

    fn connector(&self) -> &dyn ConnectorPort {
        self
    }

    fn reality(&self) -> &dyn RealityPort {
        self
    }

    fn work_context(&self) -> &dyn WorkContextPort {
        self
    }
}

#[async_trait]
impl RuntimePort for GatewayAppHostBinding {
    async fn execute(
        &self,
        _context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        Err(Self::unsupported("runtime", &intent.kind))
    }
}

#[async_trait]
impl ApprovalPort for GatewayAppHostBinding {
    async fn request(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        let principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            APPROVAL_SUBMIT_INTENT_V1 => {
                let request = ApplicationApprovalSubmitIntentV1::parse(intent)?;
                let runtime = state
                    .services
                    .runtime
                    .as_ref()
                    .ok_or_else(|| {
                        AppHostError::Unavailable(
                            "Gateway runtime approval service is not configured".into(),
                        )
                    })?
                    .runtime_services();
                let replayed = runtime.approval_queue().get(&request.approval_id).is_some();
                let approval = runtime
                    .approval_queue()
                    .submit_scoped(
                        request.approval_id.clone(),
                        SubmitGlobalApprovalRequest {
                            source: ApprovalSource {
                                kind: ApprovalSourceKind::Mfg,
                                session_id: None,
                                agent_id: None,
                                team_id: None,
                                mission_id: None,
                                resource_ref: Some(request.resource_ref),
                                review_ref: Some(request.review_ref),
                            },
                            action: request.action,
                            summary: request.summary,
                            risk: request.risk,
                            evidence_refs: request.evidence_refs,
                            timeout_policy: request.timeout_policy,
                        },
                    )
                    .map_err(AppHostError::Failed)?;
                Ok(HostReceipt {
                    id: approval.approval_id.clone(),
                    status: approval.status.as_str().to_string(),
                    replayed,
                    payload: serde_json::json!({
                        "kind": "cowd.approval.submit.receipt.v1",
                        "approval": approval,
                    }),
                })
            }
            APPROVAL_DECIDE_INTENT_V1 => {
                let request = ApplicationApprovalDecisionIntentV1::parse(intent)?;
                if !principal.is_human_interactive()
                    || !principal.has_capability("approval.respond")
                    || !principal.has_capability("mfg.report.review")
                {
                    return Err(AppHostError::Denied(
                        "application approval decisions require a human-interactive mfg.report.review principal with approval.respond"
                            .into(),
                    ));
                }
                let credential = state
                    .auth_token
                    .as_deref()
                    .unwrap_or("test-only-credential");
                let (lease, public_key) = crate::api_routes::issue_human_decision_lease(
                    &state.config_home,
                    credential,
                    request.review_ref.clone(),
                    request.action.clone(),
                    request.scope.clone(),
                    request.evidence_digest.clone(),
                    current_time_ms().saturating_add(60_000),
                )
                .map_err(|_| {
                    AppHostError::Unavailable("decision authority is unavailable".into())
                })?;
                let verified = runtime::PrincipalVerifier::from_base64(&lease.key_id, &public_key)
                    .map_err(|_| AppHostError::Denied("decision lease verification failed".into()))?
                    .requiring_credential_epoch(principal.credential_epoch())
                    .verify_decision_lease(
                        &lease,
                        &principal,
                        &runtime::DecisionLeaseExpectation::new(
                            request.review_ref.clone(),
                            request.action.clone(),
                            request.scope.clone(),
                            request.evidence_digest.clone(),
                        ),
                    )
                    .map_err(|error| {
                        AppHostError::Denied(format!("decision lease verification failed: {error}"))
                    })?;
                let runtime = state
                    .services
                    .runtime
                    .as_ref()
                    .ok_or_else(|| {
                        AppHostError::Unavailable(
                            "Gateway runtime approval service is not configured".into(),
                        )
                    })?
                    .runtime_services();
                runtime
                    .consume_verified_decision_lease(verified)
                    .map_err(|error| {
                        if error.contains("already been consumed") {
                            AppHostError::Denied("decision lease was already consumed".into())
                        } else {
                            AppHostError::Unavailable(
                                "runtime decision lease store is unavailable".into(),
                            )
                        }
                    })?;
                let actor = format!("principal:{}", principal.claims().principal_id);
                let receipt = runtime
                    .approval_queue()
                    .record_mfg_decision_fact(
                        &request.approval_id,
                        &request.review_ref,
                        &actor,
                        request.approved,
                        &request.decision,
                        &request.reason,
                        &lease.claims.lease_id,
                    )
                    .map_err(AppHostError::Denied)?;
                Ok(HostReceipt {
                    id: receipt.approval_id.clone(),
                    status: receipt.status.as_str().to_string(),
                    replayed: receipt.message.starts_with("approval already "),
                    payload: serde_json::json!({
                        "kind": "cowd.approval.decision.receipt.v1",
                        "approval": receipt,
                        "decision_lease_ref": lease.claims.lease_id,
                    }),
                })
            }
            _ => Err(Self::unsupported("approval", &intent.kind)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationApprovalSubmitIntentV1 {
    approval_id: String,
    resource_ref: String,
    review_ref: String,
    action: String,
    summary: String,
    risk: TaskRisk,
    evidence_refs: Vec<String>,
    timeout_policy: ApprovalTimeoutPolicy,
}

impl ApplicationApprovalSubmitIntentV1 {
    fn parse(intent: HostIntent) -> Result<Self, AppHostError> {
        if intent.kind != APPROVAL_SUBMIT_INTENT_V1 {
            return Err(AppHostError::Denied(format!(
                "approval intent kind {} is not allowed",
                intent.kind
            )));
        }
        let request = serde_json::from_value::<Self>(intent.payload).map_err(|error| {
            AppHostError::Denied(format!("invalid approval submit intent: {error}"))
        })?;
        for (field, value) in [
            ("approval_id", request.approval_id.trim()),
            ("resource_ref", request.resource_ref.trim()),
            ("review_ref", request.review_ref.trim()),
            ("action", request.action.trim()),
            ("summary", request.summary.trim()),
        ] {
            if value.is_empty() {
                return Err(AppHostError::Denied(format!("{field} must not be empty")));
            }
        }
        Ok(request)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationApprovalDecisionIntentV1 {
    approval_id: String,
    review_ref: String,
    action: String,
    scope: String,
    evidence_digest: String,
    approved: bool,
    decision: String,
    reason: String,
}

impl ApplicationApprovalDecisionIntentV1 {
    fn parse(intent: HostIntent) -> Result<Self, AppHostError> {
        if intent.kind != APPROVAL_DECIDE_INTENT_V1 {
            return Err(AppHostError::Denied(format!(
                "approval intent kind {} is not allowed",
                intent.kind
            )));
        }
        let request = serde_json::from_value::<Self>(intent.payload).map_err(|error| {
            AppHostError::Denied(format!("invalid approval decision intent: {error}"))
        })?;
        for (field, value) in [
            ("approval_id", request.approval_id.trim()),
            ("review_ref", request.review_ref.trim()),
            ("action", request.action.trim()),
            ("scope", request.scope.trim()),
            ("evidence_digest", request.evidence_digest.trim()),
            ("decision", request.decision.trim()),
        ] {
            if value.is_empty() {
                return Err(AppHostError::Denied(format!("{field} must not be empty")));
            }
        }
        Ok(request)
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[async_trait]
impl ConnectorPort for GatewayAppHostBinding {
    async fn dispatch(
        &self,
        _context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        Err(Self::unsupported("connector", &intent.kind))
    }
}

#[async_trait]
impl RealityPort for GatewayAppHostBinding {
    async fn query(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        let _principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            REALITY_RECOMPUTE_METRICS_INTENT_V1 => {
                if !intent.payload.is_null()
                    && intent
                        .payload
                        .as_object()
                        .is_none_or(|value| !value.is_empty())
                {
                    return Err(AppHostError::Denied(
                        "recompute_metrics intent must not contain an application payload".into(),
                    ));
                }
                let matrix = state.services.matrix.clone();
                let config_home = state.config_home.clone();
                let result =
                    tokio::task::spawn_blocking(move || matrix.recompute_metrics(&config_home))
                        .await
                        .map_err(|error| {
                            AppHostError::Unavailable(format!(
                                "Reality recompute worker failed: {error}"
                            ))
                        })?
                        .map_err(|error| AppHostError::Failed(error.to_string()))?;
                Ok(HostReceipt {
                    id: format!("reality:metrics:recompute:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.reality.recompute_metrics.receipt.v1",
                        "result": result,
                    }),
                })
            }
            _ => Err(Self::unsupported("reality", &intent.kind)),
        }
    }
}

#[async_trait]
impl WorkContextPort for GatewayAppHostBinding {
    async fn read(
        &self,
        _context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        Err(Self::unsupported("work_context", &intent.kind))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CrossPlaneDispatchIntentV1 {
    mode: String,
    idempotency_key: String,
    requested_capability: String,
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    provider_account: Option<String>,
    #[serde(default)]
    target_ref: Option<String>,
    #[serde(default)]
    resource_ref: Option<String>,
    #[serde(default = "default_risk")]
    risk: CrossPlaneRisk,
    #[serde(default = "default_data_classification")]
    data_classification: DataClassification,
    #[serde(default = "default_identity_trust")]
    identity_trust: IdentityTrust,
    dispatch: CrossPlaneDispatchSpecV1,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CrossPlaneDispatchSpecV1 {
    platform: String,
    operation: String,
}

fn default_risk() -> CrossPlaneRisk {
    CrossPlaneRisk::Low
}

fn default_data_classification() -> DataClassification {
    DataClassification::Internal
}

fn default_identity_trust() -> IdentityTrust {
    IdentityTrust::Unknown
}

impl CrossPlaneDispatchIntentV1 {
    fn parse(intent: HostIntent) -> Result<Self, AppHostError> {
        if intent.kind != CROSS_PLANE_DISPATCH_INTENT_V1 {
            return Err(AppHostError::Denied(format!(
                "cross-plane intent kind {} is not allowed",
                intent.kind
            )));
        }
        let parsed = serde_json::from_value::<Self>(intent.payload).map_err(|error| {
            AppHostError::Denied(format!("invalid cross-plane intent: {error}"))
        })?;
        if !matches!(parsed.mode.trim(), "dry_run" | "commit") {
            return Err(AppHostError::Denied(
                "cross-plane mode must be dry_run or commit".to_string(),
            ));
        }
        for (field, value) in [
            ("idempotency_key", parsed.idempotency_key.trim()),
            ("requested_capability", parsed.requested_capability.trim()),
            ("dispatch.platform", parsed.dispatch.platform.trim()),
            ("dispatch.operation", parsed.dispatch.operation.trim()),
        ] {
            if value.is_empty() {
                return Err(AppHostError::Denied(format!("{field} must not be empty")));
            }
        }
        Ok(parsed)
    }

    fn action(self, context: &InvocationContext) -> (String, CrossPlaneAction, String, String) {
        let idempotency_key = self.idempotency_key.trim().to_string();
        let mut action = CrossPlaneAction::new(
            format!("principal:{}", context.principal_id.trim()),
            self.requested_capability.trim(),
        );
        action.actor_identity_ref = self.actor_identity_ref;
        action.source_channel = self.source_channel;
        action.session_id = self.session_id;
        action.provider_account = self.provider_account;
        action.target_ref = self.target_ref;
        action.resource_ref = self.resource_ref;
        action.risk = self.risk;
        action.data_classification = self.data_classification;
        action.identity_trust = self.identity_trust;
        (
            idempotency_key,
            action,
            self.dispatch.platform.trim().to_string(),
            self.dispatch.operation.trim().to_string(),
        )
    }
}

#[async_trait]
impl CrossPlanePort for GatewayAppHostBinding {
    async fn submit(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        // An embedded APP must not be able to manufacture another principal's
        // invocation context.  Resolve the context back to the request that
        // Gateway authenticated before accepting a CrossPlane action.
        let _principal = self.verified_principal(context)?;
        let request = CrossPlaneDispatchIntentV1::parse(intent)?;
        let mode = request.mode.trim().to_string();
        let (idempotency_key, requested_action, platform, operation) = request.action(context);
        let state = self.state()?;
        let snapshot = connector_snapshot(&state);
        let (action, decision, evidence) = state.services.cross_plane.decide_connector_action(
            &snapshot,
            requested_action,
            &mode,
            chrono::Utc::now(),
        );

        let (receipt, replayed) = if mode == "dry_run" {
            (
                state.services.cross_plane.preview_action(
                    Some(idempotency_key),
                    mode,
                    action,
                    decision,
                ),
                false,
            )
        } else if let Some(existing) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(&idempotency_key)
        {
            if existing.action != action {
                return Err(AppHostError::Denied(
                    "idempotency key belongs to another cross-plane action".to_string(),
                ));
            }
            (existing, true)
        } else if decision.decision == PolicyDecisionKind::Allow {
            let target =
                CrossPlaneDispatchTarget::from_action(&action, Some(&platform), Some(&operation))
                    .ok_or_else(|| {
                    AppHostError::Failed("cross-plane target is not dispatchable".into())
                })?;
            let executor = Arc::new(GatewayCrossPlaneExecutor::new(
                state.services.surface.clone(),
                target.clone(),
                state.services.cross_plane.runtime_control(),
            ));
            let projection = state
                .services
                .cross_plane
                .execute_commit_graph(
                    &action,
                    &decision,
                    &idempotency_key,
                    Some(&target),
                    executor,
                )
                .await
                .map_err(|error| AppHostError::Failed(error.to_string()))?;
            (
                state
                    .services
                    .cross_plane
                    .record_message_dispatch_graph(
                        idempotency_key,
                        action,
                        decision,
                        evidence,
                        target,
                        &projection,
                    )
                    .map_err(|error| AppHostError::Failed(error.to_string()))?,
                false,
            )
        } else {
            (
                state
                    .services
                    .cross_plane
                    .record_non_commit_action(
                        Some(idempotency_key),
                        mode,
                        action,
                        decision,
                        evidence,
                    )
                    .map_err(|error| AppHostError::Failed(error.to_string()))?,
                false,
            )
        };
        Ok(HostReceipt {
            id: receipt.id.clone(),
            status: receipt.status.clone(),
            replayed,
            payload: serde_json::json!({
                "kind": "cowd.cross_plane.dispatch.receipt.v1",
                "receipt": receipt,
            }),
        })
    }
}
