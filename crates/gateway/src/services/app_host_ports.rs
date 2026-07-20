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
    HostIntent, HostReceipt, InvocationContext, PlatformPort, RealityPort, RuntimePort,
    WorkContextPort,
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

use matrix_core::MatrixEvidencePacket;
use memory::store::session::SessionRecord;
use surface::SurfaceSendRequest;

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
/// Closed Matrix compute-job transition. The APP may select only an existing
/// job identifier; Gateway keeps the state transition and metric recompute
/// authority behind the verified request-principal binding.
pub(crate) const REALITY_RUN_COMPUTE_JOB_INTENT_V1: &str = "cowd.reality.run_compute_job.v1";
/// Closed, application-neutral append-only session-domain-event effect.
/// The application supplies data only; Gateway owns the verified-principal
/// binding, session lifecycle and durable event allocation.
pub(crate) const WORK_CONTEXT_APPEND_SESSION_EVENT_INTENT_V1: &str =
    "cowd.work_context.append_session_event.v1";
/// Closed context projection for Matrix evidence. Matrix is a Cowd core
/// domain; the application never receives the Context service or its types.
pub(crate) const WORK_CONTEXT_STRUCTURED_EVIDENCE_ITEM_INTENT_V1: &str =
    "cowd.work_context.structured_evidence_item.v1";
/// Closed task lookup used by an APP to bind its own domain assignment to an
/// already-existing Cowd task without obtaining task service access.
pub(crate) const WORK_CONTEXT_TASK_EXISTS_INTENT_V1: &str = "cowd.work_context.task_exists.v1";
/// Closed task-terminal observation. Gateway verifies and records Runtime
/// completion evidence; an APP can consume only the resulting durable fact.
pub(crate) const WORK_CONTEXT_RECORD_TASK_TERMINAL_OBSERVATION_INTENT_V1: &str =
    "cowd.work_context.record_task_terminal_observation.v1";
/// Closed task-terminal probe. The APP can learn only whether a referenced
/// Runtime task is terminal; it never receives a task service or graph.
pub(crate) const WORK_CONTEXT_INSPECT_TASK_TERMINAL_INTENT_V1: &str =
    "cowd.work_context.inspect_task_terminal.v1";
/// Closed surface dispatch batch. APPs supply no transport credential and the
/// host owns the durable outbox/idempotency identity for every delivery.
pub(crate) const CONNECTOR_SURFACE_DISPATCH_BATCH_INTENT_V1: &str =
    "cowd.connector.surface_dispatch_batch.v1";
/// Closed, read-only host snapshot for an APP's own production-governance
/// projection. The response is product status only, not a configuration API.
pub(crate) const PLATFORM_GOVERNANCE_SNAPSHOT_INTENT_V1: &str =
    "cowd.platform.governance_snapshot.v1";

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

    fn platform(&self) -> &dyn PlatformPort {
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
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        let _principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            CONNECTOR_SURFACE_DISPATCH_BATCH_INTENT_V1 => {
                let request: ConnectorSurfaceDispatchBatchIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "surface dispatch batch intent must contain only bounded delivery entries: {error}"
                        ))
                    })?;
                if request.deliveries.is_empty() || request.deliveries.len() > 32 {
                    return Err(AppHostError::Denied(
                        "surface dispatch batch must contain between 1 and 32 deliveries"
                            .to_string(),
                    ));
                }
                let mut notification_refs = Vec::with_capacity(request.deliveries.len());
                for delivery in request.deliveries {
                    validate_surface_delivery(&delivery)?;
                    let delivery_key = delivery.idempotency_key.clone();
                    let send = state.services.surface.send(SurfaceSendRequest {
                        surface: delivery.surface,
                        recipient: delivery.recipient,
                        thread: delivery.thread,
                        text: delivery.text,
                        idempotency_key: Some(delivery_key.clone()),
                        metadata: delivery.metadata,
                    });
                    let result = send.await;
                    if let Some(outbox) = state
                        .services
                        .surface
                        .all_outbox()
                        .into_iter()
                        .find(|entry| entry.idempotency_key == delivery_key)
                    {
                        notification_refs.push(format!(
                            "surface://{}/delivery/{}",
                            outbox.surface, outbox.delivery_id
                        ));
                    } else if let Err(error) = result {
                        return Err(AppHostError::Failed(format!(
                            "surface delivery failed before durable outbox acceptance: {error}"
                        )));
                    } else {
                        return Err(AppHostError::Failed(
                            "surface delivery completed without a durable outbox record"
                                .to_string(),
                        ));
                    }
                }
                Ok(HostReceipt {
                    id: format!("connector:surface-batch:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.connector.surface_dispatch_batch.receipt.v1",
                        "notification_refs": notification_refs,
                    }),
                })
            }
            _ => Err(Self::unsupported("connector", &intent.kind)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorSurfaceDispatchBatchIntentV1 {
    deliveries: Vec<ConnectorSurfaceDeliveryV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorSurfaceDeliveryV1 {
    surface: String,
    recipient: String,
    #[serde(default)]
    thread: Option<String>,
    text: String,
    idempotency_key: String,
    metadata: serde_json::Value,
}

fn validate_surface_delivery(delivery: &ConnectorSurfaceDeliveryV1) -> Result<(), AppHostError> {
    let valid_label = |value: &str, limit: usize| {
        !value.trim().is_empty()
            && value.len() <= limit
            && value
                .chars()
                .all(|character| !character.is_control() && character != '\n' && character != '\r')
    };
    let metadata_size = serde_json::to_vec(&delivery.metadata)
        .map_err(|error| {
            AppHostError::Denied(format!("surface delivery metadata is invalid: {error}"))
        })?
        .len();
    if !valid_label(&delivery.surface, 96)
        || !valid_label(&delivery.recipient, 512)
        || !valid_label(&delivery.idempotency_key, 256)
        || delivery.text.trim().is_empty()
        || delivery.text.len() > 16 * 1024
        || !delivery.metadata.is_object()
        || metadata_size > 8 * 1024
    {
        return Err(AppHostError::Denied(
            "surface delivery contains an invalid bounded target, text, idempotency key or metadata"
                .to_string(),
        ));
    }
    if delivery
        .thread
        .as_deref()
        .is_some_and(|value| !valid_label(value, 512))
    {
        return Err(AppHostError::Denied(
            "surface delivery thread is invalid".to_string(),
        ));
    }
    Ok(())
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
            REALITY_RUN_COMPUTE_JOB_INTENT_V1 => {
                let request: RealityRunComputeJobIntentV1 = serde_json::from_value(intent.payload)
                    .map_err(|error| {
                        AppHostError::Denied(format!(
                            "run_compute_job intent must contain only a non-empty job_id: {error}"
                        ))
                    })?;
                if request.job_id.trim().is_empty() {
                    return Err(AppHostError::Denied(
                        "run_compute_job intent job_id must not be empty".to_string(),
                    ));
                }
                let matrix = state.services.matrix.clone();
                let config_home = state.config_home.clone();
                let job_id = request.job_id;
                let job = tokio::task::spawn_blocking(move || {
                    matrix.run_compute_job(&config_home, &job_id)
                })
                .await
                .map_err(|error| {
                    AppHostError::Unavailable(format!("Reality compute-job worker failed: {error}"))
                })?
                .map_err(|error| AppHostError::Failed(error.to_string()))?;
                Ok(HostReceipt {
                    id: format!("reality:compute-job:run:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.reality.run_compute_job.receipt.v1",
                        "job": job,
                    }),
                })
            }
            _ => Err(Self::unsupported("reality", &intent.kind)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RealityRunComputeJobIntentV1 {
    job_id: String,
}

#[async_trait]
impl WorkContextPort for GatewayAppHostBinding {
    async fn execute(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        // Every WorkContext effect is request-bound before it can touch
        // durable session state or reveal a host projection.
        let _principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            WORK_CONTEXT_APPEND_SESSION_EVENT_INTENT_V1 => {
                let request: WorkContextAppendSessionEventIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "append_session_event intent must contain only the closed session event envelope: {error}"
                        ))
                    })?;
                validate_work_context_session_event(&request)?;
                let scope = parse_work_context_scope(&request.scope)?;
                append_work_context_session_event(&state, &request, scope).await?;
                Ok(HostReceipt {
                    id: format!("work-context:session-event:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.append_session_event.receipt.v1",
                        "session_id": request.session_id,
                        "event_type": request.event_type,
                    }),
                })
            }
            WORK_CONTEXT_STRUCTURED_EVIDENCE_ITEM_INTENT_V1 => {
                let request: WorkContextStructuredEvidenceItemIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "structured_evidence_item intent must contain one Matrix evidence packet: {error}"
                        ))
                    })?;
                let context_item = state
                    .services
                    .context
                    .structured_evidence_item(&request.packet);
                Ok(HostReceipt {
                    id: format!("work-context:structured-evidence:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.structured_evidence_item.receipt.v1",
                        "context_item": context_item,
                    }),
                })
            }
            WORK_CONTEXT_TASK_EXISTS_INTENT_V1 => {
                let request: WorkContextTaskExistsIntentV1 = serde_json::from_value(intent.payload)
                    .map_err(|error| {
                        AppHostError::Denied(format!(
                            "task_exists intent must contain exactly one task_ref: {error}"
                        ))
                    })?;
                let task_id = canonical_task_id(&request.task_ref)?;
                let exists = state
                    .services
                    .task
                    .list_records()
                    .map_err(AppHostError::Unavailable)?
                    .into_iter()
                    .any(|task| task.id == task_id);
                Ok(HostReceipt {
                    id: format!("work-context:task-exists:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.task_exists.receipt.v1",
                        "task_id": task_id,
                        "exists": exists,
                    }),
                })
            }
            WORK_CONTEXT_RECORD_TASK_TERMINAL_OBSERVATION_INTENT_V1 => {
                let request: WorkContextRecordTaskTerminalObservationIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "record_task_terminal_observation intent must contain the closed task observation envelope: {error}"
                        ))
                    })?;
                let task_id = canonical_task_id(&request.task_ref)?;
                if request.correlation_id.trim().is_empty()
                    || request.correlation_id.len() > 256
                    || request.correlation_id.chars().any(char::is_control)
                {
                    return Err(AppHostError::Denied(
                        "task terminal observation correlation_id is invalid".to_string(),
                    ));
                }
                let terminal =
                    observe_task_terminal(&state, &task_id, request.workflow_node_id.as_deref())
                        .await?;
                let Some(terminal) = terminal else {
                    return Ok(HostReceipt {
                        id: format!("work-context:task-terminal:{}", context.request_id),
                        status: "not_ready".to_string(),
                        replayed: false,
                        payload: serde_json::json!({
                            "kind": "cowd.work_context.task_terminal_observation.receipt.v1",
                            "task_id": task_id,
                            "terminal": serde_json::Value::Null,
                        }),
                    });
                };
                let observation = state
                    .services
                    .task
                    .record_assignment_terminal_observation(
                        &terminal.task_id,
                        &terminal.terminal_status,
                        &terminal.source_receipt_ref,
                        &request.correlation_id,
                    )
                    .map_err(AppHostError::Unavailable)?;
                Ok(HostReceipt {
                    id: format!("work-context:task-terminal:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.task_terminal_observation.receipt.v1",
                        "task_id": terminal.task_id,
                        "completion_evidence": {
                            "correlation_id": request.correlation_id,
                            "owner_kind": "runtime_assignment_terminal_observation",
                            "task_ref": request.task_ref,
                            "workflow_node_id": terminal.workflow_node_id,
                            "terminal_status": terminal.terminal_status,
                            "receipt_ref": format!(
                                "runtime-event://{}?cursor={}&transaction={}",
                                observation.event_id, observation.commit_cursor, observation.transaction_id
                            ),
                        },
                    }),
                })
            }
            WORK_CONTEXT_INSPECT_TASK_TERMINAL_INTENT_V1 => {
                let request: WorkContextInspectTaskTerminalIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "inspect_task_terminal intent must contain the closed task reference: {error}"
                        ))
                    })?;
                let task_id = canonical_task_id(&request.task_ref)?;
                let terminal =
                    observe_task_terminal(&state, &task_id, request.workflow_node_id.as_deref())
                        .await?;
                Ok(HostReceipt {
                    id: format!("work-context:task-terminal-inspect:{}", context.request_id),
                    status: if terminal.is_some() {
                        "completed".to_string()
                    } else {
                        "not_ready".to_string()
                    },
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.inspect_task_terminal.receipt.v1",
                        "task_id": task_id,
                        "terminal": terminal.as_ref().map(|terminal| serde_json::json!({
                            "workflow_node_id": terminal.workflow_node_id,
                            "terminal_status": terminal.terminal_status,
                            "source_receipt_ref": terminal.source_receipt_ref,
                        })),
                    }),
                })
            }
            _ => Err(Self::unsupported("work_context", &intent.kind)),
        }
    }
}

#[async_trait]
impl PlatformPort for GatewayAppHostBinding {
    async fn query(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        let _principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            PLATFORM_GOVERNANCE_SNAPSHOT_INTENT_V1 if intent.payload.is_null() => Ok(HostReceipt {
                id: format!("platform:governance:{}", context.request_id),
                status: "completed".to_string(),
                replayed: false,
                payload: serde_json::json!({
                    "kind": "cowd.platform.governance_snapshot.receipt.v1",
                    "snapshot": {
                        "auth_token_configured": state.auth_token.is_some(),
                        "approval_gate_configured": state.services.approval.is_configured(),
                        "session_store_ready": state.services.session.has_unified_store(),
                        "surface_runtime_ready": state.services.surface.is_runtime_available(),
                        "audit_export_surface": true,
                        "cross_plane_audit_surface": true,
                    },
                }),
            }),
            PLATFORM_GOVERNANCE_SNAPSHOT_INTENT_V1 => Err(AppHostError::Denied(
                "platform governance snapshot intent must have a null payload".to_string(),
            )),
            _ => Err(Self::unsupported("platform", &intent.kind)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextAppendSessionEventIntentV1 {
    session_id: String,
    platform: String,
    scope: String,
    event_type: String,
    payload: serde_json::Value,
    occurred_at_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextStructuredEvidenceItemIntentV1 {
    packet: MatrixEvidencePacket,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextTaskExistsIntentV1 {
    task_ref: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextRecordTaskTerminalObservationIntentV1 {
    task_ref: String,
    #[serde(default)]
    workflow_node_id: Option<String>,
    correlation_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextInspectTaskTerminalIntentV1 {
    task_ref: String,
    #[serde(default)]
    workflow_node_id: Option<String>,
}

struct WorkContextTaskTerminal {
    task_id: String,
    workflow_node_id: Option<String>,
    terminal_status: String,
    source_receipt_ref: String,
}

fn canonical_task_id(task_ref: &str) -> Result<String, AppHostError> {
    let task_id = task_ref
        .trim()
        .strip_prefix("task://")
        .or_else(|| task_ref.trim().strip_prefix("task:"))
        .unwrap_or(task_ref.trim());
    if task_id.is_empty() || task_id.len() > 256 || task_id.chars().any(char::is_control) {
        return Err(AppHostError::Denied(
            "work-context task_ref is invalid".to_string(),
        ));
    }
    Ok(task_id.to_string())
}

async fn observe_task_terminal(
    state: &AppState,
    task_id: &str,
    workflow_node_id: Option<&str>,
) -> Result<Option<WorkContextTaskTerminal>, AppHostError> {
    let task = state
        .services
        .task
        .list_records()
        .map_err(AppHostError::Unavailable)?
        .into_iter()
        .find(|task| task.id == task_id);
    let Some(task) = task else {
        return Ok(None);
    };
    if let Some(node_id) = workflow_node_id {
        let projection = state
            .services
            .task
            .execution_graph(&task.id)
            .await
            .map_err(AppHostError::Unavailable)?;
        let Some(projection) = projection else {
            return Ok(None);
        };
        let node = projection.nodes.iter().find(|node| node.node_id == node_id);
        let Some(node) = node else {
            return Ok(None);
        };
        if !node.status.is_terminal() {
            return Ok(None);
        }
        return Ok(Some(WorkContextTaskTerminal {
            task_id: task.id,
            workflow_node_id: Some(node_id.to_string()),
            terminal_status: format!("{:?}", node.status).to_ascii_lowercase(),
            source_receipt_ref: node.result_ref.clone().unwrap_or_else(|| {
                format!(
                    "execution://{}/nodes/{}?revision={}",
                    projection.graph_id, node_id, projection.revision
                )
            }),
        }));
    }
    let receipt = state
        .services
        .task
        .latest_terminal_runtime_receipt(&task.id)
        .map_err(AppHostError::Unavailable)?;
    if let Some(receipt) = receipt {
        let terminal_status = receipt
            .payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppHostError::Failed(
                    "canonical Runtime terminal receipt omitted its terminal status".to_string(),
                )
            })?;
        return Ok(Some(WorkContextTaskTerminal {
            task_id: task.id,
            workflow_node_id: None,
            terminal_status: terminal_status.to_string(),
            source_receipt_ref: format!(
                "runtime-event://{}?cursor={}&transaction={}",
                receipt.event_id, receipt.commit_cursor, receipt.transaction_id
            ),
        }));
    }
    if matches!(
        task.status,
        crate::task_kernel::TaskStatus::Completed
            | crate::task_kernel::TaskStatus::Blocked
            | crate::task_kernel::TaskStatus::Cancelled
            | crate::task_kernel::TaskStatus::Failed
    ) {
        let task_id = task.id;
        let terminal_status = task.status.as_str().to_string();
        return Ok(Some(WorkContextTaskTerminal {
            task_id: task_id.clone(),
            workflow_node_id: None,
            terminal_status: terminal_status.clone(),
            source_receipt_ref: format!("task://{task_id}?status={terminal_status}"),
        }));
    }
    Ok(None)
}

fn validate_work_context_session_event(
    request: &WorkContextAppendSessionEventIntentV1,
) -> Result<(), AppHostError> {
    let valid_label = |value: &str, limit: usize| {
        !value.trim().is_empty()
            && value.len() <= limit
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':')
            })
    };
    // Session identifiers are host keys, not application capability names.
    // Preserve existing session formats (for example provider-prefixed or
    // slash-containing IDs) while rejecting blank, oversized and control
    // character input before it reaches a durable store.
    if request.session_id.trim().is_empty()
        || request.session_id.len() > 256
        || request.session_id.chars().any(char::is_control)
    {
        return Err(AppHostError::Denied(
            "work-context session_id must be a non-empty bounded non-control value".to_string(),
        ));
    }
    if !valid_label(&request.platform, 64) {
        return Err(AppHostError::Denied(
            "work-context platform must be a non-empty bounded identifier".to_string(),
        ));
    }
    if !valid_label(&request.event_type, 128) {
        return Err(AppHostError::Denied(
            "work-context event_type must be a non-empty bounded identifier".to_string(),
        ));
    }
    if !request.payload.is_object() {
        return Err(AppHostError::Denied(
            "work-context event payload must be a JSON object".to_string(),
        ));
    }
    Ok(())
}

fn parse_work_context_scope(value: &str) -> Result<memory::SessionDomainScope, AppHostError> {
    match value {
        "memory" => Ok(memory::SessionDomainScope::Memory),
        "application_task" => Ok(memory::SessionDomainScope::ApplicationTask),
        "context" => Ok(memory::SessionDomainScope::Context),
        "tool" => Ok(memory::SessionDomainScope::Tool),
        _ => Err(AppHostError::Denied(
            "work-context scope must be one of memory, application_task, context or tool"
                .to_string(),
        )),
    }
}

async fn append_work_context_session_event(
    state: &AppState,
    request: &WorkContextAppendSessionEventIntentV1,
    scope: memory::SessionDomainScope,
) -> Result<(), AppHostError> {
    let Some(store) = state.services.session.unified_store() else {
        // Embedded products may intentionally omit the optional unified
        // session store. Preserve Matrix's existing best-effort projection
        // behavior while keeping the outcome explicit at the host boundary.
        return Ok(());
    };
    ensure_work_context_session_record(state, &request.session_id, &request.platform)
        .await
        .map_err(AppHostError::Failed)?;
    let event = memory::SessionDomainEvent::new(
        &request.session_id,
        0,
        scope,
        &request.event_type,
        request.payload.clone(),
        request.occurred_at_ms,
    );
    store
        .append_session_domain_event_allocating_sequence(&event)
        .await
        .map(|_| ())
        .map_err(|error| AppHostError::Failed(error.to_string()))
}

async fn ensure_work_context_session_record(
    state: &AppState,
    session_id: &str,
    platform: &str,
) -> Result<(), String> {
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    if let Some(mut record) = store
        .get_session(session_id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.last_activity = now;
        record.platform = platform.to_string();
        return store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string());
    }
    let metadata_json = serde_json::json!({
        "kind": "cowd.work_context.session",
        "session_id": session_id,
        "platform": platform,
    })
    .to_string();
    let record = SessionRecord {
        session_id: session_id.to_string(),
        platform: platform.to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    };
    store
        .create_session(&record)
        .await
        .map_err(|error| error.to_string())
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
