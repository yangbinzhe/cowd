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
    AppHostConflict, AppHostError, AppHostPorts, AppId, ApplicationExecutionOutcomeIntentV1,
    ApprovalPort, ConnectorPort, CowdAppContext, CredentialLifecycleCheck,
    CredentialLifecycleError, CredentialLifecyclePort, CrossPlanePort, HostIntent, HostReceipt,
    InvocationContext, PlatformPort, RealityPort, RuntimePort, WorkContextPort,
    APPEND_APPLICATION_EXECUTION_OUTCOME_INTENT_V1,
};
use harness_contract::{
    core::TaskRisk,
    policy::{CrossPlaneRisk, DataClassification},
};
use runtime::{
    ApprovalApplicationSource, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    CrossPlaneAction, CrossPlaneDecisionKind, CrossPlaneDispatchTarget, IdentityTrust,
    SubmitGlobalApprovalRequest, VerifiedPrincipal,
};
use serde::Deserialize;

use matrix_core::MatrixEvidencePacket;
use surface::SurfaceSendRequest;

use crate::api_routes::{connector_routes::connector_snapshot, AppState};

use super::matrix_app_reality;
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
/// Versioned, bounded Matrix operation envelope for an APP that owns a
/// product projection while Gateway owns the selected Reality backend.
pub(crate) const REALITY_MATRIX_OPERATION_INTENT_V1: &str =
    matrix_app_reality::MATRIX_OPERATION_INTENT_V1;
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
/// Closed Runtime task creation. APPs supply a bounded objective and stable
/// id; Gateway owns task persistence, scheduling and the returned task fact.
pub(crate) const RUNTIME_START_GOAL_INTENT_V1: &str = "cowd.runtime.start_goal.v1";
/// Closed, read-only host snapshot for an APP's own production-governance
/// projection. The response is product status only, not a configuration API.
pub(crate) const PLATFORM_GOVERNANCE_SNAPSHOT_INTENT_V1: &str =
    "cowd.platform.governance_snapshot.v1";

const APP_REQUEST_PRINCIPAL_TTL: Duration = Duration::from_secs(300);
const APP_REQUEST_PRINCIPAL_LIMIT: usize = 4096;

#[derive(Clone)]
struct BoundAppPrincipal {
    principal: VerifiedPrincipal,
    producer_id: String,
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
        producer_id: String,
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
                producer_id,
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
        self.verified_binding(context).map(|bound| bound.principal)
    }

    fn verified_binding(
        &self,
        context: &InvocationContext,
    ) -> Result<BoundAppPrincipal, AppHostError> {
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
        Ok(bound.clone())
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

    fn credential_lifecycle(&self) -> &dyn CredentialLifecyclePort {
        self
    }
}

impl CredentialLifecyclePort for GatewayAppHostBinding {
    fn verify(&self, check: CredentialLifecycleCheck) -> Result<(), CredentialLifecycleError> {
        let state = self
            .state()
            .map_err(|error| CredentialLifecycleError::AuthorityUnavailable(error.to_string()))?;
        let socket =
            auth_broker::BrokerClient::default_socket(state.config_home.join("auth-broker"));

        // Isolated Gateway tests intentionally have no broker child. Production
        // composition never takes this branch and therefore always revalidates
        // an active broker lifecycle before an APP stream advances.
        #[cfg(any(test, feature = "test-support"))]
        if !socket.exists() {
            return Ok(());
        }

        let lifecycle = auth_broker::BrokerClient::new(socket)
            .credential_lifecycle()
            .map_err(|error| CredentialLifecycleError::AuthorityUnavailable(error.to_string()))?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err(CredentialLifecycleError::CredentialInactive);
        }
        if lifecycle.credential_epoch != check.credential_epoch {
            return Err(CredentialLifecycleError::CredentialEpochChanged);
        }
        if lifecycle.profile_revision != check.profile_revision {
            return Err(CredentialLifecycleError::ProfileRevisionChanged);
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimePort for GatewayAppHostBinding {
    async fn execute(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        let state = self.state()?;
        let _principal = self.verified_principal(context)?;
        match intent.kind.as_str() {
            RUNTIME_START_GOAL_INTENT_V1 => {
                let request: RuntimeStartGoalIntentV1 = serde_json::from_value(intent.payload)
                    .map_err(|error| {
                        AppHostError::Denied(format!(
                            "start_goal intent must contain the closed task envelope: {error}"
                        ))
                    })?;
                if !valid_runtime_task_id(&request.task_id)
                    || request.objective.trim().is_empty()
                    || request.objective.len() > 4 * 1024
                    || request.objective.chars().any(char::is_control)
                {
                    return Err(AppHostError::Denied(
                        "runtime task id or objective is invalid".to_string(),
                    ));
                }
                let mission_id = match request.mission {
                    RuntimeMissionSelectorV1::WorkspaceDefault => state
                        .services
                        .task
                        .workspace_default_mission_id()
                        .map_err(AppHostError::Unavailable)?,
                    RuntimeMissionSelectorV1::MissionId { mission_id } => {
                        if !valid_runtime_identity(&mission_id) {
                            return Err(AppHostError::Denied(
                                "runtime mission id is invalid".to_string(),
                            ));
                        }
                        mission_id
                    }
                };
                let task = state
                    .services
                    .task
                    .create(
                        request.task_id,
                        mission_id,
                        request.source_session_id,
                        request.source_turn_id,
                        request.objective,
                        request.preemptive,
                        vec![harness_contract::reality::EvidenceRef::observed(
                            "app_intent",
                            format!("app://{}/requests/{}", context.surface, context.request_id),
                        )],
                    )
                    .map_err(AppHostError::Unavailable)?;
                Ok(HostReceipt {
                    id: format!("runtime:start-goal:{}", task.task_id),
                    status: task.status.as_str().to_string(),
                    replayed: false,
                    payload: serde_json::json!({
                        "kind": "cowd.runtime.start_goal.receipt.v1",
                        "task": task,
                    }),
                })
            }
            _ => Err(Self::unsupported("runtime", &intent.kind)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeStartGoalIntentV1 {
    task_id: String,
    mission: RuntimeMissionSelectorV1,
    source_session_id: String,
    source_turn_id: String,
    objective: String,
    #[serde(default)]
    preemptive: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "selector", rename_all = "snake_case", deny_unknown_fields)]
enum RuntimeMissionSelectorV1 {
    WorkspaceDefault,
    MissionId { mission_id: String },
}

fn valid_runtime_task_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 256
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn valid_runtime_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
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
                let application = registered_application_approval_source(
                    state.services.app_registry.as_ref(),
                    &request.app_id,
                    &request.correlation_schema,
                    &request.decision_capability,
                )?;
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
                let source = ApprovalSource {
                    kind: ApprovalSourceKind::Application,
                    session_id: None,
                    agent_id: None,
                    team_id: None,
                    mission_id: None,
                    resource_ref: Some(request.resource_ref),
                    review_ref: Some(request.review_ref),
                    application: Some(application),
                };
                let action = request.action;
                let approval = runtime
                    .approval_queue()
                    .submit_scoped(
                        request.approval_id.clone(),
                        SubmitGlobalApprovalRequest {
                            context: harness_contract::policy::ApprovalContext::owned(
                                &source,
                                &action,
                                "application",
                            ),
                            source,
                            action,
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
                let approval = runtime
                    .approval_queue()
                    .get(&request.approval_id)
                    .ok_or_else(|| {
                        AppHostError::Denied("application approval request not found".into())
                    })?;
                let application =
                    approval
                        .source
                        .typed_application()
                        .cloned()
                        .ok_or_else(|| {
                            AppHostError::Denied(
                                "application approval request has no typed source metadata".into(),
                            )
                        })?;
                if application.app_id != request.app_id
                    || application.correlation_schema != request.correlation_schema
                {
                    return Err(AppHostError::Denied(
                        "application approval source correlation mismatch".into(),
                    ));
                }
                if approval.source.review_ref.as_deref() != Some(request.review_ref.as_str()) {
                    return Err(AppHostError::Denied(
                        "application approval review correlation mismatch".into(),
                    ));
                }
                let registered = registered_application_approval_source(
                    state.services.app_registry.as_ref(),
                    &application.app_id,
                    &application.correlation_schema,
                    &application.decision_capability,
                )?;
                if registered != application {
                    return Err(AppHostError::Denied(
                        "registered application approval metadata no longer matches request".into(),
                    ));
                }
                if !principal.is_human_interactive()
                    || !principal.has_capability("approval.respond")
                    || !principal.has_capability(&application.decision_capability)
                {
                    return Err(AppHostError::Denied(
                        "application approval decisions require a human-interactive principal with the registered decision capability and approval.respond"
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
                    .record_application_decision_fact(
                        &request.approval_id,
                        &application,
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
    app_id: String,
    correlation_schema: String,
    decision_capability: String,
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
            ("app_id", request.app_id.trim()),
            ("correlation_schema", request.correlation_schema.trim()),
            ("decision_capability", request.decision_capability.trim()),
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
    app_id: String,
    correlation_schema: String,
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
            ("app_id", request.app_id.trim()),
            ("correlation_schema", request.correlation_schema.trim()),
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

fn application_execution_host_error(
    error: session::SessionError,
    producer_id: &str,
    contract_version: u16,
    outcome_id: &str,
) -> AppHostError {
    match error {
        session::SessionError::IdempotencyConflict { namespace, key } => {
            AppHostError::Conflict(AppHostConflict::Idempotency {
                namespace: namespace.to_string(),
                key,
                producer_id: producer_id.to_string(),
                contract_version,
                outcome_id: outcome_id.to_string(),
            })
        }
        other => AppHostError::Failed(other.to_string()),
    }
}

fn registered_application_approval_source(
    app_registry: &cowd_app_host::AppRegistry,
    app_id: &str,
    correlation_schema: &str,
    decision_capability: &str,
) -> Result<ApprovalApplicationSource, AppHostError> {
    let parsed_id = AppId::parse(app_id.to_string())
        .map_err(|_| AppHostError::Denied("application approval app_id is invalid".into()))?;
    let application = app_registry
        .app(&parsed_id)
        .ok_or_else(|| AppHostError::Denied("application approval app is not registered".into()))?;
    if !application
        .descriptor
        .capabilities
        .iter()
        .any(|capability| capability == decision_capability)
    {
        return Err(AppHostError::Denied(
            "application approval decision capability is not declared by the registered app".into(),
        ));
    }
    Ok(ApprovalApplicationSource {
        app_id: parsed_id.as_str().to_string(),
        correlation_schema: correlation_schema.to_string(),
        decision_capability: decision_capability.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer_principal(
        principal_id: &str,
        credential_fingerprint: &str,
    ) -> runtime::VerifiedPrincipal {
        runtime::VerifiedPrincipal::from_test_claims(harness_contract::security::PrincipalClaims {
            principal_id: principal_id.to_string(),
            kind: harness_contract::security::PrincipalKind::Service,
            scopes: vec!["gateway".to_string()],
            capabilities: Vec::new(),
            assurance: harness_contract::security::PrincipalAssurance::Normal,
            issuer: "test.gateway".to_string(),
            issued_at_ms: 1,
            expires_at_ms: None,
            credential_fingerprint: credential_fingerprint.to_string(),
            credential_epoch: 1,
            profile_revision: 1,
        })
    }

    #[test]
    fn application_producer_is_read_only_from_the_gateway_bound_request_record() {
        let host = GatewayAppHostBinding::new();
        let principal = producer_principal("same-human", "credential-1");
        let app_a = InvocationContext {
            principal_id: "same-human".to_string(),
            workspace_id: "workspace".to_string(),
            surface: "gateway".to_string(),
            request_id: "request-app-a".to_string(),
        };
        let app_b = InvocationContext {
            request_id: "request-app-b".to_string(),
            ..app_a.clone()
        };
        host.bind_request_principal(&principal, &app_a, "app:app-a".to_string());
        host.bind_request_principal(&principal, &app_b, "app:app-b".to_string());
        assert_eq!(
            host.verified_binding(&app_a).unwrap().producer_id,
            "app:app-a"
        );
        assert_eq!(
            host.verified_binding(&app_b).unwrap().producer_id,
            "app:app-b"
        );
    }

    #[test]
    fn session_idempotency_conflict_maps_to_typed_app_host_conflict() {
        let error = application_execution_host_error(
            session::SessionError::IdempotencyConflict {
                namespace: "session_domain_event",
                key: "application-execution:v1:fixture".to_string(),
            },
            "app:mfg",
            1,
            "outcome-1",
        );

        assert!(matches!(
            error,
            AppHostError::Conflict(AppHostConflict::Idempotency {
                namespace,
                key,
                producer_id,
                contract_version: 1,
                outcome_id,
            }) if namespace == "session_domain_event"
                && key == "application-execution:v1:fixture"
                && producer_id == "app:mfg"
                && outcome_id == "outcome-1"
        ));
    }

    #[test]
    fn approval_source_must_match_a_registered_app_and_declared_capability() {
        let services = crate::services::GatewayServices::baseline();
        let registry = services.app_registry.as_ref();
        let source = registered_application_approval_source(
            registry,
            "mfg",
            "mfg.report.delivery.review.v1",
            "mfg.report.review",
        )
        .expect("embedded APP declares its review capability");
        assert_eq!(source.app_id, "mfg");
        assert_eq!(source.decision_capability, "mfg.report.review");
        assert!(matches!(
            registered_application_approval_source(
                registry,
                "unknown-app",
                "unknown.review.v1",
                "unknown.review",
            )
            .unwrap_err(),
            AppHostError::Denied(message) if message == "application approval app is not registered"
        ));
        assert!(matches!(
            registered_application_approval_source(
                registry,
                "mfg",
                "mfg.report.delivery.review.v1",
                "other.review",
            )
            .unwrap_err(),
            AppHostError::Denied(message)
                if message == "application approval decision capability is not declared by the registered app"
        ));
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
                        .map_err(AppHostError::Failed)?
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
            REALITY_MATRIX_OPERATION_INTENT_V1 => {
                let request = RealityMatrixOperationIntentV1::parse(intent.payload)?;
                let matrix = state.services.matrix.clone();
                let config_home = state.config_home.clone();
                let operation = request.operation;
                let input = request.input;
                let outcome = tokio::task::spawn_blocking(move || {
                    let store = matrix
                        .store(&config_home)
                        .map_err(matrix_app_reality::MatrixAppRealityError::from)?;
                    matrix_app_reality::dispatch(store.as_ref(), &operation, &input)
                })
                .await
                .map_err(|error| {
                    AppHostError::Unavailable(format!(
                        "Reality Matrix operation worker failed: {error}"
                    ))
                })?;
                let payload = match outcome {
                    Ok(result) => serde_json::json!({"result": result}),
                    Err(error) => serde_json::json!({
                        "error": {
                            "code": error.code(),
                            "message": error.to_string(),
                        }
                    }),
                };
                Ok(HostReceipt {
                    id: format!("reality:matrix-operation:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: false,
                    payload,
                })
            }
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RealityMatrixOperationIntentV1 {
    operation: String,
    input: serde_json::Value,
}

impl RealityMatrixOperationIntentV1 {
    fn parse(payload: serde_json::Value) -> Result<Self, AppHostError> {
        let encoded_len = serde_json::to_vec(&payload)
            .map_err(|error| {
                AppHostError::Denied(format!("Matrix operation payload is invalid: {error}"))
            })?
            .len();
        if encoded_len > 4 * 1024 * 1024 {
            return Err(AppHostError::Denied(
                "Matrix operation payload exceeds the 4 MiB host boundary".to_string(),
            ));
        }
        let request: Self = serde_json::from_value(payload).map_err(|error| {
            AppHostError::Denied(format!(
                "Matrix operation intent must contain only operation and object input: {error}"
            ))
        })?;
        if request.operation.is_empty()
            || request.operation.len() > 96
            || !request.input.is_object()
            || !matrix_app_reality::supports(&request.operation)
        {
            return Err(AppHostError::Denied(
                "Matrix operation is unknown or has an invalid bounded envelope".to_string(),
            ));
        }
        Ok(request)
    }
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
        let binding = self.verified_binding(context)?;
        match intent.kind.as_str() {
            APPEND_APPLICATION_EXECUTION_OUTCOME_INTENT_V1 => {
                let request: ApplicationExecutionOutcomeIntentV1 =
                    serde_json::from_value(intent.payload).map_err(|error| {
                        AppHostError::Denied(format!(
                            "application execution outcome intent is invalid: {error}"
                        ))
                    })?;
                request.validate().map_err(|error| {
                    AppHostError::Denied(format!(
                        "application execution outcome intent is invalid: {error}"
                    ))
                })?;
                let receipt = state
                    .services
                    .session
                    .append_application_execution_outcome_for_producer(
                        &request.session_id,
                        &binding.producer_id,
                        &request.outcome,
                    )
                    .await
                    .map_err(|error| {
                        application_execution_host_error(
                            error,
                            &binding.producer_id,
                            request.outcome.contract_version,
                            &request.outcome.outcome_id,
                        )
                    })?;
                Ok(HostReceipt {
                    id: format!("work-context:execution-outcome:{}", context.request_id),
                    status: "completed".to_string(),
                    replayed: receipt.replayed,
                    payload: serde_json::json!({
                        "kind": "cowd.work_context.append_application_execution_outcome.receipt.v1",
                        "session_id": request.session_id,
                        "receipt": receipt,
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
                    .any(|task| task.task_id == task_id);
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
                        "approval_coordinator_configured": state.services.approval.is_configured(),
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
        .find(|task| task.task_id == task_id);
    let Some(task) = task else {
        return Ok(None);
    };
    if let Some(node_id) = workflow_node_id {
        let projection = state
            .services
            .task
            .execution_graph(&task.task_id)
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
            task_id: task.task_id,
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
        .latest_terminal_runtime_receipt(&task.task_id)
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
            task_id: task.task_id,
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
        harness_contract::task::TaskStatus::Completed
            | harness_contract::task::TaskStatus::Blocked
            | harness_contract::task::TaskStatus::Cancelled
            | harness_contract::task::TaskStatus::Failed
    ) {
        let task_id = task.task_id;
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
        } else if decision.decision == CrossPlaneDecisionKind::Allow {
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
