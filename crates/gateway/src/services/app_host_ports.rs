//! Concrete Cowd effect ports exposed to compile-time linked applications.
//!
//! Applications receive only the stable SDK traits.  This module is the
//! product-side adapter that owns Gateway state, policy and dispatch details;
//! no application can import or downcast it.

use std::sync::{Arc, RwLock, Weak};

use async_trait::async_trait;
use cowd_app_sdk::{
    AppHostError, AppHostPorts, ApprovalPort, ConnectorPort, CowdAppContext, CrossPlanePort,
    HostIntent, HostReceipt, InvocationContext, RealityPort, RuntimePort, WorkContextPort,
};
use harness_contract::policy::{CrossPlaneRisk, DataClassification};
use runtime::{CrossPlaneAction, CrossPlaneDispatchTarget, IdentityTrust, PolicyDecisionKind};
use serde::Deserialize;

use crate::api_routes::{connector_routes::connector_snapshot, AppState};

use super::GatewayCrossPlaneExecutor;

/// Closed intent name for a policy-gated, durable cross-plane message
/// dispatch.  The payload deliberately uses only transport data, never an
/// application-domain DTO or a Gateway service type.
pub(crate) const CROSS_PLANE_DISPATCH_INTENT_V1: &str = "cowd.cross_plane.dispatch.v1";

/// One binding is created during product composition and attached once the
/// fully assembled [`AppState`] exists.  It solves startup ordering without
/// making applications aware of Gateway internals or giving them a mutable
/// service handle.
#[derive(Clone, Default)]
pub(crate) struct GatewayAppHostBinding {
    state: Arc<RwLock<Weak<AppState>>>,
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

    fn state(&self) -> Result<Arc<AppState>, AppHostError> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade()
            .ok_or_else(|| {
                AppHostError::Unavailable("Gateway application host is not active".into())
            })
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
        _context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        Err(Self::unsupported("approval", &intent.kind))
    }
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
        _context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError> {
        Err(Self::unsupported("reality", &intent.kind))
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
