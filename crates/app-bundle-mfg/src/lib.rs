//! Cowd product composition for the first-party MFG APP.
//!
//! This crate is intentionally the only Cowd crate that imports an MFG
//! package. It owns no MFG DTO, route, store or policy: it only turns the
//! external APP's contribution factory into a registered product capability.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use cowd_app_host::{AppRegistry, AppRegistryError};
use cowd_app_mfg_adapter::{
    contribution_with_live_host,
    effects::{
        MfgHostEffect, MfgHostEffectError, MfgHostEffectPort, MfgHostIntent, MfgHostInvocation,
        MfgHostReceipt,
    },
    MfgLiveAuthorization, MfgLiveAuthorizationFailure, MfgLiveHost, MfgLivePrincipalContext,
};
use cowd_app_sdk::{AppDescriptor, AppHostError, CowdAppContext, HostIntent, InvocationContext};

/// Product-side bridge from MFG's repository-stable effect ABI to Cowd's
/// stable SDK. The external APP never receives this type or a Gateway service.
#[derive(Clone)]
struct CowdSdkMfgHostEffects {
    context: CowdAppContext,
}

impl CowdSdkMfgHostEffects {
    fn new(context: CowdAppContext) -> Self {
        Self { context }
    }

    fn invocation(invocation: MfgHostInvocation) -> InvocationContext {
        InvocationContext {
            principal_id: invocation.principal_id,
            workspace_id: invocation.workspace_id,
            surface: invocation.surface,
            request_id: invocation.request_id,
        }
    }

    fn intent(intent: MfgHostIntent) -> HostIntent {
        HostIntent {
            kind: intent.kind,
            payload: intent.payload,
        }
    }

    fn receipt(receipt: cowd_app_sdk::HostReceipt) -> MfgHostReceipt {
        MfgHostReceipt {
            id: receipt.id,
            status: receipt.status,
            replayed: receipt.replayed,
            payload: receipt.payload,
        }
    }

    fn error(error: AppHostError) -> MfgHostEffectError {
        match error {
            AppHostError::Unavailable(message) => MfgHostEffectError::Unavailable(message),
            AppHostError::Denied(message) => MfgHostEffectError::Denied(message),
            AppHostError::Failed(message) => MfgHostEffectError::Failed(message),
        }
    }
}

#[async_trait]
impl MfgHostEffectPort for CowdSdkMfgHostEffects {
    async fn submit(
        &self,
        effect: MfgHostEffect,
        invocation: MfgHostInvocation,
        intent: MfgHostIntent,
    ) -> Result<MfgHostReceipt, MfgHostEffectError> {
        let invocation = Self::invocation(invocation);
        let intent = Self::intent(intent);
        let receipt = match effect {
            MfgHostEffect::Runtime => {
                self.context
                    .ports()
                    .runtime()
                    .execute(&invocation, intent)
                    .await
            }
            MfgHostEffect::Approval => {
                self.context
                    .ports()
                    .approval()
                    .request(&invocation, intent)
                    .await
            }
            MfgHostEffect::CrossPlane => {
                self.context
                    .ports()
                    .cross_plane()
                    .submit(&invocation, intent)
                    .await
            }
            MfgHostEffect::Connector => {
                self.context
                    .ports()
                    .connector()
                    .dispatch(&invocation, intent)
                    .await
            }
            MfgHostEffect::Reality => {
                self.context
                    .ports()
                    .reality()
                    .query(&invocation, intent)
                    .await
            }
            MfgHostEffect::WorkContext => {
                self.context
                    .ports()
                    .work_context()
                    .execute(&invocation, intent)
                    .await
            }
        }
        .map_err(Self::error)?;
        Ok(Self::receipt(receipt))
    }
}

/// Product-owned credential lifecycle adapter for MFG live projections.
///
/// MFG owns cursor binding, scope filtering and the `mfg.read` check. Cowd
/// owns the local broker and can therefore decide whether a credential was
/// revoked or re-profiled after the HTTP response became a long-lived SSE
/// stream.  Keeping this adapter in product composition prevents either the
/// app or Gateway core from learning the other's implementation details.
#[derive(Debug, Clone, Copy)]
pub struct BrokerCredentialLifecycleAuthorization {
    enforce_broker_lifecycle: bool,
}

impl BrokerCredentialLifecycleAuthorization {
    #[must_use]
    pub const fn required() -> Self {
        Self {
            enforce_broker_lifecycle: true,
        }
    }

    /// Local in-memory/test products do not run a broker. They still receive
    /// the APP's expiry/capability checks, while production always uses
    /// [`Self::required`].
    #[must_use]
    pub const fn embedded_trusted() -> Self {
        Self {
            enforce_broker_lifecycle: false,
        }
    }
}

impl MfgLiveAuthorization for BrokerCredentialLifecycleAuthorization {
    fn verify(
        &self,
        config_home: &std::path::Path,
        principal: &MfgLivePrincipalContext,
    ) -> Result<(), MfgLiveAuthorizationFailure> {
        if !self.enforce_broker_lifecycle {
            return Ok(());
        }

        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        let lifecycle =
            client
                .credential_lifecycle()
                .map_err(|error| MfgLiveAuthorizationFailure {
                    reason: "authority_unavailable",
                    message: format!("MFG live authorization authority is unavailable: {error}"),
                })?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err(MfgLiveAuthorizationFailure {
                reason: "credential_inactive",
                message: "MFG live credential is no longer active".to_string(),
            });
        }
        if lifecycle.credential_epoch != principal.credential_epoch {
            return Err(MfgLiveAuthorizationFailure {
                reason: "credential_epoch_changed",
                message: "MFG live credential epoch changed; authenticate again".to_string(),
            });
        }
        if lifecycle.profile_revision != principal.profile_revision {
            return Err(MfgLiveAuthorizationFailure {
                reason: "profile_revision_changed",
                message: "MFG live authorization changed; authenticate again".to_string(),
            });
        }
        Ok(())
    }
}

/// Register the compile-time linked MFG APP into a product registry.
///
/// The host supplies only the generic credential-lifecycle authority required
/// by MFG's long-lived live projection. MFG receives neither Gateway state nor
/// a credential/token, and its router is then mounted by `AppRegistry`.
pub fn register_mfg(
    registry: &mut AppRegistry,
    config_home: PathBuf,
    authorization: Arc<dyn MfgLiveAuthorization>,
    host_context: CowdAppContext,
) -> Result<(), AppRegistryError> {
    registry.register(contribution_with_live_host(MfgLiveHost::new(
        config_home,
        authorization,
        Arc::new(CowdSdkMfgHostEffects::new(host_context)),
    )))
}

/// Register MFG with the production broker lifecycle authority.
pub fn register_mfg_with_broker(
    registry: &mut AppRegistry,
    config_home: PathBuf,
    host_context: CowdAppContext,
) -> Result<(), AppRegistryError> {
    register_mfg(
        registry,
        config_home,
        Arc::new(BrokerCredentialLifecycleAuthorization::required()),
        host_context,
    )
}

/// Register MFG for an embedded local product where no broker process is
/// configured. This is intentionally explicit so production callers cannot
/// silently weaken live-stream revocation checks.
pub fn register_mfg_embedded_trusted(
    registry: &mut AppRegistry,
    config_home: PathBuf,
    host_context: CowdAppContext,
) -> Result<(), AppRegistryError> {
    register_mfg(
        registry,
        config_home,
        Arc::new(BrokerCredentialLifecycleAuthorization::embedded_trusted()),
        host_context,
    )
}

/// Return the generic MFG descriptor for product code that needs to compose
/// authentication before its HTTP host is fully assembled.  This exposes no
/// MFG domain type or handler to Gateway.
#[must_use]
pub fn mfg_app_descriptor() -> AppDescriptor {
    cowd_app_mfg_adapter::mfg_descriptor()
}

/// Forward MFG-owned OpenAPI schemas through product composition. Gateway can
/// aggregate APP documentation without recreating request DTOs that belong to
/// the external MFG adapter.
pub fn register_mfg_openapi_schemas(registry: &mut app_mfg_contract::MfgOpenApiSchemaRegistry) {
    cowd_app_mfg_adapter::register_mfg_openapi_schemas(registry);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_exposes_the_real_external_mfg_descriptor_before_host_assembly() {
        let descriptor = mfg_app_descriptor();
        assert_eq!(descriptor.id.as_str(), "mfg");
        assert_eq!(descriptor.routes.len(), 104);
        assert_eq!(descriptor.sdk_api, cowd_app_sdk::SDK_API_VERSION);
    }
}
