//! Cowd product composition for the first-party MFG APP.
//!
//! This crate is intentionally the only Cowd crate that imports an MFG
//! package. It owns no MFG DTO, route, store or policy: it only turns the
//! external APP's contribution factory into a registered product capability.

use std::{path::PathBuf, sync::Arc};

use cowd_app_host::{AppRegistry, AppRegistryError};
use cowd_app_mfg_adapter::{
    contribution_with_live_host, MfgLiveAuthorization, MfgLiveAuthorizationFailure, MfgLiveHost,
    MfgLivePrincipalContext,
};

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
) -> Result<(), AppRegistryError> {
    registry.register(contribution_with_live_host(MfgLiveHost::new(
        config_home,
        authorization,
    )))
}

/// Register MFG with the production broker lifecycle authority.
pub fn register_mfg_with_broker(
    registry: &mut AppRegistry,
    config_home: PathBuf,
) -> Result<(), AppRegistryError> {
    register_mfg(
        registry,
        config_home,
        Arc::new(BrokerCredentialLifecycleAuthorization::required()),
    )
}

/// Register MFG for an embedded local product where no broker process is
/// configured. This is intentionally explicit so production callers cannot
/// silently weaken live-stream revocation checks.
pub fn register_mfg_embedded_trusted(
    registry: &mut AppRegistry,
    config_home: PathBuf,
) -> Result<(), AppRegistryError> {
    register_mfg(
        registry,
        config_home,
        Arc::new(BrokerCredentialLifecycleAuthorization::embedded_trusted()),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cowd_app_mfg_adapter::{MfgLiveAuthorizationFailure, MfgLivePrincipalContext};

    use super::*;

    struct FixtureAuthorization;

    impl MfgLiveAuthorization for FixtureAuthorization {
        fn verify(
            &self,
            _config_home: &std::path::Path,
            _principal: &MfgLivePrincipalContext,
        ) -> Result<(), MfgLiveAuthorizationFailure> {
            Ok(())
        }
    }

    #[test]
    fn bundle_is_the_real_external_mfg_factory_consumer() {
        let mut registry = AppRegistry::default();
        register_mfg(
            &mut registry,
            std::env::temp_dir(),
            Arc::new(FixtureAuthorization),
        )
        .expect("MFG contribution registers");
        let apps = registry.apps();
        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].descriptor.id.as_str(), "mfg");
        assert_eq!(apps[0].descriptor.routes.len(), 104);
        assert!(apps[0].http_registered);
        assert!(apps[0].tui_registered);
    }
}
