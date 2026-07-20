//! Cowd product composition for the first-party MFG APP.
//!
//! This crate is intentionally the only Cowd crate that imports an MFG
//! package. It owns no MFG DTO, route, store or policy: it only turns the
//! external APP's contribution factory into a registered product capability.

use std::{path::PathBuf, sync::Arc};

use cowd_app_host::{AppRegistry, AppRegistryError};
use cowd_app_mfg_adapter::{contribution_with_live_host, MfgLiveAuthorization, MfgLiveHost};

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
