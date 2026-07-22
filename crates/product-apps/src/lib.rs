//! Static product composition for all reviewed Cowd applications.
//!
//! The generated catalogue chooses which external APP bundles participate in
//! this binary.  This crate applies only generic descriptor, registration and
//! terminal-surface operations; it contains no product-domain logic.

use std::{collections::BTreeSet, path::Path};

use cowd_app_host::{AppRegistry, AppRegistryError, StaticAppProduct, TuiAppSurfaceContribution};
use cowd_app_sdk::{AppDescriptor, AppStorageBackend, AppStorageScope, CowdAppContext};
use thiserror::Error;

mod generated;

/// Every APP bundle statically linked into this build.
#[must_use]
pub fn compiled_products() -> Vec<StaticAppProduct> {
    generated::compiled_products()
}

/// Descriptors that the runtime startup policy admits for this process.
#[must_use]
pub fn enabled_descriptors(is_enabled: &dyn Fn(&str) -> bool) -> Vec<AppDescriptor> {
    compiled_products()
        .into_iter()
        .filter_map(|product| {
            let descriptor = product.descriptor();
            is_enabled(descriptor.id.as_str()).then_some(descriptor)
        })
        .collect()
}

/// Resolves every enabled APP's declarative storage requirements into the
/// host-owned inventory. Product code can name its data domain but cannot
/// choose a path, connection string, or another APP's namespace.
pub fn registry_with_enabled_app_storage(
    mut registry: storage::StorageRegistry,
    is_enabled: &dyn Fn(&str) -> bool,
) -> Result<storage::StorageRegistry, AppStorageResolutionError> {
    let mut registered = BTreeSet::new();
    for product in compiled_products() {
        let descriptor = product.descriptor();
        if !is_enabled(descriptor.id.as_str()) {
            continue;
        }
        for requirement in product.storage_requirements() {
            requirement.validate(&descriptor.id)?;
            if !registered.insert((
                descriptor.id.as_str().to_string(),
                requirement.domain.clone(),
                requirement.scope.clone(),
            )) {
                return Err(AppStorageResolutionError::DuplicateRequirement {
                    app_id: descriptor.id.to_string(),
                    domain: requirement.domain,
                });
            }
            if requirement.backend != AppStorageBackend::Sqlite
                || requirement.scope != AppStorageScope::App
            {
                return Err(AppStorageResolutionError::UnsupportedRequirement {
                    app_id: descriptor.id.to_string(),
                    domain: requirement.domain,
                });
            }
            registry = registry.with_app_sqlite(
                descriptor.id.as_str(),
                requirement.domain,
                requirement.migration,
            )?;
        }
    }
    Ok(registry)
}

#[derive(Debug, Error)]
pub enum AppStorageResolutionError {
    #[error(transparent)]
    Contract(#[from] cowd_app_sdk::AppContractError),
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error("duplicate storage requirement `{domain}` declared by app {app_id}")]
    DuplicateRequirement { app_id: String, domain: String },
    #[error("unsupported storage requirement `{domain}` declared by app {app_id}")]
    UnsupportedRequirement { app_id: String, domain: String },
}

/// Mount all statically linked APPs allowed by the runtime startup policy.
/// The policy may disable a reviewed product contribution but can never load
/// new code or select an arbitrary source revision.
pub fn register_enabled(
    registry: &mut AppRegistry,
    config_home: &Path,
    context: CowdAppContext,
    is_enabled: &dyn Fn(&str) -> bool,
) -> Result<(), AppRegistryError> {
    for product in compiled_products() {
        let app_id = product.app_id();
        if is_enabled(app_id.as_str()) {
            product.register(registry, config_home.to_path_buf(), context.clone())?;
        }
    }
    Ok(())
}

/// All application terminal surfaces included in this build. The TUI filters
/// this result by Gateway's enabled APP identifiers before displaying a panel.
#[must_use]
pub fn tui_surface_contributions() -> Vec<TuiAppSurfaceContribution> {
    compiled_products()
        .into_iter()
        .filter_map(StaticAppProduct::tui_surface)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn compiled_products_have_valid_unique_descriptors() {
        let mut ids = BTreeSet::new();
        for product in compiled_products() {
            let descriptor = product.descriptor();
            descriptor.validate().expect("static APP descriptor");
            assert!(ids.insert(descriptor.id));
        }
    }

    #[test]
    fn enabled_product_storage_is_resolved_from_app_declaration() {
        let registry = registry_with_enabled_app_storage(
            storage::StorageRegistry::default_for_config_home("/tmp/cowd-product-storage"),
            &|app_id| app_id == "mfg",
        )
        .expect("declared MFG storage must resolve");
        let scope = storage::StorageScope::App {
            app_id: "mfg".to_string(),
        };
        let endpoint = registry
            .endpoint_in_scope(&storage::StorageDomainId::app("mfg", "primary"), &scope)
            .expect("MFG declared endpoint");
        assert!(endpoint
            .as_handle()
            .path
            .ends_with("storage/apps/mfg/primary.sqlite"));

        let disabled = registry_with_enabled_app_storage(
            storage::StorageRegistry::default_for_config_home("/tmp/cowd-product-storage"),
            &|_| false,
        )
        .expect("disabled app has no storage collision");
        assert!(disabled
            .endpoint_in_scope(&storage::StorageDomainId::app("mfg", "primary"), &scope)
            .is_err());
    }
}
