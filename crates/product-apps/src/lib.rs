//! Static product composition for all reviewed Cowd applications.
//!
//! The generated catalogue chooses which external APP bundles participate in
//! this binary.  This crate applies only generic descriptor, registration and
//! terminal-surface operations; it contains no product-domain logic.

use std::path::Path;

use cowd_app_host::{AppRegistry, AppRegistryError, StaticAppProduct, TuiAppSurfaceContribution};
use cowd_app_sdk::{AppDescriptor, CowdAppContext};

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
}
