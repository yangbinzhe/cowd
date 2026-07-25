#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_repo(path: &str) -> String {
    std::fs::read_to_string(repo_root().join(path))
        .unwrap_or_else(|error| panic!("{path} should be readable: {error}"))
}

fn production_part(source: &str) -> &str {
    source.split("#[cfg(test)]").next().unwrap_or(source)
}

/// Gateway route inventory has two deliberate source paths only:
///
/// * literal Axum registrations are collected once at build time;
/// * typed execution-projection and multiplex-live registrations carry their
///   own stable schemas.
///
/// The runtime manifest/OpenAPI path must consume those artifacts, never parse
/// Rust source while serving a request. Keeping this as a source test makes a
/// future reintroduction of a second hand-maintained route list fail loudly.
#[test]
fn route_inventory_has_single_build_or_typed_source_of_truth() {
    let build_source = read_repo("crates/gateway/build.rs");
    let build = production_part(&build_source);
    assert!(build.contains("generate_route_registry()"));
    assert!(build.contains("collect_route_sources"));
    assert!(build.contains("parse_routes"));
    assert!(build.contains("gateway_route_registry.rs"));
    assert!(build.contains("cargo:rerun-if-changed"));

    let registry_source = read_repo("crates/gateway/src/api_routes/route_registry.rs");
    let registry = production_part(&registry_source);
    assert!(
        registry.contains("include!(concat!(env!(\"OUT_DIR\"), \"/gateway_route_registry.rs\"))")
    );
    assert!(registry.contains("pub(crate) struct TypedRouteSpec"));
    assert!(registry.contains("execution_projection_snapshot_spec"));
    assert!(registry.contains("execution_projection_command_spec"));
    assert!(registry.contains("live_create_spec"));
    assert!(registry.contains("live_patch_spec"));
    assert!(registry.contains("live_delete_spec"));
    assert!(registry.contains("live_stream_spec"));
    assert!(!registry.contains("execution_projection_events_spec"));
    assert!(registry.contains("register_execution_projection_routes"));

    let runtime_routes_source = read_repo("crates/gateway/src/api_routes/runtime_routes.rs");
    let runtime_routes = production_part(&runtime_routes_source);
    assert!(runtime_routes.contains("route_registry::register_execution_projection_routes"));

    let route_manifest_source = read_repo("crates/gateway/src/api_routes/route_manifest.rs");
    let route_manifest = production_part(&route_manifest_source);
    assert!(route_manifest.contains("generated_route_metadata"));
    assert!(route_manifest.contains("typed_route_metadata"));
    assert!(
        !route_manifest.contains("std::fs::read_to_string")
            && !route_manifest.contains("fs::read_to_string")
            && !route_manifest.contains("parse_routes("),
        "runtime route manifest must consume generated/typed metadata rather than source text"
    );
}

/// Product composition may depend on an external APP, but Gateway production
/// code must see only the bundle/host ABI.  Test fixtures may seed external
/// app data directly, so this deliberately excludes the `cfg(test)` module.
#[test]
fn gateway_production_has_no_direct_mfg_core_or_contract_imports() {
    for path in [
        "crates/gateway/src/api_routes/capability_contract.rs",
        "crates/gateway/src/api_routes/core_routes.rs",
        "crates/gateway/src/api_routes/route_manifest.rs",
        "crates/gateway/src/api_routes/route_registry.rs",
        "crates/gateway/src/api_routes/skill_routes.rs",
        "crates/gateway/src/services/skill_service.rs",
        "crates/gateway/src/services/skill_service/projection.rs",
        "crates/gateway/src/entry/skill_entry.rs",
    ] {
        let owned_source = read_repo(path);
        let source = production_part(&owned_source);
        assert!(
            !source.contains("app_mfg::") && !source.contains("app_mfg_contract::"),
            "{path} must consume the app bundle/host ABI instead of MFG implementation types"
        );
    }

    let api_routes = read_repo("crates/gateway/src/api_routes/mod.rs");
    let production = api_routes
        .split("#[cfg(test)]\npub(crate) mod tests {")
        .next()
        .expect("production api routes");
    assert!(!production.contains("app_mfg::"));
    assert!(!production.contains("app_mfg_contract::"));
    assert!(!production.contains("mod mfg_outcomes;"));

    let manifest = read_repo("crates/gateway/Cargo.toml");
    let normal_dependencies = manifest
        .split("[dev-dependencies]")
        .next()
        .expect("normal dependencies");
    assert!(
        !normal_dependencies
            .lines()
            .any(|line| line.trim_start().starts_with("app-mfg = {")),
        "Gateway must not import the MFG core package directly; the app-mfg feature may only forward to cowd-product-apps"
    );
    assert!(
        !normal_dependencies
            .lines()
            .any(|line| line.trim_start().starts_with("app-mfg-contract = {")),
        "Gateway must not import the MFG contract package directly"
    );
}
