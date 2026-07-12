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
/// * typed execution-projection registrations carry their own stable schema.
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
    assert!(registry.contains("execution_projection_events_spec"));
    assert!(registry.contains("execution_projection_command_spec"));
    assert!(registry.contains("register_execution_projection_routes"));

    let runtime_routes_source = read_repo("crates/gateway/src/api_routes/runtime_routes.rs");
    let runtime_routes = production_part(&runtime_routes_source);
    assert!(runtime_routes.contains("route_registry::register_execution_projection_routes"));

    let route_manifest_source = read_repo("crates/gateway/src/api_routes/route_manifest.rs");
    let route_manifest = production_part(&route_manifest_source);
    assert!(route_manifest.contains("generated_route_metadata"));
    assert!(route_manifest.contains("execution_projection_route_metadata"));
    assert!(
        !route_manifest.contains("std::fs::read_to_string")
            && !route_manifest.contains("fs::read_to_string")
            && !route_manifest.contains("parse_routes("),
        "runtime route manifest must consume generated/typed metadata rather than source text"
    );
}

#[test]
fn capability_contract_consumes_typed_route_metadata() {
    let capability_contract_source =
        read_repo("crates/gateway/src/api_routes/capability_contract.rs");
    let capability_contract = production_part(&capability_contract_source);
    assert!(capability_contract.contains("route_registry::stable_route_metadata"));
    assert!(capability_contract.contains("stable_route_metadata(&capability.http.method"));

    let api_routes_source = read_repo("crates/gateway/src/api_routes/mod.rs");
    // `api_routes/mod.rs` has test-only imports near its header, so splitting
    // at the first cfg(test) marker would discard the production module list.
    let api_routes = api_routes_source.as_str();
    assert!(api_routes.contains("mod route_registry;"));
    assert!(api_routes.contains("mod route_manifest;"));
}
