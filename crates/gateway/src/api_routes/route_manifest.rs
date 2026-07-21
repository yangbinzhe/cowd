use std::collections::BTreeSet;

use serde::Serialize;

use super::route_registry::{
    execution_projection_route_metadata, generated_route_metadata, GeneratedRouteMetadata,
};

/// Public, deterministic route inventory. Its source is the build-generated
/// registry plus the small typed registration family whose paths are Rust
/// constants rather than literals. Runtime callers never inspect source text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct GatewayRouteManifestEntry {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) group: String,
    pub(crate) owner: &'static str,
    pub(crate) criticality: &'static str,
    pub(crate) stability: &'static str,
    pub(crate) source: String,
    pub(crate) handler: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mfg: Option<GatewayMfgSemanticMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct GatewayMfgSemanticMetadata {
    pub(crate) route_id: String,
    pub(crate) request_schema: String,
    pub(crate) response_schema: String,
    pub(crate) class: String,
    pub(crate) capability: String,
    pub(crate) risk: String,
    pub(crate) confirmation: String,
    pub(crate) emits_live_event: bool,
}

pub(crate) fn gateway_route_manifest() -> Vec<GatewayRouteManifestEntry> {
    let mut entries = BTreeSet::new();
    for route in generated_route_metadata() {
        entries.insert(manifest_entry(route));
    }
    // Typed routes are registered through constant specs, so there is no
    // literal `.route("...")` for the build generator to collect.
    for route in execution_projection_route_metadata() {
        // Session execution/evidence routes have literal Axum registrations,
        // so the build-generated inventory already owns their manifest row.
        // The typed metadata enriches OpenAPI response schemas, but must not
        // produce a second public method/path entry with a different source.
        if !entries.iter().any(|entry: &GatewayRouteManifestEntry| {
            entry.method == route.method && entry.path == route.path
        }) {
            entries.insert(GatewayRouteManifestEntry {
                method: route.method,
                path: route.path,
                group: "route_registry".to_string(),
                owner: "gateway",
                criticality: route_criticality("runtime"),
                stability: "stable",
                source: "route_registry.rs".to_string(),
                handler: route.operation_id,
                mfg: None,
            });
        }
    }
    // APP routers are composed at startup and are therefore not visible to
    // the literal-route build scanner. Complete the public inventory from the
    // static APP contract, preserving any Gateway-owned route entry already
    // found above. The resulting metadata explicitly records that these are
    // APP-owned handlers rather than synthetic Gateway handlers.
    for contract in app_bundle_mfg::mfg_route_metadata()
        .into_iter()
        .filter(|route| route.active)
    {
        if entries.iter().any(|entry: &GatewayRouteManifestEntry| {
            entry.method == contract.method && entry.path == contract.path
        }) {
            continue;
        }
        entries.insert(app_mfg_manifest_entry(contract));
    }
    entries.into_iter().collect()
}

fn manifest_entry(route: &GeneratedRouteMetadata) -> GatewayRouteManifestEntry {
    let mfg = app_bundle_mfg::mfg_route_metadata()
        .into_iter()
        .find(|contract| contract.method == route.method && contract.path == route.path)
        .map(mfg_semantic_metadata);
    GatewayRouteManifestEntry {
        method: route.method,
        path: route.path.to_string(),
        group: route_group(route.source),
        owner: "gateway",
        criticality: route_criticality(route.path),
        stability: route_stability(route.path),
        source: route.source.to_string(),
        handler: route.handler.to_string(),
        mfg,
    }
}

fn app_mfg_manifest_entry(contract: app_bundle_mfg::MfgRouteMetadata) -> GatewayRouteManifestEntry {
    GatewayRouteManifestEntry {
        method: mfg_static_method(&contract.method),
        path: contract.path.to_string(),
        group: "app".to_string(),
        owner: "app:mfg",
        criticality: route_criticality(&contract.path),
        stability: "stable",
        source: "app_registry:mfg".to_string(),
        handler: contract.route_id.clone(),
        mfg: Some(mfg_semantic_metadata(contract)),
    }
}

fn mfg_static_method(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        other => panic!("MFG contract has unsupported HTTP method: {other}"),
    }
}

fn mfg_semantic_metadata(contract: app_bundle_mfg::MfgRouteMetadata) -> GatewayMfgSemanticMetadata {
    GatewayMfgSemanticMetadata {
        route_id: contract.route_id,
        request_schema: contract.request_schema,
        response_schema: contract.response_schema,
        class: contract.class,
        capability: contract.capability,
        risk: contract.risk,
        confirmation: contract.confirmation,
        emits_live_event: contract.streaming,
    }
}

fn route_group(file: &str) -> String {
    file.split_once("_routes")
        .map(|(group, _)| group)
        .unwrap_or("gateway")
        .to_string()
}

fn route_criticality(path: &str) -> &'static str {
    const P1_TOKENS: &[&str] = &[
        "/actions/",
        "/approval",
        "/context",
        "/cross-plane",
        "/matrix",
        "/memory",
        "/mission",
        "/reality",
        "/release-gate",
        "/resources",
        "/runtime",
        "/sessions",
        "/surfaces",
        "/tools",
        "/workspace",
    ];

    if P1_TOKENS.iter().any(|token| path.contains(token)) {
        "p1"
    } else {
        "p2"
    }
}

fn route_stability(path: &str) -> &'static str {
    if path.starts_with("/api/") {
        "stable"
    } else {
        "surface"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_manifest_contains_skill_lifecycle_routes() {
        let manifest = gateway_route_manifest();
        let has = |method: &str, path: &str| {
            manifest
                .iter()
                .any(|entry| entry.method == method && entry.path == path)
        };

        assert!(has("GET", "/api/skills/runs"));
        assert!(has("GET", "/api/skills/runs/:id"));
        assert!(has("POST", "/api/skills/:id/actions/validate"));
        assert!(has("POST", "/api/skills/:id/actions/plan"));
        assert!(has("POST", "/api/skills/:id/actions/run"));
        assert!(has("GET", "/api/evolution/evaluation-policy"));
        assert!(has("GET", "/api/evolution/evaluation-policy/reviews"));
        assert!(has(
            "POST",
            "/api/evolution/evaluation-policy/reviews/:id/decision"
        ));
        assert!(has("GET", "/api/runtime/managed-agents"));
        assert!(has("POST", "/api/runtime/managed-agents/definitions"));
        assert!(has("POST", "/api/runtime/managed-agents/:id/trigger"));
        assert!(has("POST", "/api/runtime/managed-agents/dispatch"));
        assert!(has("POST", "/api/runtime/managed-agents/events"));
        assert!(has("GET", "/api/runtime/managed-agents/effects"));
        assert!(has("GET", "/api/surfaces/:id/trigger-events"));
        assert!(has("POST", "/api/surfaces/:id/trigger-events/retry"));
        assert!(has("GET", "/api/cowd/release-gate"));
        assert!(has("POST", "/api/resources"));
        assert!(manifest
            .iter()
            .any(|entry| { entry.path == "/api/approval/pending" && entry.criticality == "p1" }));
    }

    #[test]
    fn route_manifest_is_generated_and_has_unique_method_path_entries() {
        let manifest = gateway_route_manifest();
        let unique = manifest
            .iter()
            .map(|entry| (entry.method, entry.path.as_str()))
            .collect::<BTreeSet<_>>();

        assert_eq!(unique.len(), manifest.len());
        assert!(manifest.len() > 50);
        assert!(manifest.iter().all(|entry| !entry.source.is_empty()));
        assert!(manifest.iter().all(|entry| !entry.handler.is_empty()));
        assert!(manifest.iter().any(|entry| {
            entry.path == "/api/gateway/route-manifest"
                && entry.handler == "route_manifest_handler"
                && entry.source == "public_routes.rs"
        }));
        assert!(generated_route_metadata().len() > 50);
    }

    #[test]
    fn active_mfg_contract_matches_global_route_inventory_bidirectionally() {
        let manifest = gateway_route_manifest()
            .into_iter()
            .filter(|entry| entry.path.starts_with("/api/apps/mfg/"))
            .map(|entry| (entry.method.to_string(), entry.path))
            .collect::<BTreeSet<_>>();
        let contract = app_bundle_mfg::mfg_route_metadata()
            .into_iter()
            .filter(|route| route.active)
            .map(|route| (route.method, route.path))
            .collect::<BTreeSet<_>>();
        assert_eq!(contract.len(), 104);
        assert_eq!(manifest, contract);
        let external_contract = gateway_route_manifest()
            .into_iter()
            .find(|entry| entry.method == "GET" && entry.path == "/api/apps/mfg/contract")
            .expect("external MFG contract route is inventoried");
        assert_eq!(external_contract.owner, "app:mfg");
        assert_eq!(external_contract.source, "app_registry:mfg");
        assert_eq!(external_contract.handler, "mfg.contract.get");
    }
}
