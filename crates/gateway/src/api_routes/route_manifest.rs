use std::collections::BTreeSet;

use serde::Serialize;

use super::route_registry::{
    generated_route_metadata, typed_route_metadata, GeneratedRouteMetadata,
};

/// Public, deterministic route inventory. Its source is the build-generated
/// registry plus the small typed registration family whose paths are Rust
/// constants rather than literals. Runtime callers never inspect source text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct GatewayRouteManifestEntry {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) group: String,
    pub(crate) owner: String,
    pub(crate) criticality: &'static str,
    pub(crate) stability: &'static str,
    pub(crate) source: String,
    pub(crate) handler: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app: Option<GatewayAppSemanticMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct GatewayAppSemanticMetadata {
    pub(crate) route_id: String,
    pub(crate) request_schema: String,
    pub(crate) response_schema: String,
    pub(crate) class: String,
    pub(crate) capability: String,
    pub(crate) risk: String,
    pub(crate) confirmation: String,
    pub(crate) emits_live_event: bool,
    pub(crate) auth_error_schema: Option<String>,
}

/// Full product manifest used by build-time contract tests.  Runtime callers
/// must use [`gateway_route_manifest_for_apps`] so a disabled APP is never
/// advertised as an available route family.
pub(crate) fn gateway_route_manifest() -> Vec<GatewayRouteManifestEntry> {
    gateway_route_manifest_with_apps(Vec::new())
}

pub(crate) fn gateway_route_manifest_for_apps(
    app_registry: &cowd_app_host::AppRegistry,
) -> Vec<GatewayRouteManifestEntry> {
    gateway_route_manifest_with_apps(app_registry.route_metadata())
}

fn gateway_route_manifest_with_apps(
    app_routes: Vec<cowd_app_host::RegisteredAppRouteMetadata>,
) -> Vec<GatewayRouteManifestEntry> {
    let mut entries = BTreeSet::new();
    for route in generated_route_metadata() {
        entries.insert(manifest_entry(route));
    }
    // Typed routes are registered through constant specs, so there is no
    // literal `.route("...")` for the build generator to collect.
    for route in typed_route_metadata() {
        // Session execution/evidence routes have literal Axum registrations,
        // so the build-generated inventory already owns their manifest row.
        // The typed metadata enriches OpenAPI response schemas, but must not
        // produce a second public method/path entry with a different source.
        if !entries.iter().any(|entry: &GatewayRouteManifestEntry| {
            entry.method == route.method && entry.path == route.path
        }) {
            entries.insert(GatewayRouteManifestEntry {
                method: route.method.to_string(),
                path: route.path,
                group: "route_registry".to_string(),
                owner: "gateway".to_string(),
                criticality: route_criticality("runtime"),
                stability: "stable",
                source: "route_registry.rs".to_string(),
                handler: route.operation_id,
                app: None,
            });
        }
    }
    // APP routers are composed at startup and are therefore not visible to
    // the literal-route build scanner. Complete the public inventory from the
    // static APP contract, preserving any Gateway-owned route entry already
    // found above. The resulting metadata explicitly records that these are
    // APP-owned handlers rather than synthetic Gateway handlers.
    for registered in app_routes
        .into_iter()
        .filter(|registered| registered.route.active)
    {
        let contract = registered.route;
        let auth_error_schema = registered.auth_error_schema;
        if entries.iter().any(|entry: &GatewayRouteManifestEntry| {
            entry.method == contract.method && entry.path == contract.path
        }) {
            continue;
        }
        entries.insert(app_manifest_entry(
            registered.app_id,
            contract,
            auth_error_schema,
        ));
    }
    entries.into_iter().collect()
}

fn manifest_entry(route: &GeneratedRouteMetadata) -> GatewayRouteManifestEntry {
    GatewayRouteManifestEntry {
        method: route.method.to_string(),
        path: route.path.to_string(),
        group: route_group(route.source),
        owner: "gateway".to_string(),
        criticality: route_criticality(route.path),
        stability: route_stability(route.path),
        source: route.source.to_string(),
        handler: route.handler.to_string(),
        app: None,
    }
}

fn app_manifest_entry(
    app_id: cowd_app_sdk::AppId,
    contract: cowd_app_sdk::AppRouteMetadata,
    auth_error_schema: Option<String>,
) -> GatewayRouteManifestEntry {
    GatewayRouteManifestEntry {
        method: contract.method.clone(),
        path: contract.path.to_string(),
        group: "app".to_string(),
        owner: format!("app:{app_id}"),
        criticality: route_criticality(&contract.path),
        stability: "stable",
        source: format!("app_registry:{app_id}"),
        handler: contract.route_id.clone(),
        app: Some(app_semantic_metadata(contract, auth_error_schema)),
    }
}

fn app_semantic_metadata(
    contract: cowd_app_sdk::AppRouteMetadata,
    auth_error_schema: Option<String>,
) -> GatewayAppSemanticMetadata {
    GatewayAppSemanticMetadata {
        route_id: contract.route_id,
        request_schema: contract.request_schema,
        response_schema: contract.response_schema,
        class: contract.class,
        capability: contract.capability,
        risk: contract.risk,
        confirmation: contract.confirmation,
        emits_live_event: contract.streaming,
        auth_error_schema,
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
        assert!(has("POST", "/api/skills"));
        assert!(has("DELETE", "/api/skills/:id"));
        assert!(has("GET", "/api/evolution/evaluation-policy"));
        assert!(has("GET", "/api/evolution/evaluation-policy/reviews"));
        assert!(has(
            "POST",
            "/api/evolution/evaluation-policy/reviews/:id/decision"
        ));
        assert!(has("GET", "/api/runtime/managed-agents"));
        assert!(has("POST", "/api/runtime/managed-agents/definitions"));
        assert!(has("DELETE", "/api/runtime/managed-agents/definitions/:id"));
        assert!(has("POST", "/api/runtime/managed-agents/:id/trigger"));
        assert!(has("POST", "/api/runtime/managed-agents/dispatch"));
        assert!(has("POST", "/api/runtime/managed-agents/events"));
        assert!(has("GET", "/api/runtime/managed-agents/effects"));
        assert!(has("GET", "/api/surfaces/:id/trigger-events"));
        assert!(has("POST", "/api/surfaces/:id/trigger-events/retry"));
        assert!(has("GET", "/api/cowd/release-gate"));
        assert!(has("POST", "/api/resources"));
        assert!(has("GET", "/api/resources/:id/content"));
        assert!(manifest
            .iter()
            .any(|entry| { entry.path == "/api/approval/pending" && entry.criticality == "p1" }));
    }

    #[test]
    fn route_manifest_is_generated_and_has_unique_method_path_entries() {
        let manifest = gateway_route_manifest();
        let unique = manifest
            .iter()
            .map(|entry| (entry.method.as_str(), entry.path.as_str()))
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
    fn runtime_manifest_does_not_advertise_an_unregistered_app() {
        let manifest = gateway_route_manifest_for_apps(&cowd_app_host::AppRegistry::default());
        assert!(manifest
            .iter()
            .all(|entry| !entry.path.starts_with("/api/apps/mfg/")));
    }
}
