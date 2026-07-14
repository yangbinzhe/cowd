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
}

pub(crate) fn gateway_route_manifest() -> Vec<GatewayRouteManifestEntry> {
    let mut entries = BTreeSet::new();
    for route in generated_route_metadata() {
        entries.insert(manifest_entry(route));
    }
    // Typed routes are registered through constant specs, so there is no
    // literal `.route("...")` for the build generator to collect.
    for route in execution_projection_route_metadata() {
        let generated = GeneratedRouteMetadata {
            method: route.method,
            path: route.path,
            source: "route_registry.rs",
            handler: route.operation_id,
        };
        entries.insert(manifest_entry(&generated));
    }
    entries.into_iter().collect()
}

fn manifest_entry(route: &GeneratedRouteMetadata) -> GatewayRouteManifestEntry {
    GatewayRouteManifestEntry {
        method: route.method,
        path: route.path.to_string(),
        group: route_group(route.source),
        owner: "gateway",
        criticality: route_criticality(route.path),
        stability: route_stability(route.path),
        source: route.source.to_string(),
        handler: route.handler.to_string(),
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
}
