use std::collections::BTreeSet;

use serde::Serialize;

use super::{
    binding::{GatewayRouteBinding, GATEWAY_ROUTE_BINDINGS},
    route_registry::typed_route_metadata,
};

/// Public, deterministic route inventory derived from the Surface route
/// catalog and Gateway's handler binding adapter.
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
    gateway_route_manifest_with_apps()
}

pub(crate) fn gateway_route_manifest_for_apps() -> Vec<GatewayRouteManifestEntry> {
    gateway_route_manifest_with_apps()
}

fn gateway_route_manifest_with_apps() -> Vec<GatewayRouteManifestEntry> {
    let mut entries = BTreeSet::new();
    for route in GATEWAY_ROUTE_BINDINGS {
        entries.insert(manifest_entry(route));
    }
    // Typed metadata enriches the Surface-owned transport declarations with
    // schemas and writer policy; it never creates a second public route.
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
    entries.into_iter().collect()
}

fn manifest_entry(binding: &GatewayRouteBinding) -> GatewayRouteManifestEntry {
    let path = binding.route.path().template();
    GatewayRouteManifestEntry {
        method: binding.route.method().as_str().to_string(),
        path: path.to_string(),
        group: route_group(binding.source),
        owner: "gateway".to_string(),
        criticality: route_criticality(path),
        stability: route_stability(path),
        source: binding.source.to_string(),
        handler: binding.handler.to_string(),
        app: None,
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
        assert!(!has("POST", "/api/skills/install"));
        assert!(has("POST", "/api/skills/install/plan"));
        assert!(has("POST", "/api/skills/install/commit"));
        assert!(has("POST", "/api/skills/install/upload/plan"));
        assert!(has("POST", "/api/skills/install/upload/commit"));
        assert!(has("DELETE", "/api/skills/:id"));
        assert!(has("GET", "/api/skills/maintenance"));
        assert!(has("GET", "/api/skills/maintenance/:id"));
        assert!(has(
            "POST",
            "/api/skills/maintenance/:id/activation-reviews"
        ));
        assert!(has("POST", "/api/skills/:id/rollback-reviews"));
        assert!(has("GET", "/api/skills/revision-reviews/:id"));
        assert!(has("POST", "/api/skills/revision-reviews/:id/decision"));
        assert!(has("GET", "/api/skills/:id/active-pointer"));
        assert!(!has("POST", "/api/skills/maintenance/evaluate"));
        assert!(has("GET", "/api/evolution/evaluation-policy"));
        assert!(has("GET", "/api/evolution/collaboration-patterns"));
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
        assert_eq!(GATEWAY_ROUTE_BINDINGS.len(), 482);
    }

    #[test]
    fn dynamic_app_manifest_exposes_the_complete_generic_proxy_surface_only() {
        let manifest = gateway_route_manifest_for_apps();
        let app_routes = manifest
            .iter()
            .filter(|entry| entry.path.starts_with("/api/apps"))
            .map(|entry| (entry.method.as_str(), entry.path.as_str()))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            app_routes,
            BTreeSet::from([
                ("DELETE", "/api/apps/:app_id/subscriptions/:subscription_id"),
                ("GET", "/api/apps"),
                ("GET", "/api/apps/:app_id"),
                ("GET", "/api/apps/:app_id/logs"),
                ("GET", "/api/apps/:app_id/receipts/:receipt_id"),
                ("POST", "/api/apps/:app_id/operations/:operation_id/invoke",),
                ("POST", "/api/apps/:app_id/operations/:operation_id/stream",),
                ("POST", "/api/apps/:app_id/restart"),
                (
                    "POST",
                    "/api/apps/:app_id/subscriptions/:subscription_id/ack",
                ),
                ("POST", "/api/apps/:app_id/tui/views/:view_id/actions"),
                ("POST", "/api/apps/:app_id/tui/views/:view_id/open"),
                ("POST", "/api/apps/:app_id/tui/views/:view_id/stream"),
            ])
        );
        assert!(manifest
            .iter()
            .filter(|entry| entry.path.starts_with("/api/apps"))
            .all(|entry| entry.app.is_none()));
    }

    #[test]
    fn runtime_manifest_does_not_advertise_an_unregistered_app() {
        let manifest = gateway_route_manifest_for_apps();
        assert!(manifest
            .iter()
            .all(|entry| !entry.path.starts_with("/api/apps/mfg/")));
    }
}
