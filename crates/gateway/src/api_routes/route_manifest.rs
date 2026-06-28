use std::collections::BTreeSet;

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct GatewayRouteManifestEntry {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) group: String,
    pub(crate) owner: &'static str,
    pub(crate) criticality: &'static str,
    pub(crate) stability: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct GatewayRouteSource {
    file: &'static str,
    source: &'static str,
}

const ROUTE_SOURCES: &[GatewayRouteSource] = &[
    source("agent_routes.rs", include_str!("agent_routes.rs")),
    source("approval_routes.rs", include_str!("approval_routes.rs")),
    source("audit_routes.rs", include_str!("audit_routes.rs")),
    source("channel_routes.rs", include_str!("channel_routes.rs")),
    source("connector_routes.rs", include_str!("connector_routes.rs")),
    source(
        "connector_routes/mcp.rs",
        include_str!("connector_routes/mcp.rs"),
    ),
    source(
        "connector_routes/resources.rs",
        include_str!("connector_routes/resources.rs"),
    ),
    source(
        "connector_routes/tools.rs",
        include_str!("connector_routes/tools.rs"),
    ),
    source("context_routes.rs", include_str!("context_routes.rs")),
    source("core_routes.rs", include_str!("core_routes.rs")),
    source(
        "cross_plane_routes.rs",
        include_str!("cross_plane_routes.rs"),
    ),
    source("growth_routes.rs", include_str!("growth_routes.rs")),
    source(
        "harness_eval_routes.rs",
        include_str!("harness_eval_routes.rs"),
    ),
    source("matrix_routes.rs", include_str!("matrix_routes.rs")),
    source(
        "matrix_routes/entities.rs",
        include_str!("matrix_routes/entities.rs"),
    ),
    source(
        "matrix_routes/evidence.rs",
        include_str!("matrix_routes/evidence.rs"),
    ),
    source(
        "matrix_routes/metrics.rs",
        include_str!("matrix_routes/metrics.rs"),
    ),
    source(
        "matrix_routes/source.rs",
        include_str!("matrix_routes/source.rs"),
    ),
    source("memory_routes.rs", include_str!("memory_routes.rs")),
    source("message_routes.rs", include_str!("message_routes.rs")),
    source("mfg_routes.rs", include_str!("mfg_routes.rs")),
    source(
        "mfg_routes/cockpit.rs",
        include_str!("mfg_routes/cockpit.rs"),
    ),
    source(
        "mfg_routes/decision.rs",
        include_str!("mfg_routes/decision.rs"),
    ),
    source(
        "mfg_routes/incidents.rs",
        include_str!("mfg_routes/incidents.rs"),
    ),
    source("mission_routes.rs", include_str!("mission_routes.rs")),
    source("profile_routes.rs", include_str!("profile_routes.rs")),
    source("public_routes.rs", include_str!("public_routes.rs")),
    source("reality_routes.rs", include_str!("reality_routes.rs")),
    source("runtime_routes.rs", include_str!("runtime_routes.rs")),
    source(
        "runtime_routes/control.rs",
        include_str!("runtime_routes/control.rs"),
    ),
    source(
        "runtime_routes/control/agent_value.rs",
        include_str!("runtime_routes/control/agent_value.rs"),
    ),
    source(
        "runtime_routes/control/health.rs",
        include_str!("runtime_routes/control/health.rs"),
    ),
    source(
        "runtime_routes/control/value_loop.rs",
        include_str!("runtime_routes/control/value_loop.rs"),
    ),
    source(
        "runtime_routes/control/workgraph.rs",
        include_str!("runtime_routes/control/workgraph.rs"),
    ),
    source("session_routes.rs", include_str!("session_routes.rs")),
    source("skill_routes.rs", include_str!("skill_routes.rs")),
    source("slash_routes.rs", include_str!("slash_routes.rs")),
    source("surface_routes.rs", include_str!("surface_routes.rs")),
    source("system_routes.rs", include_str!("system_routes.rs")),
    source("task_routes.rs", include_str!("task_routes.rs")),
    source("workspace_routes.rs", include_str!("workspace_routes.rs")),
];

const fn source(file: &'static str, source: &'static str) -> GatewayRouteSource {
    GatewayRouteSource { file, source }
}

pub(crate) fn gateway_route_manifest() -> Vec<GatewayRouteManifestEntry> {
    let mut entries = BTreeSet::new();
    for source in ROUTE_SOURCES {
        for (method, path) in parse_routes(source.source) {
            let criticality = route_criticality(&path);
            let stability = route_stability(&path);
            entries.insert(GatewayRouteManifestEntry {
                method,
                path,
                group: route_group(source.file),
                owner: "gateway",
                criticality,
                stability,
            });
        }
    }
    entries.into_iter().collect()
}

fn parse_routes(source: &str) -> Vec<(&'static str, String)> {
    let mut routes = Vec::new();
    let mut rest = source;
    while let Some(index) = rest.find(".route(") {
        rest = &rest[index + ".route(".len()..];
        let trimmed = rest.trim_start();
        let Some(path_start) = trimmed.strip_prefix('"') else {
            continue;
        };
        let Some(path_end) = path_start.find('"') else {
            continue;
        };
        let path = path_start[..path_end].to_string();
        let after_path = &path_start[path_end..];
        let handler_end = after_path
            .find(".route(")
            .unwrap_or(after_path.len())
            .min(512);
        let handler_window = &after_path[..handler_end];
        for (needle, method) in [
            ("get(", "GET"),
            ("post(", "POST"),
            ("put(", "PUT"),
            ("patch(", "PATCH"),
            ("delete(", "DELETE"),
        ] {
            if handler_window.contains(needle) {
                routes.push((method, path.clone()));
            }
        }
    }
    routes
}

fn route_group(file: &str) -> String {
    file.split_once("_routes")
        .map(|(group, _)| group)
        .unwrap_or("gateway")
        .to_string()
}

fn route_criticality(path: &str) -> &'static str {
    if path.contains("/actions/")
        || path.contains("/release-gate")
        || path.contains("/surfaces")
        || path.contains("/sessions")
        || path.contains("/runtime")
    {
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
        assert!(has("GET", "/api/cowd/release-gate"));
    }

    #[test]
    fn route_manifest_has_unique_method_path_entries() {
        let manifest = gateway_route_manifest();
        let unique = manifest
            .iter()
            .map(|entry| (entry.method, entry.path.as_str()))
            .collect::<BTreeSet<_>>();

        assert_eq!(unique.len(), manifest.len());
        assert!(manifest.len() > 50);
    }
}
