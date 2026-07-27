use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSelfAuditCheck {
    pub id: String,
    pub owner: String,
    pub passed: bool,
    pub summary: String,
    pub evidence: Vec<String>,
    pub repair_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRepairAction {
    pub id: String,
    pub check_id: String,
    pub action: String,
    pub target_files: Vec<String>,
    pub rationale: String,
    pub apply_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSelfAuditReport {
    pub ok: bool,
    pub repo_root: String,
    pub checks: Vec<SourceSelfAuditCheck>,
    pub repair_plan: Vec<SourceRepairAction>,
}

pub struct RuntimeSourceSelfAudit;

impl RuntimeSourceSelfAudit {
    #[must_use]
    pub fn audit_repo(repo_root: impl AsRef<Path>) -> SourceSelfAuditReport {
        let repo_root = repo_root.as_ref();
        let checks = vec![
            runtime_has_no_surface_sdk_dependency(repo_root),
            gateway_owns_surface_boundary(repo_root),
            runtime_host_uses_runtime_service(repo_root),
            growth_routes_are_observable(repo_root),
            ai_eval_has_repair_hints(repo_root),
        ];
        let repair_plan = checks
            .iter()
            .filter(|check| !check.passed)
            .map(repair_action_for)
            .collect::<Vec<_>>();
        SourceSelfAuditReport {
            ok: checks.iter().all(|check| check.passed),
            repo_root: repo_root.display().to_string(),
            checks,
            repair_plan,
        }
    }
}

fn runtime_has_no_surface_sdk_dependency(repo_root: &Path) -> SourceSelfAuditCheck {
    let path = repo_root.join("crates/runtime/Cargo.toml");
    let source = read_source(&path);
    let passed = source.as_deref().is_some_and(|source| {
        !source.contains("channel = ") && !source.contains("channel-adapters")
    });
    check(
        "runtime.no_surface_sdk_dependency",
        "runtime",
        passed,
        "runtime must not depend on external surface SDK crates",
        vec![path],
        "remove external surface SDK dependencies from crates/runtime/Cargo.toml and route external traffic through Gateway Surface services",
    )
}

fn gateway_owns_surface_boundary(repo_root: &Path) -> SourceSelfAuditCheck {
    let path = repo_root.join("crates/gateway/Cargo.toml");
    let message_connector_routes =
        repo_root.join("crates/gateway/src/api_routes/message_connector_routes.rs");
    let source = read_source(&path);
    let route_source = read_source(&message_connector_routes);
    let passed = source.as_deref().is_some_and(|source| {
        source.contains("surface = { path = \"../surface\" }")
            && !source.contains("channel = { path = \"../channel\" }")
            && !source.contains("channel-adapters = ")
    }) && route_source.as_deref().is_some_and(|source| {
        source.contains("surface::message")
            && source.contains("MessageConnectorContract")
            && source.contains("/api/message-connectors")
    });
    check(
        "gateway.owns_surface_boundary",
        "gateway",
        passed,
        "gateway must expose message connector APIs through Surface contracts without adapter SDK coupling",
        vec![path, message_connector_routes],
        "depend on surface, use surface::message contracts, and keep platform SDKs behind JSONL Edge sidecars",
    )
}

fn runtime_host_uses_runtime_service(repo_root: &Path) -> SourceSelfAuditCheck {
    let path = repo_root.join("crates/gateway/src/runtime_host/mod.rs");
    let source = read_source(&path);
    let passed = source.as_deref().is_some_and(|source| {
        source.contains("RuntimeService::new_with_gateway_tasks(")
            && source.contains("runtime_service.runtime_services()")
    });
    check(
        "gateway.runtime_host_uses_runtime_service",
        "gateway-runtime-host",
        passed,
        "external inbound execution must go through RuntimeService",
        vec![path],
        "submit through RuntimeService::run_turn -> StandardRuntimeHost::submit_turn and preserve turn receipts",
    )
}

fn growth_routes_are_observable(repo_root: &Path) -> SourceSelfAuditCheck {
    let routes = repo_root.join("crates/gateway/src/api_routes/mod.rs");
    let growth = repo_root.join("crates/gateway/src/api_routes/growth_routes.rs");
    let routes_source = read_source(&routes);
    let growth_source = read_source(&growth);
    let passed = routes_source.as_deref().is_some_and(|source| {
        source.contains("mod growth_routes") && source.contains("growth_routes::router()")
    }) && growth_source.as_deref().is_some_and(|source| {
        source.contains("/api/growth/events") && source.contains("/api/growth/status")
    });
    check(
        "gateway.growth_observable",
        "growth",
        passed,
        "growth events must be observable through Gateway API",
        vec![routes, growth],
        "register growth_routes in api_router and expose /api/growth/status plus /api/growth/events",
    )
}

fn ai_eval_has_repair_hints(repo_root: &Path) -> SourceSelfAuditCheck {
    let path = repo_root.join("crates/harness-eval/src/lib.rs");
    let source = read_source(&path);
    let passed = source.as_deref().is_some_and(|source| {
        source.contains("repair_hint") && source.contains("FailedScenarioCheck")
    });
    check(
        "harness_eval.repair_hints",
        "harness-eval",
        passed,
        "scenario checks must produce repair hints for self-repair planning",
        vec![path],
        "ensure ScenarioCheck and FailedScenarioCheck carry repair_hint and failed checks preserve it",
    )
}

fn check(
    id: &str,
    owner: &str,
    passed: bool,
    summary: &str,
    target_files: Vec<PathBuf>,
    repair_hint: &str,
) -> SourceSelfAuditCheck {
    SourceSelfAuditCheck {
        id: id.to_string(),
        owner: owner.to_string(),
        passed,
        summary: summary.to_string(),
        evidence: target_files
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        repair_hint: repair_hint.to_string(),
    }
}

fn repair_action_for(check: &SourceSelfAuditCheck) -> SourceRepairAction {
    SourceRepairAction {
        id: format!("repair-{}", check.id),
        check_id: check.id.clone(),
        action: check.repair_hint.clone(),
        target_files: check.evidence.clone(),
        rationale: check.summary.clone(),
        apply_mode: "plan_only_requires_checkpoint_and_tests".to_string(),
    }
}

fn read_source(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[cfg(test)]
mod tests {
    use super::RuntimeSourceSelfAudit;

    #[test]
    fn source_self_audit_passes_current_workspace_boundaries() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

        let report = RuntimeSourceSelfAudit::audit_repo(repo_root);

        assert!(report.ok, "{report:#?}");
        assert!(report.repair_plan.is_empty());
        assert!(report
            .checks
            .iter()
            .any(|check| check.id == "runtime.no_surface_sdk_dependency"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.id == "harness_eval.repair_hints"));
    }
}
