use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_repo(path: &str) -> String {
    std::fs::read_to_string(repo_root().join(path)).expect("source file should read")
}

fn production_part(source: &str) -> &str {
    source.split("#[cfg(test)]").next().unwrap_or(source)
}

fn manifest_dependencies(source: &str) -> &str {
    source.split("[dev-dependencies]").next().unwrap_or(source)
}

#[test]
fn daemon_module_is_only_a_runtime_host_transition_shim() {
    let source = read_repo("crates/gateway/src/daemon/mod.rs");
    assert!(source.contains("status: reviewed_0.9.305"));
    assert!(source.contains("pub(crate) use crate::runtime_host::*;"));
    assert!(!source.contains("fn "));
    assert!(!source.contains("struct "));
    assert!(!source.contains("enum "));
}

#[test]
fn runtime_host_owns_gateway_runtime_implementation() {
    let root = repo_root();
    assert!(root
        .join("crates/gateway/src/runtime_host/mod.rs")
        .is_file());
    assert!(!root
        .join("crates/gateway/src/runtime_host/commands.rs")
        .exists());
    let source = production_part(&read_repo("crates/gateway/src/runtime_host/mod.rs")).to_string();
    assert!(source.contains("pub struct RuntimeHostConfig"));
    assert!(source.contains("pub async fn run_gateway_runtime"));
    assert!(!source.contains("pub struct DaemonConfig"));
    assert!(!source.contains("pub async fn run_daemon"));
}

#[test]
fn production_code_does_not_depend_on_daemon_module_except_transition_shim() {
    let files = [
        "crates/gateway/src/main.rs",
        "crates/gateway/src/api_routes.rs",
        "crates/gateway/src/runtime_service.rs",
        "crates/gateway/src/runtime_host/mod.rs",
    ];
    for file in files {
        let full_source = read_repo(file);
        let source = production_part(&full_source);
        assert!(
            !source.contains("crate::daemon::"),
            "{file} must use crate::runtime_host directly"
        );
        assert!(
            !source.contains("DaemonConfig"),
            "{file} must not expose DaemonConfig"
        );
        assert!(
            !source.contains("run_daemon"),
            "{file} must not expose run_daemon"
        );
    }
}

#[test]
fn socket_business_commands_are_removed_after_tui_gateway_migration() {
    let source = production_part(&read_repo("crates/gateway/src/runtime_host/mod.rs")).to_string();
    for forbidden in [
        "acquire_session_lease",
        "approval_pending",
        "approval_respond",
        "chat_stream",
        "connector_resource_list",
        "connector_resource_promote_memory",
        "connector_resource_revalidate",
        "context_snapshot",
        "memory_status",
        "runtime_snapshot",
        "subscribe_session",
        "task_cancel",
        "task_complete",
        "task_list",
        "task_start",
        "tool_approve",
        "tool_deny",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime_host must not retain socket business command {forbidden}"
        );
    }
}

#[test]
fn compat_harness_is_not_a_workspace_dependency() {
    let root = repo_root();
    assert!(
        !root.join("crates/compat-harness/Cargo.toml").exists(),
        "compat-harness must stay outside the main crates workspace"
    );

    let manifests = [
        "Cargo.toml",
        "crates/gateway/Cargo.toml",
        "crates/runtime/Cargo.toml",
    ];
    for manifest in manifests {
        let source = read_repo(manifest);
        assert!(
            !source.contains("compat-harness") && !source.contains("compat_harness"),
            "{manifest} must not depend on compat-harness"
        );
    }
}

#[test]
fn entry_boundary_crates_exist_as_migration_targets() {
    let root = repo_root();
    for crate_name in ["cli", "gateway", "tui"] {
        let manifest = root.join(format!("crates/{crate_name}/Cargo.toml"));
        assert!(
            manifest.is_file(),
            "missing entry boundary crate manifest: {}",
            manifest.display()
        );
    }

    let cli_manifest = read_repo("crates/cli/Cargo.toml");
    let cli_dependencies = manifest_dependencies(&cli_manifest);
    assert!(cli_manifest.contains("name = \"cli\""));
    assert!(!cli_dependencies.contains("runtime"));
    assert!(!cli_dependencies.contains("ratatui"));
    assert!(!cli_dependencies.contains("axum"));

    let gateway_manifest = read_repo("crates/gateway/Cargo.toml");
    assert!(gateway_manifest.contains("name = \"gateway\""));

    let tui_manifest = read_repo("crates/tui/Cargo.toml");
    let tui_dependencies = manifest_dependencies(&tui_manifest);
    assert!(tui_manifest.contains("name = \"tui\""));
    for forbidden in [
        "runtime",
        "matrix",
        "memory",
        "commands",
        "command-runtime",
        "storage",
        "tools",
        "rusqlite",
    ] {
        assert!(
            !tui_dependencies.contains(forbidden),
            "tui manifest must not depend directly on {forbidden}"
        );
    }
}

#[test]
fn api_route_direct_dependencies_are_closed() {
    let allowlist = read_repo("crates/gateway/src/api_routes/service_transition_allowlist.txt");

    assert!(
        allowlist.contains("status: closed_by=0.9.303"),
        "service transition allowlist must be closed, not extended"
    );
    assert!(!allowlist.contains("file="));
    let direct_dependency_key = ["direct", "_dependency="].concat();
    assert!(!allowlist.contains(&direct_dependency_key));
    let stale_deadline_key = ["delete", "_by="].concat();
    assert!(!allowlist.contains(&stale_deadline_key));

    let scanned_files = [
        "api_routes/agent_routes.rs",
        "api_routes/approval_routes.rs",
        "api_routes/audit_routes.rs",
        "api_routes/channel_routes.rs",
        "api_routes/connector_routes.rs",
        "api_routes/context_routes.rs",
        "api_routes/core_routes.rs",
        "api_routes/cross_plane_routes.rs",
        "api_routes/matrix_mfg_routes.rs",
        "api_routes/memory_routes.rs",
        "api_routes/message_routes.rs",
        "api_routes/profile_routes.rs",
        "api_routes/public_routes.rs",
        "api_routes/runtime_routes.rs",
        "api_routes/session_routes.rs",
        "api_routes/skill_routes.rs",
        "api_routes/system_routes.rs",
        "api_routes/task_routes.rs",
        "api_routes/workspace_routes.rs",
    ];
    for file in scanned_files {
        let source = read_repo(&format!("crates/gateway/src/{file}"));
        for dependency in [
            "open_runtime_store",
            "MfgStore::open",
            "SessionKernel",
            "UnifiedSessionStore",
            "CognitiveContextManager",
            "ContextRuntimeKernel",
            "SqliteResourceDirectory",
            "SmartApprovalGate",
            "TaskKernel",
        ] {
            assert!(
                !source.contains(dependency),
                "{file} must use GatewayServices instead of route direct dependency {dependency}"
            );
        }
    }
}
