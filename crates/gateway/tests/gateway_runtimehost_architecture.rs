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

fn source_between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = source
        .find(start)
        .expect("source start marker should exist");
    let remainder = &source[start_index..];
    let end_index = remainder.find(end).expect("source end marker should exist");
    &remainder[..end_index]
}

fn manifest_dependencies(source: &str) -> &str {
    source.split("[dev-dependencies]").next().unwrap_or(source)
}

#[test]
fn daemon_module_is_only_a_runtime_host_transition_shim() {
    let source = read_repo("crates/gateway/src/daemon/mod.rs");
    assert!(source.contains("status: 0618_final_boundary"));
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
fn ai_kernel_is_pure_semantic_crate() {
    let root = repo_root();
    let manifest_path = root.join("crates/ai-kernel/Cargo.toml");
    assert!(
        manifest_path.is_file(),
        "ai-kernel must exist as the unified AI harness semantic crate"
    );
    let manifest = read_repo("crates/ai-kernel/Cargo.toml");
    let dependencies = manifest_dependencies(&manifest);
    for forbidden in [
        "rusqlite",
        "reqwest",
        "gateway",
        "runtime",
        "storage",
        "provider",
        "memory",
        "matrix-repository",
        "mcp",
        "plugins",
        "tools",
        "axum",
    ] {
        assert!(
            !dependencies.contains(forbidden),
            "ai-kernel must not depend on heavy implementation crate {forbidden}"
        );
    }

    let source = read_repo("crates/ai-kernel/src/lib.rs");
    for module in [
        "pub mod core",
        "pub mod turn",
        "pub mod task",
        "pub mod strategy",
        "pub mod context",
        "pub mod agent",
        "pub mod workgraph",
        "pub mod tool",
        "pub mod verification",
        "pub mod policy",
        "pub mod growth",
        "pub mod harness",
    ] {
        assert!(source.contains(module), "ai-kernel missing {module}");
    }
}

#[test]
fn fact_kernel_is_pure_semantic_crate() {
    let root = repo_root();
    let manifest_path = root.join("crates/fact-kernel/Cargo.toml");
    assert!(
        manifest_path.is_file(),
        "fact-kernel must exist as the unified fact semantic crate"
    );
    let manifest = read_repo("crates/fact-kernel/Cargo.toml");
    let dependencies = manifest_dependencies(&manifest);
    for forbidden in [
        "rusqlite",
        "reqwest",
        "gateway",
        "runtime",
        "storage",
        "provider",
        "memory",
        "matrix-core",
        "matrix-repository",
        "mcp",
        "plugins",
        "tools",
        "axum",
    ] {
        assert!(
            !dependencies.contains(forbidden),
            "fact-kernel must not depend on implementation crate {forbidden}"
        );
    }

    let source = read_repo("crates/fact-kernel/src/lib.rs");
    for module in [
        "pub mod core",
        "pub mod memory",
        "pub mod matrix",
        "pub mod growth",
        "pub mod hypothesis",
        "pub mod bridge",
        "pub mod health",
    ] {
        assert!(source.contains(module), "fact-kernel missing {module}");
    }
}

#[test]
fn external_boundary_crates_do_not_depend_on_runtime_or_gateway() {
    for crate_name in ["model-protocol", "channel", "connector"] {
        let manifest_path = format!("crates/{crate_name}/Cargo.toml");
        let manifest = read_repo(&manifest_path);
        let dependencies = manifest_dependencies(&manifest);
        for forbidden in [
            "gateway",
            "runtime",
            "memory",
            "matrix-repository",
            "provider",
            "mcp",
            "plugins",
            "tools",
            "axum",
            "reqwest",
            "rusqlite",
        ] {
            assert!(
                !dependencies.contains(forbidden),
                "{crate_name} must not depend on implementation crate {forbidden}"
            );
        }
    }
}

#[test]
fn runtime_uses_ai_kernel_as_harness_semantic_entrypoint() {
    let manifest = read_repo("crates/runtime/Cargo.toml");
    let dependencies = manifest_dependencies(&manifest);
    assert!(
        dependencies.contains("ai-kernel = { path = \"../ai-kernel\" }"),
        "runtime must depend on ai-kernel as the unified harness semantic entrypoint"
    );
    for forbidden in [
        "ai-core",
        "ai-agent-spec",
        "ai-behavior-policy",
        "ai-context",
        "ai-growth",
        "ai-harness",
        "ai-policy",
        "ai-strategy",
        "ai-tool-transaction",
        "ai-verification",
        "ai-workgraph",
    ] {
        assert!(
            !dependencies.contains(forbidden),
            "runtime must not directly depend on legacy AI semantic crate {forbidden}; use ai-kernel"
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
    let cli_main = read_repo("crates/cli/src/main.rs");
    assert!(cli_manifest.contains("name = \"cli\""));
    assert!(
        cli_dependencies.contains("gateway = { path = \"../gateway\" }"),
        "cli must depend on gateway only for backend management commands"
    );
    assert!(
        cli_dependencies.contains("tui = { path = \"../tui\" }"),
        "cli must depend on tui directly for the terminal UI launcher"
    );
    assert!(
        cli_main.contains("tui::terminal_entry()"),
        "cli launcher must route no-arg or explicit tui usage directly to the TUI entry"
    );
    assert!(
        cli_main.contains("fn should_open_tui(") && cli_main.contains("\"--resume\""),
        "cli launcher must route resume/session startup flags directly to the TUI entry"
    );
    assert!(
        cli_main.contains("gateway::backend_entry()"),
        "cli launcher must route gateway management commands to the backend entry"
    );
    assert!(
        cli_main.contains("gateway::static_entry()"),
        "cli launcher must route explicit static commands to the static command entry"
    );
    assert!(
        !cli_main.contains("gateway::main_entry()"),
        "cli must not fall back into gateway's historical full CLI parser"
    );
    assert!(!cli_dependencies.contains("runtime"));
    assert!(!cli_dependencies.contains("ratatui"));
    assert!(!cli_dependencies.contains("axum"));
    for dependency in ["crossterm", "pulldown-cmark", "syntect", "unicode-width"] {
        assert!(
            !cli_dependencies.contains(&format!("{dependency} = ")),
            "cli must stay plain-text and must not own terminal rendering dependency {dependency}"
        );
    }

    let gateway_manifest = read_repo("crates/gateway/Cargo.toml");
    let gateway_dependencies = manifest_dependencies(&gateway_manifest);
    assert!(gateway_manifest.contains("name = \"gateway\""));
    assert!(
        !root.join("crates/cli-ui/Cargo.toml").exists(),
        "terminal rendering is a cli module, not a standalone workspace crate"
    );
    assert!(
        !gateway_dependencies.contains("cli-ui") && !gateway_dependencies.contains("cli_ui"),
        "gateway must not depend on the historical cli-ui terminal adapter"
    );
    assert!(
        !gateway_manifest.contains("terminal-ui"),
        "gateway must not expose a terminal UI feature"
    );
    for dependency in [
        "crossterm",
        "pulldown-cmark",
        "ratatui",
        "rustyline",
        "syntect",
        "tui",
        "tui-textarea",
        "unicode-width",
    ] {
        assert!(
            !gateway_dependencies.contains(&format!("{dependency} = ")),
            "gateway must not depend directly on terminal rendering crate {dependency}"
        );
    }

    let tui_manifest = read_repo("crates/tui/Cargo.toml");
    let tui_dependencies = manifest_dependencies(&tui_manifest);
    assert!(tui_manifest.contains("name = \"tui\""));
    for dependency in [
        "crossterm",
        "pulldown-cmark",
        "ratatui",
        "syntect",
        "unicode-width",
    ] {
        assert!(
            tui_dependencies.contains(&format!("{dependency} = ")),
            "tui owns terminal rendering dependency {dependency}"
        );
    }
    for forbidden in [
        "runtime",
        "matrix-core",
        "matrix-repository",
        "memory",
        "command-contract",
        "command-service",
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
fn tui_terminal_path_requires_gateway_backend_instead_of_local_runtime_fallback() {
    let full_source = read_repo("crates/tui/src/runner.rs");
    let source = source_between(
        &full_source,
        "pub fn run_gateway_tui(config: GatewayTuiConfig)",
        "fn attach_gateway_session",
    );
    for forbidden in [
        "local TUI runtime fallback",
        "local TUI runtime active",
        "local runtime remains active",
        "Gateway API unavailable for this message",
        "capture_stdout(|| cli.handle_repl_command",
        "cli.handle_repl_command",
        "run_turn_async",
        "Gateway turn cancellation API is not wired yet",
    ] {
        assert!(
            !source.contains(forbidden),
            "TUI must not silently fall back to a second local runtime path: {forbidden}"
        );
    }
    assert!(
        source.contains("Gateway API is required for TUI"),
        "TUI startup must fail explicitly when Gateway cannot be reached"
    );
    assert!(
        source.contains("dispatch_gateway_message("),
        "TUI chat submit must use Gateway HTTP message API"
    );
    assert!(
        source.contains("dispatch_gateway_command("),
        "TUI slash command submit must use Gateway HTTP command API"
    );
    assert!(
        source.contains("dispatch_gateway_cancel("),
        "TUI cancel must use Gateway HTTP control API"
    );
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
        "api_routes/matrix_routes.rs",
        "api_routes/mfg_routes.rs",
        "api_routes/matrix_outcomes.rs",
        "api_routes/mfg_outcomes.rs",
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
