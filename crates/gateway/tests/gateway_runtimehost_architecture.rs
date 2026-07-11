use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn mission_team_and_steward_entries_do_not_own_execution() {
    let mission_service_source = read_repo("crates/gateway/src/services/mission_service.rs");
    let mission_service = production_part(&mission_service_source);
    for forbidden in [
        "global_team_runtime_service().cancel(",
        "global_team_runtime_service().handoff(",
        ".finalize_execution_summary(",
        ".tick_all_once(",
    ] {
        assert!(
            !mission_service.contains(forbidden),
            "MissionService must not execute through scoped globals: {forbidden}"
        );
    }
    assert!(mission_service.contains(".command_graph("));
    assert!(mission_service.contains("\"status\": \"capability_unavailable\""));
    assert!(!mission_service.contains("global_agent_lifecycle_service().command("));

    let steward_runtime_source = read_repo("crates/runtime/src/steward/steward_runtime.rs");
    let steward_runtime = production_part(&steward_runtime_source);
    assert!(
        !steward_runtime.contains("pub fn tick_all_once"),
        "scoped steward globals may retain state but must not expose a scheduler execution loop"
    );

    let scheduler_source = read_repo("crates/runtime/src/steward/steward_scheduler.rs");
    let scheduler = production_part(&scheduler_source);
    assert!(!scheduler.contains(".tick_all_once("));
    assert!(scheduler.contains("capability_unavailable:steward_execution:V8"));

    let mission_control_source = read_repo("crates/runtime/src/mission/mission_control.rs");
    let mission_control = production_part(&mission_control_source);
    assert!(!mission_control.contains("global_agent_lifecycle_service().command("));
    assert!(!mission_control.contains("global_steward_runtime_service().start("));
    assert!(mission_control.contains("\"capability\": \"agent_execution\""));

    let team_runtime_source = read_repo("crates/runtime/src/team/team_runtime.rs");
    let team_runtime = production_part(&team_runtime_source);
    for forbidden in [
        "pub fn cancel(&self, team_id:",
        "pub fn handoff(",
        "pub fn finalize_execution_summary(",
    ] {
        assert!(
            !team_runtime.contains(forbidden),
            "scoped team globals must not expose execution owner API: {forbidden}"
        );
    }
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

fn production_rust_sources(roots: &[&str]) -> Vec<(String, String)> {
    fn visit(root: &std::path::Path, files: &mut Vec<PathBuf>) {
        let mut entries = std::fs::read_dir(root)
            .unwrap_or_else(|error| panic!("{} should be readable: {error}", root.display()))
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    let root = repo_root();
    let mut files = Vec::new();
    for relative in roots {
        visit(&root.join(relative), &mut files);
    }
    files
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&root)
                .expect("source must be inside repository")
                .to_string_lossy()
                .to_string();
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{relative} should read: {error}"));
            (relative, production_part(&source).to_string())
        })
        .collect()
}

#[test]
fn removed_builtin_channel_document_operations_do_not_reappear() {
    let forbidden_terms = [
        "service.feishu.docx",
        "feishu.readonly",
        "docx:read",
        "doc_ops",
    ];
    let checked_files = [
        "crates/surface/src/message.rs",
        "crates/connector/src/lib.rs",
        "crates/gateway/src/api_routes/mod.rs",
        "crates/gateway/src/runtime_host/mod.rs",
        "crates/runtime/src/lib.rs",
        "crates/tui/src/app_core/runtime_control_store.rs",
        "crates/tui/src/components/command_palette.rs",
        "crates/tui/src/components/gateway_panel.rs",
    ];

    for file in checked_files {
        let source = read_repo(file);
        for forbidden in forbidden_terms {
            assert!(
                !source.contains(forbidden),
                "{file} must not retain built-in channel document operation residue `{forbidden}`"
            );
        }
    }
}

#[test]
fn macro_crates_keep_source_files_grouped_by_business_boundary() {
    assert_root_rs_files("crates/gateway/src", &["main.rs"]);
    assert_root_rs_files("crates/tui/src", &["lib.rs"]);
    assert_root_rs_files("crates/tools/src", &["host.rs", "lib.rs"]);
    assert_root_rs_files("crates/matrix/core/src", &["lib.rs"]);

    for (root, dirs) in [
        (
            "crates/gateway/src",
            &[
                "api_routes",
                "cli",
                "command",
                "core",
                "entry",
                "infrastructure",
                "kernel",
                "runtime",
                "runtime_host",
                "server",
                "services",
                "static",
                "surface_host",
            ][..],
        ),
        (
            "crates/tui/src",
            &[
                "app_core",
                "components",
                "event",
                "gateway",
                "integration",
                "keybind",
                "layout",
                "platform",
                "rendering",
                "test_utils",
                "theme",
            ][..],
        ),
        (
            "crates/tools/src",
            &["execution", "filesystem", "policy", "registry", "state"][..],
        ),
        (
            "crates/matrix/core/src",
            &["contract", "entity", "fact", "metric", "source"][..],
        ),
    ] {
        for dir in dirs {
            assert!(
                repo_root().join(root).join(dir).is_dir(),
                "{root}/{dir} must exist as an architecture directory"
            );
        }
    }
}

#[test]
fn daemon_module_is_removed_after_runtime_host_consolidation() {
    let root = repo_root();
    assert!(
        !root.join("crates/gateway/src/daemon/mod.rs").exists(),
        "gateway must not retain a daemon compatibility re-export module"
    );
    let main_source = read_repo("crates/gateway/src/main.rs");
    assert!(
        !production_part(&main_source).contains("mod daemon;"),
        "gateway main must not register daemon as a module"
    );
}

fn assert_root_rs_files(src: &str, allowed: &[&str]) {
    let allowed = allowed
        .iter()
        .map(|name| name.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let actual = std::fs::read_dir(repo_root().join(src))
        .expect("source directory should read")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .map(|path| {
            path.file_name()
                .expect("source file name")
                .to_string_lossy()
                .to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual, allowed,
        "{src} must not regain root-level source file sprawl"
    );
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
fn production_code_does_not_depend_on_daemon_module() {
    let files = [
        "crates/gateway/src/main.rs",
        "crates/gateway/src/api_routes/mod.rs",
        "crates/gateway/src/runtime/runtime_service.rs",
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
fn production_turns_enter_the_execution_graph_host_once() {
    let gateway_entry_source = read_repo("crates/gateway/src/runtime/runtime_entry.rs");
    let gateway_service_source = read_repo("crates/gateway/src/runtime/runtime_service.rs");
    let in_process_agent_source = read_repo("crates/runtime/src/agent/in_process_worker.rs");
    let agent_source = read_repo("crates/runtime/src/agent/agent.rs");
    let host_source = read_repo("crates/runtime/src/conversation/host.rs");
    let gateway_entry = gateway_entry_source.as_str();
    let gateway_service = production_part(&gateway_service_source);
    let in_process_agent = production_part(&in_process_agent_source);
    let agent = production_part(&agent_source);
    let host = production_part(&host_source);

    assert!(gateway_entry.contains(".submit_turn(content, prompter)"));
    assert!(gateway_service.contains(".submit_turn(&content"));
    assert!(in_process_agent.contains("submit_turn(&packet.objective"));
    assert!(agent.contains("production implementations submit canonical AgentTask nodes"));
    assert!(host.contains("services.graph_runner().start(graph).await"));
    assert!(host.contains("ExecutionGraphCompiler"));
    assert!(!host.contains("execute_model_tool_cycle"));

    for (path, source) in [
        ("gateway runtime entry", gateway_entry),
        ("gateway runtime service", gateway_service),
        ("in-process agent", in_process_agent),
        ("generic agent", agent),
    ] {
        assert!(
            !source.contains("execute_model_tool_cycle"),
            "{path} must not bypass StandardRuntimeHost/ExecutionGraphRunner"
        );
        assert!(
            !source.contains("run_turn_async"),
            "{path} must not restore the removed turn loop entry"
        );
        assert!(
            !source.contains("assistant_messages.last()"),
            "{path} must consume the synthesized terminal result"
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
fn approval_api_uses_workspace_runtime_queue_for_decisions() {
    let full_source = read_repo("crates/gateway/src/services/approval_service.rs");
    let source = production_part(&full_source);
    assert!(source.contains("runtime_services"));
    assert!(source.contains("approval_queue()"));
    assert!(source.contains(".pending()"));
    assert!(source.contains(".decide("));
    for forbidden in [
        "get_pending_requests",
        "resolve_approval",
        "runtime::global_",
    ] {
        assert!(
            !source.contains(forbidden),
            "ApprovalService must not use SmartApprovalGate {forbidden} as a production decision path"
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
    let manifest_path = root.join("crates/harness-contract/Cargo.toml");
    assert!(
        manifest_path.is_file(),
        "harness-contract must exist as the unified AI harness semantic crate"
    );
    let manifest = read_repo("crates/harness-contract/Cargo.toml");
    let dependencies = manifest_dependencies(&manifest);
    let absorbed_crates = [
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
    ];
    let workspace_manifest = read_repo("Cargo.toml");
    for absorbed in absorbed_crates {
        assert!(
            !root.join(format!("crates/{absorbed}/Cargo.toml")).exists(),
            "absorbed AI semantic crate {absorbed} must not remain as a top-level crate"
        );
        assert!(
            !workspace_manifest.contains(&format!("\"crates/{absorbed}\"")),
            "workspace must not keep absorbed AI semantic crate {absorbed}"
        );
        assert!(
            !dependencies.contains(absorbed),
            "harness-contract must own {absorbed} semantics internally, not depend on it"
        );
    }
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
            "harness-contract must not depend on heavy implementation crate {forbidden}"
        );
    }

    let source = read_repo("crates/harness-contract/src/lib.rs");
    for module in [
        "pub mod core",
        "pub mod turn",
        "pub mod task",
        "pub mod strategy",
        "pub mod context",
        "pub mod agent",
        "pub mod execution_graph",
        "pub mod tool",
        "pub mod verification",
        "pub mod policy",
        "pub mod growth",
        "pub mod harness",
    ] {
        assert!(source.contains(module), "harness-contract missing {module}");
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
        "pub mod store",
        "pub mod indexer",
        "pub mod service",
    ] {
        assert!(source.contains(module), "fact-kernel missing {module}");
    }
}

#[test]
fn external_boundary_crates_do_not_depend_on_runtime_or_gateway() {
    for crate_name in ["model-protocol", "surface", "connector"] {
        let manifest_path = format!("crates/{crate_name}/Cargo.toml");
        let manifest = read_repo(&manifest_path);
        let dependencies = manifest_dependencies(&manifest);
        let forbidden_dependencies: &[&str] = if crate_name == "connector" {
            &[
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
            ]
        } else {
            &[
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
            ]
        };
        for forbidden in forbidden_dependencies {
            assert!(
                !dependencies.contains(forbidden),
                "{crate_name} must not depend on implementation crate {forbidden}"
            );
        }
    }
}

#[test]
fn connector_is_independent_external_resource_boundary() {
    let root = repo_root();
    assert!(
        !root.join("crates/runtime/src/connector.rs").exists(),
        "runtime must not retain connector implementation after connector crate extraction"
    );
    let connector_manifest = read_repo("crates/connector/Cargo.toml");
    let connector_dependencies = manifest_dependencies(&connector_manifest);
    assert!(
        connector_dependencies.contains("storage = { path = \"../storage\" }")
            && connector_dependencies.contains("rusqlite"),
        "connector must own its resource directory persistence boundary"
    );

    let runtime_lib = production_part(&read_repo("crates/runtime/src/lib.rs")).to_string();
    assert!(
        !runtime_lib.contains("pub mod connector") && !runtime_lib.contains("pub use connector::{"),
        "runtime must not republish connector as a compatibility module"
    );

    let runtime_cross_plane = production_part(&read_repo(
        "crates/runtime/src/policy/cross_plane_policy.rs",
    ))
    .to_string();
    assert!(
        runtime_cross_plane
            .contains("use harness_contract::policy::{CrossPlaneRisk, DataClassification};"),
        "runtime policy must consume harness-contract risk/data contracts"
    );

    let gateway_connector_route = production_part(&read_repo(
        "crates/gateway/src/api_routes/connector_routes.rs",
    ))
    .to_string();
    assert!(
        gateway_connector_route.contains("use connector::{"),
        "gateway connector routes must import connector contracts directly"
    );
    for forbidden in [
        "runtime::default_capabilities",
        "runtime::Connector",
        "runtime::ExternalResourceRef",
        "runtime::ProviderAccount",
        "runtime::ServiceConnector",
        "runtime::SqliteResourceDirectory",
    ] {
        assert!(
            !gateway_connector_route.contains(forbidden),
            "gateway connector route must not use old runtime connector path {forbidden}"
        );
    }
}

#[test]
fn message_connector_contracts_drive_gateway_surface_readiness() {
    assert!(
        !repo_root().join("crates/channel-adapters").exists(),
        "core workspace must not retain channel-adapters; non-TUI surfaces live in cowd-edge"
    );
    assert!(
        !repo_root().join("crates/channel").exists(),
        "legacy channel contracts are absorbed into surface::message"
    );

    let message_source = production_part(&read_repo("crates/surface/src/message.rs")).to_string();
    for required in [
        "pub struct MessageConnectorContract",
        "pub fn message_connector_required_fields",
        "pub fn message_connector_capabilities",
        "pub fn normalize_message_connector",
    ] {
        assert!(
            message_source.contains(required),
            "message connector contract missing {required}"
        );
    }
    assert!(
        !repo_root()
            .join(["crates/surface/src/", "channel.rs"].concat())
            .exists(),
        "legacy channel contract file must be deleted from production source"
    );

    let gateway_manifest = read_repo("crates/gateway/Cargo.toml");
    assert!(
        manifest_dependencies(&gateway_manifest).contains("surface = { path = \"../surface\" }"),
        "gateway must depend on surface for JSONL sidecar protocol hosting"
    );
    assert!(
        !manifest_dependencies(&gateway_manifest).contains("channel = { path = \"../channel\" }"),
        "gateway must consume message connector contracts through surface::message"
    );
    assert!(
        !manifest_dependencies(&gateway_manifest).contains("channel-adapters"),
        "gateway must not depend on channel-adapters; platform SDKs live in Cowd Edge sidecars"
    );
    let message_connector_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/message_connector_routes.rs",
    ))
    .to_string();
    assert!(
        message_connector_routes.contains(
            "use surface::message::{message_connector_required_fields, MessageConnectorContract};"
        ),
        "gateway message readiness routes must consume message connector contract"
    );
    assert!(
        !message_connector_routes.contains("fn platform_capabilities"),
        "gateway must not own a duplicate platform capability table"
    );
    assert!(
        !message_source.contains("feishu_document_operation"),
        "message connector contract must not expose Feishu document operations as built-in capability"
    );

    let gateway_services_source = read_repo("crates/gateway/src/services/mod.rs");
    let gateway_services = production_part(&gateway_services_source);
    let surface_service_source = read_repo("crates/gateway/src/services/surface_service.rs");
    let surface_service = production_part(&surface_service_source);
    let api_state_source = read_repo("crates/gateway/src/api_routes/mod.rs");
    let api_state = production_part(&api_state_source);
    assert!(
        gateway_services.contains("pub(crate) surface: SurfaceService"),
        "GatewayServices must expose the Surface service boundary"
    );
    assert!(
        surface_service.contains("pub(crate) struct SurfaceService")
            && surface_service.contains("host: Arc<SurfaceHost>")
            && surface_service.contains("send(")
            && surface_service.contains("action("),
        "SurfaceService must own gateway-side SurfaceHost access"
    );
    assert!(
        !api_state.contains("pub platform_runtime:"),
        "AppState must not expose PlatformRuntime directly to gateway routes"
    );

    for source_path in [
        "crates/gateway/src/main.rs",
        "crates/gateway/src/api_routes/mod.rs",
        "crates/gateway/src/runtime_host/mod.rs",
        "crates/gateway/src/api_routes/message_connector_routes.rs",
        "crates/gateway/src/api_routes/cross_plane_routes.rs",
        "crates/gateway/src/entry/workspace_entry.rs",
    ] {
        let source = production_part(&read_repo(source_path)).to_string();
        assert!(
            !source.contains("runtime::platform"),
            "{source_path} must not use runtime::platform as the channel host"
        );
        assert!(
            !source.contains("channel_adapters"),
            "{source_path} must not directly use channel-adapters"
        );
    }

    for source_path in [
        "crates/gateway/src/api_routes/message_connector_routes.rs",
        "crates/gateway/src/api_routes/mfg_routes.rs",
        "crates/gateway/src/infrastructure/gateway_health.rs",
    ] {
        let source = production_part(&read_repo(source_path)).to_string();
        assert!(
            source.contains("services.surface"),
            "{source_path} must access external ingress/egress through GatewayServices.surface"
        );
        assert!(
            !source.contains("state.platform_runtime"),
            "{source_path} must not bypass ChannelService with AppState.platform_runtime"
        );
    }

    let surface_source = production_part(&read_repo("crates/surface/src/lib.rs")).to_string();
    assert!(
        surface_source.contains("pub enum SurfaceFrame")
            && surface_source.contains("pub struct SurfaceManifest")
            && surface_source.contains("StdioJsonl"),
        "surface crate must expose the JSONL sidecar protocol contract"
    );
    assert!(
        !repo_root().join("crates/runtime/src/platform").exists(),
        "runtime must not physically own platform adapter sources"
    );
    let runtime_manifest = read_repo("crates/runtime/Cargo.toml");
    for forbidden in [
        "channel-adapters",
        "lettre",
        "imap",
        "tokio-tungstenite",
        "prost",
        "native-tls",
    ] {
        assert!(
            !manifest_dependencies(&runtime_manifest).contains(forbidden),
            "runtime must not depend on platform adapter dependency {forbidden}"
        );
    }
    let runtime_lib = production_part(&read_repo("crates/runtime/src/lib.rs")).to_string();
    assert!(
        !runtime_lib.contains("pub mod platform") && !runtime_lib.contains("pub mod mirror"),
        "runtime must not re-export channel adapter platform or mirror modules"
    );
}

#[test]
fn fact_kernel_is_consumed_by_memory_and_matrix_engines() {
    let memory_manifest = read_repo("crates/memory/Cargo.toml");
    assert!(
        manifest_dependencies(&memory_manifest)
            .contains("fact-kernel = { path = \"../fact-kernel\" }"),
        "memory must depend on fact-kernel for non-structured fact semantics"
    );
    let memory_types = production_part(&read_repo("crates/memory/src/ops/types.rs")).to_string();
    for required in [
        "to_fact_memory_candidate",
        "to_fact_record",
        "write_to_fact_kernel",
        "FactMemoryCandidate",
        "FactKernelService",
        "HypothesisBoundary::observed()",
    ] {
        assert!(
            memory_types.contains(required),
            "memory fact bridge missing {required}"
        );
    }

    let matrix_manifest = read_repo("crates/matrix/core/Cargo.toml");
    assert!(
        manifest_dependencies(&matrix_manifest)
            .contains("fact-kernel = { path = \"../../fact-kernel\" }"),
        "matrix-core must depend on fact-kernel for structured fact semantics"
    );
    let matrix_fact =
        production_part(&read_repo("crates/matrix/core/src/fact/fact.rs")).to_string();
    for required in [
        "to_fact_kernel_matrix_fact",
        "from_fact_kernel_matrix_fact",
        "to_fact_record",
        "write_to_fact_kernel",
        "KernelMatrixFact",
        "FactKernelService",
        "HypothesisBoundary::observed()",
    ] {
        assert!(
            matrix_fact.contains(required),
            "matrix fact bridge missing {required}"
        );
    }
}

#[test]
fn runtime_approval_gate_projects_to_ai_kernel_policy_receipts() {
    let runtime_approval =
        production_part(&read_repo("crates/runtime/src/approval/approval_gate.rs")).to_string();
    assert!(
        runtime_approval.contains("use harness_contract::policy::{"),
        "runtime approval gate must consume harness-contract policy contracts"
    );
    for required in [
        "pub async fn policy_receipt",
        "RiskGateReceipt",
        "PermissionScope",
        "KernelPolicyDecisionKind::Ask",
        "KernelPolicyDecisionKind::Escalate",
    ] {
        assert!(
            runtime_approval.contains(required),
            "runtime approval policy bridge missing {required}"
        );
    }

    let gateway_approval = production_part(&read_repo(
        "crates/gateway/src/services/approval_service.rs",
    ))
    .to_string();
    assert!(
        gateway_approval.contains("pub(crate) async fn risk_receipt")
            && gateway_approval.contains("gate.policy_receipt"),
        "gateway approval service must expose runtime risk receipt without route-level gate coupling"
    );
    let gateway_approval_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/approval_routes.rs",
    ))
    .to_string();
    assert!(
        gateway_approval_routes.contains("/api/approval/risk-receipt")
            && gateway_approval_routes.contains("risk_receipt_handler")
            && gateway_approval_routes.contains("state.services.audit.risk_gate_projection")
            && gateway_approval_routes.contains(".ingest_risk_gate_receipt(")
            && gateway_approval_routes.contains("&state.services.memory")
            && gateway_approval_routes.contains("&state.services.matrix"),
        "approval API must expose risk receipt projection through audit and durable growth promotion services"
    );
    let gateway_services = read_repo("crates/gateway/src/services/mod.rs");
    let gateway_service_registry = read_repo("crates/gateway/src/services/registry.rs");
    assert!(
        gateway_services.contains("pub(crate) struct ProviderService")
            && gateway_services.contains("pub(crate) struct GrowthService")
            && gateway_services.contains("config_projection")
            && gateway_services.contains("risk_gate_event")
            && gateway_services.contains("pub(crate) use reality_service::RealityService")
            && gateway_services.contains("pub(crate) reality: RealityService")
            && gateway_service_registry.contains("reality: RealityService::new()")
            && gateway_service_registry.contains("contracts.extend(self.reality.contracts())")
            && gateway_services.contains("risk_gate_projection"),
        "GatewayServices must include concrete provider, reality, growth, and audit projections"
    );
    let api_routes = read_repo("crates/gateway/src/api_routes/mod.rs");
    let reality_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/reality_routes.rs",
    ))
    .to_string();
    assert!(
        api_routes.contains("mod reality_routes")
            && api_routes.contains(".merge(reality_routes::router())")
            && reality_routes.contains("/api/reality/status")
            && reality_routes.contains("/api/reality/static")
            && reality_routes.contains("/api/reality/flow")
            && reality_routes.contains("/api/reality/promotions")
            && reality_routes.contains("/api/reality/boundaries"),
        "Reality Core must expose read-only /api/reality/* projections"
    );
    let reality_service =
        production_part(&read_repo("crates/gateway/src/services/reality_service.rs")).to_string();
    assert!(
        reality_service.contains("pub(crate) struct RealityService")
            && reality_service.contains("FactFlowProjection")
            && reality_service.contains("status_projection")
            && reality_service.contains("flow_projection")
            && reality_service.contains("MemoryService")
            && reality_service.contains("MatrixService")
            && reality_service.contains("GrowthService")
            && !reality_service.contains("remember_entry_with_context(")
            && !reality_service.contains("ingest_fact("),
        "RealityService must be a read-only projection over Memory, Matrix, and Growth"
    );
    let growth_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/growth_routes.rs")).to_string();
    assert!(
        api_routes.contains("mod growth_routes")
            && api_routes.contains(".merge(growth_routes::router())")
            && growth_routes.contains("/api/growth/events")
            && growth_routes.contains("/api/growth/status")
            && growth_routes.contains("durable_event_log")
            && growth_routes.contains("durable_promotion_log")
            && growth_routes.contains("event_log_contract"),
        "GrowthService must expose an observable gateway route, not only an internal projection"
    );
    let growth_service =
        production_part(&read_repo("crates/gateway/src/services/growth_service.rs")).to_string();
    let storage_source = production_part(&read_repo("crates/storage/src/lib.rs")).to_string();
    assert!(
        storage_source.contains("(\"growth\".to_string(), root.join(\"growth.sqlite\"))")
            && growth_service.contains("StorageRegistry::default_for_config_home")
            && growth_service.contains(".sqlite_handle(\"growth\")")
            && growth_service.contains("MigrationRunner::run_sqlite_domain")
            && growth_service.contains("growth_storage_migrations")
            && growth_service.contains("growth_events")
            && growth_service.contains("growth_promotions")
            && growth_service.contains("promote_event_to_memory")
            && growth_service.contains("promote_event_to_matrix")
            && growth_service.contains("promote_event_to_fact_kernel"),
        "GrowthService must use registered storage and own memory/matrix/fact promotion pipeline"
    );
    assert!(
        growth_service.contains("growth_memory_slot_key")
            && growth_service.contains("growth_memory_assertion_fingerprint")
            && growth_service.contains("deterministic_memory_contradiction"),
        "Growth memory governance must use slot plus assertion keys instead of a single coarse semantic key"
    );
    let gateway_health = production_part(&read_repo(
        "crates/gateway/src/infrastructure/gateway_health.rs",
    ))
    .to_string();
    assert!(
        gateway_health.contains("growth_storage_migrations")
            && gateway_health.contains("inspect_growth_migrations")
            && gateway_health.contains("MigrationRunner::inspect_sqlite_domain"),
        "Gateway health must expose real Growth schema migration status, not only storage registry layout"
    );

    let task_service =
        production_part(&read_repo("crates/gateway/src/services/task_service.rs")).to_string();
    assert!(
        task_service.contains("pub(crate) fn record_lifecycle_event")
            && task_service.contains("runtime_events: &RuntimeEventService")
            && !task_service.contains("ensure_task_session_record"),
        "task service must write lifecycle state to the scoped runtime store without fake sessions"
    );
    let task_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/task_routes.rs")).to_string();
    assert!(
        !task_routes.contains("async fn append_task_runtime_event")
            && task_routes.contains(".record_lifecycle_event(&state.services.runtime_events"),
        "task routes must delegate lifecycle projection to TaskService"
    );
    let context_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/context_routes.rs",
    ))
    .to_string();
    assert!(
        !context_routes.contains("active_runtime(")
            && context_routes.contains("last_context_envelope_nonblocking"),
        "context routes must delegate active runtime context projection to SessionService"
    );
    let message_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/message_routes.rs",
    ))
    .to_string();
    assert!(
        message_routes.contains(".active_messages_page(")
            && message_routes.contains("\"turn_id\"")
            && message_routes.contains("\"turn\"")
            && !message_routes.contains("state.active_runtime(")
            && !message_routes.contains("run_turn_async("),
        "message routes must delegate active runtime reads and turn execution receipt projection to services"
    );
    let runtime_service =
        production_part(&read_repo("crates/gateway/src/runtime/runtime_service.rs")).to_string();
    assert!(
        runtime_service.contains("pub(crate) struct RuntimeTurnExecution")
            && runtime_service.contains("fn start_running_turn")
            && runtime_service.contains("fn finish_turn")
            && runtime_service.contains("TaskTurnBinding")
            && runtime_service.contains("fn record_turn_binding")
            && runtime_service.contains("TurnStatus::Running")
            && runtime_service.contains("TurnStatus::Completed")
            && runtime_service.contains("TurnStatus::Failed"),
        "RuntimeService must own real turn receipt lifecycle and task-turn binding projection"
    );
    assert!(
        !runtime_service.contains("attach_session_value")
            && !runtime_service.contains("detach_session_value")
            && !runtime_service.contains("replay_session_value"),
        "RuntimeService must not own session lifecycle APIs"
    );
    let system_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/system_routes.rs")).to_string();
    assert!(
        system_routes.contains("state.services.provider.config_projection(&runtime_config)"),
        "provider config route must delegate projection to ProviderService"
    );
    let session_service =
        production_part(&read_repo("crates/gateway/src/services/session_service.rs")).to_string();
    assert!(
        runtime_service.contains("pub(crate) fn last_context_envelope_nonblocking")
            && runtime_service.contains("pub(crate) async fn active_messages_page")
            && runtime_service.contains("pub(crate) async fn compact_active_session")
            && runtime_service.contains("pub(crate) async fn active_session_stats")
            && !session_service.contains("last_context_envelope_nonblocking")
            && !session_service.contains("active_messages_page")
            && !session_service.contains("runtime_guard"),
        "RuntimeService must own active runtime read projections while SessionService stays durable-session focused"
    );
    let session_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/session_routes.rs",
    ))
    .to_string();
    assert!(
        !session_routes.contains(".active_runtime(")
            && !session_routes.contains(".remove_active_runtime(")
            && session_routes.contains(".attach_session_value(")
            && session_routes.contains(".replay_session_value(")
            && session_routes.contains("runtime_service.has_active_session")
            && session_routes.contains("session_exists"),
        "session routes must use RuntimeService active-session helpers and SessionService durable-session helpers"
    );
    let runtime_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/runtime_routes.rs",
    ))
    .to_string();
    let tui_gateway_client_source = read_repo("crates/tui/src/gateway/gateway_client.rs");
    let tui_gateway_client = production_part(&tui_gateway_client_source);
    assert!(
        !runtime_routes.contains("/api/runtime/sessions/")
            && session_routes.contains("/api/sessions/:id/attach")
            && session_routes.contains("/api/sessions/:id/detach")
            && session_routes.contains("/api/sessions/:id/lifecycle")
            && session_routes.contains("/api/sessions/:id/replay")
            && !tui_gateway_client.contains("/api/runtime/sessions/")
            && tui_gateway_client.contains("/api/sessions/{}/attach"),
        "session lifecycle API must live under /api/sessions/*, not /api/runtime/sessions/*"
    );

    let ai_policy =
        production_part(&read_repo("crates/harness-contract/src/policy/mod.rs")).to_string();
    assert!(
        ai_policy.contains("pub enum PermissionResource")
            && ai_policy.contains("Tool")
            && ai_policy.contains("pub struct RiskGateReceipt"),
        "harness-contract policy must expose generic tool risk-gate contracts"
    );
}

#[test]
fn prompt_cache_is_owned_by_model_protocol_boundary() {
    let model_protocol_manifest = read_repo("crates/model-protocol/Cargo.toml");
    let model_protocol_source = read_repo("crates/model-protocol/src/lib.rs");
    assert!(model_protocol_manifest.contains("name = \"model-protocol\""));
    assert!(
        model_protocol_source.contains("pub mod prompt_cache"),
        "model-protocol must own prompt cache protocol semantics"
    );

    assert!(
        !repo_root()
            .join("crates/runtime/src/prompt_cache.rs")
            .exists(),
        "runtime must not retain a prompt_cache compatibility module"
    );

    let provider_manifest = read_repo("crates/provider/Cargo.toml");
    let provider_source = [
        read_repo("crates/provider/src/lib.rs"),
        read_repo("crates/provider/src/client.rs"),
        read_repo("crates/provider/src/cached_client.rs"),
        read_repo("crates/provider/src/providers/anthropic.rs"),
    ]
    .join("\n");
    assert!(
        manifest_dependencies(&provider_manifest)
            .contains("model-protocol = { path = \"../model-protocol\" }"),
        "provider must depend on model-protocol for prompt cache contracts"
    );
    assert!(
        provider_source.contains("model_protocol::prompt_cache"),
        "provider must import prompt cache contracts from model-protocol"
    );
    assert!(
        !provider_source.contains("runtime::prompt_cache"),
        "provider must not import prompt cache contracts from runtime"
    );
}

#[test]
fn usage_contracts_are_owned_by_model_protocol_boundary() {
    let model_protocol_source = read_repo("crates/model-protocol/src/lib.rs");
    let usage_source = read_repo("crates/model-protocol/src/usage.rs");
    assert!(
        model_protocol_source.contains("pub mod usage"),
        "model-protocol must expose usage protocol contracts"
    );
    for contract in [
        "pub struct ModelPricing",
        "pub struct TokenUsage",
        "pub struct UsageCostEstimate",
        "pub fn format_usd",
        "pub fn heuristic_pricing_for_model",
    ] {
        assert!(
            usage_source.contains(contract),
            "model-protocol usage contract missing {contract}"
        );
    }

    let runtime_usage = read_repo("crates/runtime/src/provider/usage.rs");
    assert!(
        runtime_usage.contains("pub struct UsageTracker"),
        "runtime keeps session-derived usage tracking"
    );
    assert!(
        runtime_usage.contains("model_protocol::model_registry::pricing_for_model(model)"),
        "runtime pricing lookup must delegate to model-protocol registry"
    );

    let provider_types = read_repo("crates/provider/src/types.rs");
    assert!(
        provider_types.contains("use model_protocol::usage::{TokenUsage, UsageCostEstimate};"),
        "provider protocol response types must use model-protocol usage contracts"
    );
}

#[test]
fn model_registry_is_owned_by_model_protocol_boundary() {
    let model_protocol_source = read_repo("crates/model-protocol/src/lib.rs");
    let model_registry = read_repo("crates/model-protocol/src/model_registry.rs");
    assert!(
        model_protocol_source.contains("pub mod model_registry"),
        "model-protocol must expose model registry protocol metadata"
    );
    for contract in [
        "pub struct ModelRegistry",
        "pub struct ModelInfo",
        "pub struct Pricing",
        "pub struct ModelResolver",
        "pub fn global_registry",
        "pub fn pricing_for_model",
    ] {
        assert!(
            model_registry.contains(contract),
            "model-protocol model registry missing {contract}"
        );
    }

    assert!(
        !repo_root()
            .join("crates/runtime/src/model_registry.rs")
            .exists(),
        "runtime must not retain a model_registry compatibility module"
    );

    let runtime_usage = read_repo("crates/runtime/src/provider/usage.rs");
    assert!(
        runtime_usage.contains("model_protocol::model_registry::pricing_for_model(model)"),
        "runtime pricing lookup must delegate to model-protocol model registry"
    );

    let provider_sources = [
        read_repo("crates/provider/src/types.rs"),
        read_repo("crates/provider/src/providers/mod.rs"),
    ]
    .join("\n");
    assert!(
        provider_sources.contains("model_protocol::model_registry"),
        "provider must read model metadata through model-protocol"
    );
    assert!(
        !provider_sources.contains("runtime::model_registry")
            && !provider_sources.contains("runtime::pricing_for_model"),
        "provider must not use runtime for model metadata or pricing"
    );
}

#[test]
fn provider_config_and_oauth_contracts_are_owned_by_model_protocol_boundary() {
    let model_protocol_source = read_repo("crates/model-protocol/src/lib.rs");
    assert!(
        model_protocol_source.contains("pub mod provider_config")
            && model_protocol_source.contains("pub mod oauth"),
        "model-protocol must expose provider config and OAuth contracts"
    );

    let provider_config = read_repo("crates/model-protocol/src/provider_config.rs");
    for contract in [
        "pub struct ProviderConfig",
        "pub struct ProvidersConfig",
        "pub fn resolve(",
        "pub fn resolve_full(",
    ] {
        assert!(
            provider_config.contains(contract),
            "provider config contract missing {contract}"
        );
    }

    let oauth = read_repo("crates/model-protocol/src/oauth.rs");
    for contract in [
        "pub struct OAuthConfig",
        "pub struct OAuthTokenSet",
        "pub struct OAuthRefreshRequest",
        "pub struct OAuthTokenExchangeRequest",
        "pub fn load_oauth_credentials",
        "pub fn save_oauth_credentials",
        "pub fn clear_oauth_credentials",
    ] {
        assert!(
            oauth.contains(contract),
            "OAuth contract missing {contract}"
        );
    }

    assert!(
        !repo_root().join("crates/runtime/src/oauth.rs").exists(),
        "runtime must not retain an OAuth compatibility module"
    );

    let provider_manifest = read_repo("crates/provider/Cargo.toml");
    let provider_dependencies = manifest_dependencies(&provider_manifest);
    assert!(
        provider_dependencies.contains("model-protocol = { path = \"../model-protocol\" }"),
        "provider must depend on model-protocol contracts"
    );
    assert!(
        !provider_dependencies.contains("runtime = { path = \"../runtime\" }"),
        "provider production dependencies must not include runtime"
    );

    let provider_sources = [
        read_repo("crates/provider/src/client.rs"),
        read_repo("crates/provider/src/providers/anthropic.rs"),
    ]
    .join("\n");
    assert!(
        provider_sources.contains("model_protocol::provider_config::ProviderConfig")
            && provider_sources.contains("model_protocol::oauth"),
        "provider source must use model-protocol provider/OAuth contracts"
    );
    assert!(
        !provider_sources.contains("runtime::ProviderConfig")
            && !provider_sources.contains("runtime::OAuth")
            && !provider_sources.contains("runtime::load_oauth_credentials")
            && !provider_sources.contains("runtime::save_oauth_credentials"),
        "provider source must not use runtime provider/OAuth contracts"
    );
}

#[test]
fn runtime_uses_ai_kernel_as_harness_semantic_entrypoint() {
    let manifest = read_repo("crates/runtime/Cargo.toml");
    let dependencies = manifest_dependencies(&manifest);
    assert!(
        dependencies.contains("harness-contract = { path = \"../harness-contract\" }"),
        "runtime must depend on harness-contract as the unified harness semantic entrypoint"
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
    ] {
        assert!(
            !dependencies.contains(forbidden),
            "runtime must not directly depend on legacy AI semantic crate {forbidden}; use harness-contract"
        );
    }
}

#[test]
fn entry_boundary_crates_exist_as_migration_targets() {
    let root = repo_root();
    let workspace_manifest = read_repo("Cargo.toml");
    assert!(
        workspace_manifest.contains("default-members = [")
            && !source_between(&workspace_manifest, "default-members = [", "]")
                .contains("\"crates/tui\""),
        "workspace default members must exclude tui; TUI builds only through explicit full/surface selection"
    );

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
        cli_dependencies.contains("tui = { path = \"../tui\", optional = true }")
            && cli_manifest.contains("\"tui-surface\"")
            && cli_manifest.contains("\"tui/code-highlight\"")
            && cli_manifest.contains("\"gateway/code-index\""),
        "cli must keep TUI as a full-build-only optional surface dependency"
    );
    assert!(
        cli_main.contains("open_tui_or_exit()")
            && cli_main.contains("#[cfg(feature = \"tui-surface\")]")
            && cli_main.contains("tui::terminal_entry()"),
        "cli launcher must route TUI usage through the feature-gated TUI surface entry"
    );
    assert!(
        cli_main.contains("TUI surface is not built in this binary"),
        "minimal CLI build must fail explicitly when a user asks for TUI-only startup"
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
    assert!(
        !cli_manifest.contains("runtime = ") && !cli_manifest.contains("memory = "),
        "cli must not depend on runtime or memory, including dev-dependencies"
    );
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
        "storage",
        "tools",
        "rusqlite",
    ] {
        assert!(
            !tui_dependencies.contains(forbidden),
            "tui manifest must not depend directly on {forbidden}"
        );
    }

    assert!(
        !root.join("crates/slash").exists(),
        "slash crates are absorbed into gateway"
    );
    let lockfile = read_repo("Cargo.lock");
    assert!(
        !lockfile.contains("name = \"slash-contract\"")
            && !lockfile.contains("name = \"slash-catalog\"")
            && !lockfile.contains("name = \"command-contract\"")
            && !lockfile.contains("name = \"command-service\""),
        "slash command parsing must not be split into standalone command crates"
    );
    let slash_catalog_source = read_repo("crates/gateway/src/command/slash/mod.rs");
    assert!(
        slash_catalog_source.contains("pub mod parser")
            && slash_catalog_source.contains("classify_skills_slash_command")
            && slash_catalog_source.contains("pub mod specs"),
        "gateway command::slash must own slash parser/spec projection"
    );
    let system_routes = read_repo("crates/gateway/src/api_routes/system_routes.rs");
    let slash_routes = read_repo("crates/gateway/src/api_routes/slash_routes.rs");
    assert!(
        !system_routes.contains("/api/commands")
            && !system_routes.contains("/api/slash")
            && slash_routes.contains("/api/slash")
            && slash_routes.contains("slash_dispatch_handler"),
        "slash API must be owned by slash_routes, not system_routes or legacy /api/commands"
    );
    let gateway_services = read_repo("crates/gateway/src/services/mod.rs");
    assert!(
        gateway_services.contains("pub(crate) slash: SlashController")
            && !gateway_services.contains("pub(crate) command:"),
        "GatewayServices must expose slash controller, not a command execution service"
    );
}

#[test]
fn tui_terminal_path_requires_gateway_backend_instead_of_local_runtime_fallback() {
    let full_source = read_repo("crates/tui/src/gateway/runner.rs");
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
        source.contains("dispatch_gateway_slash("),
        "TUI slash command submit must use Gateway HTTP slash API"
    );
    assert!(
        source.contains("dispatch_gateway_cancel("),
        "TUI cancel must use Gateway HTTP control API"
    );
}

#[test]
fn tui_surface_projection_uses_gateway_surface_api_without_platform_channel_templates() {
    let gateway_client =
        production_part(&read_repo("crates/tui/src/gateway/gateway_client.rs")).to_string();
    for required in [
        "pub async fn surface_registry",
        "pub async fn surface_health_summary",
        "pub async fn surface_status",
        "pub async fn surface_health_check",
        "pub async fn surface_repair",
        "pub async fn surface_events",
        "pub async fn surface_messages",
        "pub async fn surface_archive_messages",
        "pub async fn surface_purge_archived_events",
        "pub async fn surface_send",
        "pub async fn surface_action",
        "/api/surfaces",
        "/api/surfaces/{}/status",
        "/api/surfaces/{}/health-check",
        "/api/surfaces/{}/repair",
        "/api/surfaces/{}/messages",
        "/api/surfaces/{}/messages/archive",
        "/api/surfaces/{}/messages/purge-archived-events",
        "/api/surfaces/{}/send",
        "/api/surfaces/{}/action",
        "pub async fn message_connectors",
        "pub async fn message_connector_status",
        "pub async fn message_connector_repair",
        "pub async fn message_endpoints",
        "pub async fn message_routes",
        "pub async fn message_bindings",
        "/api/message-connectors",
        "/api/message-endpoints",
        "/api/message-routes",
        "/api/message-bindings",
    ] {
        assert!(
            gateway_client.contains(required),
            "TUI GatewayApiClient must expose Gateway surface API `{required}`"
        );
    }

    for file in [
        "crates/tui/src/components/gateway_panel.rs",
        "crates/tui/src/components/command_palette.rs",
        "crates/tui/src/app_core/state.rs",
        "crates/tui/src/gateway/runner.rs",
    ] {
        let source = production_part(&read_repo(file)).to_string();
        for forbidden in [
            "channel.feishu.send_text",
            "channel.feishu.send_image",
            "channel.feishu.send_file",
            "Cross-Plane Adapters",
            "GatewayActionTemplate",
            "GatewayAdapterCapability",
        ] {
            assert!(
                !source.contains(forbidden),
                "{file} must not retain legacy platform channel template `{forbidden}`"
            );
        }
    }

    let runtime_store = production_part(&read_repo(
        "crates/tui/src/app_core/runtime_control_store.rs",
    ))
    .to_string();
    for required in [
        "pub struct MessageConnectorSummary",
        "pub struct MessageEndpointSummary",
        "pub struct MessageRouteSummary",
        "pub struct MessageBindingSummary",
        "ingest_message_connectors",
        "ingest_message_endpoints",
        "ingest_message_routes",
        "ingest_message_bindings",
        "projection.message_connectors()",
        "projection.message_endpoints()",
        "projection.message_routes()",
        "projection.message_bindings()",
    ] {
        assert!(
            runtime_store.contains(required),
            "TUI runtime control store must consume Message Plane `{required}`"
        );
    }

    let gateway_panel =
        production_part(&read_repo("crates/tui/src/components/gateway_panel.rs")).to_string();
    let surface_panel =
        production_part(&read_repo("crates/tui/src/components/surface_panel.rs")).to_string();
    assert!(
        gateway_panel.contains("Message Plane")
            && gateway_panel.contains("message_connectors")
            && surface_panel.contains("Message Plane")
            && surface_panel.contains("message_connectors")
            && surface_panel.contains("g ledger")
            && surface_panel.contains("A archive")
            && surface_panel.contains("P purge"),
        "TUI Gateway and Surface panels must display Message Plane and Surface ledger state"
    );
}

#[test]
fn surface_is_gateway_owned_and_runtime_host_uses_runtime_service_turns() {
    let runtime_manifest = read_repo("crates/runtime/Cargo.toml");
    let runtime_dependencies = manifest_dependencies(&runtime_manifest);
    assert!(
        !runtime_dependencies.contains("channel = ")
            && !runtime_dependencies.contains("channel-adapters = "),
        "runtime must not depend on channel crates; channel is a Gateway ingress/egress boundary"
    );

    let gateway_manifest = read_repo("crates/gateway/Cargo.toml");
    let gateway_dependencies = manifest_dependencies(&gateway_manifest);
    assert!(
        !gateway_dependencies.contains("channel = { path = \"../channel\" }")
            && gateway_dependencies.contains("surface = { path = \"../surface\" }")
            && !gateway_dependencies.contains("channel-adapters = "),
        "gateway must consume surface::message contracts and Edge sidecar protocol without adapter SDK coupling"
    );

    let runtime_host =
        production_part(&read_repo("crates/gateway/src/runtime_host/mod.rs")).to_string();
    assert!(
        !runtime_host.contains(".run_turn_async("),
        "runtime host must not directly call run_turn_async"
    );
    assert!(
        runtime_host.contains("RuntimeService::new("),
        "runtime host must assemble Gateway RuntimeService instead of executing turns directly"
    );
    assert!(
        (runtime_host.contains("GatewayServices::new(")
            || runtime_host.contains("GatewayServices::new_with_config_home("))
            && runtime_host.contains("RuntimeService::new(")
            && runtime_host.contains(".with_approval_gate("),
        "runtime host must register an approval-wired RuntimeService through GatewayServices"
    );
    assert!(
        !runtime_host.contains("PlatformRuntime"),
        "runtime host must not embed platform runtime"
    );

    let gateway_services = read_repo("crates/gateway/src/services/registry.rs");
    assert!(
        gateway_services.contains("self.cross_plane.label")
            && gateway_services.contains("self.cross_plane.contracts()")
            && gateway_services.contains("(\"cross_plane\", \"summary\")"),
        "CrossPlaneService must be part of GatewayServices labels, contracts, and health gates"
    );
}

#[test]
fn cross_plane_state_and_execution_are_runtime_scoped() {
    let gateway_service_source = read_repo("crates/gateway/src/services/cross_plane_service.rs");
    let gateway_service = production_part(&gateway_service_source);
    let gateway_routes_source = read_repo("crates/gateway/src/api_routes/cross_plane_routes.rs");
    let gateway_routes = production_part(&gateway_routes_source);
    let runtime_service_source =
        read_repo("crates/runtime/src/execution_core/cross_plane/service.rs");
    let runtime_service = production_part(&runtime_service_source);
    for forbidden in [
        "CROSS_PLANE_CONTROL",
        "CROSS_PLANE_LOADED",
        "control-state.json",
        "save_to_path",
        "load_from_path",
    ] {
        assert!(!gateway_service.contains(forbidden));
        assert!(!runtime_service.contains(forbidden));
    }
    assert!(gateway_service.contains("runtime_services.cross_plane()"));
    assert!(runtime_service.contains("RuntimeEventScope::CrossPlane"));
    assert!(runtime_service.contains("compile_commit_graph"));
    assert!(!gateway_routes.contains("dispatch_ready_target"));
    assert!(!gateway_routes.contains("services.surface.send"));
}

#[test]
fn production_gateway_entry_does_not_run_ai_turns_directly() {
    let main_source = read_repo("crates/gateway/src/main.rs");
    let production_main = production_part(&main_source);
    assert!(
        !production_main.contains("run_turn_async("),
        "production gateway entry must not directly execute AI turns; use RuntimeService or Gateway HTTP surfaces"
    );

    let runtime_service_source = read_repo("crates/gateway/src/runtime/runtime_service.rs");
    let runtime_service = production_part(&runtime_service_source);
    assert!(
        runtime_service.contains("run_turn_with_timeout")
            && runtime_service.contains(".submit_turn(&content")
            && !runtime_service.contains(".run_turn_async("),
        "RuntimeService must submit through StandardRuntimeHost and the canonical graph runner"
    );
}

#[test]
fn gateway_runtime_factory_owns_runtime_assembly_without_legacy_direct_ai_shell() {
    let main_source = read_repo("crates/gateway/src/main.rs");
    let production_main = production_part(&main_source);
    for forbidden in [
        "struct BuiltRuntime",
        "impl BuiltRuntime",
        "AnthropicRuntimeClient",
        "CliToolExecutor",
        "CliPermissionPrompter",
        "LiveCli",
        "struct PromptHistoryEntry",
        "run_prompt(",
        "handle_repl_command",
        "run_removed_repl",
    ] {
        assert!(
            !main_source.contains(forbidden),
            "gateway main must not retain legacy direct AI shell symbol {forbidden}"
        );
    }
    for forbidden in [
        "use provider::",
        "provider::ProviderClient",
        "ApiProviderClient",
        "CachedProviderClient",
        "PromptCache",
        "MessageRequest",
        "ApiStreamEvent",
        "ContentBlockDelta",
    ] {
        assert!(
            !production_main.contains(forbidden),
            "gateway production main must not directly create provider clients: {forbidden}"
        );
    }

    let runtime_factory =
        production_part(&read_repo("crates/gateway/src/runtime/runtime_factory.rs")).to_string();
    assert!(
        runtime_factory.contains("pub(crate) fn create_runtime_entry(")
            && runtime_factory.contains("pub(crate) fn create_runtime_entry_with_session_store(")
            && runtime_factory.contains("runtime::StandardRuntimeHost::new")
            && runtime_factory.contains("GatewayToolExecutor::from_tool_host")
            && !runtime_factory.contains("ProviderRuntimeClient")
            && !runtime_factory.contains("ConversationRuntime::new"),
        "runtime_factory must delegate provider/conversation assembly to runtime::StandardRuntimeHost"
    );

    let runtime_entry =
        production_part(&read_repo("crates/gateway/src/runtime/runtime_entry.rs")).to_string();
    assert!(
        runtime_entry.contains("runtime::StandardRuntimeHost<GatewayToolExecutor>")
            && !runtime_entry.contains("ProviderRuntimeClient")
            && !runtime_entry.contains("ConversationRuntime<"),
        "GatewayRuntimeEntry must keep provider/conversation concrete types hidden behind Runtime host"
    );

    let session_routes = read_repo("crates/gateway/src/api_routes/session_routes.rs");
    assert!(
        session_routes.contains("crate::runtime_factory::create_runtime_entry(")
            && session_routes
                .contains("crate::runtime_factory::create_runtime_entry_with_session_store(")
            && !session_routes.contains("crate::create_runtime_entry(")
            && !session_routes.contains("crate::create_runtime_entry_with_session_store("),
        "session routes must call runtime_factory instead of gateway root factories"
    );
}

#[test]
fn v3_removed_execution_owners_cannot_reappear_in_production() {
    let sources = production_rust_sources(&[
        "crates/runtime/src",
        "crates/gateway/src",
        "crates/harness-contract/src",
        "crates/app-mfg/src",
    ]);
    let forbidden = [
        ("WorkGraph", "legacy work graph contract"),
        ("AgentWorkGraph", "legacy agent work graph"),
        ("AgentRunGraph", "legacy agent run graph"),
        ("workgraph::", "legacy workgraph module"),
        ("agent_workgraph", "legacy agent workgraph module"),
        ("run_turn_async(", "graph-bypassing turn loop"),
        (
            "assistant_messages.last()",
            "transcript-tail result inference",
        ),
        ("TeamExecutionLoop::tick_ready", "second team scheduler"),
        ("blocked_missing_executor", "late missing-executor fallback"),
        ("agent.waiting_executor", "fake waiting executor state"),
        (
            "SessionExecutionPlane",
            "graph-external session execution owner",
        ),
        (
            "materialize_session_input_decision",
            "gateway session materializer",
        ),
        ("CROSS_PLANE_CONTROL", "gateway cross-plane state singleton"),
        ("CROSS_PLANE_LOADED", "gateway cross-plane load singleton"),
        ("control-state.json", "cross-plane JSON truth source"),
        ("global_runtime_event_store", "global runtime event store"),
        ("global_approval_queue", "global approval truth source"),
        ("global_conflict_arbiter", "global conflict truth source"),
        (
            "global_runtime_control_plane",
            "global runtime control plane",
        ),
        ("global_task_registry", "global task registry"),
    ];

    for (path, source) in &sources {
        if path.ends_with("/recovery/source_self_audit.rs") {
            continue;
        }
        for (needle, owner) in forbidden {
            assert!(
                !source.contains(needle),
                "{path} must not restore {owner}: `{needle}`"
            );
        }
    }

    assert!(
        !repo_root().join("crates/runtime/src/persistence").exists(),
        "dead runtime persistence facade must be deleted"
    );
    assert!(
        !repo_root()
            .join("crates/runtime/src/orchestration/executor.rs")
            .exists(),
        "orchestration must compile commands for the canonical Runner, not own an executor"
    );
}

#[test]
fn v3_turn_call_trace_has_one_compiler_runner_and_commit_owner() {
    let gateway_service = read_repo("crates/gateway/src/runtime/runtime_service.rs");
    let gateway_entry = read_repo("crates/gateway/src/runtime/runtime_entry.rs");
    let host = read_repo("crates/runtime/src/conversation/host.rs");
    let runner = read_repo("crates/runtime/src/execution_core/graph/runner.rs");
    let commit = read_repo("crates/runtime/src/execution_core/graph/commit_service.rs");

    assert!(production_part(&gateway_service).contains(".submit_turn(&content"));
    assert!(gateway_entry.contains("self.runtime_mut().submit_turn"));
    assert!(production_part(&host).contains("ExecutionGraphCompiler"));
    assert!(production_part(&host).contains("services.graph_runner().start(graph).await"));
    assert!(production_part(&runner).contains("self.commit_service.register_graph_async(graph)"));
    assert!(production_part(&runner).contains("bind_and_start_node_async"));
    assert!(production_part(&commit).contains("append_transaction"));
}

#[test]
fn v3_runtime_services_are_scoped_and_executors_are_fixed_at_assembly() {
    let services = read_repo("crates/runtime/src/execution_core/services.rs");
    let host = read_repo("crates/runtime/src/conversation/host.rs");
    let services = production_part(&services);
    let host = production_part(&host);

    for required in [
        "pub struct RuntimeServices",
        "pub fn builder(",
        "pub fn in_memory()",
        "workspace_key",
        "event_store: Arc<RuntimeEventStore>",
        "executor_registry: Arc<NodeExecutorRegistry>",
        "graph_runner: Arc<ExecutionGraphRunner>",
        "resource_manager: Arc<ExecutionResourceManager>",
        "scope_locks: Arc<ScopeLockManager>",
        "worktree_leases: Arc<WorktreeLeaseManager>",
    ] {
        assert!(
            services.contains(required),
            "RuntimeServices missing `{required}`"
        );
    }

    assert!(
        services.contains("register_builtin_executors")
            || services.contains("install_builtin_executors"),
        "RuntimeServices assembly must install the fixed canonical executor set"
    );
    assert!(
        !host.contains("executor_registry().register(")
            && !host.contains("executor_registry().unregister("),
        "turn execution must not mutate the shared executor registry per request"
    );

    for (path, source) in production_rust_sources(&["crates/runtime/src", "crates/gateway/src"]) {
        if path == "crates/runtime/src/execution_core/services.rs" {
            continue;
        }
        assert!(
            !source.contains("executor_registry().register(")
                && !source.contains("executor_registry().unregister(")
                && !source.contains(".unregister(&executor_kind)"),
            "{path} must bind graph-scoped backends instead of mutating the executor registry"
        );
    }

    let cross_plane = read_repo("crates/gateway/src/services/cross_plane_service.rs");
    let cross_plane = production_part(&cross_plane);
    assert!(cross_plane.contains("cross_plane_connector_executor()"));
    assert!(cross_plane.contains(".install_resolver("));
    assert!(!cross_plane.contains(".bind(") && !cross_plane.contains(".unbind("));
    assert!(!cross_plane.contains("executor_registry()"));
}

#[test]
fn v3_bidirectional_outboxes_have_started_production_pumps() {
    let services = read_repo("crates/runtime/src/execution_core/services.rs");
    let ingress_bridge = read_repo("crates/runtime/src/session/session_execution.rs");
    let delivery_bridge = read_repo("crates/gateway/src/runtime/session_runtime_bridge.rs");
    let gateway_host = read_repo("crates/gateway/src/runtime_host/mod.rs");
    let services = production_part(&services);
    let ingress_bridge = production_part(&ingress_bridge);
    let delivery_bridge = production_part(&delivery_bridge);
    let gateway_host = production_part(&gateway_host);

    assert!(ingress_bridge.contains("claim_session_runtime_outbox"));
    assert!(ingress_bridge.contains("ack_session_runtime_outbox"));
    assert!(delivery_bridge.contains("claim_session_terminals"));
    assert!(delivery_bridge.contains("ack_session_terminal"));
    assert!(
        services.contains("SessionRuntimeBridge") || services.contains("SessionInputRouter"),
        "workspace RuntimeServices must own the durable session bridge"
    );
    assert!(
        gateway_host.contains("SessionRuntimeBridge::start("),
        "Gateway startup must start the bidirectional outbox pump; request-path polling is insufficient"
    );
}

#[test]
fn v3_gateway_startup_recovers_persistent_execution_graphs() {
    let gateway_host = read_repo("crates/gateway/src/runtime_host/mod.rs");
    let services = read_repo("crates/runtime/src/execution_core/services.rs");
    let state_store = read_repo("crates/runtime/src/execution_core/graph/state_store.rs");
    let gateway_host = production_part(&gateway_host);
    let services = production_part(&services);
    let state_store = production_part(&state_store);

    assert!(
        gateway_host.contains(".recover_execution_graphs_on_startup()"),
        "Gateway startup must invoke runtime-owned startup recovery after executor resolvers are installed"
    );
    assert!(
        gateway_host.contains("emit_execution_startup_recovery"),
        "Gateway startup must emit structured recovery diagnostics"
    );
    assert!(
        services.contains("pub async fn recover_execution_graphs_on_startup"),
        "RuntimeServices must own the startup recovery coordinator"
    );
    assert!(
        services.contains("ExecutionGraphRecovery::new"),
        "RuntimeServices recovery must reuse the canonical graph recovery service"
    );
    assert!(
        services.contains("self.graph_runner.run_until_quiescent"),
        "RuntimeServices recovery must continue ready/planned graphs through the canonical runner"
    );
    assert!(
        state_store.contains("pub fn nonterminal_graph_ids"),
        "ExecutionGraphStateStore must enumerate persisted nonterminal graphs without Gateway SQL"
    );
}

#[test]
fn v3_approval_and_turn_boundaries_have_no_gateway_dual_write_state() {
    let approval = read_repo("crates/gateway/src/services/approval_service.rs");
    let commit = read_repo("crates/runtime/src/execution_core/graph/commit_service.rs");
    let routes = production_rust_sources(&["crates/gateway/src/api_routes"]);

    assert!(!approval.contains("reconcile_graph_decisions"));
    assert!(commit.contains("ExecutionGraphCommand::SubmitApproval"));
    assert!(commit.contains("\"approval.decided\""));
    assert!(commit.contains("append_transaction(request)"));

    for (path, source) in routes {
        for forbidden in [
            "ACTIVE_TURN_CONTROLS",
            "ACTIVE_TURN_PARTIALS",
            "register_active_turn_control",
            "take_active_turn_partial",
            "append_session_timeline_event",
        ] {
            assert!(
                !source.contains(forbidden),
                "{path} must not restore dead turn state or direct timeline writes: `{forbidden}`"
            );
        }
    }
}

#[test]
fn v3_executor_binding_is_committed_before_running_state() {
    let runner = read_repo("crates/runtime/src/execution_core/graph/runner.rs");
    let commit = read_repo("crates/runtime/src/execution_core/graph/commit_service.rs");
    let runner = production_part(&runner);
    let commit = production_part(&commit);
    let ready_wave = source_between(
        runner,
        "async fn start_and_execute_node(",
        "async fn acquire_node_resources(",
    );

    let start = ready_wave
        .find("let ticket = executor")
        .expect("executor must return a durable ticket before Running");
    let bind = ready_wave
        .find("bind_and_start_node_async")
        .expect("Runner must atomically bind and start a node");
    assert!(
        start < bind,
        "executor ticket/binding must exist before Running commit"
    );
    assert!(
        !ready_wave[..bind].contains("ExecutionNodeStatus::Running"),
        "Runner must not persist Running before executor binding"
    );

    let bind_commit = source_between(
        commit,
        "pub fn bind_and_start_node(",
        "pub async fn bind_and_start_node_async",
    );
    assert!(bind_commit.contains("binding: Some(binding)"));
    assert!(bind_commit.contains("ExecutionNodeStatus::Running"));
    assert!(bind_commit.contains("append_graph_event("));
}

#[test]
fn runtime_source_self_audit_is_exposed_through_gateway_api() {
    let runtime_lib = read_repo("crates/runtime/src/lib.rs");
    let runtime_audit = read_repo("crates/runtime/src/recovery/source_self_audit.rs");
    let old_route_file = ["channel", "_routes.rs"].concat();
    let runtime_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/runtime_routes.rs",
    ))
    .to_string();

    assert!(
        runtime_lib.contains("pub mod source_self_audit")
            && runtime_lib.contains("RuntimeSourceSelfAudit")
            && runtime_audit.contains("runtime.no_surface_sdk_dependency")
            && runtime_audit.contains("gateway.owns_surface_boundary")
            && runtime_audit.contains("gateway.runtime_host_uses_runtime_service")
            && runtime_audit.contains("harness_eval.repair_hints")
            && runtime_audit.contains("message_connector_routes.rs")
            && !runtime_audit.contains(&old_route_file),
        "runtime must expose source-aware self audit checks with repair hints"
    );
    assert!(
        runtime_routes.contains("/api/runtime/source-audit")
            && runtime_routes.contains("/api/runtime/source-repair-plan")
            && runtime_routes.contains("RuntimeSourceSelfAudit::audit_repo"),
        "Gateway must expose runtime source audit and repair plan APIs"
    );
}

#[test]
fn context_envelope_projection_is_exposed_to_reality_and_surfaces() {
    let memory_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/memory_routes.rs")).to_string();
    let reality_service =
        production_part(&read_repo("crates/gateway/src/services/reality_service.rs")).to_string();
    let context_history = production_part(&read_repo(
        "crates/gateway/src/services/context_service/history.rs",
    ))
    .to_string();
    let tui_gateway_panel =
        production_part(&read_repo("crates/tui/src/components/gateway_panel.rs")).to_string();
    let tui_runtime_store = production_part(&read_repo(
        "crates/tui/src/app_core/runtime_control_store.rs",
    ))
    .to_string();
    let edge_root = repo_root().join("../cowd-edge");
    let webui_client = std::fs::read_to_string(edge_root.join("surfaces/webui/src/api/client.ts"))
        .expect("webui client should read");
    let webui_memory =
        std::fs::read_to_string(edge_root.join("surfaces/webui/src/pages/MemoryPage.vue"))
            .expect("webui memory page should read");
    let webui_reality =
        std::fs::read_to_string(edge_root.join("surfaces/webui/src/pages/RealityCorePage.vue"))
            .expect("webui reality page should read");

    assert!(
        memory_routes.contains("/api/memory/context-envelope")
            && memory_routes.contains("/api/memory/context-envelope/:session_id")
            && memory_routes.contains("memory_context_envelope_projection_value")
            && memory_routes.contains("context_envelope_capability_from_projection"),
        "memory routes must expose real ContextEnvelope projection APIs and capability status"
    );
    assert!(
        context_history.contains("context_envelope_projection(")
            && context_history.contains("stored_events_by_type_page")
            && context_history.contains("\"ContextEnvelope\"")
            && context_history.contains("memory.context_envelope_projection")
            && context_history.contains("compression_status")
            && context_history.contains("recall_quality_status"),
        "ContextService must derive ContextEnvelope projection from persisted session events"
    );
    let stale_context_envelope_fallback = [
        "ContextEnvelope status is not exposed by ",
        "memory projection",
    ]
    .concat();
    assert!(
        reality_service.contains("\"context_runtime\"")
            && reality_service.contains("context_runtime_projection")
            && reality_service.contains("inject_context_envelope_projection")
            && !reality_service.contains(&stale_context_envelope_fallback),
        "Reality status must consume ContextEnvelope projection instead of a stale fallback"
    );
    assert!(
        tui_gateway_panel.contains("ContextEnvelope:")
            && tui_runtime_store.contains("context_envelope_projection")
            && tui_runtime_store.contains("memory_context_envelope_used_ratio"),
        "TUI Gateway status panel must render ContextEnvelope runtime projection from Gateway status"
    );
    assert!(
        webui_client.contains("memoryContextEnvelope")
            && webui_client.contains("/api/memory/context-envelope")
            && webui_memory.contains("data-section=\"context-envelope\"")
            && webui_reality.contains("data-section=\"context-runtime\""),
        "WebUI must consume and render ContextEnvelope and Reality context-runtime sections"
    );
}

#[test]
fn knowledge_governance_projection_is_exposed_to_reality_and_webui() {
    let memory_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/memory_routes.rs")).to_string();
    let memory_service =
        production_part(&read_repo("crates/gateway/src/services/memory_service.rs")).to_string();
    let reality_service =
        production_part(&read_repo("crates/gateway/src/services/reality_service.rs")).to_string();
    let runtime_activation = read_repo("crates/runtime/src/context/knowledge_activation.rs");
    let memory_knowledge =
        production_part(&read_repo("crates/memory/src/knowledge/mod.rs")).to_string();
    let edge_root = repo_root().join("../cowd-edge");
    let webui_memory =
        std::fs::read_to_string(edge_root.join("surfaces/webui/src/pages/MemoryPage.vue"))
            .expect("webui memory page should read");
    let webui_reality =
        std::fs::read_to_string(edge_root.join("surfaces/webui/src/pages/RealityCorePage.vue"))
            .expect("webui reality page should read");
    let webui_client = std::fs::read_to_string(edge_root.join("surfaces/webui/src/api/client.ts"))
        .expect("webui client should read");

    for route in [
        "/api/memory/knowledge",
        "/api/memory/knowledge/namespaces",
        "/api/memory/knowledge/conflicts",
        "/api/memory/knowledge/maintenance",
    ] {
        assert!(
            memory_routes.contains(route),
            "memory knowledge governance route `{route}` must be exposed"
        );
    }
    for field in [
        "namespace_tree",
        "activation_policy_distribution",
        "governance_distribution",
        "conflict_projection",
        "maintenance_candidates",
        "recall_quality",
    ] {
        assert!(
            memory_knowledge.contains(field),
            "KnowledgeFabric projection missing `{field}`"
        );
    }
    assert!(
        memory_service.contains("knowledge_projection"),
        "MemoryService must expose knowledge_projection"
    );
    assert!(
        reality_service.contains("recall_quality"),
        "RealityService must surface knowledge recall quality"
    );
    assert!(
        runtime_activation.contains("Knowledge pointer only")
            && runtime_activation.contains("is_generic_knowledge_relevance_term"),
        "Runtime knowledge activation must keep low relevance knowledge pointer-only"
    );
    assert!(
        webui_client.contains("memoryKnowledgeNamespaces")
            && webui_client.contains("memoryKnowledgeConflicts")
            && webui_client.contains("memoryKnowledgeMaintenance")
            && webui_memory.contains("data-section=\"knowledge-governance\"")
            && webui_reality.contains("knowledgeRecallQuality"),
        "WebUI must consume knowledge governance APIs and render Memory/Reality projections"
    );
}

#[test]
fn source_connector_realtime_watermark_is_wired_to_matrix_and_webui() {
    let connector_routes = read_repo("crates/gateway/src/api_routes/connector_routes.rs");
    let surface_service = read_repo("crates/gateway/src/services/surface_service.rs");
    let matrix_service = read_repo("crates/gateway/src/services/matrix_service.rs");
    let connector_source = read_repo("crates/connector/src/source.rs");
    let edge_root = std::path::Path::new("/media/yi/Datas/workspace/cowd-edge");
    let edge_sidecar =
        std::fs::read_to_string(edge_root.join("crates/edge-adapters/src/source_sidecar.rs"))
            .expect("edge source sidecar should read");
    let edge_db = std::fs::read_to_string(edge_root.join("crates/edge-adapters/src/source_db.rs"))
        .expect("edge source db should read");
    let webui_client = std::fs::read_to_string(edge_root.join("surfaces/webui/src/api/client.ts"))
        .expect("webui client should read");
    let gateway_page =
        std::fs::read_to_string(edge_root.join("surfaces/webui/src/pages/GatewayPage.vue"))
            .expect("gateway page should read");

    for route in [
        "/api/connectors/sources/:adapter_id/state",
        "/api/connectors/sources/:adapter_id/run-incremental",
        "/api/connectors/sources/:adapter_id/poll-events",
        "/api/connectors/sources/:adapter_id/commit-watermark",
    ] {
        assert!(
            connector_routes.contains(route),
            "connector source route `{route}` must be exposed"
        );
    }
    for dto in [
        "SourceWatermark",
        "SourceIncrementalRunRequest",
        "SourceIncrementalRunResult",
        "SourceEventBatch",
        "SourceIngestionReceipt",
        "SourceConnectorState",
    ] {
        assert!(
            connector_source.contains(dto),
            "connector source contract missing `{dto}`"
        );
    }
    assert!(
        surface_service.contains("source_incremental_run")
            && surface_service.contains("source_event_poll")
            && surface_service.contains("commit_source_watermark")
            && surface_service.contains("\"source.incremental.run\""),
        "SurfaceService must expose typed source action helpers"
    );
    assert!(
        matrix_service.contains("ingest_source_record_batch")
            && matrix_service.contains("SourceIngestionReceipt")
            && matrix_service.contains("plan_data_plane_ingest"),
        "MatrixService must turn source batches into source pack, snapshot, watermark, and receipt"
    );
    for action in [
        "\"source.state\"",
        "\"source.watermark.get\"",
        "\"source.watermark.commit\"",
        "\"source.incremental.run\"",
        "\"source.event.poll\"",
    ] {
        assert!(
            edge_sidecar.contains(action),
            "edge source sidecar missing action {action}"
        );
    }
    assert!(
        edge_db.contains("updated_at_field")
            && edge_db.contains("cursor_field")
            && edge_db.contains("degraded_incremental_offset_only"),
        "edge DB source must implement updated_at/cursor/offset incremental semantics"
    );
    assert!(
        webui_client.contains("connectorSourceRunIncremental")
            && webui_client.contains("connectorSourcePollEvents")
            && gateway_page.contains("sourceRuntimeRows")
            && gateway_page.contains("runEdgeSourceIncremental")
            && gateway_page.contains("pollEdgeSourceEvents"),
        "WebUI must manage source runtime state, watermark, incremental run, and event poll"
    );
}

#[test]
fn api_route_direct_dependencies_are_closed() {
    let allowlist = read_repo("crates/gateway/src/api_routes/service_boundary_policy.txt");

    assert!(
        allowlist.contains("status: closed_by=0.9.303"),
        "service boundary policy must remain closed, not extended"
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
        "api_routes/message_connector_routes.rs",
        "api_routes/connector_routes.rs",
        "api_routes/context_routes.rs",
        "api_routes/core_routes.rs",
        "api_routes/cross_plane_routes.rs",
        "api_routes/growth_routes.rs",
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
