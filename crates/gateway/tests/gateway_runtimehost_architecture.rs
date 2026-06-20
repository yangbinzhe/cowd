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
fn removed_builtin_channel_document_operations_do_not_reappear() {
    let forbidden_terms = [
        "service.feishu.docx",
        "feishu.readonly",
        "docx:read",
        "doc_ops",
    ];
    let checked_files = [
        "crates/channel/src/lib.rs",
        "crates/channel-adapters/src/lib.rs",
        "crates/channel-adapters/src/platform/mod.rs",
        "crates/connector/src/lib.rs",
        "crates/gateway/src/api_routes.rs",
        "crates/gateway/src/runtime_host/mod.rs",
        "crates/runtime/src/lib.rs",
        "crates/tui/src/runtime_control_store.rs",
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
        "ai-workgraph",
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
            "ai-kernel must own {absorbed} semantics internally, not depend on it"
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
        "pub mod store",
        "pub mod indexer",
        "pub mod service",
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

    let runtime_cross_plane =
        production_part(&read_repo("crates/runtime/src/cross_plane_policy.rs")).to_string();
    assert!(
        runtime_cross_plane.contains("use connector::{CrossPlaneRisk, DataClassification};"),
        "runtime policy must consume connector-owned risk/data contracts"
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
fn channel_contracts_drive_gateway_platform_readiness() {
    let channel_source = production_part(&read_repo("crates/channel/src/lib.rs")).to_string();
    for required in [
        "pub struct ChannelContract",
        "pub fn channel_required_fields",
        "pub fn channel_transport_capabilities",
        "pub fn normalize_channel",
    ] {
        assert!(
            channel_source.contains(required),
            "channel contract missing {required}"
        );
    }

    let gateway_manifest = read_repo("crates/gateway/Cargo.toml");
    assert!(
        manifest_dependencies(&gateway_manifest).contains("channel = { path = \"../channel\" }"),
        "gateway must depend on channel for platform/channel contracts"
    );
    assert!(
        manifest_dependencies(&gateway_manifest)
            .contains("channel-adapters = { path = \"../channel-adapters\" }"),
        "gateway must depend on channel-adapters for platform SDK hosting"
    );
    let channel_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/channel_routes.rs",
    ))
    .to_string();
    assert!(
        channel_routes.contains("use channel::{channel_required_fields, ChannelContract};"),
        "gateway channel routes must consume channel contract"
    );
    assert!(
        !channel_routes.contains("fn platform_capabilities"),
        "gateway must not own a duplicate platform capability table"
    );
    assert!(
        !channel_source.contains("feishu_document_operation"),
        "channel contract must not expose Feishu document operations as built-in channel capability"
    );

    let gateway_services_source = read_repo("crates/gateway/src/services/mod.rs");
    let gateway_services = production_part(&gateway_services_source);
    let channel_service_source = read_repo("crates/gateway/src/services/channel_service.rs");
    let channel_service = production_part(&channel_service_source);
    let api_state_source = read_repo("crates/gateway/src/api_routes.rs");
    let api_state = production_part(&api_state_source);
    assert!(
        gateway_services.contains("pub(crate) channel: ChannelService"),
        "GatewayServices must expose the channel service boundary"
    );
    assert!(
        channel_service.contains("pub(crate) struct ChannelService")
            && channel_service.contains("runtime: Option<Arc<PlatformRuntime>>")
            && channel_service.contains("dispatch_payload("),
        "ChannelService must own gateway-side platform runtime access"
    );
    assert!(
        !api_state.contains("pub platform_runtime:"),
        "AppState must not expose PlatformRuntime directly to gateway routes"
    );

    for source_path in [
        "crates/gateway/src/main.rs",
        "crates/gateway/src/api_routes.rs",
        "crates/gateway/src/runtime_host/mod.rs",
        "crates/gateway/src/api_routes/channel_routes.rs",
        "crates/gateway/src/api_routes/cross_plane_routes.rs",
        "crates/gateway/src/entry/workspace_entry.rs",
    ] {
        let source = production_part(&read_repo(source_path)).to_string();
        assert!(
            !source.contains("runtime::platform"),
            "{source_path} must not use runtime::platform as the channel host"
        );
    }

    for source_path in [
        "crates/gateway/src/api_routes/channel_routes.rs",
        "crates/gateway/src/api_routes/cross_plane_routes.rs",
        "crates/gateway/src/api_routes/mfg_routes.rs",
        "crates/gateway/src/gateway_health.rs",
    ] {
        let source = production_part(&read_repo(source_path)).to_string();
        assert!(
            source.contains("services.channel"),
            "{source_path} must access channel runtime through GatewayServices.channel"
        );
        assert!(
            !source.contains("state.platform_runtime"),
            "{source_path} must not bypass ChannelService with AppState.platform_runtime"
        );
    }

    let channel_adapters_source =
        production_part(&read_repo("crates/channel-adapters/src/lib.rs")).to_string();
    assert!(
        channel_adapters_source.contains("pub mod platform"),
        "channel-adapters must expose the gateway-owned platform adapter host"
    );
    assert!(
        repo_root()
            .join("crates/channel-adapters/src/platform")
            .exists(),
        "platform adapter sources must live under channel-adapters"
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
    let memory_types = production_part(&read_repo("crates/memory/src/types.rs")).to_string();
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
    let matrix_fact = production_part(&read_repo("crates/matrix/core/src/fact.rs")).to_string();
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
        production_part(&read_repo("crates/runtime/src/approval_gate.rs")).to_string();
    assert!(
        runtime_approval.contains("use ai_kernel::policy::{"),
        "runtime approval gate must consume ai-kernel policy contracts"
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
            && gateway_approval_routes.contains("state.services.growth.risk_gate_event"),
        "approval API must expose risk receipt projection through audit and growth services"
    );
    let gateway_services =
        production_part(&read_repo("crates/gateway/src/services/mod.rs")).to_string();
    assert!(
        gateway_services.contains("pub(crate) struct ProviderService")
            && gateway_services.contains("pub(crate) struct GrowthService")
            && gateway_services.contains("config_projection")
            && gateway_services.contains("risk_gate_event")
            && gateway_services.contains("risk_gate_projection"),
        "GatewayServices must include concrete provider, growth, and audit projections"
    );

    let task_service =
        production_part(&read_repo("crates/gateway/src/services/task_service.rs")).to_string();
    assert!(
        task_service.contains("pub(crate) async fn append_runtime_event")
            && task_service.contains("ensure_task_session_record"),
        "task service must own task lifecycle runtime-event projection"
    );
    let task_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/task_routes.rs")).to_string();
    assert!(
        !task_routes.contains("async fn append_task_runtime_event")
            && task_routes.contains(".append_runtime_event(&state.services.session"),
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
        production_part(&read_repo("crates/gateway/src/runtime_service.rs")).to_string();
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
    let system_routes =
        production_part(&read_repo("crates/gateway/src/api_routes/system_routes.rs")).to_string();
    assert!(
        system_routes.contains("state.services.provider.config_projection(&runtime_config)"),
        "provider config route must delegate projection to ProviderService"
    );
    let session_service =
        production_part(&read_repo("crates/gateway/src/services/session_service.rs")).to_string();
    assert!(
        session_service.contains("pub(crate) fn last_context_envelope_nonblocking")
            && session_service.contains("pub(crate) async fn active_messages_page"),
        "session service must own active runtime read projections"
    );
    let session_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/session_routes.rs",
    ))
    .to_string();
    assert!(
        !session_routes.contains(".active_runtime(")
            && !session_routes.contains(".remove_active_runtime(")
            && session_routes.contains("has_active_runtime")
            && session_routes.contains("session_exists"),
        "session routes must use SessionService semantic helpers instead of runtime registry internals"
    );
    let runtime_routes = production_part(&read_repo(
        "crates/gateway/src/api_routes/runtime_routes.rs",
    ))
    .to_string();
    let tui_gateway_client_source = read_repo("crates/tui/src/gateway_client.rs");
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

    let ai_policy = production_part(&read_repo("crates/ai-kernel/src/policy/mod.rs")).to_string();
    assert!(
        ai_policy.contains("pub enum PermissionResource")
            && ai_policy.contains("Tool")
            && ai_policy.contains("pub struct RiskGateReceipt"),
        "ai-kernel policy must expose generic tool risk-gate contracts"
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

    let runtime_usage = read_repo("crates/runtime/src/usage.rs");
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

    let runtime_usage = read_repo("crates/runtime/src/usage.rs");
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
        "slash-contract",
        "slash-catalog",
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
        !root.join("crates/slash/catalog/src/parser.rs").exists(),
        "slash-catalog must not duplicate the slash parser owned by slash-contract"
    );
    let lockfile = read_repo("Cargo.lock");
    assert!(
        lockfile.contains("name = \"slash-contract\"")
            && lockfile.contains("name = \"slash-catalog\"")
            && !lockfile.contains("name = \"command-contract\"")
            && !lockfile.contains("name = \"command-service\""),
        "slash crates must use final package names, not legacy command package names"
    );
    let slash_catalog_source = read_repo("crates/slash/catalog/src/lib.rs");
    assert!(
        slash_catalog_source.contains("pub use slash_contract::{")
            && slash_catalog_source.contains("classify_skills_slash_command")
            && !slash_catalog_source.contains("pub mod parser"),
        "slash-catalog must re-export slash-contract parser APIs instead of owning parser logic"
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
        source.contains("dispatch_gateway_slash("),
        "TUI slash command submit must use Gateway HTTP slash API"
    );
    assert!(
        source.contains("dispatch_gateway_cancel("),
        "TUI cancel must use Gateway HTTP control API"
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
