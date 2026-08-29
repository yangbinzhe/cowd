#![allow(unused_imports)]

mod cli;
mod process;
mod setup;
use super::{
    build_memory_config, build_system_prompt_for_mode, cli_turn_context_profile,
    embeddings_endpoint, filter_tool_specs, format_bughunter_report,
    format_commit_preflight_report, format_commit_skipped_report, format_connected_line,
    format_issue_report, format_pr_report, format_startup_banner, format_startup_banner_with_task,
    format_status_report, format_tool_call_start, format_tool_result, format_ultraplan_report,
    format_unknown_slash_command_message, format_user_visible_api_error,
    gateway_allocator_arena_limit, gateway_auth_token_from_platform, handoff_resume_context_packet,
    load_gateway_runtime_config, merge_prompt_with_stdin, normalize_permission_mode, parse_args,
    parse_gateway_args, parse_git_status_branch, parse_git_status_metadata_for,
    parse_git_workspace_summary, permission_policy, print_help_to, push_output_block,
    render_config_report, render_diff_report, render_diff_report_for, render_memory_report,
    render_setup_json, render_setup_report, render_terminal_help, resolve_model_alias_with_config,
    resolve_tui_model, response_to_events, runtime_capability_context_item,
    semantic_checkpoint_resume_context_packet, session_db_resume_context_packet,
    slash_command_completion_candidates_with_sessions, status_context, strip_ansi_for_tui,
    suggestions::format_unknown_slash_command, truncate_for_banner, try_resolve_bare_skill_prompt,
    validate_no_args, workspace_context_item, write_mcp_server_fixture, CliAction, CliOutputFormat,
    GatewayAction, GatewayToolExecutor, GitWorkspaceSummary, LocalHelpTopic, StatusUsage,
    DEFAULT_MODEL_ALIAS, LATEST_SESSION_REFERENCE, NON_EXECUTABLE_SLASH_COMMANDS, SHARED_RT,
};

#[test]
fn managed_gateway_bounds_glibc_arenas_without_overriding_operator_policy() {
    assert_eq!(gateway_allocator_arena_limit(None), Some("2"));
    assert_eq!(
        gateway_allocator_arena_limit(Some(std::ffi::OsString::from("4"))),
        None
    );
}
use crate::command::slash::{resume_supported_slash_commands, SlashCommand};
use crate::provider_crate::{ApiError, MessageResponse, OutputContentBlock, Usage};
use crate::runtime_bootstrap::GatewayToolRegistry as TestToolRegistry;
use crate::runtime_factory::create_runtime_entry_with_bootstrap_state;
use harness_contract::task::{
    TaskAggregate, TaskExecutionPolicy, TaskKind, TaskMissionAssignment, TaskOrigin, TaskPhase,
    TaskPhaseArtifact, TaskPhaseStatus, TaskStatus,
};
use model_protocol::oauth::{save_oauth_credentials, OAuthConfig, OAuthTokenSet};
use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};
use model_protocol::usage::TokenUsage;
use plugins::{
    PluginManager as Pm, PluginManagerConfig as Pmc, PluginTool, PluginToolDefinition,
    PluginToolPermission,
};
use runtime::{
    AssistantEvent, ConfigLoader, ContentBlock, ContextProfile, ConversationMessage,
    GatewayPlatformConfig, JsonValue, MessageRole, PermissionMode, Session, ToolExecutor,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn memory_config_derives_semantic_clients_from_declared_providers() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    fs::write(
        config_home.join("config.yaml"),
        r#"
model: chat-model
providers:
  compatible:
    base_url: https://provider.example/v1
    protocol: completions
    api_key: test-key
    models:
      - chat-model
      - embedding-model
memory:
  enabled: true
  extraction:
    auto_extract: true
  vector:
    enabled: true
    model: embedding-model
"#,
    )
    .expect("write config");

    let runtime_config = ConfigLoader::new(&workspace, &config_home)
        .load()
        .expect("load config");
    let memory_config = build_memory_config(&runtime_config, &workspace).expect("memory config");

    assert_eq!(
        memory_config.store.vector.api_url,
        "https://provider.example/v1/embeddings"
    );
    assert_eq!(memory_config.store.vector.api_key, "test-key");
    assert!(memory_config.compression.llm.enabled);
    assert_eq!(memory_config.compression.llm.api_url, "");
    assert!(memory_config.compression.llm.api_key.is_empty());
    assert_eq!(memory_config.compression.llm.model, "chat-model");
    assert_eq!(
        embeddings_endpoint("https://provider.example/v1/embeddings/"),
        "https://provider.example/v1/embeddings"
    );

    let _ = fs::remove_dir_all(config_home);
    let _ = fs::remove_dir_all(workspace);
}

#[test]
fn gateway_runtime_rejects_invalid_config_instead_of_falling_back_to_sqlite() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    fs::write(
        config_home.join("config.yaml"),
        "storage:\n  backend: invalid-backend\n",
    )
    .expect("invalid config fixture");

    let error = load_gateway_runtime_config(&ConfigLoader::new(&workspace, &config_home))
        .expect_err("invalid config must block Gateway startup");

    assert!(error.contains("refusing to change the selected storage"));
    assert!(error.contains("invalid-backend"));
    fs::remove_dir_all(config_home).expect("config cleanup");
    fs::remove_dir_all(workspace).expect("workspace cleanup");
}

#[test]
fn lark_runtime_config_targets_shared_feishu_surface() {
    let mut extra = BTreeMap::new();
    extra.insert(
        "app_id".to_string(),
        JsonValue::String("app-id".to_string()),
    );
    extra.insert(
        "app_secret".to_string(),
        JsonValue::String("app-secret".to_string()),
    );
    let gateway = runtime::GatewayConfig {
        enabled: true,
        platforms: vec![GatewayPlatformConfig {
            platform_type: "lark".to_string(),
            enabled: true,
            extra,
        }],
        ..Default::default()
    };

    let configs = super::build_surface_runtime_configs(&gateway);
    let config = configs
        .get("feishu")
        .expect("Lark config must target the shared Feishu surface");
    assert!(!configs.contains_key("lark"));
    assert_eq!(config["platform_type"], "lark");
    assert_eq!(config["app_id"], "app-id");
    assert_eq!(config["app_secret"], "app-secret");
}

fn gateway_platform_with_auth_token(auth_token: &str) -> GatewayPlatformConfig {
    let mut extra = BTreeMap::new();
    extra.insert(
        "auth_token".to_string(),
        JsonValue::String(auth_token.to_string()),
    );
    GatewayPlatformConfig {
        platform_type: "api_server".to_string(),
        enabled: true,
        extra,
    }
}

#[test]
fn gateway_auth_token_ignores_empty_and_blank_values() {
    let empty = gateway_platform_with_auth_token("");
    let blank = gateway_platform_with_auth_token("   ");

    assert_eq!(gateway_auth_token_from_platform(&empty), None);
    assert_eq!(gateway_auth_token_from_platform(&blank), None);
}

#[test]
fn gateway_auth_token_trims_non_empty_value() {
    let platform = gateway_platform_with_auth_token("  secret-token  ");

    assert_eq!(
        gateway_auth_token_from_platform(&platform).as_deref(),
        Some("secret-token")
    );
}

fn registry_with_plugin_tool() -> TestToolRegistry {
    TestToolRegistry::with_plugin_tools(vec![PluginTool::new(
        "plugin-demo@external",
        "plugin-demo",
        PluginToolDefinition {
            name: "plugin_echo".to_string(),
            description: Some("Echo plugin payload".to_string()),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" }
                },
                "required": ["message"],
                "additionalProperties": false
            }),
        },
        "echo".to_string(),
        Vec::new(),
        PluginToolPermission::WorkspaceWrite,
        None,
    )])
    .expect("plugin tool registry should build")
}

#[test]
fn opaque_provider_wrapper_surfaces_failure_class_session_and_trace() {
    let error = ApiError::Api {
            status: "500".parse().expect("status"),
            error_type: Some("api_error".to_string()),
            message: Some(
                "Something went wrong while processing your request. Please try again, or use /new to start a fresh session."
                    .to_string(),
            ),
            request_id: Some("req_jobdori_789".to_string()),
            body: String::new(),
            retryable: true,
            retry_after: None,
            suggested_action: None,
        };

    let rendered = format_user_visible_api_error("session-issue-22", &error);
    assert!(rendered.contains("provider_internal"));
    assert!(rendered.contains("session session-issue-22"));
    assert!(rendered.contains("trace req_jobdori_789"));
}

#[test]
fn retry_exhaustion_uses_retry_failure_class_for_generic_provider_wrapper() {
    let error = ApiError::RetriesExhausted {
            attempts: 3,
            last_error: Box::new(ApiError::Api {
                status: "502".parse().expect("status"),
                error_type: Some("api_error".to_string()),
                message: Some(
                    "Something went wrong while processing your request. Please try again, or use /new to start a fresh session."
                        .to_string(),
                ),
                request_id: Some("req_jobdori_790".to_string()),
                body: String::new(),
                retryable: true,
                retry_after: None,
                suggested_action: None,
            }),
        };

    let rendered = format_user_visible_api_error("session-issue-22", &error);
    assert!(rendered.contains("provider_retry_exhausted"), "{rendered}");
    assert!(rendered.contains("session session-issue-22"));
    assert!(rendered.contains("trace req_jobdori_790"));
}

#[test]
fn parse_gateway_wechat_qr_subcommand() {
    let parsed = parse_gateway_args(&["wechat-qr".to_string()], CliOutputFormat::Text)
        .expect("wechat qr gateway subcommand should parse");
    match parsed {
        CliAction::Gateway { action, .. } => assert_eq!(action, GatewayAction::WechatQr),
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn parse_gateway_doctor_core_subcommand() {
    let parsed = parse_gateway_args(&["doctor".to_string()], CliOutputFormat::Text)
        .expect("gateway doctor should parse");
    match parsed {
        CliAction::Gateway { action, .. } => assert_eq!(action, GatewayAction::Doctor),
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn parse_gateway_local_diagnostics_subcommands() {
    for (name, expected) in [
        ("logs", GatewayAction::Logs),
        ("repair", GatewayAction::Repair),
        ("open", GatewayAction::Open),
    ] {
        let parsed = parse_gateway_args(&[name.to_string()], CliOutputFormat::Text)
            .expect("gateway diagnostic subcommand should parse");
        match parsed {
            CliAction::Gateway { action, .. } => assert_eq!(action, expected),
            other => panic!("unexpected action: {other:?}"),
        }
    }
}

#[test]
fn parse_mcp_serve_is_removed_from_cli_surface() {
    let error =
        parse_args(&["mcp".to_string(), "serve".to_string()]).expect_err("serve is removed");
    assert!(error.contains("no longer a top-level CLI management surface"));
    assert!(error.contains("Gateway/WebUI"));
}

#[test]
fn context_window_preflight_errors_render_recovery_steps() {
    let error = ApiError::ContextWindowExceeded {
        model: "claude-sonnet-4-6".to_string(),
        estimated_input_tokens: 182_000,
        requested_output_tokens: 64_000,
        estimated_total_tokens: 246_000,
        context_window_tokens: 200_000,
    };

    let rendered = format_user_visible_api_error("session-issue-32", &error);
    assert!(rendered.contains("Context window blocked"), "{rendered}");
    assert!(rendered.contains("context_window_blocked"), "{rendered}");
    assert!(
        rendered.contains("Session          session-issue-32"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Model            claude-sonnet-4-6"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Input estimate   ~182000 tokens (heuristic)"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Total estimate   ~246000 tokens (heuristic)"),
        "{rendered}"
    );
    assert!(rendered.contains("Compact          /compact"), "{rendered}");
    assert!(
        rendered.contains("Resume TUI       cowd --resume session-issue-32"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Fresh session    /clear --confirm"),
        "{rendered}"
    );
    assert!(rendered.contains("Reduce scope"), "{rendered}");
    assert!(rendered.contains("Retry            rerun"), "{rendered}");
}

#[test]
fn provider_context_window_errors_are_reframed_with_same_guidance() {
    let error = ApiError::Api {
            status: "400".parse().expect("status"),
            error_type: Some("invalid_request_error".to_string()),
            message: Some(
                "This model's maximum context length is 200000 tokens, but your request used 230000 tokens."
                    .to_string(),
            ),
            request_id: Some("req_ctx_456".to_string()),
            body: String::new(),
            retryable: false,
            retry_after: None,
            suggested_action: None,
        };

    let rendered = format_user_visible_api_error("session-issue-32", &error);
    assert!(rendered.contains("context_window_blocked"), "{rendered}");
    assert!(
        rendered.contains("Trace            req_ctx_456"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Detail           This model's maximum context length is 200000 tokens"),
        "{rendered}"
    );
    assert!(rendered.contains("Compact          /compact"), "{rendered}");
    assert!(
        rendered.contains("Fresh session    /clear --confirm"),
        "{rendered}"
    );
}

#[test]
fn retry_wrapped_context_window_errors_keep_recovery_guidance() {
    let error = ApiError::RetriesExhausted {
        attempts: 2,
        last_error: Box::new(ApiError::Api {
            status: "413".parse().expect("status"),
            error_type: Some("invalid_request_error".to_string()),
            message: Some("Request is too large for this model's context window.".to_string()),
            request_id: Some("req_ctx_retry_789".to_string()),
            body: String::new(),
            retryable: false,
            retry_after: None,
            suggested_action: None,
        }),
    };

    let rendered = format_user_visible_api_error("session-issue-32", &error);
    assert!(rendered.contains("Context window blocked"), "{rendered}");
    assert!(rendered.contains("context_window_blocked"), "{rendered}");
    assert!(
        rendered.contains("Trace            req_ctx_retry_789"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Detail           Request is too large for this model's context window."),
        "{rendered}"
    );
    assert!(rendered.contains("Compact          /compact"), "{rendered}");
    assert!(
        rendered.contains("Resume TUI       cowd --resume session-issue-32"),
        "{rendered}"
    );
}

fn temp_dir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after epoch")
        .as_nanos();
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("gateway-{nanos}-{unique}"))
}

fn git(args: &[&str], cwd: &Path) {
    let status = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .status()
        .expect("git command should run");
    assert!(
        status.success(),
        "git command failed: git {}",
        args.join(" ")
    );
}

struct ConfigHomeGuard {
    original: Option<std::ffi::OsString>,
    _directory: tempfile::TempDir,
}

impl ConfigHomeGuard {
    fn new() -> Self {
        let original = std::env::var_os("COWD_CONFIG_HOME");
        let directory = tempfile::tempdir().expect("isolated Gateway test config home");
        std::env::set_var("COWD_CONFIG_HOME", directory.path());
        Self {
            original,
            _directory: directory,
        }
    }
}

impl Drop for ConfigHomeGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(v) => std::env::set_var("COWD_CONFIG_HOME", v),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
    }
}

struct EnvVarGuard {
    name: &'static str,
    original: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let original = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, original }
    }

    fn remove(name: &'static str) -> Self {
        let original = std::env::var_os(name);
        std::env::remove_var(name);
        Self { name, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

fn env_lock() -> MutexGuard<'static, ()> {
    crate::test_process_env_lock()
}

fn with_current_dir<T>(cwd: &Path, f: impl FnOnce() -> T) -> T {
    let _guard = cwd_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = std::env::current_dir().expect("cwd should load");
    std::env::set_current_dir(cwd).expect("cwd should change");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    std::env::set_current_dir(previous).expect("cwd should restore");
    match result {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn write_skill_fixture(root: &Path, name: &str, description: &str) {
    let skill_dir = root.join(name);
    fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n\n# {name}\n"),
    )
    .expect("skill file should write");
}

fn write_plugin_fixture(root: &Path, name: &str, include_hooks: bool, include_lifecycle: bool) {
    fs::create_dir_all(root.join(".cowd/plugins")).expect("manifest dir");
    if include_hooks {
        fs::create_dir_all(root.join("hooks")).expect("hooks dir");
        fs::write(
            root.join("hooks").join("pre.sh"),
            "#!/bin/sh\nprintf 'plugin pre hook'\n",
        )
        .expect("write hook");
    }
    if include_lifecycle {
        fs::create_dir_all(root.join("lifecycle")).expect("lifecycle dir");
        fs::write(
            root.join("lifecycle").join("init.sh"),
            "#!/bin/sh\nprintf 'init\\n' >> lifecycle.log\n",
        )
        .expect("write init lifecycle");
        fs::write(
            root.join("lifecycle").join("shutdown.sh"),
            "#!/bin/sh\nprintf 'shutdown\\n' >> lifecycle.log\n",
        )
        .expect("write shutdown lifecycle");
    }

    let hooks = if include_hooks {
        ",\n  \"hooks\": {\n    \"PreToolUse\": [\"./hooks/pre.sh\"]\n  }"
    } else {
        ""
    };
    let lifecycle = if include_lifecycle {
        ",\n  \"lifecycle\": {\n    \"Init\": [\"./lifecycle/init.sh\"],\n    \"Shutdown\": [\"./lifecycle/shutdown.sh\"]\n  }"
    } else {
        ""
    };
    fs::write(
            root.join(".cowd/plugins").join("plugin.json"),
            format!(
                "{{\n  \"name\": \"{name}\",\n  \"version\": \"1.0.0\",\n  \"description\": \"runtime plugin fixture\"{hooks}{lifecycle}\n}}"
            ),
        )
        .expect("write plugin manifest");
}
#[test]
fn defaults_to_tui_when_no_args() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");
    assert_eq!(
        parse_args(&[]).expect("args should parse"),
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: None,
            allowed_tools: None,
            permission_mode: PermissionMode::WorkspaceWrite,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );
}

#[test]
fn default_permission_mode_uses_project_config_when_env_is_unset() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    let root = temp_dir();
    let cwd = root.join("project");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("project config dir should exist");
    std::fs::create_dir_all(&config_home).expect("config home should exist");
    std::fs::write(
        cwd.join(".cowd").join("config.yaml"),
        r#"{"permissions":{"default_mode":"workspace-write"}}"#,
    )
    .expect("project config should write");

    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_cc_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_permission_mode = std::env::var("COWD_PERMISSION_MODE").ok();
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::remove_var("COWD_PERMISSION_MODE");

    let resolved = with_current_dir(&cwd, super::default_permission_mode);

    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_cc_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_permission_mode {
        Some(value) => std::env::set_var("COWD_PERMISSION_MODE", value),
        None => std::env::remove_var("COWD_PERMISSION_MODE"),
    }
    std::fs::remove_dir_all(root).expect("temp config root should clean up");

    assert_eq!(resolved, PermissionMode::WorkspaceWrite);
}

#[test]
fn env_permission_mode_overrides_project_config_default() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    let root = temp_dir();
    let cwd = root.join("project");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("project config dir should exist");
    std::fs::create_dir_all(&config_home).expect("config home should exist");
    std::fs::write(
        cwd.join(".cowd").join("config.yaml"),
        r#"{"permissions":{"default_mode":"workspace-write"}}"#,
    )
    .expect("project config should write");

    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_cc_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_permission_mode = std::env::var("COWD_PERMISSION_MODE").ok();
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::set_var("COWD_PERMISSION_MODE", "read-only");

    let resolved = with_current_dir(&cwd, super::default_permission_mode);

    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_cc_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_permission_mode {
        Some(value) => std::env::set_var("COWD_PERMISSION_MODE", value),
        None => std::env::remove_var("COWD_PERMISSION_MODE"),
    }
    std::fs::remove_dir_all(root).expect("temp config root should clean up");

    assert_eq!(resolved, PermissionMode::ReadOnly);
}

#[test]
fn resolve_cli_auth_source_ignores_saved_oauth_credentials() {
    let _guard = env_lock();
    let config_home = temp_dir();
    std::fs::create_dir_all(&config_home).expect("config home should exist");

    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_api_key = std::env::var("ANTHROPIC_API_KEY").ok();
    let original_auth_token = std::env::var("ANTHROPIC_AUTH_TOKEN").ok();
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::remove_var("ANTHROPIC_API_KEY");
    std::env::remove_var("ANTHROPIC_AUTH_TOKEN");

    save_oauth_credentials(&OAuthTokenSet {
        access_token: "expired-access-token".to_string(),
        refresh_token: Some("refresh-token".to_string()),
        expires_at: Some(0),
        scopes: vec!["org:create_api_key".to_string(), "user:profile".to_string()],
    })
    .expect("save expired oauth credentials");

    let error = super::resolve_cli_auth_source_for_cwd()
        .expect_err("saved oauth should be ignored without env auth");

    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_api_key {
        Some(value) => std::env::set_var("ANTHROPIC_API_KEY", value),
        None => std::env::remove_var("ANTHROPIC_API_KEY"),
    }
    match original_auth_token {
        Some(value) => std::env::set_var("ANTHROPIC_AUTH_TOKEN", value),
        None => std::env::remove_var("ANTHROPIC_AUTH_TOKEN"),
    }
    std::fs::remove_dir_all(config_home).expect("temp config home should clean up");

    assert!(error.to_string().contains("ANTHROPIC_API_KEY"));
}

#[test]
fn merge_prompt_with_stdin_returns_prompt_unchanged_when_no_pipe() {
    // given
    let prompt = "Review this";

    // when
    let merged = merge_prompt_with_stdin(prompt, None);

    // then
    assert_eq!(merged, "Review this");
}

#[test]
fn merge_prompt_with_stdin_ignores_whitespace_only_pipe() {
    // given
    let prompt = "Review this";
    let piped = "   \n\t\n  ";

    // when
    let merged = merge_prompt_with_stdin(prompt, Some(piped));

    // then
    assert_eq!(merged, "Review this");
}

#[test]
fn merge_prompt_with_stdin_appends_piped_content_as_context() {
    // given
    let prompt = "Review this";
    let piped = "fn main() { println!(\"hi\"); }\n";

    // when
    let merged = merge_prompt_with_stdin(prompt, Some(piped));

    // then
    assert_eq!(merged, "Review this\n\nfn main() { println!(\"hi\"); }");
}

#[test]
fn merge_prompt_with_stdin_trims_surrounding_whitespace_on_pipe() {
    // given
    let prompt = "Summarize";
    let piped = "\n\n  some notes  \n\n";

    // when
    let merged = merge_prompt_with_stdin(prompt, Some(piped));

    // then
    assert_eq!(merged, "Summarize\n\nsome notes");
}

#[test]
fn merge_prompt_with_stdin_returns_pipe_when_prompt_is_empty() {
    // given
    let prompt = "";
    let piped = "standalone body";

    // when
    let merged = merge_prompt_with_stdin(prompt, Some(piped));

    // then
    assert_eq!(merged, "standalone body");
}

#[test]
fn model_aliases_are_not_invented_without_config() {
    let resolver = model_protocol::model_registry::ModelResolver::default();
    assert_eq!(resolver.resolve("main"), "main");
    assert_eq!(resolver.resolve("fast"), "fast");
    assert_eq!(resolver.resolve("opus"), "opus");
    assert_eq!(resolver.resolve("grok-3"), "grok-3");
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn user_defined_aliases_resolve_before_provider_dispatch() {
    // given
    let _guard = env_lock();
    let root = temp_dir();
    let cwd = root.join("project");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("project config dir should exist");
    std::fs::create_dir_all(&config_home).expect("config home should exist");
    std::fs::write(
        cwd.join(".cowd").join("config.yaml"),
        r#"{"aliases":{"fast":"claude-haiku-4-5-20251213","smart":"opus","cheap":"grok-3-mini"}}"#,
    )
    .expect("project config should write");

    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_cc_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    std::env::set_var("COWD_CONFIG_HOME", &config_home);
    std::env::set_var("COWD_CONFIG_HOME", &config_home);

    // when
    let direct = with_current_dir(&cwd, || resolve_model_alias_with_config("fast"));
    let chained = with_current_dir(&cwd, || resolve_model_alias_with_config("smart"));
    let cross_provider = with_current_dir(&cwd, || resolve_model_alias_with_config("cheap"));
    let unknown = with_current_dir(&cwd, || resolve_model_alias_with_config("unknown-model"));
    let builtin = with_current_dir(&cwd, || resolve_model_alias_with_config("haiku"));

    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    match original_cc_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    std::fs::remove_dir_all(root).expect("temp config root should clean up");

    // then
    assert_eq!(direct, "claude-haiku-4-5-20251213");
    assert_eq!(chained, "opus");
    assert_eq!(cross_provider, "grok-3-mini");
    assert_eq!(unknown, "unknown-model");
    assert_eq!(builtin, "haiku");
}

#[test]
fn parses_version_flags_without_initializing_prompt_mode() {
    assert_eq!(
        parse_args(&["--version".to_string()]).expect("args should parse"),
        CliAction::Version {
            output_format: CliOutputFormat::Text,
        }
    );
    assert_eq!(
        parse_args(&["-V".to_string()]).expect("args should parse"),
        CliAction::Version {
            output_format: CliOutputFormat::Text,
        }
    );
}

#[test]
fn parses_permission_mode_flag() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    let args = vec!["--permission-mode=read-only".to_string()];
    assert_eq!(
        parse_args(&args).expect("args should parse"),
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: None,
            allowed_tools: None,
            permission_mode: PermissionMode::ReadOnly,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );
}

#[test]
fn parses_tui_session_flag() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");

    assert_eq!(
        parse_args(&["--session".to_string(), "session-alpha".to_string()])
            .expect("session flag should parse"),
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: Some("session-alpha".to_string()),
            allowed_tools: None,
            permission_mode: PermissionMode::WorkspaceWrite,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );

    assert_eq!(
        parse_args(&["--session=session-beta".to_string()])
            .expect("inline session flag should parse"),
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: Some("session-beta".to_string()),
            allowed_tools: None,
            permission_mode: PermissionMode::WorkspaceWrite,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );
}

#[test]
fn dangerously_skip_permissions_flag_forces_danger_full_access_in_tui() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::set_var("COWD_PERMISSION_MODE", "read-only");
    let args = vec!["--dangerously-skip-permissions".to_string()];
    let parsed = parse_args(&args).expect("args should parse");
    std::env::remove_var("COWD_PERMISSION_MODE");

    assert_eq!(
        parsed,
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: None,
            allowed_tools: None,
            permission_mode: PermissionMode::DangerFullAccess,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn yolo_flag_forces_danger_full_access_and_marks_tui_mode() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::set_var("COWD_PERMISSION_MODE", "read-only");
    let args = vec!["--yolo".to_string()];
    let parsed = parse_args(&args).expect("args should parse");
    std::env::remove_var("COWD_PERMISSION_MODE");

    assert_eq!(
        parsed,
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: None,
            allowed_tools: None,
            permission_mode: PermissionMode::DangerFullAccess,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: true,
        }
    );
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn yolo_system_prompt_adds_continuous_execution_instruction() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");

    let prompt = with_current_dir(&root, || {
        build_system_prompt_for_mode(true).expect("system prompt should build")
    });

    assert!(prompt
        .iter()
        .any(|section| section.contains("YOLO continuous execution mode is active")));
}

#[test]
fn parses_allowed_tools_flags_with_canonical_names_and_lists() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");
    let args = vec![
        "--allowedTools".to_string(),
        "read_file,glob_search".to_string(),
        "--allowed-tools=write_file".to_string(),
    ];
    assert_eq!(
        parse_args(&args).expect("args should parse"),
        CliAction::Tui {
            model: DEFAULT_MODEL_ALIAS.to_string(),
            session_id: None,
            allowed_tools: Some(
                ["glob_search", "read_file", "write_file"]
                    .into_iter()
                    .map(str::to_string)
                    .collect()
            ),
            permission_mode: PermissionMode::WorkspaceWrite,
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        }
    );
}

#[test]
fn rejects_removed_allowed_tool_aliases() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");
    let error = parse_args(&["--allowedTools".to_string(), "read,glob".to_string()])
        .expect_err("legacy aliases must not bypass the canonical tool contract");
    assert!(error.contains("unsupported tool in --allowedTools: read"));
}

#[test]
fn rejects_unknown_allowed_tools() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");
    let error = parse_args(&[
        "--allowedTools".to_string(),
        "definitely_not_a_tool".to_string(),
    ])
    .expect_err("tool should be rejected");
    assert!(error.contains("unsupported tool in --allowedTools: definitely_not_a_tool"));
}

#[test]
fn parses_system_prompt_options() {
    let error = parse_args(&[
        "system-prompt".to_string(),
        "--cwd".to_string(),
        "/tmp/project".to_string(),
        "--date".to_string(),
        "2026-04-01".to_string(),
    ])
    .expect_err("system-prompt should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn help_prioritizes_minimal_core_surface() {
    let mut out = Vec::new();
    print_help_to(&mut out).expect("help should render");
    let help = String::from_utf8(out).expect("help should be utf8");

    assert!(help.contains("Core commands:"));
    assert!(help.contains("cowd tui"));
    assert!(help.contains("cowd gateway start|stop|restart|status|doctor|logs|repair|open"));
    assert!(help.contains("cowd config list|show|doctor"));
    assert!(help.contains("cowd doctor"));
    assert!(help.contains("cowd skill list|show|validate"));
    assert!(help.contains("cowd tool list|doctor"));

    for complex in [
        "cowd agents",
        "cowd mcp",
        "cowd plugins",
        "cowd dump-manifests",
        "cowd bootstrap-plan",
        "cowd system-prompt",
        "cowd export",
        "cowd import-session",
        "Interactive slash commands:",
    ] {
        assert!(
            !help.contains(complex),
            "{complex} must not be presented as a top-level CLI command"
        );
    }
}

#[test]
fn gateway_help_keeps_channel_helpers_out_of_core_surface() {
    let mut out = Vec::new();
    print_help_to(&mut out).expect("help should render");
    let help = String::from_utf8(out).expect("help should be utf8");
    let core_start = help.find("Core commands:").expect("core section");
    let core = &help[core_start..];

    assert!(core.contains("gateway start|stop|restart|status|doctor|logs|repair|open"));
    assert!(!core.contains("wechat-qr"));
    assert!(!help.contains("Advanced local tools:"));
}

#[test]
fn removed_login_and_logout_subcommands_error_helpfully() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    let login = parse_args(&["login".to_string()]).expect_err("login should be removed");
    assert!(login.contains("providers"));
    let logout = parse_args(&["logout".to_string()]).expect_err("logout should be removed");
    assert!(logout.contains("config.yaml"));
    assert_eq!(
        parse_args(&["doctor".to_string()]).expect("doctor should parse"),
        CliAction::Doctor {
            output_format: CliOutputFormat::Text,
        }
    );
    for removed in ["state", "setup", "init", "agents", "mcp", "skills"] {
        let error = parse_args(&[removed.to_string()]).expect_err("command should be removed");
        assert!(
            error.contains("minimal CLI surface")
                || error.contains("no longer a top-level CLI management surface"),
            "{removed}: {error}"
        );
    }
}

#[test]
fn dump_manifests_subcommand_accepts_explicit_manifest_dir() {
    let error = parse_args(&[
        "dump-manifests".to_string(),
        "--manifests-dir".to_string(),
        "/tmp/upstream".to_string(),
    ])
    .expect_err("dump-manifests should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn local_command_help_flags_stay_on_the_local_parser_path() {
    assert_eq!(
        parse_args(&["doctor".to_string(), "--help".to_string()])
            .expect("doctor help should parse"),
        CliAction::HelpTopic(LocalHelpTopic::Doctor)
    );
    for removed in ["status", "sandbox", "setup"] {
        let error = parse_args(&[removed.to_string(), "--help".to_string()])
            .expect_err("removed help topic should fail");
        assert!(error.contains("minimal CLI surface"), "{removed}: {error}");
    }
}

#[test]
fn parses_single_word_command_aliases_without_falling_back_to_prompt_mode() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    std::env::remove_var("COWD_PERMISSION_MODE");
    assert_eq!(
        parse_args(&["help".to_string()]).expect("help should parse"),
        CliAction::Help {
            output_format: CliOutputFormat::Text,
        }
    );
    assert_eq!(
        parse_args(&["version".to_string()]).expect("version should parse"),
        CliAction::Version {
            output_format: CliOutputFormat::Text,
        }
    );
    for removed in ["status", "sandbox", "setup"] {
        let error = parse_args(&[removed.to_string()]).expect_err("command should be removed");
        assert!(error.contains("minimal CLI surface"), "{removed}: {error}");
    }
}

#[test]
fn parses_bare_export_subcommand_targeting_latest_session() {
    let _guard = env_lock();
    std::env::remove_var("COWD_PERMISSION_MODE");
    let error = parse_args(&["export".to_string()]).expect_err("export should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn parses_export_subcommand_with_positional_output_path() {
    let error = parse_args(&["export".to_string(), "conversation.md".to_string()])
        .expect_err("export should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn parses_export_subcommand_with_session_and_output_flags() {
    let error = parse_args(&[
        "export".to_string(),
        "--session".to_string(),
        "session-alpha".to_string(),
        "--output".to_string(),
        "/tmp/share.md".to_string(),
    ])
    .expect_err("export should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn parses_export_subcommand_with_inline_flag_values() {
    let error = parse_args(&[
        "export".to_string(),
        "--session=session-beta".to_string(),
        "--output=/tmp/beta.md".to_string(),
    ])
    .expect_err("export should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn parses_export_subcommand_with_json_output_format() {
    let error = parse_args(&[
        "--output-format=json".to_string(),
        "export".to_string(),
        "/tmp/notes.md".to_string(),
    ])
    .expect_err("export should be removed");
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn rejects_unknown_export_options_with_helpful_message() {
    // given
    let args = vec!["export".to_string(), "--bogus".to_string()];

    // when
    let error = parse_args(&args).expect_err("unknown export option should fail");

    // then
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn rejects_export_with_extra_positional_after_path() {
    // given
    let args = vec![
        "export".to_string(),
        "first.md".to_string(),
        "second.md".to_string(),
    ];

    // when
    let error = parse_args(&args).expect_err("multiple positionals should fail");

    // then
    assert!(error.contains("minimal CLI surface"));
}

#[test]
fn parses_json_output_for_mcp_and_skills_commands() {
    let mcp_error = parse_args(&["--output-format=json".to_string(), "mcp".to_string()])
        .expect_err("mcp should be removed");
    assert!(mcp_error.contains("no longer a top-level CLI management surface"));
    let skills_error = parse_args(&[
        "--output-format=json".to_string(),
        "skills".to_string(),
        "help".to_string(),
    ])
    .expect_err("skills alias should be removed");
    assert!(skills_error.contains("no longer a top-level CLI management surface"));
    let tools_error = parse_args(&[
        "--output-format=json".to_string(),
        "tools".to_string(),
        "list".to_string(),
    ])
    .expect_err("tools alias should be removed");
    assert!(tools_error.contains("no longer a top-level CLI management surface"));
}

#[test]
fn single_word_slash_command_names_return_guidance_instead_of_hitting_prompt_mode() {
    let error = parse_args(&["cost".to_string()]).expect_err("cost should return guidance");
    assert!(error.contains("slash command"));
    assert!(error.contains("/cost"));
}

#[test]
fn direct_slash_commands_return_tui_guidance() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();
    for args in [
        vec!["/agents".to_string()],
        vec!["/mcp".to_string(), "show".to_string(), "demo".to_string()],
        vec!["/skills".to_string()],
        vec!["/skill".to_string()],
        vec!["/skills".to_string(), "help".to_string()],
        vec!["/skill".to_string(), "list".to_string()],
        vec!["/setup".to_string()],
        vec![
            "/skills".to_string(),
            "install".to_string(),
            "./fixtures/help-skill".to_string(),
        ],
        vec!["/status".to_string()],
    ] {
        let error = parse_args(&args).expect_err("direct slash command should be TUI-only");
        assert!(error.contains("top-level slash commands were removed"));
        assert!(error.contains("Start the TUI"));
    }
}

#[test]
fn direct_slash_commands_surface_shared_validation_errors() {
    let compact_error = parse_args(&["/compact".to_string(), "now".to_string()])
        .expect_err("invalid /compact shape should be rejected");
    assert!(compact_error.contains("top-level slash commands were removed"));

    let plugins_error = parse_args(&[
        "/plugins".to_string(),
        "list".to_string(),
        "extra".to_string(),
    ])
    .expect_err("invalid /plugins list shape should be rejected");
    assert!(plugins_error.contains("top-level slash commands were removed"));

    let setup_error = parse_args(&["/setup".to_string(), "now".to_string()])
        .expect_err("invalid /setup shape should be rejected");
    assert!(setup_error.contains("top-level slash commands were removed"));
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn setup_report_and_json_are_redacted_and_actionable() {
    let _guard = env_lock();
    let _cfg_guard = ConfigHomeGuard::new();

    let report = render_setup_report().expect("setup report should render");
    assert!(report.contains("Setup Center"));
    assert!(report.contains("Checks"));
    assert!(report.contains("cowd gateway open"));
    assert!(!report.contains("app_secret"));
    assert!(!report.contains("auth_token"));

    let json = render_setup_json().expect("setup json should render");
    assert_eq!(json["kind"], "setup");
    let items = json["items"]
        .as_array()
        .expect("setup json should include items");
    assert!(items.iter().any(|item| item["id"] == "wechat"));
    assert!(items.iter().any(|item| item["id"] == "permission"));
    let encoded = serde_json::to_string(&json).expect("json should encode");
    assert!(!encoded.contains("app_secret"));
    assert!(!encoded.contains("auth_token"));
}

#[test]
fn setup_next_action_prioritizes_action_items_over_warnings() {
    let snapshot = super::SetupSnapshot {
        cwd: PathBuf::from("/workspace"),
        config_home: PathBuf::from("/config"),
        loaded_files: vec![],
        gateway_running: false,
        items: vec![
            super::SetupItem {
                id: "gateway",
                label: "Gateway",
                status: "warn",
                summary: "configured but not running".to_string(),
                next: Some("cowd gateway start".to_string()),
            },
            super::SetupItem {
                id: "wechat",
                label: "WeChat",
                status: "action",
                summary: "not authorized".to_string(),
                next: Some("Configure the WeChat platform in Gateway/WebUI".to_string()),
            },
        ],
    };

    assert_eq!(snapshot.overall_status(), "action");
    assert_eq!(
        snapshot.next_action(),
        "Configure the WeChat platform in Gateway/WebUI"
    );
}

#[test]
fn formats_unknown_slash_command_with_suggestions() {
    let report = format_unknown_slash_command_message("statsu");
    assert!(report.contains("unknown slash command: /statsu"));
    assert!(report.contains("Did you mean"));
    assert!(report.contains("Use /help"));
}

#[test]
fn formats_namespaced_omc_slash_command_with_contract_guidance() {
    let report = format_unknown_slash_command_message("oh-my-claudecode:hud");
    assert!(report.contains("unknown slash command: /oh-my-claudecode:hud"));
    assert!(report.contains("Claude Code/OMC plugin command"));
    assert!(report.contains("plugin slash commands"));
    assert!(report.contains("statusline"));
    assert!(report.contains("session hooks"));
}

#[test]
fn parses_resume_flag_without_path_as_latest_session() {
    let action = parse_args(&["--resume".to_string()]).expect("args should parse");
    let CliAction::Tui {
        model,
        session_id,
        allowed_tools,
        permission_mode: _,
        base_commit,
        reasoning_effort,
        allow_broad_cwd,
        yolo_mode,
    } = action
    else {
        panic!("resume must enter the TUI surface");
    };
    assert_eq!(model, DEFAULT_MODEL_ALIAS);
    assert_eq!(session_id.as_deref(), Some("latest"));
    assert!(allowed_tools.is_none());
    assert!(base_commit.is_none());
    assert!(reasoning_effort.is_none());
    assert!(!allow_broad_cwd);
    assert!(!yolo_mode);
    let error = parse_args(&["--resume".to_string(), "/status".to_string()])
        .expect_err("resume slash shortcut should be removed");
    assert!(error.contains("was removed from the CLI surface"));
    assert!(error.contains("run slash commands inside the TUI"));
}

#[test]
fn rejects_unknown_options_with_helpful_guidance() {
    let error = parse_args(&["--resum".to_string()]).expect_err("unknown option should fail");
    assert!(error.contains("unknown option: --resum"));
    assert!(error.contains("Did you mean --resume?"));
    assert!(error.contains("cowd --help"));
}

#[test]
fn filtered_tool_specs_respect_allowlist() {
    let allowed = ["read_file", "grep_search"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let filtered = filter_tool_specs(&TestToolRegistry::builtin(), Some(&allowed));
    let names = filtered
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["read_file", "grep_search"]);
}

#[test]
fn filtered_tool_specs_include_plugin_tools() {
    let filtered = filter_tool_specs(&registry_with_plugin_tool(), None);
    let names = filtered
        .into_iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    assert!(names.contains(&"bash".to_string()));
    assert!(names.contains(&"plugin_echo".to_string()));
}

#[test]
fn permission_policy_uses_plugin_tool_permissions() {
    let feature_config = runtime::RuntimeFeatureConfig::default();
    let policy = permission_policy(
        PermissionMode::ReadOnly,
        &feature_config,
        &registry_with_plugin_tool(),
    )
    .expect("permission policy should build");
    let required = policy.required_mode_for("plugin_echo");
    assert_eq!(required, PermissionMode::WorkspaceWrite);
}

#[test]
fn shared_help_uses_resume_annotation_copy() {
    let help = crate::command::slash::render_slash_command_help();
    assert!(help.contains("Slash commands"));
    assert!(help.contains("[resumed TUI]     available after `cowd --resume <session-id|latest>`"));
}

#[test]
fn bare_skill_dispatch_resolves_known_project_skill_to_prompt() {
    let _guard = env_lock();
    let workspace = temp_dir();
    write_skill_fixture(
        &workspace.join(".codex").join("skills"),
        "caveman",
        "Project skill fixture",
    );

    let prompt = try_resolve_bare_skill_prompt(&workspace, "caveman sharpen club")
        .expect("known bare skill should dispatch");
    assert_eq!(prompt, "$caveman sharpen club");

    fs::remove_dir_all(workspace).expect("workspace should clean up");
}

#[test]
fn bare_skill_dispatch_ignores_unknown_or_non_skill_input() {
    let _guard = env_lock();
    let workspace = temp_dir();
    fs::create_dir_all(&workspace).expect("workspace should exist");

    assert_eq!(
        try_resolve_bare_skill_prompt(&workspace, "not-a-known-skill do thing"),
        None
    );
    assert_eq!(try_resolve_bare_skill_prompt(&workspace, "/status"), None);

    fs::remove_dir_all(workspace).expect("workspace should clean up");
}

#[test]
fn tui_help_includes_shared_commands_and_exit() {
    let help = render_terminal_help();
    assert!(help.lines().any(|line| line.trim() == "Terminal controls"));
    for command in [
        "/help",
        "/status",
        "/sandbox",
        "/model",
        "/permissions",
        "/clear",
        "/cost",
        "/resume",
        "/config",
        "/mcp",
        "/memory",
        "/init",
        "/diff",
        "/version",
        "/export",
        "/session",
        "/plugin",
        "/agents",
        "/skills",
        "/exit",
    ] {
        assert!(
            tui_help_contains_command(&help, command),
            "missing command {command} in help:\n{help}"
        );
    }
}

#[test]
fn completion_candidates_include_workflow_shortcuts_and_dynamic_sessions() {
    let completions = slash_command_completion_candidates_with_sessions(
        "sonnet",
        Some("session-current"),
        vec!["session-old".to_string()],
    );

    assert!(completions.contains(&"/model sonnet".to_string()));
    assert!(completions.contains(&"/permissions workspace-write".to_string()));
    assert!(completions.contains(&"/session list".to_string()));
    assert!(completions.contains(&"/session switch session-current".to_string()));
    assert!(completions.contains(&"/resume session-old".to_string()));
    assert!(completions.contains(&"/mcp list".to_string()));
    assert!(completions.contains(&"/ultraplan ".to_string()));
}

#[test]
fn startup_banner_uses_codex_style_context_card() {
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");

    let banner = with_current_dir(&root, || {
        format_startup_banner("claude-sonnet-4-6", false, "session-banner-test")
    });
    let rows = parse_startup_banner_rows(&banner);

    assert_eq!(
        rows.get("model").map(String::as_str),
        Some("claude-sonnet-4-6")
    );
    assert_eq!(
        rows.get("directory").map(String::as_str),
        Some(truncate_for_banner(root.to_str().unwrap(), 59).as_str())
    );
    assert!(rows.contains_key("branch"));
    assert!(rows.contains_key("git"));
    assert_eq!(rows.get("mode").map(String::as_str), Some("standard"));
    assert_eq!(
        rows.get("session").map(String::as_str),
        Some("session-banner-test")
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn startup_banner_marks_yolo_mode() {
    let banner = format_startup_banner("claude-sonnet-4-6", true, "session-yolo-test");

    assert!(banner.contains("yolo"));
    assert!(banner.contains("session-yolo-test"));
}

#[test]
fn tui_startup_banner_strip_removes_ansi_codes() {
    let plain = strip_ansi_for_tui("\u{1b}[1mready\u{1b}[0m");

    assert_eq!(plain, "ready");
}

#[test]
fn cli_turn_context_profile_maps_runtime_modes() {
    assert_eq!(
        cli_turn_context_profile(false, PermissionMode::WorkspaceWrite, false, false),
        ContextProfile::MainTurn
    );
    assert_eq!(
        cli_turn_context_profile(false, PermissionMode::DangerFullAccess, false, false),
        ContextProfile::AutonomousGoal
    );
    assert_eq!(
        cli_turn_context_profile(true, PermissionMode::DangerFullAccess, false, false),
        ContextProfile::YoloGoal
    );
    assert_eq!(
        cli_turn_context_profile(true, PermissionMode::DangerFullAccess, true, false),
        ContextProfile::Resume
    );
    assert_eq!(
        cli_turn_context_profile(true, PermissionMode::DangerFullAccess, true, true),
        ContextProfile::Review
    );
}

#[test]
fn startup_banner_shows_yolo_task_summary() {
    let task = TaskAggregate {
        task_id: "task-abcdef123456".to_string(),
        mission_id: "mission-test".to_string(),
        kind: TaskKind::Root,
        origin: TaskOrigin::User,
        origin_session_id: "session-yolo-test".to_string(),
        origin_turn_id: "turn-yolo-test".to_string(),
        root_task_id: "task-abcdef123456".to_string(),
        parent_task_id: None,
        predecessor_task_id: None,
        mission_assignment: TaskMissionAssignment::ExplicitLocked,
        mission_assignment_revision: 1,
        mission_assigned_by: "test".to_string(),
        mission_assignment_evidence_refs: Vec::new(),
        objective: "complete v0.8.10 enterprise AI framework".to_string(),
        status: TaskStatus::Running,
        revision: 2,
        current_phase_id: Some("phase-1".to_string()),
        phases: vec![TaskPhase {
            phase_id: "phase-1".to_string(),
            name: "tui-cockpit".to_string(),
            objective: "surface durable task state in TUI".to_string(),
            status: TaskPhaseStatus::Completed,
            revision: 2,
            dependency_refs: Vec::new(),
            plan: Vec::new(),
            acceptance: Vec::new(),
            test_commands: Vec::new(),
            artifacts: vec![TaskPhaseArtifact {
                kind: "test".to_string(),
                label: "status-bar".to_string(),
                value: "passed".to_string(),
                created_at_ms: 1,
            }],
            review_result: Some("accepted".to_string()),
            terminal_receipt: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }],
        execution_policy: TaskExecutionPolicy {
            max_failures_before_block: 3,
            ..TaskExecutionPolicy::default()
        },
        failure_count: 0,
        blocker_reason: None,
        strategy_ref: None,
        graph_refs: Vec::new(),
        application_provenance: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    };

    let banner = format_startup_banner_with_task(
        "claude-sonnet-4-6",
        true,
        "session-yolo-test",
        Some(&task),
    );
    let rows = parse_startup_banner_rows(&banner);

    let task_row = rows.get("task").expect("task row");
    assert!(task_row.contains("running"));
    assert!(task_row.contains("task-abc"));
    assert!(task_row.contains("complete v0.8.10 enterprise AI framework"));
    assert_eq!(
        rows.get("phase").map(String::as_str),
        Some("tui-cockpit:completed")
    );
}

fn parse_startup_banner_rows(banner: &str) -> BTreeMap<String, String> {
    banner
        .lines()
        .filter_map(|line| {
            let body = line.strip_prefix("│ ")?.strip_suffix(" │")?;
            let (label, value) = body.split_once(' ')?;
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                Some((label.trim().to_string(), value.to_string()))
            }
        })
        .collect()
}

fn parse_report_fields(report: &str) -> BTreeMap<String, String> {
    report
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let (label, value) = if let Some(split_at) = trimmed.find("  ") {
                trimmed.split_at(split_at)
            } else {
                trimmed.rsplit_once(char::is_whitespace)?
            };
            let value = value.trim();
            (!label.is_empty() && !value.is_empty())
                .then(|| (label.trim().to_string(), value.to_string()))
        })
        .collect()
}

fn tui_help_contains_command(help: &str, command: &str) -> bool {
    help.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed == command
            || trimmed
                .strip_prefix(command)
                .is_some_and(|rest| rest.starts_with(char::is_whitespace))
            || trimmed.split_whitespace().any(|token| token == command)
    })
}

#[test]
fn format_connected_line_renders_anthropic_provider_for_claude_model() {
    let model = "claude-sonnet-4-6";

    let line = format_connected_line(model);

    assert_eq!(line, "Connected: claude-sonnet-4-6 via anthropic");
}

#[test]
fn format_connected_line_renders_xai_provider_for_grok_model() {
    let model = "grok-3";

    let line = format_connected_line(model);

    assert_eq!(line, "Connected: grok-3 via xai");
}

#[test]
fn resolve_tui_model_returns_user_supplied_model_unchanged_when_explicit() {
    let user_model = "gpt-4o".to_string();

    let resolved = resolve_tui_model(user_model);

    assert_eq!(resolved, "gpt-4o");
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn resolve_tui_model_ignores_provider_specific_model_environment() {
    let _guard = env_lock();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");
    let config_home = root.join("config");
    fs::create_dir_all(&config_home).expect("config home dir");
    let _config_home = EnvVarGuard::set("COWD_CONFIG_HOME", &config_home);
    let _cowd_model = EnvVarGuard::remove("COWD_MODEL");
    let _anthropic_model = EnvVarGuard::set("ANTHROPIC_MODEL", "claude-sonnet-4-6");

    let resolved = with_current_dir(&root, || resolve_tui_model(DEFAULT_MODEL_ALIAS.to_string()));

    assert_eq!(resolved, DEFAULT_MODEL_ALIAS);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
#[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"]
fn resolve_tui_model_returns_default_when_env_unset_and_no_config() {
    let _guard = env_lock();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");
    let config_home = root.join("config");
    fs::create_dir_all(&config_home).expect("config home dir");
    let _config_home = EnvVarGuard::set("COWD_CONFIG_HOME", &config_home);
    let _cowd_model = EnvVarGuard::remove("COWD_MODEL");
    let _anthropic_model = EnvVarGuard::remove("ANTHROPIC_MODEL");

    let resolved = with_current_dir(&root, || resolve_tui_model(DEFAULT_MODEL_ALIAS.to_string()));

    assert_eq!(resolved, DEFAULT_MODEL_ALIAS);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn resumed_tui_preserves_local_recovery_commands_without_runtime_compaction() {
    let names = resume_supported_slash_commands()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"help"));
    assert!(names.contains(&"status"));
    // Semantic compaction requires a live Gateway Runtime and durable
    // session store, so it must not be advertised as an offline action.
    assert!(!names.contains(&"compact"));
}

#[test]
fn session_db_resume_packet_summarizes_recent_session_state() {
    let mut session = runtime::Session::new();
    session.session_id = "session-resume-packet".to_string();
    session
        .push_message(runtime::ConversationMessage {
            role: runtime::MessageRole::User,
            blocks: vec![runtime::ContentBlock::Text {
                text: "continue the context runtime work".to_string(),
            }],
            usage: None,
        })
        .expect("append user");
    session
        .push_message(runtime::ConversationMessage {
            role: runtime::MessageRole::Assistant,
            blocks: vec![
                runtime::ContentBlock::ReasoningSummary {
                    text: "public resume rationale".to_string(),
                },
                runtime::ContentBlock::Thinking {
                    thinking: "private provider reasoning".to_string(),
                    signature: Some("private provider signature".to_string()),
                },
                runtime::ContentBlock::Text {
                    text: "context timeline is persisted".to_string(),
                },
            ],
            usage: None,
        })
        .expect("append assistant");
    session.compaction = Some(runtime::SessionCompaction {
        count: 2,
        removed_message_count: 8,
        summary: "older context summarized".to_string(),
    });

    let packet = session_db_resume_context_packet(&session).expect("resume packet");

    assert_eq!(packet.session_id, "session-resume-packet");
    assert_eq!(packet.source, runtime::ResumeContextSource::SessionDb);
    assert!(packet
        .active_task
        .as_deref()
        .is_some_and(|task| task.contains("context timeline is persisted")));
    assert!(packet
        .active_task
        .as_deref()
        .is_some_and(|task| task.contains("public resume rationale")));
    assert!(!packet
        .active_task
        .as_deref()
        .is_some_and(|task| task.contains("private provider")));
    assert!(packet
        .recent_decisions
        .iter()
        .any(|decision| decision.contains("compaction#2")));
}

#[test]
fn handoff_resume_packet_summarizes_handoff_state() {
    let handoff = memory::HandoffData {
        session_id: "handoff-session".to_string(),
        timestamp: chrono::Utc::now(),
        work_items: vec![memory::WorkItem {
            id: "work-1".to_string(),
            title: "Finish context resume".to_string(),
            description: "Wire handoff into runtime context".to_string(),
            status: memory::WorkItemStatus::Pending,
            priority: memory::Priority::High,
        }],
        decisions: vec![memory::Decision {
            id: "decision-1".to_string(),
            summary: "Use typed packets".to_string(),
            rationale: "Keep runtime prompt deterministic".to_string(),
            status: memory::DecisionStatus::Implemented,
            made_at: chrono::Utc::now(),
        }],
        blockers: vec![memory::Blocker {
            id: "blocker-1".to_string(),
            description: "Need exact handoff lookup".to_string(),
            resolution_hint: Some("fallback to latest".to_string()),
        }],
        task_states: vec![memory::TaskState {
            task_id: "task-1".to_string(),
            progress_percent: 70,
            last_checkpoint: "timeline complete".to_string(),
            context: serde_json::json!({"phase":"resume"}),
        }],
        summary: "resume from handoff".to_string(),
    };

    let packet = handoff_resume_context_packet(&handoff);

    assert_eq!(packet.session_id, "handoff-session");
    assert_eq!(packet.source, runtime::ResumeContextSource::Handoff);
    assert!(packet
        .active_task
        .as_deref()
        .is_some_and(|task| task.contains("timeline complete")));
    assert!(packet
        .recent_decisions
        .iter()
        .any(|decision| decision.contains("Use typed packets")));
    assert!(packet
        .blockers
        .iter()
        .any(|blocker| blocker.contains("fallback to latest")));
}

#[test]
fn semantic_checkpoint_resume_restores_all_runtime_critical_fields() {
    let store = std::sync::Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        store
            .create_session(&session::SessionRecord {
                session_id: "resume-semantic".to_string(),
                platform: "test".to_string(),
                chat_id: "resume-semantic".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .expect("create session");
    });
    let checkpoint: memory::compression::session::SessionSemanticCheckpoint =
        serde_json::from_value(serde_json::json!({
            "schema_version": 3,
            "checkpoint_id": "checkpoint-resume-1",
            "execution_identity": {
                "kind": "task_graph",
                "principal_id": "principal-resume",
                "workspace_id": "project-a",
                "mission_id": "mission-a",
                "task_id": "task-a",
                "session_id": "resume-semantic",
                "turn_id": "turn-a",
                "graph_id": "graph-a",
                "team_run_id": null,
                "agent_run_id": null,
                "node_id": null,
                "invocation_id": null,
                "schedule_id": null,
                "fire_id": null
            },
            "session_id": "resume-semantic",
            "agent_id": "primary",
            "project_id": "project-a",
            "task_id": "task-a",
            "team_id": null,
            "summary": "Implement the evidence boundary",
            "user_rules": ["never copy raw evidence"],
            "goal": "finish the V2 compensation",
            "constraints": ["keep SessionStore canonical"],
            "decisions": ["use typed handoff references"],
            "evidence_refs": [],
            "unresolved": ["verify restart path"],
            "file_changes": ["crates/runtime/src/context/evidence/raw.rs"],
            "resume_cursor": {
                "message_index": 17,
                "event_sequence": 9,
                "checkpoint_id": "checkpoint-resume-1"
            },
            "token_stats": {"before": 1200, "after": 240, "message_count": 9},
            "source_range": {
                "session_id": "resume-semantic",
                "message_start": 0,
                "message_end_exclusive": 9,
                "event_start": 0,
                "event_end_exclusive": 9,
                "raw_refs": []
            },
            "facts": []
        }))
        .expect("checkpoint fixture");
    let event = session::SessionDomainEvent::new(
        "resume-semantic",
        0,
        session::SessionDomainScope::Memory,
        "memory.semantic_checkpoint.created",
        serde_json::json!({"checkpoint": checkpoint}),
        1,
    );
    runtime.block_on(async {
        store
            .append_session_domain_event_allocating_sequence(&event)
            .await
            .expect("persist checkpoint");
    });

    let stored = runtime
        .block_on(async {
            store
                .get_latest_session_domain_event_by_kind(
                    "resume-semantic",
                    "memory.semantic_checkpoint.created",
                )
                .await
        })
        .expect("read checkpoint")
        .expect("stored checkpoint");
    let packet = semantic_checkpoint_resume_context_packet(&stored, "resume-semantic")
        .expect("checkpoint resume packet");
    let context = runtime::ContextRuntimeKernel::resume_item(&packet).content;
    for expected in [
        "Implement the evidence boundary",
        "finish the V2 compensation",
        "never copy raw evidence",
        "keep SessionStore canonical",
        "use typed handoff references",
        "verify restart path",
        "crates/runtime/src/context/evidence/raw.rs",
        "checkpoint-resume-1",
    ] {
        assert!(
            context.contains(expected),
            "missing restored field: {expected}"
        );
    }
}

#[test]
fn workspace_context_item_summarizes_runtime_workspace() {
    let root =
        std::env::temp_dir().join(format!("cowd-workspace-context-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("temp workspace");
    let git_init = Command::new("git")
        .args(["-C"])
        .arg(&root)
        .arg("init")
        .output()
        .expect("run git init");
    assert!(git_init.status.success());
    let src_dir = root.join("src");
    std::fs::create_dir_all(&src_dir).expect("src dir");
    let tracked_path = src_dir.join("lib.rs");
    std::fs::write(
        &tracked_path,
        "pub fn workspace_context_probe() -> bool { true }\n",
    )
    .expect("tracked file");
    let git_add = Command::new("git")
        .args(["-c", &format!("safe.directory={}", root.display())])
        .args(["-C"])
        .arg(&root)
        .args(["add", "src/lib.rs"])
        .output()
        .expect("run git add");
    assert!(
        git_add.status.success(),
        "git add failed: {}",
        String::from_utf8_lossy(&git_add.stderr)
    );
    let session = runtime::Session::new().with_workspace_root(root.clone());

    let item = workspace_context_item(&session, 200_000);

    assert_eq!(item.source, runtime::ContextSourceKind::Workspace);
    assert!(item.content.contains(&root.display().to_string()));
    assert!(item.content.contains("model_context_window=200000"));
    assert!(item.content.contains("src/lib.rs"));
    #[cfg(feature = "code-index")]
    assert!(item.content.contains("workspace_context_probe"));
    #[cfg(not(feature = "code-index"))]
    assert!(
        !item.content.contains("workspace_context_probe"),
        "default gateway build should not force code-index hot symbol extraction"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn runtime_capability_context_item_reflects_filtered_tools() {
    let tools = vec![
        runtime::ProviderToolDefinition {
            name: "read_many".to_string(),
            description: Some("read many".to_string()),
            input_schema: serde_json::json!({ "type": "object" }),
        },
        runtime::ProviderToolDefinition {
            name: "tool_batch_readonly".to_string(),
            description: Some("batch".to_string()),
            input_schema: serde_json::json!({ "type": "object" }),
        },
        runtime::ProviderToolDefinition {
            name: "runtime_capabilities".to_string(),
            description: Some("capabilities".to_string()),
            input_schema: serde_json::json!({ "type": "object" }),
        },
    ];

    let item = runtime_capability_context_item(&tools, None, 1_000_000);

    assert_eq!(item.source, runtime::ContextSourceKind::RuntimeHeader);
    assert_eq!(item.role, runtime::ContextRole::Orientation);
    assert!(item.content.contains("runtime_capabilities=registered"));
    assert!(item.content.contains("runtime_orchestrate=not_registered"));
    assert!(item.content.contains("read_many"));
    assert!(item.content.contains("tool_batch_readonly"));
}

#[test]
fn help_mentions_minimal_local_commands() {
    let mut help = Vec::new();
    print_help_to(&mut help).expect("help should render");
    let help = String::from_utf8(help).expect("help should be utf8");
    assert!(help.contains("cowd help"));
    assert!(help.contains("cowd version"));
    assert!(help.contains("cowd config list|show|doctor"));
    assert!(help.contains("cowd tool list|doctor"));
    assert!(help.contains("cowd skill list"));
    assert!(!help.contains("cowd status"));
    assert!(!help.contains("cowd sandbox"));
    assert!(!help.contains("cowd init"));
    assert!(!help.contains("cowd agents"));
    assert!(!help.contains("cowd mcp"));
    assert!(!help.contains(&["cowd ", "skills list"].concat()));
    assert!(!help.contains("/skills"));
    assert!(!help.contains("cowd /skills"));
    assert!(help.contains("ultraworkers/cowd"));
    assert!(help.contains("cargo install cowd"));
    assert!(!help.contains("login command"));
    assert!(!help.contains("logout command"));
}

#[test]
fn status_line_reports_model_and_token_totals() {
    let status = format_status_report(
        "claude-sonnet",
        StatusUsage {
            message_count: 7,
            turns: 3,
            latest: TokenUsage {
                input_tokens: 5,
                output_tokens: 4,
                cache_creation_input_tokens: 1,
                cache_read_input_tokens: 0,
            },
            cumulative: TokenUsage {
                input_tokens: 20,
                output_tokens: 8,
                cache_creation_input_tokens: 2,
                cache_read_input_tokens: 1,
            },
            estimated_tokens: 128,
        },
        "workspace-write",
        "yolo",
        &super::StatusContext {
            cwd: PathBuf::from("/tmp/project"),
            session_path: Some(PathBuf::from("session.jsonl")),
            session_id: Some("session".to_string()),
            session_store: "local import/export file".to_string(),
            loaded_config_files: 2,
            discovered_config_files: 3,
            memory_file_count: 4,
            project_root: Some(PathBuf::from("/tmp")),
            git_branch: Some("main".to_string()),
            git_summary: GitWorkspaceSummary {
                changed_files: 3,
                staged_files: 1,
                unstaged_files: 1,
                untracked_files: 1,
                conflicted_files: 0,
            },
            sandbox_status: runtime::SandboxStatus::default(),
        },
    );
    let fields = parse_report_fields(&status);
    assert_eq!(
        fields.get("Model").map(String::as_str),
        Some("claude-sonnet")
    );
    assert_eq!(
        fields.get("Permission mode").map(String::as_str),
        Some("workspace-write")
    );
    assert_eq!(
        fields.get("Execution mode").map(String::as_str),
        Some("yolo")
    );
    assert_eq!(fields.get("Messages").map(String::as_str), Some("7"));
    assert_eq!(fields.get("Latest total").map(String::as_str), Some("10"));
    assert_eq!(
        fields.get("Cumulative total").map(String::as_str),
        Some("31")
    );
    assert_eq!(fields.get("Cwd").map(String::as_str), Some("/tmp/project"));
    assert_eq!(fields.get("Project root").map(String::as_str), Some("/tmp"));
    assert_eq!(fields.get("Git branch").map(String::as_str), Some("main"));
    assert_eq!(
        fields.get("Git state").map(String::as_str),
        Some("dirty · 3 files · 1 staged, 1 unstaged, 1 untracked")
    );
    assert_eq!(fields.get("Changed files").map(String::as_str), Some("3"));
    assert_eq!(fields.get("Staged").map(String::as_str), Some("1"));
    assert_eq!(fields.get("Unstaged").map(String::as_str), Some("1"));
    assert_eq!(fields.get("Untracked").map(String::as_str), Some("1"));
    assert_eq!(
        fields.get("Session").map(String::as_str),
        Some("session.jsonl")
    );
    assert_eq!(
        fields.get("Session id").map(String::as_str),
        Some("session")
    );
    assert_eq!(
        fields.get("Session store").map(String::as_str),
        Some("local import/export file")
    );
    assert_eq!(
        fields.get("Config files").map(String::as_str),
        Some("loaded 2/3")
    );
    assert_eq!(fields.get("Memory files").map(String::as_str), Some("4"));
}

#[test]
fn commit_reports_surface_workspace_context() {
    let summary = GitWorkspaceSummary {
        changed_files: 2,
        staged_files: 1,
        unstaged_files: 1,
        untracked_files: 0,
        conflicted_files: 0,
    };

    let preflight = format_commit_preflight_report(Some("feature/ux"), summary);
    assert!(preflight.contains("Result           ready"));
    assert!(preflight.contains("Branch           feature/ux"));
    assert!(preflight.contains("Workspace        dirty · 2 files · 1 staged, 1 unstaged"));
    assert!(preflight
        .contains("Action           create a git commit from the current workspace changes"));
}

#[test]
fn commit_skipped_report_points_to_next_steps() {
    let report = format_commit_skipped_report();
    assert!(report.contains("Reason           no workspace changes"));
    assert!(
        report.contains("Action           create a git commit from the current workspace changes")
    );
    assert!(report.contains("/status to inspect context"));
    assert!(report.contains("/diff to inspect repo changes"));
}

#[test]
fn runtime_slash_reports_describe_command_behavior() {
    let bughunter = format_bughunter_report(Some("runtime"));
    assert!(bughunter.contains("Scope            runtime"));
    assert!(bughunter.contains("inspect the selected code for likely bugs"));

    let ultraplan = format_ultraplan_report(Some("ship the release"));
    assert!(ultraplan.contains("Task             ship the release"));
    assert!(ultraplan.contains("break work into a multi-step execution plan"));

    let pr = format_pr_report("feature/ux", Some("ready for review"));
    assert!(pr.contains("Branch           feature/ux"));
    assert!(pr.contains("draft or create a pull request"));

    let issue = format_issue_report(Some("flaky test"));
    assert!(issue.contains("Context          flaky test"));
    assert!(issue.contains("draft or create a GitHub issue"));
}

#[test]
fn no_arg_commands_reject_unexpected_arguments() {
    assert!(validate_no_args("/commit", None).is_ok());

    let error = validate_no_args("/commit", Some("now"))
        .expect_err("unexpected arguments should fail")
        .to_string();
    assert!(error.contains("/commit does not accept arguments"));
    assert!(error.contains("Received: now"));
}

#[test]
fn config_report_supports_section_views() {
    let report = render_config_report(Some("env")).expect("config report should render");
    assert!(report.contains("Merged section: env"));
    let plugins_report =
        render_config_report(Some("plugins")).expect("plugins config report should render");
    assert!(plugins_report.contains("Merged section: plugins"));
}

#[test]
fn memory_report_uses_sectioned_layout() {
    let report = render_memory_report().expect("memory report should render");
    assert!(report.contains("Working directory"));
    assert!(report.contains("Instruction files"));
    assert!(report.contains("Discovered files"));
}

#[test]
fn config_report_uses_sectioned_layout() {
    let report = render_config_report(None).expect("config report should render");
    assert!(report.contains("Config"));
    assert!(report.contains("Discovered files"));
    assert!(report.contains("Merged JSON"));
}

#[test]
fn parses_git_status_metadata() {
    let _guard = env_lock();
    let temp_root = temp_dir();
    fs::create_dir_all(&temp_root).expect("root dir");
    let (project_root, branch) = parse_git_status_metadata_for(
        &temp_root,
        Some(
            "## rcc/cli...origin/rcc/cli
 M src/main.rs",
        ),
    );
    assert_eq!(branch.as_deref(), Some("rcc/cli"));
    assert!(project_root.is_none());
    fs::remove_dir_all(temp_root).expect("cleanup temp dir");
}

#[test]
fn parses_detached_head_from_status_snapshot() {
    let _guard = env_lock();
    assert_eq!(
        parse_git_status_branch(Some(
            "## HEAD (no branch)
 M src/main.rs"
        )),
        Some("detached HEAD".to_string())
    );
}

#[test]
fn parses_git_workspace_summary_counts() {
    let summary = parse_git_workspace_summary(Some(
        "## feature/ux
M  src/main.rs
 M README.md
?? notes.md
UU conflicted.rs",
    ));

    assert_eq!(
        summary,
        GitWorkspaceSummary {
            changed_files: 4,
            staged_files: 2,
            unstaged_files: 2,
            untracked_files: 1,
            conflicted_files: 1,
        }
    );
    assert_eq!(
        summary.headline(),
        "dirty · 4 files · 2 staged, 2 unstaged, 1 untracked, 1 conflicted"
    );
}

#[test]
fn render_diff_report_shows_clean_tree_for_committed_repo() {
    let _guard = env_lock();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");
    git(&["init", "--quiet"], &root);
    git(&["config", "user.email", "tests@example.com"], &root);
    git(&["config", "user.name", "Rusty Claude Tests"], &root);
    fs::write(root.join("tracked.txt"), "hello\n").expect("write file");
    git(&["add", "tracked.txt"], &root);
    git(&["commit", "-m", "init", "--quiet"], &root);

    let report = render_diff_report_for(&root).expect("diff report should render");
    assert!(report.contains("clean working tree"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn render_diff_report_includes_staged_and_unstaged_sections() {
    let _guard = env_lock();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");
    git(&["init", "--quiet"], &root);
    git(&["config", "user.email", "tests@example.com"], &root);
    git(&["config", "user.name", "Rusty Claude Tests"], &root);
    fs::write(root.join("tracked.txt"), "hello\n").expect("write file");
    git(&["add", "tracked.txt"], &root);
    git(&["commit", "-m", "init", "--quiet"], &root);

    fs::write(root.join("tracked.txt"), "hello\nstaged\n").expect("update file");
    git(&["add", "tracked.txt"], &root);
    fs::write(root.join("tracked.txt"), "hello\nstaged\nunstaged\n").expect("update file twice");

    let report = render_diff_report_for(&root).expect("diff report should render");
    assert!(report.contains("Staged changes:"));
    assert!(report.contains("Unstaged changes:"));
    assert!(report.contains("tracked.txt"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn render_diff_report_omits_ignored_files() {
    let _guard = env_lock();
    let root = temp_dir();
    fs::create_dir_all(&root).expect("root dir");
    git(&["init", "--quiet"], &root);
    git(&["config", "user.email", "tests@example.com"], &root);
    git(&["config", "user.name", "Rusty Claude Tests"], &root);
    fs::write(root.join(".gitignore"), ".omx/\nignored.txt\n").expect("write gitignore");
    fs::write(root.join("tracked.txt"), "hello\n").expect("write tracked");
    git(&["add", ".gitignore", "tracked.txt"], &root);
    git(&["commit", "-m", "init", "--quiet"], &root);
    fs::create_dir_all(root.join(".omx")).expect("write omx dir");
    fs::write(root.join(".omx").join("state.json"), "{}").expect("write ignored omx");
    fs::write(root.join("ignored.txt"), "secret\n").expect("write ignored file");
    fs::write(root.join("tracked.txt"), "hello\nworld\n").expect("write tracked change");

    let report = render_diff_report_for(&root).expect("diff report should render");
    assert!(report.contains("tracked.txt"));
    assert!(!report.contains("+++ b/ignored.txt"));
    assert!(!report.contains("+++ b/.omx/state.json"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn status_context_reads_real_workspace_metadata() {
    let context = status_context(None).expect("status context should load");
    assert!(context.cwd.is_absolute());
    assert!(context.discovered_config_files >= context.loaded_config_files);
    assert!(context.loaded_config_files <= context.discovered_config_files);
}

#[test]
fn normalizes_supported_permission_modes() {
    assert_eq!(normalize_permission_mode("read-only"), Some("read-only"));
    assert_eq!(
        normalize_permission_mode("workspace-write"),
        Some("workspace-write")
    );
    assert_eq!(
        normalize_permission_mode("danger-full-access"),
        Some("danger-full-access")
    );
    assert_eq!(normalize_permission_mode("unknown"), None);
}

#[test]
fn clear_command_requires_explicit_confirmation_flag() {
    assert_eq!(
        SlashCommand::parse("/clear"),
        Ok(Some(SlashCommand::Clear { confirm: false }))
    );
    assert_eq!(
        SlashCommand::parse("/clear --confirm"),
        Ok(Some(SlashCommand::Clear { confirm: true }))
    );
}

#[test]
fn parses_resume_and_config_slash_commands() {
    assert_eq!(
        SlashCommand::parse("/resume session-123"),
        Ok(Some(SlashCommand::Resume {
            session_path: Some("session-123".to_string())
        }))
    );
    assert_eq!(
        SlashCommand::parse("/clear --confirm"),
        Ok(Some(SlashCommand::Clear { confirm: true }))
    );
    assert_eq!(
        SlashCommand::parse("/config"),
        Ok(Some(SlashCommand::Config { section: None }))
    );
    assert_eq!(
        SlashCommand::parse("/config env"),
        Ok(Some(SlashCommand::Config {
            section: Some("env".to_string())
        }))
    );
    assert_eq!(
        SlashCommand::parse("/memory"),
        Ok(Some(SlashCommand::Memory))
    );
    assert_eq!(SlashCommand::parse("/init"), Ok(Some(SlashCommand::Init)));
    assert_eq!(
        SlashCommand::parse("/session fork incident-review"),
        Ok(Some(SlashCommand::Session {
            action: Some("fork".to_string()),
            target: Some("incident-review".to_string())
        }))
    );
}

#[test]
fn help_mentions_resume_as_tui_startup_only() {
    let mut help = Vec::new();
    print_help_to(&mut help).expect("help should render");
    let help = String::from_utf8(help).expect("help should be utf8");
    assert!(help.contains("cowd --resume latest"));
    assert!(!help.contains("cowd import-session PATH"));
    assert!(!help.contains("/session switch"));
    assert!(!help.contains("cowd --resume latest /status"));
}

#[test]
fn unknown_slash_command_guidance_suggests_nearby_commands() {
    let message = crate::suggestions::format_unknown_slash_command("stats");
    assert!(message.contains("Unknown slash command: /stats"));
    assert!(message.contains("/status"));
    assert!(message.contains("/help"));
}

#[test]
fn unknown_omc_slash_command_guidance_explains_runtime_gap() {
    let message = crate::suggestions::format_unknown_slash_command("oh-my-claudecode:hud");
    assert!(message.contains("Unknown slash command: /oh-my-claudecode:hud"));
    assert!(message.contains("Claude Code/OMC plugin command"));
    assert!(message.contains("does not yet load plugin slash commands"));
}

fn cwd_lock() -> &'static Mutex<()> {
    crate::services::process_cwd_lock()
}

fn temp_workspace(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("gateway-{label}-{nanos}"))
}

#[test]
fn gateway_workspace_prefers_config_and_falls_back_to_startup_directory() {
    let _guard = env_lock();
    let root = temp_workspace("workspace-resolution");
    let startup = root.join("startup");
    let configured = root.join("configured");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(&startup).expect("startup workspace");
    std::fs::create_dir_all(&configured).expect("configured workspace");
    std::fs::create_dir_all(&config_home).expect("config home");
    let _config_home = EnvVarGuard::set("COWD_CONFIG_HOME", &config_home);

    std::fs::write(
        config_home.join("config.yaml"),
        format!("workspace: {}\n", configured.display()),
    )
    .expect("configured workspace setting");
    let (resolved, config) = super::resolve_gateway_workspace_and_config(&startup)
        .expect("configured workspace should resolve");
    assert_eq!(resolved, configured.canonicalize().expect("canonical"));
    assert_eq!(config.workspace(), Some(configured.as_path()));

    std::fs::write(config_home.join("config.yaml"), "{}\n").expect("clear workspace setting");
    let (resolved, config) = super::resolve_gateway_workspace_and_config(&startup)
        .expect("startup workspace should resolve");
    assert_eq!(resolved, startup.canonicalize().expect("canonical"));
    assert!(config.workspace().is_none());

    std::fs::remove_dir_all(root).expect("cleanup workspace fixture");
}

#[test]
fn gateway_workspace_expands_user_home() {
    let _guard = env_lock();
    let root = temp_workspace("workspace-home");
    let startup = root.join("startup");
    let home = root.join("home");
    let configured = home.join("AI");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(&startup).expect("startup workspace");
    std::fs::create_dir_all(&configured).expect("configured workspace");
    std::fs::create_dir_all(&config_home).expect("config home");
    let _home = EnvVarGuard::set("HOME", &home);
    let _config_home = EnvVarGuard::set("COWD_CONFIG_HOME", &config_home);
    std::fs::write(config_home.join("config.yaml"), "workspace: ~/AI\n")
        .expect("configured workspace setting");

    let (resolved, config) = super::resolve_gateway_workspace_and_config(&startup)
        .expect("home workspace should resolve");
    assert_eq!(resolved, configured.canonicalize().expect("canonical"));
    assert_eq!(config.workspace(), Some(Path::new("~/AI")));

    std::fs::remove_dir_all(root).expect("cleanup workspace fixture");
}

#[test]
fn gateway_workspace_rejects_recursive_redirection() {
    let _guard = env_lock();
    let root = temp_workspace("workspace-redirection");
    let startup = root.join("startup");
    let configured = root.join("configured");
    let redirected = root.join("redirected");
    let config_home = root.join("config-home");
    std::fs::create_dir_all(&startup).expect("startup workspace");
    std::fs::create_dir_all(configured.join(".cowd")).expect("configured workspace");
    std::fs::create_dir_all(&redirected).expect("redirected workspace");
    std::fs::create_dir_all(&config_home).expect("config home");
    let _config_home = EnvVarGuard::set("COWD_CONFIG_HOME", &config_home);
    std::fs::write(
        config_home.join("config.yaml"),
        format!("workspace: {}\n", configured.display()),
    )
    .expect("bootstrap workspace setting");
    std::fs::write(
        configured.join(".cowd/config.local.yaml"),
        format!("workspace: {}\n", redirected.display()),
    )
    .expect("recursive workspace setting");

    let error = super::resolve_gateway_workspace_and_config(&startup)
        .expect_err("recursive workspace redirect must fail");
    assert!(error.contains("workspace configuration is unstable"));

    std::fs::remove_dir_all(root).expect("cleanup workspace fixture");
}

#[test]
fn init_template_mentions_detected_rust_workspace() {
    let _guard = cwd_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let rendered = crate::init::render_init_cowd_md(&workspace_root);
    assert!(rendered.contains("# COWD.md"));
    assert!(rendered.contains("cargo clippy --workspace --all-targets -- -D warnings"));
}

#[test]
fn converts_tool_roundtrip_messages() {
    let messages = vec![
        ConversationMessage::user_text("hello"),
        ConversationMessage::assistant(vec![ContentBlock::ToolUse {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            input: "{\"command\":\"pwd\"}".to_string(),
        }]),
        ConversationMessage {
            role: MessageRole::Tool,
            blocks: vec![ContentBlock::ToolResult {
                tool_use_id: "tool-1".to_string(),
                tool_name: "bash".to_string(),
                output: "ok".to_string(),
                is_error: false,
            }],
            usage: None,
        },
    ];

    let converted = super::convert_messages(&messages);
    assert_eq!(converted.len(), 3);
    assert_eq!(converted[1].role, "assistant");
    assert_eq!(converted[2].role, "user");
}
#[test]
fn tui_help_mentions_history_completion_and_multiline() {
    let help = render_terminal_help();
    for command in [
        "/history",
        "/tasks",
        "/approvals",
        "/context",
        "/cross-plane",
    ] {
        assert!(
            tui_help_contains_command(&help, command),
            "missing command {command} in help:\n{help}"
        );
    }
    for key in ["Up/Down", "Tab", "Shift+Enter/Ctrl+J", "Ctrl-R"] {
        assert!(
            parse_report_fields(&help).contains_key(key),
            "missing key hint {key}"
        );
    }
}

#[test]
fn tool_rendering_helpers_compact_display() {
    let start = format_tool_call_start("read_file", r#"{"path":"src/main.rs"}"#);
    assert!(start.contains("read_file"));
    assert!(start.contains("src/main.rs"));

    let done = format_tool_result(
        "read_file",
        r#"{"file":{"filePath":"src/main.rs","content":"hello","numLines":1,"startLine":1,"totalLines":1}}"#,
        false,
    );
    assert!(done.contains("📄 Read src/main.rs"));
    assert!(done.contains("hello"));
}

#[test]
fn tool_rendering_truncates_large_read_output_for_display_only() {
    let content = (0..200)
        .map(|index| format!("line {index:03}"))
        .collect::<Vec<_>>()
        .join("\n");
    let output = json!({
        "file": {
            "filePath": "src/main.rs",
            "content": content,
            "numLines": 200,
            "startLine": 1,
            "totalLines": 200
        }
    })
    .to_string();

    let rendered = format_tool_result("read_file", &output, false);

    assert!(rendered.contains("line 000"));
    assert!(rendered.contains("line 079"));
    assert!(!rendered.contains("line 199"));
    assert!(rendered.contains("full result preserved in session"));
    assert!(output.contains("line 199"));
}

#[test]
fn tool_rendering_truncates_large_bash_output_for_display_only() {
    let stdout = (0..120)
        .map(|index| format!("stdout {index:03}"))
        .collect::<Vec<_>>()
        .join("\n");
    let output = json!({
        "stdout": stdout,
        "stderr": "",
        "returnCodeInterpretation": "completed successfully"
    })
    .to_string();

    let rendered = format_tool_result("bash", &output, false);

    assert!(rendered.contains("stdout 000"));
    assert!(rendered.contains("stdout 059"));
    assert!(!rendered.contains("stdout 119"));
    assert!(rendered.contains("full result preserved in session"));
    assert!(output.contains("stdout 119"));
}

#[test]
fn tool_rendering_truncates_generic_long_output_for_display_only() {
    let items = (0..120)
        .map(|index| format!("payload {index:03}"))
        .collect::<Vec<_>>();
    let output = json!({
        "summary": "plugin payload",
        "items": items,
    })
    .to_string();

    let rendered = format_tool_result("plugin_echo", &output, false);

    assert!(rendered.contains("plugin_echo"));
    assert!(rendered.contains("payload 000"));
    assert!(rendered.contains("payload 040"));
    assert!(!rendered.contains("payload 080"));
    assert!(!rendered.contains("payload 119"));
    assert!(rendered.contains("full result preserved in session"));
    assert!(output.contains("payload 119"));
}

#[test]
fn tool_rendering_truncates_raw_generic_output_for_display_only() {
    let output = (0..120)
        .map(|index| format!("raw {index:03}"))
        .collect::<Vec<_>>()
        .join("\n");

    let rendered = format_tool_result("plugin_echo", &output, false);

    assert!(rendered.contains("plugin_echo"));
    assert!(rendered.contains("raw 000"));
    assert!(rendered.contains("raw 059"));
    assert!(!rendered.contains("raw 119"));
    assert!(rendered.contains("full result preserved in session"));
    assert!(output.contains("raw 119"));
}

#[test]
fn push_output_block_renders_markdown_text() {
    let mut out = Vec::new();
    let mut events = Vec::new();
    let mut pending_tool = None;
    let mut block_has_thinking_summary = false;

    push_output_block(
        OutputContentBlock::Text {
            text: "# Heading".to_string(),
        },
        &mut out,
        &mut events,
        &mut pending_tool,
        false,
        &mut block_has_thinking_summary,
    )
    .expect("text block should render");

    let rendered = String::from_utf8(out).expect("utf8");
    assert!(rendered.contains("Heading"));
    assert!(
        !rendered.contains('\u{1b}'),
        "gateway response conversion must stay plain-text; terminal rendering belongs to CLI/TUI"
    );
}

#[test]
fn push_output_block_skips_empty_object_prefix_for_tool_streams() {
    let mut out = Vec::new();
    let mut events = Vec::new();
    let mut pending_tool = None;
    let mut block_has_thinking_summary = false;

    push_output_block(
        OutputContentBlock::ToolUse {
            id: "tool-1".to_string(),
            name: "read_file".to_string(),
            input: json!({}),
        },
        &mut out,
        &mut events,
        &mut pending_tool,
        true,
        &mut block_has_thinking_summary,
    )
    .expect("tool block should accumulate");

    assert!(events.is_empty());
    assert_eq!(
        pending_tool,
        Some(("tool-1".to_string(), "read_file".to_string(), String::new(),))
    );
}

#[test]
fn response_to_events_preserves_empty_object_json_input_outside_streaming() {
    let mut out = Vec::new();
    let events = response_to_events(
        MessageResponse {
            id: "msg-1".to_string(),
            kind: "message".to_string(),
            model: "claude-opus-4-6".to_string(),
            role: "assistant".to_string(),
            content: vec![OutputContentBlock::ToolUse {
                id: "tool-1".to_string(),
                name: "read_file".to_string(),
                input: json!({}),
            }],
            stop_reason: Some("tool_use".to_string()),
            stop_sequence: None,
            usage: Usage {
                input_tokens: 1,
                output_tokens: 1,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            request_id: None,
        },
        &mut out,
    )
    .expect("response conversion should succeed");

    assert!(matches!(
        &events[0],
        AssistantEvent::ToolUse { name, input, .. }
            if name == "read_file" && input == "{}"
    ));
}

#[test]
fn response_to_events_preserves_non_empty_json_input_outside_streaming() {
    let mut out = Vec::new();
    let events = response_to_events(
        MessageResponse {
            id: "msg-2".to_string(),
            kind: "message".to_string(),
            model: "claude-opus-4-6".to_string(),
            role: "assistant".to_string(),
            content: vec![OutputContentBlock::ToolUse {
                id: "tool-2".to_string(),
                name: "read_file".to_string(),
                input: json!({ "path": "rust/Cargo.toml" }),
            }],
            stop_reason: Some("tool_use".to_string()),
            stop_sequence: None,
            usage: Usage {
                input_tokens: 1,
                output_tokens: 1,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            request_id: None,
        },
        &mut out,
    )
    .expect("response conversion should succeed");

    assert!(matches!(
        &events[0],
        AssistantEvent::ToolUse { name, input, .. }
            if name == "read_file" && input == "{\"path\":\"rust/Cargo.toml\"}"
    ));
}

#[test]
fn response_to_events_keeps_private_thinking_out_of_public_reasoning() {
    let mut out = Vec::new();
    let events = response_to_events(
        MessageResponse {
            id: "msg-3".to_string(),
            kind: "message".to_string(),
            model: "claude-opus-4-6".to_string(),
            role: "assistant".to_string(),
            content: vec![
                OutputContentBlock::Thinking {
                    thinking: "step 1".to_string(),
                    signature: Some("sig_123".to_string()),
                },
                OutputContentBlock::Text {
                    text: "Final answer".to_string(),
                },
            ],
            stop_reason: Some("end_turn".to_string()),
            stop_sequence: None,
            usage: Usage {
                input_tokens: 1,
                output_tokens: 1,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            request_id: None,
        },
        &mut out,
    )
    .expect("response conversion should succeed");

    assert!(
        matches!(
            &events[0],
            AssistantEvent::PrivateReasoningDelta(thinking) if thinking == "step 1"
        ),
        "provider-private thinking must remain private protocol state"
    );
    assert!(
        matches!(
            &events[1],
            AssistantEvent::SignatureDelta(signature) if signature == "sig_123"
        ),
        "the private reasoning signature must remain available for protocol round-trip"
    );
    assert!(
        matches!(
            &events[2],
            AssistantEvent::TextDelta(text) if text == "Final answer"
        ),
        "the visible response must remain public text"
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, AssistantEvent::ReasoningSummaryDelta(_))));
    let rendered = String::from_utf8(out).expect("utf8");
    assert!(rendered.contains("▶ Thinking (6 chars hidden)"));
    assert!(!rendered.contains("step 1"));
}

#[test]
fn runtime_bootstrap_state_merges_plugin_hooks_into_runtime_features() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    let source_root = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    fs::create_dir_all(&source_root).expect("source root");
    write_plugin_fixture(&source_root, "hook-runtime-demo", true, false);

    let mut manager = Pm::new(Pmc::new(&config_home));
    manager
        .install(source_root.to_str().expect("utf8 source path"))
        .expect("plugin install should succeed");
    let loader = ConfigLoader::new(&workspace, &config_home);
    let runtime_config = loader.load().expect("runtime config should load");
    let state = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace,
        &loader,
        &runtime_config,
    )
    .expect("plugin state should load");
    let pre_hooks = state.feature_config.hooks().pre_tool_use();
    assert_eq!(pre_hooks.len(), 1);
    assert!(
        pre_hooks[0].ends_with("hooks/pre.sh"),
        "expected installed plugin hook path, got {pre_hooks:?}"
    );

    let _ = fs::remove_dir_all(config_home);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(source_root);
}

#[test]
#[allow(clippy::too_many_lines)]
fn runtime_bootstrap_state_discovers_mcp_tools_and_surfaces_pending_servers() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    let script_path = workspace.join("fixture-mcp.py");
    write_mcp_server_fixture(&script_path);
    fs::write(
        config_home.join("config.yaml"),
        format!(
            r#"{{
                  "mcpServers": {{
                    "alpha": {{
                      "command": "python3",
                      "args": ["{}"]
                    }},
                    "broken": {{
                      "command": "python3",
                      "args": ["-c", "import sys; sys.exit(0)"]
                    }}
                  }}
                }}"#,
            script_path.to_string_lossy()
        ),
    )
    .expect("write mcp settings");

    let loader = ConfigLoader::new(&workspace, &config_home);
    let runtime_config = loader.load().expect("runtime config should load");
    let state = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace,
        &loader,
        &runtime_config,
    )
    .expect("runtime plugin state should load");
    let mcp_service = Arc::new(SHARED_RT.block_on(
        crate::runtime_host::RuntimeMcpServiceAdapter::from_runtime_config(&runtime_config),
    ));
    let tool_registry = state
        .tool_registry
        .clone()
        .extend_runtime_tools(mcp_service.runtime_tool_definitions())
        .expect("MCP tools should merge into the runtime catalog");
    assert_eq!(
        tool_registry.required_permission("runtime_orchestrate"),
        Some(harness_contract::tool::ToolPermissionMode::ReadOnly),
        "MCP discovery must not replace core Runtime tools"
    );

    let allowed = tool_registry
        .normalize_allowed_tools(&["mcp__alpha__echo".to_string(), "mcp_tool".to_string()])
        .expect("mcp tools should be allow-listable")
        .expect("allow-list should exist");
    assert!(allowed.contains("mcp__alpha__echo"));
    assert!(allowed.contains("mcp_tool"));

    let tool_host = Arc::new(
        tools::ToolHost::new(
            "bootstrap-mcp-test",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(tool_registry),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                Some(mcp_service.clone()),
            ),
        )
        .with_authorization_lease_verifier(Arc::new(
            runtime::AuthorizationNegotiator::verify_lease_signature,
        )),
    );
    let executor = GatewayToolExecutor::from_tool_host(None, false, tool_host);

    let authorize_and_execute = |tool_name: &str, input: &str| {
        let value: serde_json::Value =
            serde_json::from_str(input).expect("tool input should be valid JSON");
        let descriptor = executor
            .registered_tool_effect(tool_name, &value)
            .expect("registered tool should describe its effect");
        let request_id = format!("gateway-mcp-test:{tool_name}");
        let negotiator = runtime::AuthorizationNegotiator::new();
        let policy = runtime::PermissionPolicy::new(PermissionMode::DangerFullAccess);
        let request = runtime::AuthorizationRequest {
            principal_id: "test:gateway-mcp".to_string(),
            capability: descriptor.tool_id.clone(),
            input: value.to_string(),
            idempotency_key: request_id.clone(),
            effect: descriptor.clone(),
            parent_ceiling: PermissionMode::DangerFullAccess,
            parent_lease_id: None,
            policy_revision: 1,
            recovery_scope: request_id.clone(),
            context: runtime::PermissionContext::default(),
            safe_alternatives: Vec::new(),
        };
        let evaluated = negotiator.assess_effective(&policy, &request);
        let assessment = evaluated.assessment.lease.clone().map_or_else(
            || {
                negotiator.approve_effective(
                    &policy,
                    &request,
                    &evaluated.effective,
                    &harness_contract::policy::ApprovalGrant {
                        grant_id: format!("grant:{request_id}"),
                        approval_id: format!("approval:{request_id}"),
                        scope: harness_contract::policy::ApprovalGrantScope::Once,
                        principal_id: request.principal_id.clone(),
                        profile_id: "gateway-mcp-test".to_string(),
                        workspace_key: "gateway-mcp-test".to_string(),
                        capability: request.capability.clone(),
                        session_id: Some("gateway-mcp-session".to_string()),
                        turn_id: None,
                        task_id: None,
                        invocation_id: Some(request_id.clone()),
                        resource_targets: Vec::new(),
                        effect_descriptor_hash: Some(descriptor.descriptor_hash.clone()),
                        risk_ceiling: harness_contract::core::TaskRisk::Critical,
                        policy_revision: 1,
                        status: harness_contract::policy::ApprovalGrantStatus::Active,
                        issued_by: harness_contract::policy::ApprovalDecisionActor {
                            kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                            actor_id: "gateway-mcp-test-human".to_string(),
                        },
                        created_at_ms: 1,
                        expires_at_ms: None,
                        revoked_at_ms: None,
                        revoke_reason: None,
                    },
                )
            },
            |_| evaluated.assessment.clone(),
        );
        let authorization = runtime::ToolPolicy
            .authorize(
                &evaluated.effective,
                &assessment,
                request_id,
                assessment.lease.clone().expect("test authorization lease"),
                30,
            )
            .expect("test permission should authorize tool")
            .authorization;
        SHARED_RT.block_on(executor.execute_authorized(&authorization, tool_name, input))
    };

    assert!(SHARED_RT
        .block_on(executor.execute("mcp__alpha__echo", r#"{"text":"hello"}"#))
        .is_err());

    let tool_output = authorize_and_execute("mcp__alpha__echo", r#"{"text":"hello"}"#)
        .expect("discovered mcp tool should execute");
    let tool_json: serde_json::Value =
        serde_json::from_str(&tool_output).expect("tool output should be json");
    assert_eq!(tool_json["output"]["structuredContent"]["echoed"], "hello");

    let wrapped_output = authorize_and_execute(
        "mcp_tool",
        r#"{"qualifiedName":"mcp__alpha__echo","arguments":{"text":"wrapped"}}"#,
    )
    .expect("generic mcp wrapper should execute");
    let wrapped_json: serde_json::Value =
        serde_json::from_str(&wrapped_output).expect("wrapped output should be json");
    assert_eq!(
        wrapped_json["output"]["structuredContent"]["echoed"],
        "wrapped"
    );

    let search_output = SHARED_RT
        .block_on(executor.execute("tool_search", r#"{"query":"alpha echo","max_results":5}"#))
        .expect("tool search should execute");
    let search_json: serde_json::Value =
        serde_json::from_str(&search_output).expect("search output should be json");
    assert_eq!(search_json["matches"][0], "mcp__alpha__echo");
    assert_eq!(search_json["pending_mcp_servers"][0], "broken");
    assert_eq!(
        search_json["mcp_degraded"]["failed_servers"][0]["server_name"],
        "broken"
    );
    assert!(
        search_json["mcp_degraded"]["failed_servers"][0]["phase"].is_string(),
        "failed server must retain its actual lifecycle phase"
    );
    assert_eq!(
        search_json["mcp_degraded"]["available_tools"][0],
        "mcp__alpha__echo"
    );

    let listed = authorize_and_execute("list_mcp_resources_tool", r#"{"server":"alpha"}"#)
        .expect("resources should list");
    let listed_json: serde_json::Value =
        serde_json::from_str(&listed).expect("resource output should be json");
    assert_eq!(listed_json[0]["uri"], "file://guide.txt");

    let read = authorize_and_execute(
        "read_mcp_resource_tool",
        r#"{"server":"alpha","uri":"file://guide.txt"}"#,
    )
    .expect("resource should read");
    let read_json: serde_json::Value =
        serde_json::from_str(&read).expect("resource read output should be json");
    assert_eq!(
        read_json["content"]["contents"][0]["text"],
        "contents for file://guide.txt"
    );

    mcp_service
        .shutdown()
        .expect("MCP workers should shut down");

    let _ = fs::remove_dir_all(config_home);
    let _ = fs::remove_dir_all(workspace);
}

#[test]
fn runtime_bootstrap_state_surfaces_unsupported_mcp_servers_structurally() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    fs::write(
        config_home.join("config.yaml"),
        r#"{
              "mcpServers": {
                "remote": {
                  "url": "https://example.test/mcp"
                }
              }
            }"#,
    )
    .expect("write mcp settings");

    let loader = ConfigLoader::new(&workspace, &config_home);
    let runtime_config = loader.load().expect("runtime config should load");
    let state = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace,
        &loader,
        &runtime_config,
    )
    .expect("runtime plugin state should load");
    let mcp_service = Arc::new(SHARED_RT.block_on(
        crate::runtime_host::RuntimeMcpServiceAdapter::from_runtime_config(&runtime_config),
    ));
    let tool_registry = state
        .tool_registry
        .clone()
        .extend_runtime_tools(mcp_service.runtime_tool_definitions())
        .expect("MCP wrappers should merge into the runtime catalog");
    assert_eq!(
        tool_registry.required_permission("runtime_orchestrate"),
        Some(harness_contract::tool::ToolPermissionMode::ReadOnly),
        "degraded MCP discovery must retain core Runtime tools"
    );
    let tool_host = Arc::new(
        tools::ToolHost::new(
            "bootstrap-mcp-unsupported-test",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(tool_registry),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                Some(mcp_service),
            ),
        )
        .with_authorization_lease_verifier(Arc::new(
            runtime::AuthorizationNegotiator::verify_lease_signature,
        )),
    );
    let executor = GatewayToolExecutor::from_tool_host(None, false, tool_host);

    let search_output = SHARED_RT
        .block_on(executor.execute("tool_search", r#"{"query":"remote","max_results":5}"#))
        .expect("tool search should execute");
    let search_json: serde_json::Value =
        serde_json::from_str(&search_output).expect("search output should be json");
    assert_eq!(search_json["pending_mcp_servers"][0], "remote");
    assert_eq!(
        search_json["mcp_degraded"]["failed_servers"][0]["server_name"],
        "remote"
    );
    assert_eq!(
        search_json["mcp_degraded"]["failed_servers"][0]["phase"],
        "server_registration"
    );
    assert_eq!(
        search_json["mcp_degraded"]["failed_servers"][0]["error"]["context"]["transport"],
        "http"
    );

    let _ = fs::remove_dir_all(config_home);
    let _ = fs::remove_dir_all(workspace);
}

#[test]
fn create_runtime_entry_runs_plugin_lifecycle_init_and_shutdown() {
    let config_home = temp_dir();
    let workspace = temp_dir();
    let source_root = temp_dir();
    fs::create_dir_all(&config_home).expect("config home");
    fs::create_dir_all(&workspace).expect("workspace");
    fs::create_dir_all(&source_root).expect("source root");
    write_plugin_fixture(&source_root, "lifecycle-runtime-demo", false, true);

    let mut manager = Pm::new(Pmc::new(&config_home));
    let install = manager
        .install(source_root.to_str().expect("utf8 source path"))
        .expect("plugin install should succeed");
    let log_path = install.install_path.join("lifecycle.log");
    let loader = ConfigLoader::new(&workspace, &config_home);
    let runtime_config = loader.load().expect("runtime config should load");
    let runtime_plugin_state = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace,
        &loader,
        &runtime_config,
    )
    .expect("plugin state should load");
    let runtime_session_snapshot = runtime_plugin_state.session_snapshot();
    let test_model = "test-plugin-model";
    let provider_registry = runtime::ProviderRegistry::new(ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".to_string(),
            ProviderConfig {
                name: "test".to_string(),
                base_url: "http://127.0.0.1:9/v1".to_string(),
                api_key: "test".to_string(),
                models: vec![test_model.to_string()],
                protocol: Some("completions".to_string()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    })
    .expect("test provider registry");
    let test_tool_host = Arc::new(
        tools::ToolHost::new(
            "runtime-plugin-lifecycle",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(runtime_plugin_state.tool_registry.clone()),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                None,
            ),
        )
        .with_authorization_lease_verifier(Arc::new(
            runtime::AuthorizationNegotiator::verify_lease_signature,
        )),
    );
    let mut runtime = create_runtime_entry_with_bootstrap_state(
        runtime::RuntimeServices::in_memory().expect("test runtime services"),
        Arc::new(provider_registry),
        test_tool_host,
        Session::new(),
        "runtime-plugin-lifecycle",
        test_model.to_string(),
        vec!["test system prompt".to_string()],
        true,
        false,
        None,
        runtime::SessionExecutionPolicy::from_defaults(
            PermissionMode::DangerFullAccess,
            runtime::ApprovalProfile::Balanced,
        ),
        None,
        None,
        runtime_session_snapshot,
        None,
    )
    .expect("runtime should build");

    assert_eq!(
        fs::read_to_string(&log_path).expect("init log should exist"),
        "init\n"
    );

    runtime
        .shutdown_plugins()
        .expect("plugin shutdown should succeed");

    assert_eq!(
        fs::read_to_string(&log_path).expect("shutdown log should exist"),
        "init\nshutdown\n"
    );

    let _ = fs::remove_dir_all(config_home);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(source_root);
}

#[test]
fn rejects_invalid_reasoning_effort_value() {
    let err = parse_args(&[
        "--reasoning-effort".to_string(),
        "turbo".to_string(),
        "prompt".to_string(),
        "hello".to_string(),
    ])
    .unwrap_err();
    assert!(
        err.contains("invalid value for --reasoning-effort"),
        "unexpected error: {err}"
    );
    assert!(err.contains("turbo"), "unexpected error: {err}");
}

#[test]
fn accepts_valid_reasoning_effort_values() {
    for value in ["low", "medium", "high", "max"] {
        let result = parse_args(&["--reasoning-effort".to_string(), value.to_string()]);
        assert!(
            result.is_ok(),
            "--reasoning-effort {value} should be accepted, got: {result:?}"
        );
        if let Ok(CliAction::Tui {
            reasoning_effort, ..
        }) = result
        {
            assert_eq!(reasoning_effort.as_deref(), Some(value));
        }
    }
}

#[test]
fn stub_commands_absent_from_terminal_completions() {
    let candidates =
        slash_command_completion_candidates_with_sessions("claude-3-5-sonnet", None, vec![]);
    for stub in NON_EXECUTABLE_SLASH_COMMANDS {
        let with_slash = format!("/{stub}");
        assert!(
            !candidates.contains(&with_slash),
            "stub command {with_slash} should not appear in terminal completions"
        );
    }
}
