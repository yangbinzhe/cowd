use super::{
    deep_merge_objects, parse_optional_compression_config, parse_optional_context_budget_config,
    parse_optional_gateway_config, parse_optional_hot_state_config,
    parse_optional_model_context_windows, parse_optional_session_history_config,
    parse_optional_storage_config, parse_permission_mode_label, parse_routing_mode,
    redact_serde_json, AppActivationPolicyV1, AppsConfig, ConfigLoader, ConfigSource,
    DomainProfile, GatewayConfig, McpServerConfig, McpTransport, PathBuf, ProviderProtocol,
    ResolvedPermissionMode, RoutingMode, RuntimeConfig, RuntimeFeatureConfig, RuntimeHookConfig,
    RuntimePluginConfig, SessionCompactConfig, StorageBackendSelection, COWD_SETTINGS_SCHEMA_NAME,
};
use crate::json::JsonValue;
use crate::sandbox::FilesystemIsolationMode;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

struct EnvVarGuard {
    key: String,
    original: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &str, value: Option<&str>) -> Self {
        let original = std::env::var(key).ok();
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        Self {
            key: key.to_string(),
            original,
        }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(val) => std::env::set_var(&self.key, val),
            None => std::env::remove_var(&self.key),
        }
    }
}

// Serialize tests that mutate environment variables to avoid race conditions.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn temp_dir() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("runtime-config-{nanos}"))
}

#[test]
fn rejects_non_object_settings_files() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(home.join("config.yaml"), "[]").expect("write bad settings");

    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("config should fail");
    assert!(error
        .to_string()
        .contains("top-level config value must be an object"));

    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}

#[test]
fn parses_top_level_workspace_without_reusing_sandbox_policy() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let workspace = root.join("configured-workspace");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::create_dir_all(&workspace).expect("configured workspace");
    fs::write(
        home.join("config.yaml"),
        format!(
            "workspace: {}\nsandbox:\n  workspace_root: /sandbox-only\n",
            workspace.display()
        ),
    )
    .expect("write workspace config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("workspace config");
    assert_eq!(loaded.workspace(), Some(workspace.as_path()));
    assert_eq!(
        loaded.sandbox().workspace_root.as_deref(),
        Some(std::path::Path::new("/sandbox-only"))
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn rejects_empty_top_level_workspace() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(home.join("config.yaml"), "workspace: '   '\n")
        .expect("write invalid workspace config");

    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("empty workspace must fail");
    assert!(error.to_string().contains("workspace must not be empty"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn loads_and_merges_claude_code_config_files_by_precedence() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
            home.parent().expect("home parent").join(".cowd/config.yaml"),
            r#"{"model":"haiku","env":{"A":"1"},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user compat config");
    fs::write(
            home.join("config.yaml"),
            r#"{"model":"sonnet","env":{"A2":"1"},"hooks":{"PreToolUse":["base"]},"permissions":{"default_mode":"read-only","allow":["Read"],"deny":["Bash(rm -rf)"]},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user settings");
    fs::write(
            cwd.join(".cowd").join("config.yaml"),
            r#"{"model":"project-compat","env":{"B":"2","C":"3"},"hooks":{"PostToolUse":["project"],"PostToolUseFailure":["project-failure"]},"permissions":{"ask":["Edit"]},"mcpServers":{"project":{"command":"uvx","args":["project"]}}}"#,
        )
        .expect("write project settings");
    fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{"model":"opus","permissions":{"default_mode":"workspace-write"}}"#,
    )
    .expect("write local settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(COWD_SETTINGS_SCHEMA_NAME, "SettingsSchema");
    assert_eq!(loaded.loaded_entries().len(), 3);
    assert_eq!(loaded.loaded_entries()[0].source, ConfigSource::User);
    assert!(loaded.loaded_entries()[1].source == ConfigSource::Project);
    assert!(loaded.loaded_entries()[2].source == ConfigSource::Local);
    assert_eq!(
        loaded.get("model"),
        Some(&JsonValue::String("opus".to_string()))
    );
    assert_eq!(loaded.model(), Some("opus"));
    assert_eq!(
        loaded.permission_mode(),
        Some(ResolvedPermissionMode::WorkspaceWrite)
    );
    assert_eq!(
        loaded
            .get("env")
            .and_then(JsonValue::as_object)
            .expect("env object")
            .len(),
        3
    );
    assert!(loaded
        .get("hooks")
        .and_then(JsonValue::as_object)
        .expect("hooks object")
        .contains_key("PreToolUse"));
    assert!(loaded
        .get("hooks")
        .and_then(JsonValue::as_object)
        .expect("hooks object")
        .contains_key("PostToolUse"));
    assert_eq!(loaded.hooks().pre_tool_use(), &["base".to_string()]);
    assert_eq!(loaded.hooks().post_tool_use(), &["project".to_string()]);
    assert_eq!(
        loaded.hooks().post_tool_use_failure(),
        &["project-failure".to_string()]
    );
    assert_eq!(loaded.permission_rules().allow(), &["Read".to_string()]);
    assert_eq!(
        loaded.permission_rules().deny(),
        &["Bash(rm -rf)".to_string()]
    );
    assert_eq!(loaded.permission_rules().ask(), &["Edit".to_string()]);
    assert!(loaded.mcp().get("home").is_some());
    assert!(loaded.mcp().get("project").is_some());

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_snake_case_permission_mode_from_default_template_shape() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
permissions:
  default_mode: "workspace-write"
"#,
    )
    .expect("write config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(
        loaded.permission_mode(),
        Some(ResolvedPermissionMode::WorkspaceWrite)
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_top_level_approval_from_default_template_shape() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
approval:
  profile: autonomous
  low_risk_timeout: pending
"#,
    )
    .expect("write config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(
        loaded.approval().profile,
        harness_contract::policy::ApprovalProfile::Autonomous
    );
    assert_eq!(
        loaded.approval().low_risk_timeout,
        harness_contract::policy::LowRiskTimeoutAction::Pending
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_sandbox_config() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{
              "sandbox": {
                "enabled": true,
                "namespaceRestrictions": false,
                "networkIsolation": true,
                "filesystemMode": "allow-list",
                "allowedMounts": ["logs", "tmp/cache"]
              }
            }"#,
    )
    .expect("write local settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(loaded.sandbox().enabled, Some(true));
    assert_eq!(loaded.sandbox().namespace_restrictions, Some(false));
    assert_eq!(loaded.sandbox().network_isolation, Some(true));
    assert_eq!(
        loaded.sandbox().filesystem_mode,
        Some(FilesystemIsolationMode::AllowList)
    );
    assert_eq!(loaded.sandbox().allowed_mounts, vec!["logs", "tmp/cache"]);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn config_runtime_control_merges_scenario_and_policy_overrides() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
        home.join("config.yaml"),
        r#"{
              "runtime": {
                "scenario": "research",
                "control": {
                  "agent": {
                    "max_parallel_agents": 5
                  },
                  "context": {
                    "collaboration_budget_tokens": 16000
                  },
                  "mission_schedule": {
                    "tick_interval_ms": 1500,
                    "grace_ms": 120000
                  }
                }
              }
            }"#,
    )
    .expect("write user runtime control");
    fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{
              "runtime": {
                "control": {
                  "enabled": false,
                  "agent": {
                    "min_collaboration_score": 72
                  },
                  "task": {
                    "max_failures_before_review": 1
                  },
                  "memory": {
                    "max_candidates_per_turn": 3
                  },
                  "capacity": {
                    "profile_id": "research-narrow",
                    "revision": 3,
                    "max_agent_nodes_per_team": 12,
                    "max_pending_per_key": 128
                  }
                }
              }
            }"#,
    )
    .expect("write local runtime control");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("runtime control config should load");
    let runtime = loaded.runtime_control();

    assert_eq!(runtime.scenario, DomainProfile::Research);
    assert!(!runtime.policy.enabled);
    assert_eq!(runtime.policy.agent.max_parallel_agents, 5);
    assert_eq!(runtime.policy.agent.min_collaboration_score, 72);
    assert_eq!(runtime.policy.task.max_failures_before_review, 1);
    assert_eq!(runtime.policy.context.collaboration_budget_tokens, 16_000);
    assert_eq!(runtime.policy.memory.max_candidates_per_turn, 3);
    assert_eq!(runtime.policy.capacity.profile_id, "research-narrow");
    assert_eq!(runtime.policy.capacity.revision, 3);
    assert_eq!(runtime.policy.capacity.max_agent_nodes_per_team, 12);
    assert_eq!(runtime.policy.capacity.max_pending_per_key, 128);
    assert_eq!(runtime.policy.mission_schedule.tick_interval_ms, 1_500);
    assert_eq!(runtime.policy.mission_schedule.grace_ms, 120_000);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_provider_fallbacks_legacy_single_object_format() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");
    fs::write(
        home.join("config.yaml"),
        r#"{
              "providerFallbacks": {
                "primary": "claude-opus-4-6",
                "fallbacks": ["grok-3", "grok-3-mini"]
              }
            }"#,
    )
    .expect("write provider fallback settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let chain = loaded.fallbacks();
    assert!(!chain.is_empty());
    assert_eq!(chain, &["grok-3".to_string(), "grok-3-mini".to_string()]);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_provider_fallbacks_array_format() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");
    fs::write(
        home.join("config.yaml"),
        r#"{
              "providerFallbacks": [
                {
                  "primary": "deepseek-v4-pro",
                  "fallbacks": ["deepseek-v4-flash", "qwen3.6-plus", "step-3.5-flash"]
                },
                {
                  "primary": "claude-sonnet-4-6",
                  "fallbacks": ["claude-haiku-4-6"]
                }
              ]
            }"#,
    )
    .expect("write provider fallback settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let chain = loaded.fallbacks();
    assert!(!chain.is_empty());
    assert_eq!(chain.len(), 4);
    assert!(chain.contains(&"deepseek-v4-flash".to_string()));
    assert!(chain.contains(&"qwen3.6-plus".to_string()));
    assert!(chain.contains(&"step-3.5-flash".to_string()));
    assert!(chain.contains(&"claude-haiku-4-6".to_string()));
    assert!(!chain.contains(&"nonexistent".to_string()));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn provider_fallbacks_default_is_empty_when_unset() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(home.join("config.yaml"), "{}").expect("write empty settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let chain = loaded.fallbacks();
    assert!(chain.is_empty());
    assert_eq!(chain.len(), 0);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_provider_protocols_and_detects_when_unset() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"{
              "providers": {
                "openai": {
                  "base_url": "https://api.openai.com/v1",
                  "api_key": "sk-openai",
                  "models": ["gpt-5"],
                  "protocol": "responses"
                },
                "deepseek": {
                  "base_url": "https://api.deepseek.com/v1",
                  "api_key": "sk-deepseek",
                  "models": ["deepseek-v4-pro"],
                  "protocol": "completions"
                },
                "anthropic": {
                  "base_url": "https://api.anthropic.com",
                  "api_key": "sk-ant",
                  "models": ["claude-sonnet-4-6"]
                }
              }
            }"#,
    )
    .expect("write provider settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let providers = loaded.providers();
    assert_eq!(
        ProviderProtocol::effective_for_provider(providers.get("openai").unwrap()).unwrap(),
        ProviderProtocol::Responses
    );
    assert_eq!(
        ProviderProtocol::effective_for_provider(providers.get("deepseek").unwrap()).unwrap(),
        ProviderProtocol::Completions
    );
    assert_eq!(
        ProviderProtocol::effective_for_provider(providers.get("anthropic").unwrap()).unwrap(),
        ProviderProtocol::Anthropic
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn rejects_unknown_provider_protocol() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"{
              "providers": {
                "gemini": {
                  "base_url": "https://generativelanguage.googleapis.com",
                  "api_key": "sk-test",
                  "models": ["gemini-2.5-pro"],
                  "protocol": "gemini-native"
                }
              }
            }"#,
    )
    .expect("write provider settings");

    // when
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("config should reject unsupported protocol");

    // then
    assert!(error.to_string().contains("providers.gemini.protocol"));
    assert!(error.to_string().contains("responses"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_trusted_roots_from_settings() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"{"trustedRoots": ["/tmp/worktrees", "/home/user/projects"]}"#,
    )
    .expect("write settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let roots = loaded.trusted_roots();
    assert_eq!(roots, ["/tmp/worktrees", "/home/user/projects"]);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn trusted_roots_default_is_empty_when_unset() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(home.join("config.yaml"), "{}").expect("write empty settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    assert!(loaded.trusted_roots().is_empty());

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_typed_mcp_and_oauth_config() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
            home.join("config.yaml"),
            r#"{
              "mcpServers": {
                "stdio-server": {
                  "command": "uvx",
                  "args": ["mcp-server"],
                  "env": {"TOKEN": "secret"}
                },
                "remote-server": {
                  "type": "http",
                  "url": "https://example.test/mcp",
                  "headers": {"Authorization": "Bearer token"},
                  "headersHelper": "helper.sh",
                  "oauth": {
                    "clientId": "mcp-client",
                    "callbackPort": 7777,
                    "authServerMetadataUrl": "https://issuer.test/.well-known/oauth-authorization-server",
                    "xaa": true
                  }
                }
              },
              "oauth": {
                "clientId": "runtime-client",
                "authorizeUrl": "https://console.test/oauth/authorize",
                "tokenUrl": "https://console.test/oauth/token",
                "callbackPort": 54545,
                "manualRedirectUrl": "https://console.test/oauth/callback",
                "scopes": ["org:read", "user:write"]
              }
            }"#,
        )
        .expect("write user settings");
    fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{
              "mcpServers": {
                "remote-server": {
                  "type": "ws",
                  "url": "wss://override.test/mcp",
                  "headers": {"X-Env": "local"}
                }
              }
            }"#,
    )
    .expect("write local settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    let stdio_server = loaded
        .mcp()
        .get("stdio-server")
        .expect("stdio server should exist");
    assert_eq!(stdio_server.scope, ConfigSource::User);
    assert_eq!(stdio_server.transport(), McpTransport::Stdio);

    let remote_server = loaded
        .mcp()
        .get("remote-server")
        .expect("remote server should exist");
    assert_eq!(remote_server.scope, ConfigSource::Local);
    assert_eq!(remote_server.transport(), McpTransport::Ws);
    match &remote_server.config {
        McpServerConfig::Ws(config) => {
            assert_eq!(config.url, "wss://override.test/mcp");
            assert_eq!(
                config.headers.get("X-Env").map(String::as_str),
                Some("local")
            );
        }
        other => panic!("expected ws config, got {other:?}"),
    }

    let oauth = loaded.oauth().expect("oauth config should exist");
    assert_eq!(oauth.client_id, "runtime-client");
    assert_eq!(oauth.callback_port, Some(54_545));
    assert_eq!(oauth.scopes, vec!["org:read", "user:write"]);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn infers_http_mcp_servers_from_url_only_config() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"{
              "mcpServers": {
                "remote": {
                  "url": "https://example.test/mcp"
                }
              }
            }"#,
    )
    .expect("write mcp settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    let remote_server = loaded
        .mcp()
        .get("remote")
        .expect("remote server should exist");
    assert_eq!(remote_server.transport(), McpTransport::Http);
    match &remote_server.config {
        McpServerConfig::Http(config) => {
            assert_eq!(config.url, "https://example.test/mcp");
        }
        other => panic!("expected http config, got {other:?}"),
    }

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_plugin_config_from_enabled_plugins() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
        home.join("config.yaml"),
        r#"{
              "enabledPlugins": {
                "tool-guard@builtin": true,
                "sample-plugin@external": false
              }
            }"#,
    )
    .expect("write user settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(
        loaded.plugins().enabled_plugins().get("tool-guard@builtin"),
        Some(&true)
    );
    assert_eq!(
        loaded
            .plugins()
            .enabled_plugins()
            .get("sample-plugin@external"),
        Some(&false)
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_plugin_config() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
        home.join("config.yaml"),
        r#"{
              "enabledPlugins": {
                "core-helpers@builtin": true
              },
              "plugins": {
                "externalDirectories": ["./external-plugins"],
                "installRoot": "plugin-cache/installed",
                "registryPath": "plugin-cache/installed.json",
                "bundledRoot": "./bundled-plugins"
              }
            }"#,
    )
    .expect("write plugin settings");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(
        loaded
            .plugins()
            .enabled_plugins()
            .get("core-helpers@builtin"),
        Some(&true)
    );
    assert_eq!(
        loaded.plugins().external_directories(),
        &["./external-plugins".to_string()]
    );
    assert_eq!(
        loaded.plugins().install_root(),
        Some("plugin-cache/installed")
    );
    assert_eq!(
        loaded.plugins().registry_path(),
        Some("plugin-cache/installed.json")
    );
    assert_eq!(loaded.plugins().bundled_root(), Some("./bundled-plugins"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn rejects_invalid_mcp_server_shapes() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"{"mcpServers":{"broken":{"type":"http","url":123}}}"#,
    )
    .expect("write broken settings");

    // when
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("config should fail");

    // then
    assert!(error
        .to_string()
        .contains("mcpServers.broken: missing string field url"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn parses_user_defined_model_aliases_from_settings() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
            home.join("config.yaml"),
            r#"{"model":"smart","aliases":{"fast":"claude-haiku-4-5-20251213","smart":"claude-opus-4-6"}}"#,
        )
        .expect("write user settings");
    fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{"aliases":{"smart":"claude-sonnet-4-6","cheap":"grok-3-mini"}}"#,
    )
    .expect("write local settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    // then
    let aliases = loaded.aliases();
    assert_eq!(
        aliases.get("fast").map(String::as_str),
        Some("claude-haiku-4-5-20251213")
    );
    assert_eq!(
        aliases.get("smart").map(String::as_str),
        Some("claude-sonnet-4-6")
    );
    assert_eq!(
        aliases.get("cheap").map(String::as_str),
        Some("grok-3-mini")
    );
    assert_eq!(
        loaded.resolved_model().as_deref(),
        Some("claude-sonnet-4-6")
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn empty_settings_file_loads_defaults() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(home.join("config.yaml"), "").expect("write empty settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("empty settings should still load");

    // then
    assert_eq!(loaded.loaded_entries().len(), 1);
    assert_eq!(loaded.permission_mode(), None);
    assert_eq!(loaded.plugins().enabled_plugins().len(), 0);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn deep_merge_objects_merges_nested_maps() {
    // given
    let mut target = JsonValue::parse(r#"{"env":{"A":"1","B":"2"},"model":"haiku"}"#)
        .expect("target JSON should parse")
        .as_object()
        .expect("target should be an object")
        .clone();
    let source = JsonValue::parse(r#"{"env":{"B":"override","C":"3"},"sandbox":{"enabled":true}}"#)
        .expect("source JSON should parse")
        .as_object()
        .expect("source should be an object")
        .clone();

    // when
    deep_merge_objects(&mut target, &source);

    // then
    let env = target
        .get("env")
        .and_then(JsonValue::as_object)
        .expect("env should remain an object");
    assert_eq!(env.get("A"), Some(&JsonValue::String("1".to_string())));
    assert_eq!(
        env.get("B"),
        Some(&JsonValue::String("override".to_string()))
    );
    assert_eq!(env.get("C"), Some(&JsonValue::String("3".to_string())));
    assert!(target.contains_key("sandbox"));
}

#[test]
fn rejects_invalid_hook_entries_before_merge() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let project_settings = cwd.join(".cowd").join("config.yaml");
    fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
    fs::create_dir_all(&home).expect("home config dir");

    fs::write(
        home.join("config.yaml"),
        r#"{"hooks":{"PreToolUse":["base"]}}"#,
    )
    .expect("write user settings");
    fs::write(
        &project_settings,
        r#"{"hooks":{"PreToolUse":["project",42]}}"#,
    )
    .expect("write invalid project settings");

    // when
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("config should fail");

    // then — config validation now catches the mixed array before the hooks parser
    let rendered = error.to_string();
    assert!(
        rendered.contains("hooks.PreToolUse") && rendered.contains("must be an array of strings"),
        "expected validation error for hooks.PreToolUse, got: {rendered}"
    );
    assert!(!rendered.contains("merged settings.hooks"));

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn permission_mode_contract_accepts_only_terminal_values() {
    // given / when / then
    assert_eq!(
        parse_permission_mode_label("read-only", "test").expect("read-only should resolve"),
        ResolvedPermissionMode::ReadOnly
    );
    assert_eq!(
        parse_permission_mode_label("workspace-write", "test")
            .expect("workspace-write should resolve"),
        ResolvedPermissionMode::WorkspaceWrite
    );
    assert_eq!(
        parse_permission_mode_label("danger-full-access", "test")
            .expect("danger-full-access should resolve"),
        ResolvedPermissionMode::DangerFullAccess
    );
    assert!(parse_permission_mode_label("plan", "test").is_err());
    assert!(parse_permission_mode_label("acceptEdits", "test").is_err());
    assert!(parse_permission_mode_label("dontAsk", "test").is_err());
}

#[test]
fn hook_config_merge_preserves_uniques() {
    // given
    let base = RuntimeHookConfig::new(
        vec!["pre-a".to_string()],
        vec!["post-a".to_string()],
        vec!["failure-a".to_string()],
    );
    let overlay = RuntimeHookConfig::new(
        vec!["pre-a".to_string(), "pre-b".to_string()],
        vec!["post-a".to_string(), "post-b".to_string()],
        vec!["failure-b".to_string()],
    );

    // when
    let merged = base.merged(&overlay);

    // then
    assert_eq!(
        merged.pre_tool_use(),
        &["pre-a".to_string(), "pre-b".to_string()]
    );
    assert_eq!(
        merged.post_tool_use(),
        &["post-a".to_string(), "post-b".to_string()]
    );
    assert_eq!(
        merged.post_tool_use_failure(),
        &["failure-a".to_string(), "failure-b".to_string()]
    );
}

#[test]
fn plugin_state_falls_back_to_default_for_unknown_plugin() {
    // given
    let mut config = RuntimePluginConfig::default();
    config.enabled_plugins.insert("known".to_string(), true);

    // when / then
    assert!(config.state_for("known", false));
    assert!(config.state_for("missing", true));
    assert!(!config.state_for("missing", false));
}

#[test]
fn validates_unknown_top_level_keys_with_line_and_field_name() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let user_settings = home.join("config.yaml");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        &user_settings,
        "{\n  \"model\": \"opus\",\n  \"telemetry\": true\n}\n",
    )
    .expect("write user settings");

    // when
    let _config = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn validates_deprecated_top_level_keys_with_replacement_guidance() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let user_settings = home.join("config.yaml");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        &user_settings,
        "{\n  \"model\": \"opus\",\n  \"allowedTools\": [\"Read\"]\n}\n",
    )
    .expect("write user settings");

    // when
    let _config = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn validates_wrong_type_for_known_field_with_field_path() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let user_settings = home.join("config.yaml");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        &user_settings,
        "{\n  \"hooks\": {\n    \"PreToolUse\": \"not-an-array\"\n  }\n}\n",
    )
    .expect("write user settings");

    // when
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("config should fail");

    // then
    let rendered = error.to_string();
    assert!(
        rendered.contains(&user_settings.display().to_string()),
        "error should include file path, got: {rendered}"
    );
    assert!(
        rendered.contains("hooks"),
        "error should include field path component 'hooks', got: {rendered}"
    );
    assert!(
        rendered.contains("PreToolUse"),
        "error should describe the type mismatch, got: {rendered}"
    );
    assert!(
        rendered.contains("array"),
        "error should describe the expected type, got: {rendered}"
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn unknown_top_level_key_suggests_closest_match() {
    // given
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    let user_settings = home.join("config.yaml");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(&user_settings, "{\n  \"modle\": \"opus\"\n}\n").expect("write user settings");

    // when
    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load with warning");

    // then — config loads successfully; unknown key produces a stderr warning
    assert!(
        loaded.get("modle").is_some(),
        "unknown key should be present in merged config"
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn app_runtime_config_is_strict_and_resolves_terminal_defaults() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
apps:
  directories:
    - /opt/cowd/apps
  trust_store: /etc/cowd/app-trust.json
  launcher:
    path: /opt/cowd/bin/managed-worker-launcher
    sha256: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  runtime_root: /run/cowd/apps
  data_root: /var/lib/cowd/apps
  core_bridge_socket: /run/cowd/core-bridge.sock
  postgres_socket_dirs:
    - /run/postgresql
  cgroup_root: /sys/fs/cgroup/cowd
  resources:
    nofile: 512
  supervisor:
    max_active_workers: 8
    max_starting_workers: 2
    idle_ttl_seconds: null
  entries:
    mfg:
      enabled: false
      required: true
      activation: resident
      config_file: /etc/cowd/apps/mfg.yaml
    future_app: {}
"#,
    )
    .expect("write app config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert!(!loaded.apps().is_enabled("mfg"));
    assert!(loaded.apps().is_enabled("future_app"));
    assert!(loaded.apps().is_enabled("unconfigured_app"));
    assert_eq!(
        loaded.apps().directories(),
        &[PathBuf::from("/opt/cowd/apps")]
    );
    assert_eq!(loaded.apps().supervisor().max_active_workers, 8);
    assert_eq!(loaded.apps().supervisor().max_starting_workers, 2);
    assert_eq!(loaded.apps().supervisor().idle_ttl_seconds, None);
    assert_eq!(
        loaded.apps().trust_store(),
        Some(Path::new("/etc/cowd/app-trust.json"))
    );
    assert_eq!(
        loaded.apps().launcher().expect("launcher").path,
        PathBuf::from("/opt/cowd/bin/managed-worker-launcher")
    );
    assert_eq!(loaded.apps().runtime_root(), Path::new("/run/cowd/apps"));
    assert_eq!(loaded.apps().data_root(), Path::new("/var/lib/cowd/apps"));
    assert_eq!(
        loaded.apps().core_bridge_socket(),
        Path::new("/run/cowd/core-bridge.sock")
    );
    assert_eq!(
        loaded.apps().postgres_socket_dirs(),
        &[PathBuf::from("/run/postgresql")]
    );
    assert_eq!(
        loaded.apps().cgroup_root(),
        Some(Path::new("/sys/fs/cgroup/cowd"))
    );
    assert_eq!(loaded.apps().resources().nofile, 512);
    assert_eq!(loaded.apps().resources().nproc, 4096);
    assert_eq!(
        loaded.apps().resources().address_space_bytes,
        512 * 1024 * 1024
    );
    assert_eq!(loaded.apps().resources().cgroup_pids, 64);
    let mfg = loaded.apps().entry("mfg");
    assert!(mfg.required);
    assert_eq!(mfg.activation, AppActivationPolicyV1::Resident);
    assert_eq!(
        mfg.config_file,
        Some(PathBuf::from("/etc/cowd/apps/mfg.yaml"))
    );
    assert_eq!(
        loaded.apps().configured_app_ids().collect::<Vec<_>>(),
        vec!["future_app", "mfg"]
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn shipped_default_config_uses_the_strict_zero_app_shape() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        include_str!("../../../../../config-default.yaml"),
    )
    .expect("write shipped default config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("shipped default config should load");

    assert!(loaded.apps().configured_app_ids().next().is_none());
    assert_eq!(
        loaded.apps().directories(),
        AppsConfig::default().directories()
    );
    assert!(loaded.apps().trust_store().is_none());
    assert!(loaded.apps().launcher().is_none());
    assert_eq!(
        loaded.apps().entry("unconfigured-app").activation,
        AppActivationPolicyV1::Lazy
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn legacy_or_unbounded_app_configuration_is_rejected() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        "apps:\n  mfg:\n    enabled: true\n",
    )
    .expect("write app config");
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("legacy config must fail closed");
    assert!(error.to_string().contains("unsupported field mfg"));
    fs::write(
        home.join("config.yaml"),
        "apps:\n  supervisor:\n    max_active_workers: 1\n    max_starting_workers: 2\n",
    )
    .expect("write app config");
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("invalid capacity must fail closed");
    assert!(error.to_string().contains("max_starting_workers"));
    fs::write(
        home.join("config.yaml"),
        "apps:\n  postgres_socket_dirs:\n    - ''\n",
    )
    .expect("write app config");
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("empty PostgreSQL socket root must fail closed");
    assert!(error.to_string().contains("postgres_socket_dirs"));
    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn memory_vector_accepts_embedding_model_alias() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
memory:
  enabled: true
  vector:
    enabled: true
    embedding_model: text-embedding-v4
    api_url: https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings
    api_key: test-key
    dimension: 0
    timeout_secs: 30
    batch_size: 32
"#,
    )
    .expect("write memory config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert!(loaded.memory().vector.enabled);
    assert_eq!(loaded.memory().vector.model, "text-embedding-v4");
    assert_eq!(
        loaded.memory().vector.api_url,
        "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"
    );
    assert_eq!(loaded.memory().vector.api_key, "test-key");
    assert_eq!(loaded.memory().vector.dimension, 0);
    assert_eq!(loaded.memory().vector.batch_size, 32);

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn memory_extraction_accepts_snake_case_auto_extract() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
memory:
  enabled: true
  extraction:
    auto_extract: false
"#,
    )
    .expect("write memory config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert!(!loaded.memory().extraction.auto_extract);
    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn memory_governance_is_configurable_and_rejects_invalid_schedule() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
memory:
  enabled: true
  governance:
    enabled: true
    startup_delay_secs: 5
    deep_scan_hour_local: 2
    max_candidates: 96
    stale_threshold_bp: 9900
    low_confidence_threshold_bp: 4000
"#,
    )
    .expect("write memory governance config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");
    assert!(loaded.memory().governance.enabled);
    assert_eq!(loaded.memory().governance.startup_delay_secs, 5);
    assert_eq!(loaded.memory().governance.deep_scan_hour_local, 2);
    assert_eq!(loaded.memory().governance.max_candidates, 96);
    assert_eq!(loaded.memory().governance.stale_threshold_bp, 9_900);
    assert_eq!(
        loaded.memory().governance.low_confidence_threshold_bp,
        4_000
    );

    fs::write(
        home.join("config.yaml"),
        "memory:\n  governance:\n    deep_scan_hour_local: 24\n",
    )
    .expect("write invalid memory governance config");
    assert!(ConfigLoader::new(&cwd, &home).load().is_err());
    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn redacted_json_removes_nested_credential_and_transport_values() {
    let mut merged = BTreeMap::new();
    merged.insert(
        "providers".to_string(),
        JsonValue::parse(
            r#"{
                    "apiKey":"provider-secret",
                    "headers":{"Authorization":"Bearer secret"},
                    "env":{"TOKEN":"environment-secret"},
                    "nested":{"password":"password-secret","safe":"visible"}
                }"#,
        )
        .expect("fixture parses"),
    );
    let config = RuntimeConfig {
        merged,
        loaded_entries: Vec::new(),
        feature_config: RuntimeFeatureConfig::default(),
    };

    let rendered = config.redacted_json().render();
    assert!(!rendered.contains("provider-secret"));
    assert!(!rendered.contains("environment-secret"));
    assert!(!rendered.contains("password-secret"));
    assert!(rendered.contains("visible"));
    assert_eq!(
        redact_serde_json(serde_json::json!({"authorization":"secret","safe":"ok"})),
        serde_json::json!({"authorization":"[redacted]","safe":"ok"})
    );
}

#[test]
fn gateway_webui_dir_reads_configured_static_asset_dir() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
gateway:
  enabled: true
  webui_dir: "/tmp/cowd-edge-webui-dist"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#,
    )
    .expect("write gateway config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert!(loaded.gateway().enabled);
    assert_eq!(
        loaded.gateway().webui_dir.as_deref(),
        Some(std::path::Path::new("/tmp/cowd-edge-webui-dist"))
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn compiled_artifact_defaults_share_the_single_install_root() {
    let install_root = crate::cowd_dirs::install_root_dir();
    let expected_webui = install_root.join("webui/dist");

    assert_eq!(
        AppsConfig::default().directories(),
        &[install_root.join("apps")]
    );
    assert_eq!(
        GatewayConfig::default().webui_dir.as_deref(),
        Some(expected_webui.as_path())
    );
}

#[test]
fn gateway_platform_accepts_snake_case_platform_type() {
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"
gateway:
  enabled: true
  platforms:
    - platform_type: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#,
    )
    .expect("write gateway config");

    let loaded = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    let platform = loaded
        .gateway()
        .platforms
        .first()
        .expect("platform should be parsed");
    assert_eq!(platform.platform_type, "api_server");
    assert!(!platform.extra.contains_key("platform_type"));
    assert_eq!(
        platform.extra.get("host").and_then(JsonValue::as_str),
        Some("127.0.0.1")
    );

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn provider_max_output_tokens_reads_from_environment_variable() {
    // given — set environment variable
    let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("4096"));

    // when
    let config = crate::ProviderResourceConfig::default();

    // then
    assert_eq!(config.max_output_tokens_override(), Some(4096));
}

#[test]
fn provider_max_output_tokens_falls_back_to_none_when_env_var_is_unset() {
    // given — ensure env var is unset
    let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", None);

    // when
    let config = crate::ProviderResourceConfig::default();

    // then
    assert_eq!(config.max_output_tokens_override(), None);
}

#[test]
fn provider_max_output_tokens_falls_back_to_none_when_env_var_is_invalid() {
    // given — set invalid environment variable
    let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("not-a-number"));

    // when
    let config = crate::ProviderResourceConfig::default();

    // then — should fall back to None (not panic)
    assert_eq!(config.max_output_tokens_override(), None);
}

#[test]
fn compression_session_defaults_to_semantic_checkpoint_controls() {
    let session = SessionCompactConfig::default();

    assert_eq!(session.preserve_recent, 6);
    assert_eq!(session.summary_max_tokens, 2000);
}

#[test]
fn compression_rejects_removed_ratio_thresholds() {
    let root = JsonValue::parse(
        r#"{
                "compression": {
                    "micro": {
                        "time_decay_factor": 1
                    },
                    "session": {
                        "threshold_ratio_bp": 6500,
                        "preserve_recent": 12
                    },
                    "deep": {
                        "iterative_update": false
                    },
                    "circuit_breaker": {
                        "max_retries": 5,
                        "cooldown_secs": 60
                    }
                }
            }"#,
    )
    .expect("json should parse");

    let error = parse_optional_compression_config(&root)
        .expect_err("removed request-ratio threshold must be rejected");
    assert!(error.to_string().contains("threshold_ratio_bp was removed"));
}

#[test]
fn parses_context_budget_separately_from_compression() {
    let root = JsonValue::parse(
        r#"{
                "context_budget": {
                    "subsystem_budget_ratio_bp": 6400
                }
            }"#,
    )
    .expect("json should parse");

    let budget =
        parse_optional_context_budget_config(&root).expect("context budget config should parse");

    assert_eq!(budget.subsystem_budget_ratio_bp, 6400);
}

#[test]
fn parses_session_history_chunk_and_request_cache_limits() {
    let root = JsonValue::parse(
            r#"{"session_history":{"chunk_messages":64,"chunk_bytes":131072,"request_cache_entries":8}}"#,
        )
        .expect("json should parse");
    let history =
        parse_optional_session_history_config(&root).expect("history config should parse");
    assert_eq!(history.chunk_messages, 64);
    assert_eq!(history.chunk_bytes, 131_072);
    assert_eq!(history.request_cache_entries, 8);

    let invalid = JsonValue::parse(r#"{"session_history":{"request_cache_entries":0}}"#).unwrap();
    assert!(parse_optional_session_history_config(&invalid).is_err());
}

#[test]
fn parses_gateway_recovery_working_set_and_rejects_invalid_budgets() {
    let root = JsonValue::parse(
        r#"{"gateway":{"recovery":{
                "hot_bytes":1048576,
                "attached_bytes":262144,
                "recent_bytes":524288,
                "recent_window_ms":90000,
                "manifest_page_size":64,
                "hydrate_concurrency":4,
                "activation_tail_messages":256,
                "activation_metadata_messages":1024,
                "context_card_cache_entries":128,
                "context_index_card_span":64,
                "context_index_parent_span":8,
                "stable_snapshot_attempts":8
            }}}"#,
    )
    .unwrap();
    let gateway = parse_optional_gateway_config(&root).unwrap();
    assert_eq!(gateway.recovery.hot_bytes, 1_048_576);
    assert_eq!(gateway.recovery.recent_window_ms, 90_000);
    assert_eq!(gateway.recovery.activation_tail_messages, 256);

    let invalid =
        JsonValue::parse(r#"{"gateway":{"recovery":{"hot_bytes":1024,"recent_bytes":2048}}}"#)
            .unwrap();
    assert!(parse_optional_gateway_config(&invalid).is_err());
    let invalid_parent =
        JsonValue::parse(r#"{"gateway":{"recovery":{"context_index_parent_span":1}}}"#).unwrap();
    assert!(parse_optional_gateway_config(&invalid_parent).is_err());
}

#[test]
fn parses_gateway_live_limits_and_rejects_unsafe_boundaries() {
    let root = JsonValue::parse(
        r#"{"gateway":{"live":{
                "max_sources":48,
                "max_subscriptions_per_principal_instance":3,
                "queue_capacity":768,
                "checkpoint_max_bytes":8192,
                "default_ttl_seconds":1800,
                "max_ttl_seconds":7200,
                "baseline_timeout_ms":9000
            }}}"#,
    )
    .unwrap();
    let live = parse_optional_gateway_config(&root).unwrap().live;
    assert_eq!(live.max_sources, 48);
    assert_eq!(live.queue_capacity, 768);
    assert_eq!(live.checkpoint_max_bytes, 8_192);
    assert_eq!(live.default_ttl_seconds, 1_800);
    assert_eq!(live.max_ttl_seconds, 7_200);
    assert_eq!(live.baseline_timeout_ms, 9_000);

    let invalid_header =
        JsonValue::parse(r#"{"gateway":{"live":{"checkpoint_max_bytes":512}}}"#).unwrap();
    assert!(parse_optional_gateway_config(&invalid_header).is_err());
    let invalid_ttl = JsonValue::parse(
        r#"{"gateway":{"live":{"default_ttl_seconds":7200,"max_ttl_seconds":3600}}}"#,
    )
    .unwrap();
    assert!(parse_optional_gateway_config(&invalid_ttl).is_err());
}

#[test]
fn parses_gateway_presence_independently_from_live_subscription_ttl() {
    let root = JsonValue::parse(
        r#"{"gateway":{
                "presence":{"ttl_seconds":900},
                "live":{"default_ttl_seconds":1800}
            }}"#,
    )
    .unwrap();
    let gateway = parse_optional_gateway_config(&root).unwrap();
    assert_eq!(gateway.presence.ttl_seconds, 900);
    assert_eq!(gateway.live.default_ttl_seconds, 1_800);

    let invalid = JsonValue::parse(r#"{"gateway":{"presence":{"ttl_seconds":0}}}"#).unwrap();
    assert!(parse_optional_gateway_config(&invalid).is_err());
}

#[test]
fn parses_gateway_translation_policy_and_bounds_cache() {
    let root =
        JsonValue::parse(r#"{"gateway":{"translation":{"model":"fast","cache_entries":512}}}"#)
            .unwrap();
    let translation = parse_optional_gateway_config(&root).unwrap().translation;
    assert_eq!(translation.model.as_deref(), Some("fast"));
    assert_eq!(translation.cache_entries, 512);

    let invalid =
        JsonValue::parse(r#"{"gateway":{"translation":{"cache_entries":4097}}}"#).unwrap();
    assert!(parse_optional_gateway_config(&invalid).is_err());
}

#[test]
fn parses_model_context_window_override_and_rejects_invalid_small_value() {
    let root = JsonValue::parse(
        r#"{
                "model_context_windows": {
                    "private-model": 32768
                }
            }"#,
    )
    .expect("json should parse");
    let windows =
        parse_optional_model_context_windows(&root).expect("context window override should parse");
    assert_eq!(windows["private-model"], 32_768);

    let invalid = JsonValue::parse(r#"{"model_context_windows":{"broken":1023}}"#)
        .expect("json should parse");
    assert!(parse_optional_model_context_windows(&invalid)
        .expect_err("sub-1024 context window must fail validation")
        .to_string()
        .contains("at least 1024"));
}

#[test]
fn routing_mode_is_pinned_by_default_and_rejects_unknown_values() {
    assert_eq!(
        parse_routing_mode(&JsonValue::parse("{}").unwrap()).unwrap(),
        RoutingMode::Pinned
    );
    assert_eq!(
        parse_routing_mode(&JsonValue::parse(r#"{"routing_mode":"auto"}"#).unwrap()).unwrap(),
        RoutingMode::Auto
    );
    assert!(
        parse_routing_mode(&JsonValue::parse(r#"{"routing_mode":"adaptive"}"#).unwrap())
            .expect_err("unknown routing mode must fail closed")
            .to_string()
            .contains("unsupported routing_mode")
    );
}

#[test]
fn storage_topology_defaults_to_sqlite_and_postgres_is_strict() {
    let defaults = parse_optional_storage_config(&JsonValue::parse("{}").unwrap()).unwrap();
    assert_eq!(defaults.backend, StorageBackendSelection::Auto);
    assert_eq!(defaults.preferred, StorageBackendSelection::Postgres);
    assert_eq!(defaults.fallback, StorageBackendSelection::Sqlite);
    assert!(defaults.postgres.is_none());
    assert!(defaults.session_execution.workers > 0);
    assert!(defaults.session_execution.queue_capacity > 0);
    assert_eq!(defaults.artifacts, crate::ArtifactStorageConfig::default());

    let artifact_override = JsonValue::parse(
            r#"{"storage":{"artifacts":{"compactThresholdBytes":1024,"maxObjectBytes":2048,"totalQuotaBytes":8192,"gcHighWaterBytes":6144,"gcLowWaterBytes":4096,"orphanGraceMs":250}}}"#,
        )
        .unwrap();
    let selected = parse_optional_storage_config(&artifact_override).unwrap();
    assert_eq!(selected.artifacts.compact_threshold_bytes, 1_024);
    assert_eq!(selected.artifacts.max_object_bytes, 2_048);
    assert_eq!(selected.artifacts.total_quota_bytes, 8_192);
    assert_eq!(selected.artifacts.gc_high_water_bytes, 6_144);
    assert_eq!(selected.artifacts.gc_low_water_bytes, 4_096);
    assert_eq!(selected.artifacts.orphan_grace_ms, 250);

    let postgres = JsonValue::parse(
            r#"{"storage":{"backend":"postgres","sessionExecution":{"workers":6,"queueCapacity":72},"postgres":{"logicalIdentity":"cowd-test","secretRef":"env:COWD_TEST_POSTGRES_URL","maxConnections":24,"serverReserve":6,"critical":{"maxConnections":8,"minIdleConnections":2,"checkoutTimeoutMs":250},"onlineRead":{"maxConnections":12,"minIdleConnections":3,"checkoutTimeoutMs":500},"background":{"maxConnections":4,"minIdleConnections":1,"checkoutTimeoutMs":2000}}}}"#,
        )
        .unwrap();
    let selected = parse_optional_storage_config(&postgres).unwrap();
    assert_eq!(selected.backend, StorageBackendSelection::Postgres);
    assert_eq!(selected.session_execution.workers, 6);
    assert_eq!(selected.session_execution.queue_capacity, 72);
    let postgres = selected.postgres.unwrap();
    assert_eq!(postgres.secret_ref, "env:COWD_TEST_POSTGRES_URL");
    assert_eq!(postgres.max_connections, 24);
    assert_eq!(postgres.server_reserve, 6);
    assert_eq!(postgres.critical.max_connections, Some(8));
    assert_eq!(postgres.online_read.max_connections, Some(12));
    assert_eq!(postgres.background.max_connections, Some(4));

    let missing = JsonValue::parse(r#"{"storage":{"backend":"postgres"}}"#).unwrap();
    assert!(parse_optional_storage_config(&missing).is_err());
    let invalid = JsonValue::parse(
            r#"{"storage":{"backend":"postgres","postgres":{"logicalIdentity":"cowd","secretRef":"env:X","maxConnections":0}}}"#,
        )
        .unwrap();
    assert!(parse_optional_storage_config(&invalid).is_err());
    let invalid_execution =
        JsonValue::parse(r#"{"storage":{"sessionExecution":{"workers":0,"queueCapacity":10}}}"#)
            .unwrap();
    assert!(parse_optional_storage_config(&invalid_execution).is_err());
    let invalid_artifacts = JsonValue::parse(
        r#"{"storage":{"artifacts":{"compactThresholdBytes":4096,"maxObjectBytes":1024}}}"#,
    )
    .unwrap();
    assert!(parse_optional_storage_config(&invalid_artifacts).is_err());
}

#[test]
fn auto_storage_backend_parses_with_postgres_preference() {
    let root = JsonValue::parse(
            r#"{"storage":{"backend":"auto","preferred":"postgres","fallback":"sqlite","fallbackProbeTimeoutMs":5000}}"#,
        )
        .unwrap();
    let selected = parse_optional_storage_config(&root).expect("auto storage config");
    assert_eq!(selected.backend, StorageBackendSelection::Auto);
    assert_eq!(selected.preferred, StorageBackendSelection::Postgres);
    assert_eq!(selected.fallback, StorageBackendSelection::Sqlite);
    assert_eq!(selected.fallback_probe_timeout_ms, 5_000);

    let invalid =
        JsonValue::parse(r#"{"storage":{"backend":"auto","preferred":"sqlite"}}"#).unwrap();
    assert!(parse_optional_storage_config(&invalid).is_err());
}

#[test]
fn parses_hot_state_budget_and_rejects_inverted_watermarks() {
    let root = JsonValue::parse(
            r#"{"runtime":{"hot_state":{"memory":{"ratio":"0.70","max_bytes":"512MiB","reserve_ratio":"0.20","high_watermark":"0.90","low_watermark":"0.75"},"shards":8,"materializer_queue_capacity":64}}}"#,
        )
        .unwrap();
    let config = parse_optional_hot_state_config(&root).unwrap();
    assert_eq!(config.memory.ratio, 0.70);
    assert_eq!(config.memory.max_bytes, Some(512 * 1024 * 1024));
    assert_eq!(config.shards, 8);

    let invalid = JsonValue::parse(
        r#"{"runtime":{"hot_state":{"memory":{"low_watermark":"0.95","high_watermark":"0.90"}}}}"#,
    )
    .unwrap();
    assert!(parse_optional_hot_state_config(&invalid).is_err());
}

#[test]
fn network_domain_env_invalid_mode_rejects_startup_fail_closed() {
    let _guard = ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // This test must not mutate process env: ConfigLoader tests run in
    // parallel and an invalid COWD_NETWORK_DOMAIN_MODE would fail them.
    // Startup rejection is covered by the config-file path and by a
    // direct check of the merged-map path below.
    let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", None);
    let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
    let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");

    fs::write(
        home.join("config.yaml"),
        "network:\n  domain:\n    mode: denny\n",
    )
    .expect("write invalid config");
    let error = ConfigLoader::new(&cwd, &home)
        .load()
        .expect_err("invalid network mode in config must reject startup");

    assert!(error.to_string().contains("network.domain.mode"));

    let merged =
        JsonValue::parse(r#"{"network":{"domain":{"mode":"denny"}}}"#).expect("merged map");
    let direct = super::inject_network_domain_env(merged.as_object().expect("object"))
        .expect_err("merged env override must also fail closed");
    assert!(direct.to_string().contains("network.domain.mode"));
    drop(mode);
    drop(allow);
    drop(block);
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn network_domain_config_is_injected_when_env_is_absent() {
    let _guard = ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", None);
    let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
    let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        r#"network:
  domain:
    mode: deny
    allow:
      - docs.rs
    block:
      - evil.example
"#,
    )
    .expect("write config");

    let _config = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config with network domain should load");

    assert_eq!(
        std::env::var("COWD_NETWORK_DOMAIN_MODE").expect("mode injected"),
        "deny"
    );
    assert_eq!(
        std::env::var("COWD_NETWORK_DOMAIN_ALLOW").expect("allow injected"),
        "docs.rs"
    );
    assert_eq!(
        std::env::var("COWD_NETWORK_DOMAIN_BLOCK").expect("block injected"),
        "evil.example"
    );
    drop(mode);
    drop(allow);
    drop(block);
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn network_domain_env_wins_over_config_file() {
    let _guard = ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", Some("ask"));
    let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
    let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
    let root = temp_dir();
    let cwd = root.join("project");
    let home = root.join("home").join(".cowd");
    fs::create_dir_all(&home).expect("home config dir");
    fs::create_dir_all(&cwd).expect("project dir");
    fs::write(
        home.join("config.yaml"),
        "network:\n  domain:\n    mode: deny\n",
    )
    .expect("write config");

    let _config = ConfigLoader::new(&cwd, &home)
        .load()
        .expect("config should load");

    assert_eq!(
        std::env::var("COWD_NETWORK_DOMAIN_MODE").expect("env preserved"),
        "ask"
    );
    drop(mode);
    drop(allow);
    drop(block);
    let _ = fs::remove_dir_all(&root);
}
