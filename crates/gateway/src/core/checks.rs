use std::env;
use std::io::IsTerminal;
use std::path::Path;

use serde_json::{json, Map, Value};

use model_protocol::oauth::load_oauth_credentials;
use runtime::ConfigLoader;

use crate::{
    doctor::{DiagnosticCheck, DiagnosticLevel},
    StatusContext, BUILD_TARGET, DEPRECATED_INSTALL_COMMAND, GIT_SHA, OFFICIAL_REPO_SLUG,
    OFFICIAL_REPO_URL, VERSION,
};
#[allow(clippy::too_many_lines)]
pub(crate) fn check_auth_health() -> DiagnosticCheck {
    let api_key_present = env::var("ANTHROPIC_API_KEY")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    let auth_token_present = env::var("ANTHROPIC_AUTH_TOKEN")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    let env_details = format!(
        "Environment       api_key={} auth_token={}",
        if api_key_present { "present" } else { "absent" },
        if auth_token_present {
            "present"
        } else {
            "absent"
        }
    );

    match load_oauth_credentials() {
        Ok(Some(token_set)) => DiagnosticCheck::new(
            "Auth",
            if api_key_present || auth_token_present {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warn
            },
            if api_key_present || auth_token_present {
                "supported auth env vars are configured; legacy saved OAuth is ignored"
            } else {
                "legacy saved OAuth credentials are present but unsupported"
            },
        )
        .with_details(vec![
            env_details,
            format!(
                "Legacy OAuth      expires_at={} refresh_token={} scopes={}",
                token_set
                    .expires_at
                    .map_or_else(|| "<none>".to_string(), |value| value.to_string()),
                if token_set.refresh_token.is_some() {
                    "present"
                } else {
                    "absent"
                },
                if token_set.scopes.is_empty() {
                    "<none>".to_string()
                } else {
                    token_set.scopes.join(",")
                }
            ),
            "Suggested action  set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN; legacy login is removed"
                .to_string(),
        ])
        .with_data(Map::from_iter([
            ("api_key_present".to_string(), json!(api_key_present)),
            ("auth_token_present".to_string(), json!(auth_token_present)),
            ("legacy_saved_oauth_present".to_string(), json!(true)),
            (
                "legacy_saved_oauth_expires_at".to_string(),
                json!(token_set.expires_at),
            ),
            (
                "legacy_refresh_token_present".to_string(),
                json!(token_set.refresh_token.is_some()),
            ),
            ("legacy_scopes".to_string(), json!(token_set.scopes)),
        ])),
        Ok(None) => DiagnosticCheck::new(
            "Auth",
            if api_key_present || auth_token_present {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warn
            },
            if api_key_present || auth_token_present {
                "supported auth env vars are configured"
            } else {
                "no supported auth env vars were found"
            },
        )
        .with_details(vec![env_details])
        .with_data(Map::from_iter([
            ("api_key_present".to_string(), json!(api_key_present)),
            ("auth_token_present".to_string(), json!(auth_token_present)),
            ("legacy_saved_oauth_present".to_string(), json!(false)),
            ("legacy_saved_oauth_expires_at".to_string(), Value::Null),
            ("legacy_refresh_token_present".to_string(), json!(false)),
            ("legacy_scopes".to_string(), json!(Vec::<String>::new())),
        ])),
        Err(error) => DiagnosticCheck::new(
            "Auth",
            DiagnosticLevel::Fail,
            format!("failed to inspect legacy saved credentials: {error}"),
        )
        .with_data(Map::from_iter([
            ("api_key_present".to_string(), json!(api_key_present)),
            ("auth_token_present".to_string(), json!(auth_token_present)),
            ("legacy_saved_oauth_present".to_string(), Value::Null),
            ("legacy_saved_oauth_expires_at".to_string(), Value::Null),
            ("legacy_refresh_token_present".to_string(), Value::Null),
            ("legacy_scopes".to_string(), Value::Null),
            ("legacy_saved_oauth_error".to_string(), json!(error.to_string())),
        ])),
    }
}

pub(crate) fn check_config_health(
    config_loader: &ConfigLoader,
    config: Result<&runtime::RuntimeConfig, &runtime::ConfigError>,
) -> DiagnosticCheck {
    let discovered = config_loader.discover();
    let discovered_count = discovered.len();
    // Separate candidate paths that actually exist from those that don't.
    // Showing non-existent paths as "Discovered file" implies they loaded
    // but something went wrong, which is confusing. We only surface paths
    // that exist on disk as discovered; non-existent ones are silently
    // omitted from the display (they are just the standard search locations).
    let present_paths: Vec<String> = discovered
        .iter()
        .filter(|e| e.path.exists())
        .map(|e| e.path.display().to_string())
        .collect();
    let discovered_paths = discovered
        .iter()
        .map(|entry| entry.path.display().to_string())
        .collect::<Vec<_>>();
    match config {
        Ok(runtime_config) => {
            let loaded_entries = runtime_config.loaded_entries();
            let loaded_count = loaded_entries.len();
            let present_count = present_paths.len();
            let mut details = vec![format!(
                "Config files      loaded {}/{}",
                loaded_count, present_count
            )];
            if let Some(model) = runtime_config.model() {
                details.push(format!("Resolved model    {model}"));
            }
            details.push(format!(
                "MCP servers       {}",
                runtime_config.mcp().servers().len()
            ));
            if present_paths.is_empty() {
                details.push("Discovered files  <none> (defaults active)".to_string());
            } else {
                details.extend(
                    present_paths
                        .iter()
                        .map(|path| format!("Discovered file   {path}")),
                );
            }
            DiagnosticCheck::new(
                "Config",
                DiagnosticLevel::Ok,
                if present_count == 0 {
                    "no config files present; defaults are active"
                } else {
                    "runtime config loaded successfully"
                },
            )
            .with_details(details)
            .with_data(Map::from_iter([
                ("discovered_files".to_string(), json!(present_paths)),
                ("discovered_files_count".to_string(), json!(present_count)),
                ("loaded_config_files".to_string(), json!(loaded_count)),
                ("resolved_model".to_string(), json!(runtime_config.model())),
                (
                    "mcp_servers".to_string(),
                    json!(runtime_config.mcp().servers().len()),
                ),
            ]))
        }
        Err(error) => DiagnosticCheck::new(
            "Config",
            DiagnosticLevel::Fail,
            format!("runtime config failed to load: {error}"),
        )
        .with_details(if discovered_paths.is_empty() {
            vec!["Discovered files  <none>".to_string()]
        } else {
            discovered_paths
                .iter()
                .map(|path| format!("Discovered file   {path}"))
                .collect()
        })
        .with_data(Map::from_iter([
            ("discovered_files".to_string(), json!(discovered_paths)),
            (
                "discovered_files_count".to_string(),
                json!(discovered_count),
            ),
            ("loaded_config_files".to_string(), json!(0)),
            ("resolved_model".to_string(), Value::Null),
            ("mcp_servers".to_string(), Value::Null),
            ("load_error".to_string(), json!(error.to_string())),
        ])),
    }
}

pub(crate) fn check_install_source_health() -> DiagnosticCheck {
    DiagnosticCheck::new(
        "Install source",
        DiagnosticLevel::Ok,
        format!(
            "official source of truth is {OFFICIAL_REPO_SLUG}; avoid `{DEPRECATED_INSTALL_COMMAND}`"
        ),
    )
    .with_details(vec![
        format!("Official repo     {OFFICIAL_REPO_URL}"),
        "Recommended path  build from this repo or use the upstream binary documented in README.md"
            .to_string(),
        format!(
            "Deprecated crate  `{DEPRECATED_INSTALL_COMMAND}` installs a deprecated stub and does not provide the `cowd` binary"
        )
            .to_string(),
    ])
    .with_data(Map::from_iter([
        ("official_repo".to_string(), json!(OFFICIAL_REPO_URL)),
        (
            "deprecated_install".to_string(),
            json!(DEPRECATED_INSTALL_COMMAND),
        ),
        (
            "recommended_install".to_string(),
            json!("build from source or follow the upstream binary instructions in README.md"),
        ),
    ]))
}

pub(crate) fn check_workspace_health(context: &StatusContext) -> DiagnosticCheck {
    let in_repo = context.project_root.is_some();
    DiagnosticCheck::new(
        "Workspace",
        if in_repo {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warn
        },
        if in_repo {
            format!(
                "project root detected on branch {}",
                context.git_branch.as_deref().unwrap_or("unknown")
            )
        } else {
            "current directory is not inside a git project".to_string()
        },
    )
    .with_details(vec![
        format!("Cwd              {}", context.cwd.display()),
        format!(
            "Project root     {}",
            context
                .project_root
                .as_ref()
                .map_or_else(|| "<none>".to_string(), |path| path.display().to_string())
        ),
        format!(
            "Git branch       {}",
            context.git_branch.as_deref().unwrap_or("unknown")
        ),
        format!("Git state        {}", context.git_summary.headline()),
        format!("Changed files    {}", context.git_summary.changed_files),
        format!(
            "Memory files     {} · config files loaded {}/{}",
            context.memory_file_count, context.loaded_config_files, context.discovered_config_files
        ),
    ])
    .with_data(Map::from_iter([
        ("cwd".to_string(), json!(context.cwd.display().to_string())),
        (
            "project_root".to_string(),
            json!(context
                .project_root
                .as_ref()
                .map(|path| path.display().to_string())),
        ),
        ("in_git_repo".to_string(), json!(in_repo)),
        ("git_branch".to_string(), json!(context.git_branch)),
        (
            "git_state".to_string(),
            json!(context.git_summary.headline()),
        ),
        (
            "changed_files".to_string(),
            json!(context.git_summary.changed_files),
        ),
        (
            "memory_file_count".to_string(),
            json!(context.memory_file_count),
        ),
        (
            "loaded_config_files".to_string(),
            json!(context.loaded_config_files),
        ),
        (
            "discovered_config_files".to_string(),
            json!(context.discovered_config_files),
        ),
    ]))
}

pub(crate) fn check_sandbox_health(status: &runtime::SandboxStatus) -> DiagnosticCheck {
    let degraded = status.enabled && !status.active;
    let mut details = vec![
        format!("Enabled          {}", status.enabled),
        format!("Active           {}", status.active),
        format!("Supported        {}", status.supported),
        format!("Filesystem mode  {}", status.filesystem_mode.as_str()),
        format!("Filesystem live  {}", status.filesystem_active),
    ];
    if let Some(reason) = &status.fallback_reason {
        details.push(format!("Fallback reason  {reason}"));
    }
    DiagnosticCheck::new(
        "Sandbox",
        if degraded {
            DiagnosticLevel::Warn
        } else {
            DiagnosticLevel::Ok
        },
        if degraded {
            "sandbox was requested but is not currently active"
        } else if status.active {
            "sandbox protections are active"
        } else {
            "sandbox is not active for this session"
        },
    )
    .with_details(details)
    .with_data(Map::from_iter([
        ("enabled".to_string(), json!(status.enabled)),
        ("active".to_string(), json!(status.active)),
        ("supported".to_string(), json!(status.supported)),
        (
            "namespace_supported".to_string(),
            json!(status.namespace_supported),
        ),
        (
            "namespace_active".to_string(),
            json!(status.namespace_active),
        ),
        (
            "network_supported".to_string(),
            json!(status.network_supported),
        ),
        ("network_active".to_string(), json!(status.network_active)),
        (
            "filesystem_mode".to_string(),
            json!(status.filesystem_mode.as_str()),
        ),
        (
            "filesystem_active".to_string(),
            json!(status.filesystem_active),
        ),
        ("allowed_mounts".to_string(), json!(status.allowed_mounts)),
        ("in_container".to_string(), json!(status.in_container)),
        (
            "container_markers".to_string(),
            json!(status.container_markers),
        ),
        ("fallback_reason".to_string(), json!(status.fallback_reason)),
    ]))
}

pub(crate) fn check_system_health(
    cwd: &Path,
    config: Option<&runtime::RuntimeConfig>,
) -> DiagnosticCheck {
    let default_model = config.and_then(runtime::RuntimeConfig::model);
    let mut details = vec![
        format!("OS               {} {}", env::consts::OS, env::consts::ARCH),
        format!("Working dir      {}", cwd.display()),
        format!("Version          {}", VERSION),
        format!("Build target     {}", BUILD_TARGET.unwrap_or("<unknown>")),
        format!("Git SHA          {}", GIT_SHA.unwrap_or("<unknown>")),
    ];
    if let Some(model) = default_model {
        details.push(format!("Default model    {model}"));
    }
    DiagnosticCheck::new(
        "System",
        DiagnosticLevel::Ok,
        "captured local runtime metadata",
    )
    .with_details(details)
    .with_data(Map::from_iter([
        ("os".to_string(), json!(env::consts::OS)),
        ("arch".to_string(), json!(env::consts::ARCH)),
        ("working_dir".to_string(), json!(cwd.display().to_string())),
        ("version".to_string(), json!(VERSION)),
        ("build_target".to_string(), json!(BUILD_TARGET)),
        ("git_sha".to_string(), json!(GIT_SHA)),
        ("default_model".to_string(), json!(default_model)),
    ]))
}

pub(crate) fn check_enterprise_readiness(
    cwd: &Path,
    config: Result<&runtime::RuntimeConfig, &runtime::ConfigError>,
) -> DiagnosticCheck {
    let mut details = Vec::new();
    let mut components = Map::new();
    let mut warn_count = 0usize;
    let mut fail_count = 0usize;

    let mut push_component =
        |name: &'static str, status: DiagnosticLevel, summary: String, data: Map<String, Value>| {
            if status == DiagnosticLevel::Warn {
                warn_count += 1;
            } else if status == DiagnosticLevel::Fail {
                fail_count += 1;
            }
            details.push(format!("{:<16} {:<5} {}", name, status.label(), summary));
            let mut value = Map::from_iter([
                ("status".to_string(), json!(status.label())),
                ("summary".to_string(), json!(summary)),
            ]);
            value.extend(data);
            components.insert(name.to_string(), Value::Object(value));
        };

    match config {
        Ok(runtime_config) => {
            let webui_index = cwd.join("webui").join("index.html");
            let webui_modules = [
                "api.js",
                "boot.js",
                "commands.js",
                "messages.js",
                "panels.js",
                "sessions.js",
                "state.js",
                "ui.js",
                "workspace.js",
            ];
            let missing_webui_modules = webui_modules
                .iter()
                .filter(|name| !cwd.join("webui").join(name).exists())
                .copied()
                .collect::<Vec<_>>();
            let webui_ok = webui_index.exists() && missing_webui_modules.is_empty();
            push_component(
                "webui",
                if webui_ok {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if webui_ok {
                    "static WebUI assets are present".to_string()
                } else {
                    "static WebUI assets are incomplete from this cwd".to_string()
                },
                Map::from_iter([
                    ("index_present".to_string(), json!(webui_index.exists())),
                    ("missing_modules".to_string(), json!(missing_webui_modules)),
                ]),
            );

            push_component(
                "tui",
                DiagnosticLevel::Ok,
                "TUI command path is compiled into the CLI".to_string(),
                Map::from_iter([
                    ("compiled".to_string(), json!(true)),
                    (
                        "terminal_detected".to_string(),
                        json!(std::io::stdout().is_terminal()),
                    ),
                ]),
            );

            let config_home = runtime::cowd_dirs::config_home_dir();
            let layout = storage::StorageLayout::default_for_config_home(&config_home);
            let session_db = layout
                .sqlite_path("session")
                .map(Path::to_path_buf)
                .unwrap_or_else(|| layout.root.join("session.sqlite"));
            let session_parent = session_db
                .parent()
                .map(|path| path.exists())
                .unwrap_or(false);
            push_component(
                "session",
                if session_parent {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if session_parent {
                    "session store parent directory is available".to_string()
                } else {
                    "session store parent directory does not exist yet".to_string()
                },
                Map::from_iter([
                    (
                        "config_home".to_string(),
                        json!(config_home.display().to_string()),
                    ),
                    (
                        "session_db".to_string(),
                        json!(session_db.display().to_string()),
                    ),
                    ("session_db_exists".to_string(), json!(session_db.exists())),
                ]),
            );

            let memory = runtime_config.memory();
            let memory_store_path = memory
                .store_path
                .clone()
                .unwrap_or_else(|| config_home.join("memory"));
            push_component(
                "memory",
                if memory.enabled {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if memory.enabled {
                    "memory framework is enabled".to_string()
                } else {
                    "memory framework is disabled".to_string()
                },
                Map::from_iter([
                    ("enabled".to_string(), json!(memory.enabled)),
                    (
                        "store_path".to_string(),
                        json!(memory_store_path.display().to_string()),
                    ),
                    ("vector_enabled".to_string(), json!(memory.vector.enabled)),
                ]),
            );

            let provider_env = env::var("ANTHROPIC_API_KEY")
                .ok()
                .is_some_and(|v| !v.trim().is_empty())
                || env::var("ANTHROPIC_AUTH_TOKEN")
                    .ok()
                    .is_some_and(|v| !v.trim().is_empty())
                || env::var("OPENAI_API_KEY")
                    .ok()
                    .is_some_and(|v| !v.trim().is_empty());
            let providers = runtime_config.providers();
            let resolved_model = runtime_config.model();
            let model_provider = resolved_model.and_then(|model| providers.resolve_full(model));
            let provider_ready = provider_env || model_provider.is_some() || !providers.is_empty();
            push_component(
                "provider",
                if provider_ready {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if provider_ready {
                    "provider credentials or provider mappings are configured".to_string()
                } else {
                    "no provider credentials or provider mappings detected".to_string()
                },
                Map::from_iter([
                    ("env_credentials_present".to_string(), json!(provider_env)),
                    (
                        "configured_providers".to_string(),
                        json!(providers.providers.len()),
                    ),
                    ("resolved_model".to_string(), json!(resolved_model)),
                    (
                        "resolved_provider".to_string(),
                        json!(model_provider.map(|provider| provider.name.clone())),
                    ),
                ]),
            );
        }
        Err(error) => {
            push_component(
                "config",
                DiagnosticLevel::Fail,
                format!("runtime config failed to load: {error}"),
                Map::from_iter([("load_error".to_string(), json!(error.to_string()))]),
            );
        }
    }

    let level = if fail_count > 0 {
        DiagnosticLevel::Fail
    } else if warn_count > 0 {
        DiagnosticLevel::Warn
    } else {
        DiagnosticLevel::Ok
    };
    DiagnosticCheck::new(
        "Enterprise readiness",
        level,
        match level {
            DiagnosticLevel::Ok => "all enterprise entrypoints have local readiness signals",
            DiagnosticLevel::Warn => "enterprise readiness has non-blocking gaps",
            DiagnosticLevel::Fail => "enterprise readiness has blocking failures",
        },
    )
    .with_details(details)
    .with_data(Map::from_iter([
        ("components".to_string(), Value::Object(components)),
        ("component_warnings".to_string(), json!(warn_count)),
        ("component_failures".to_string(), json!(fail_count)),
    ]))
}
