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
pub(crate) fn check_auth_health(config: Option<&runtime::RuntimeConfig>) -> DiagnosticCheck {
    let configured_model = config.and_then(runtime::RuntimeConfig::resolved_model);
    let configured_provider = config.and_then(|runtime_config| {
        configured_model
            .as_deref()
            .and_then(|model| runtime_config.providers().resolve_full(model))
    });
    let provider_ready = configured_provider.is_some();
    let (legacy_saved_oauth_present, legacy_saved_oauth_error) = match load_oauth_credentials() {
        Ok(credentials) => (Some(credentials.is_some()), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let mut details = vec![format!(
        "Configured route  model={} provider={}",
        configured_model.as_deref().unwrap_or("<none>"),
        configured_provider
            .map(|provider| provider.name.as_str())
            .unwrap_or("<none>")
    )];
    if legacy_saved_oauth_present == Some(true) {
        details.push(
            "Legacy OAuth      present but ignored; Runtime uses configured providers only"
                .to_string(),
        );
    }
    if let Some(error) = legacy_saved_oauth_error.as_deref() {
        details.push(format!("Legacy OAuth      inspection failed: {error}"));
    }
    if !provider_ready {
        details.push(
            "Suggested action  set `model` and declare it under `providers.*.models`".to_string(),
        );
    }

    DiagnosticCheck::new(
        "Auth",
        if provider_ready {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warn
        },
        if provider_ready {
            "configured default model has an explicit provider route"
        } else {
            "no explicit provider route exists for the configured default model"
        },
    )
    .with_details(details)
    .with_data(Map::from_iter([
        ("configured_model".to_string(), json!(configured_model)),
        (
            "configured_provider".to_string(),
            json!(configured_provider.map(|provider| provider.name.clone())),
        ),
        ("provider_route_ready".to_string(), json!(provider_ready)),
        (
            "legacy_saved_oauth_present".to_string(),
            json!(legacy_saved_oauth_present),
        ),
        (
            "legacy_saved_oauth_error".to_string(),
            json!(legacy_saved_oauth_error),
        ),
    ]))
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
            let resolved_model = runtime_config.resolved_model();
            if let Some(model) = resolved_model.as_deref() {
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
                ("resolved_model".to_string(), json!(resolved_model)),
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

/// P1: PostgreSQL deployments must not keep active SQLite residue under the
/// config storage directory. The check reports leftover files so operators
/// can clean them with `cowd storage cleanup` instead of silently running
/// with a dual-backend state.
pub(crate) fn check_sqlite_residuals(config: Option<&runtime::RuntimeConfig>) -> DiagnosticCheck {
    let storage_dir = runtime::cowd_dirs::config_home_dir().join("storage");
    let live_pools = memory::sqlite_pool_instance_count();
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&storage_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".sqlite")
                || name.ends_with(".sqlite3")
                || name.ends_with("-wal")
                || name.ends_with("-shm")
            {
                files.push(name);
            }
        }
    }
    files.sort();
    let postgres = config.is_some_and(|config| {
        matches!(
            config.storage().backend,
            runtime::StorageBackendSelection::Postgres | runtime::StorageBackendSelection::Auto
        )
    });
    if files.is_empty() {
        if postgres && live_pools > 0 {
            DiagnosticCheck::new(
                "SQLite residuals",
                DiagnosticLevel::Warn,
                format!(
                    "PostgreSQL is active but {live_pools} live SQLite pool(s) exist; run `cowd storage cleanup --sqlite-residuals` after stopping the Gateway"
                ),
            )
        } else {
            DiagnosticCheck::new(
                "SQLite residuals",
                DiagnosticLevel::Ok,
                format!("no SQLite files under config storage, live SQLite pools={live_pools}"),
            )
        }
    } else if postgres {
        DiagnosticCheck::new(
            "SQLite residuals",
            DiagnosticLevel::Warn,
            format!(
                "PostgreSQL is active but SQLite files remain: {}; live SQLite pools={live_pools}; run `cowd storage cleanup --sqlite-residuals` after stopping the Gateway",
                files.join(", "),
            ),
        )
    } else {
        DiagnosticCheck::new(
            "SQLite residuals",
            DiagnosticLevel::Ok,
            format!(
                "SQLite backend files present: {}; live SQLite pools={live_pools}",
                files.join(", ")
            ),
        )
    }
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
            let static_webui = crate::gateway_static::resolve_static_webui_source(
                runtime_config.gateway().webui_dir.as_deref(),
            );
            let webui_ok = static_webui.available;
            push_component(
                "webui",
                if webui_ok {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if webui_ok {
                    "configured static WebUI is ready".to_string()
                } else {
                    "gateway.webui_dir is not configured with an index.html".to_string()
                },
                Map::from_iter([
                    ("config_key".to_string(), json!(static_webui.config_key)),
                    (
                        "configured_path".to_string(),
                        json!(static_webui
                            .configured_path
                            .as_ref()
                            .map(|path| path.display().to_string())),
                    ),
                    (
                        "index_path".to_string(),
                        json!(static_webui
                            .index_path
                            .as_ref()
                            .map(|path| path.display().to_string())),
                    ),
                    (
                        "runtime_status".to_string(),
                        json!(static_webui.status.as_str()),
                    ),
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

            let providers = runtime_config.providers();
            let resolved_model = runtime_config.resolved_model();
            let model_provider = resolved_model
                .as_deref()
                .and_then(|model| providers.resolve_full(model));
            let provider_ready = model_provider.is_some();
            push_component(
                "provider",
                if provider_ready {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Warn
                },
                if provider_ready {
                    "the configured default model has an explicit provider route".to_string()
                } else {
                    "the configured default model has no explicit provider route".to_string()
                },
                Map::from_iter([
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn enterprise_webui_readiness_uses_the_configured_vite_bundle() {
        let root = tempfile::tempdir().expect("create test root");
        let workspace = root.path().join("workspace");
        let config_home = root.path().join("config");
        let webui = root.path().join("webui-dist");
        fs::create_dir_all(&workspace).expect("create workspace");
        fs::create_dir_all(&config_home).expect("create config home");
        fs::create_dir_all(&webui).expect("create webui");
        fs::write(
            webui.join("index.html"),
            r#"<script type="module" src="./assets/app/index.dev-hash.js"></script>"#,
        )
        .expect("write Vite index");
        fs::write(
            config_home.join("config.yaml"),
            format!("gateway:\n  webui_dir: {}\n", webui.display()),
        )
        .expect("write config");

        let config = ConfigLoader::new(&workspace, &config_home)
            .load()
            .expect("load config");
        let report = check_enterprise_readiness(Ok(&config)).json_value();
        let webui_report = &report["components"]["webui"];

        assert_eq!(webui_report["status"], "ok");
        assert_eq!(webui_report["runtime_status"], "ready");
        assert_eq!(webui_report["configured_path"], webui.display().to_string());
        assert!(webui_report.get("missing_modules").is_none());
    }
}
