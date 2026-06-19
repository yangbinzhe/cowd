use crate::{CliOutputFormat, DEFAULT_DATE, DEFAULT_MODEL};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use runtime::{ConfigLoader, ConfigSource, JsonValue, ProjectContext, ResolvedPermissionMode};
use serde_json::json;

pub(crate) fn print_setup(
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match output_format {
        CliOutputFormat::Text => println!("{}", render_setup_report()?),
        CliOutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&render_setup_json()?)?)
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct SetupItem {
    pub(crate) id: &'static str,
    pub(crate) label: &'static str,
    pub(crate) status: &'static str,
    pub(crate) summary: String,
    pub(crate) next: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SetupSnapshot {
    pub(crate) cwd: PathBuf,
    pub(crate) config_home: PathBuf,
    pub(crate) loaded_files: Vec<String>,
    pub(crate) gateway_running: bool,
    pub(crate) items: Vec<SetupItem>,
}

impl SetupSnapshot {
    pub(crate) fn overall_status(&self) -> &'static str {
        if self.items.iter().any(|item| item.status == "action") {
            "action"
        } else if self.items.iter().any(|item| item.status == "warn") {
            "warn"
        } else {
            "ready"
        }
    }

    pub(crate) fn next_action(&self) -> String {
        self.items
            .iter()
            .filter(|item| item.status == "action")
            .find_map(|item| item.next.clone())
            .or_else(|| self.items.iter().find_map(|item| item.next.clone()))
            .unwrap_or_else(|| "Start Cowd: cowd --yolo, or inspect runtime: /status".to_string())
    }
}

pub(crate) fn render_setup_report() -> Result<String, Box<dyn std::error::Error>> {
    let snapshot = setup_snapshot()?;
    let mut lines = vec![
        "Setup Center".to_string(),
        format!("  Status           {}", snapshot.overall_status()),
        format!("  Working dir      {}", snapshot.cwd.display()),
        format!("  Config home      {}", snapshot.config_home.display()),
        format!("  Loaded configs   {}", snapshot.loaded_files.len()),
        format!(
            "  Gateway          {}",
            if snapshot.gateway_running {
                "running"
            } else {
                "not running"
            }
        ),
        format!("  Next             {}", snapshot.next_action()),
        String::new(),
        "Checks".to_string(),
    ];

    for item in snapshot.items {
        lines.push(format!(
            "  {:<16} {:<7} {}",
            item.label, item.status, item.summary
        ));
        if let Some(next) = item.next {
            lines.push(format!("  {:<16}         next: {}", "", next));
        }
    }

    lines.push(String::new());
    lines.push("Safe commands".to_string());
    lines.push("  /setup                         Re-run this setup check in TUI".to_string());
    lines.push("  cowd gateway open               Show Gateway/WebUI URL".to_string());
    lines.push("  cowd gateway start              Start gateway/WebUI".to_string());
    lines.push("  cowd gateway restart            Reload gateway after channel auth".to_string());
    Ok(lines.join("\n"))
}

pub(crate) fn render_setup_json() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let snapshot = setup_snapshot()?;
    Ok(json!({
        "kind": "setup",
        "status": snapshot.overall_status(),
        "cwd": snapshot.cwd.display().to_string(),
        "config_home": snapshot.config_home.display().to_string(),
        "loaded_files": snapshot.loaded_files,
        "gateway_running": snapshot.gateway_running,
        "next": snapshot.next_action(),
        "items": snapshot.items.into_iter().map(|item| {
            json!({
                "id": item.id,
                "label": item.label,
                "status": item.status,
                "summary": item.summary,
                "next": item.next,
            })
        }).collect::<Vec<_>>(),
    }))
}

fn setup_snapshot() -> Result<SetupSnapshot, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let config_home = loader.config_home().to_path_buf();
    let config = loader
        .load()
        .unwrap_or_else(|_| runtime::RuntimeConfig::empty());
    let loaded_files = config
        .loaded_entries()
        .iter()
        .map(|entry| entry.path.display().to_string())
        .collect::<Vec<_>>();
    let gateway_running = crate::server::get_server_status().ok().flatten().is_some();
    let mut items = Vec::new();

    items.push(setup_config_item(&loaded_files, &config_home));
    items.push(setup_provider_item(&config));
    items.push(setup_gateway_item(&config, gateway_running));
    items.push(setup_feishu_item(&config));
    items.push(setup_wechat_item());
    items.push(setup_memory_item(&config));
    items.push(setup_session_item(&config_home));
    items.push(setup_permission_item(&config));

    Ok(SetupSnapshot {
        cwd,
        config_home,
        loaded_files,
        gateway_running,
        items,
    })
}

fn setup_config_item(loaded_files: &[String], config_home: &Path) -> SetupItem {
    if loaded_files.is_empty() {
        SetupItem {
            id: "config",
            label: "Config",
            status: "action",
            summary: "No config file loaded; defaults work but channels/providers need config"
                .to_string(),
            next: Some(format!(
                "Create or edit {}",
                config_home.join("config.yaml").display()
            )),
        }
    } else {
        SetupItem {
            id: "config",
            label: "Config",
            status: "ready",
            summary: format!("{} config file(s) loaded", loaded_files.len()),
            next: None,
        }
    }
}

fn setup_provider_item(config: &runtime::RuntimeConfig) -> SetupItem {
    let model = config.model().unwrap_or(DEFAULT_MODEL);
    if let Some(provider) = config.providers().resolve_full(model) {
        return SetupItem {
            id: "provider",
            label: "Provider",
            status: "ready",
            summary: format!("Model {model} routes through provider {}", provider.name),
            next: None,
        };
    }
    if env::var("ANTHROPIC_API_KEY").is_ok()
        || env::var("ANTHROPIC_AUTH_TOKEN").is_ok()
        || env::var("OPENAI_API_KEY").is_ok()
    {
        return SetupItem {
            id: "provider",
            label: "Provider",
            status: "ready",
            summary: format!("Model {model} can use environment credentials"),
            next: None,
        };
    }
    SetupItem {
        id: "provider",
        label: "Provider",
        status: "action",
        summary: format!("No provider route found for model {model}"),
        next: Some("Add a provider in ~/.cowd/config.yaml or set API key env".to_string()),
    }
}

fn setup_gateway_item(config: &runtime::RuntimeConfig, gateway_running: bool) -> SetupItem {
    let gateway = config.gateway();
    let api = gateway
        .platforms
        .iter()
        .find(|platform| matches!(platform.platform_type.as_str(), "api_server" | "api"));
    let Some(api) = api else {
        return SetupItem {
            id: "gateway",
            label: "Gateway",
            status: "action",
            summary: "API server platform is not configured".to_string(),
            next: Some("Enable gateway api_server in ~/.cowd/config.yaml".to_string()),
        };
    };
    if !gateway.enabled || !api.enabled {
        return SetupItem {
            id: "gateway",
            label: "Gateway",
            status: "action",
            summary: "Gateway or api_server is disabled".to_string(),
            next: Some("Set gateway.enabled and api_server.enabled to true".to_string()),
        };
    }
    let host = json_str(api.extra.get("host")).unwrap_or("127.0.0.1");
    let port = json_i64(api.extra.get("port")).unwrap_or(8642);
    SetupItem {
        id: "gateway",
        label: "Gateway",
        status: if gateway_running { "ready" } else { "warn" },
        summary: if gateway_running {
            format!("Running; WebUI should be at http://{host}:{port}")
        } else {
            format!("Configured at http://{host}:{port}, not currently running")
        },
        next: (!gateway_running).then(|| "cowd gateway start".to_string()),
    }
}

fn setup_feishu_item(config: &runtime::RuntimeConfig) -> SetupItem {
    let feishu = config
        .gateway()
        .platforms
        .iter()
        .find(|platform| matches!(platform.platform_type.as_str(), "feishu" | "lark"));
    let Some(feishu) = feishu else {
        return SetupItem {
            id: "feishu",
            label: "Feishu",
            status: "warn",
            summary: "No Feishu platform configured".to_string(),
            next: Some("Add a Feishu platform only if you need Feishu".to_string()),
        };
    };
    if !feishu.enabled {
        return SetupItem {
            id: "feishu",
            label: "Feishu",
            status: "warn",
            summary: "Configured but disabled".to_string(),
            next: Some("Set the Feishu platform enabled: true".to_string()),
        };
    }
    let has_app_id = json_str(feishu.extra.get("app_id")).is_some_and(|value| !value.is_empty());
    let has_app_secret =
        json_str(feishu.extra.get("app_secret")).is_some_and(|value| !value.is_empty());
    if has_app_id && has_app_secret {
        SetupItem {
            id: "feishu",
            label: "Feishu",
            status: "ready",
            summary: "Enabled with required credentials; secrets are not displayed".to_string(),
            next: None,
        }
    } else {
        SetupItem {
            id: "feishu",
            label: "Feishu",
            status: "action",
            summary: "Missing app_id or app_secret".to_string(),
            next: Some("Fill Feishu app_id/app_secret in ~/.cowd/config.yaml".to_string()),
        }
    }
}

fn setup_wechat_item() -> SetupItem {
    match runtime::platform::wechat_ilink::list_wechat_qr_accounts(None) {
        Ok(accounts) if !accounts.is_empty() => SetupItem {
            id: "wechat",
            label: "WeChat",
            status: "ready",
            summary: format!("{} QR-authorized account(s) available", accounts.len()),
            next: None,
        },
        Ok(_) => SetupItem {
            id: "wechat",
            label: "WeChat",
            status: "action",
            summary: "No personal WeChat QR account authorized".to_string(),
            next: Some("Configure the WeChat platform in Gateway/WebUI".to_string()),
        },
        Err(error) => SetupItem {
            id: "wechat",
            label: "WeChat",
            status: "warn",
            summary: format!("Could not read WeChat accounts: {error}"),
            next: Some("Configure the WeChat platform in Gateway/WebUI".to_string()),
        },
    }
}

fn setup_memory_item(config: &runtime::RuntimeConfig) -> SetupItem {
    let memory = config.memory();
    SetupItem {
        id: "memory",
        label: "Memory",
        status: if memory.enabled { "ready" } else { "warn" },
        summary: if memory.enabled {
            "Enabled with default organic memory runtime".to_string()
        } else {
            "Disabled; conversations still work but memory will not accumulate".to_string()
        },
        next: (!memory.enabled)
            .then(|| "Set memory.enabled: true when you want memory".to_string()),
    }
}

fn setup_session_item(config_home: &Path) -> SetupItem {
    let db_path = storage::StorageLayout::default_for_config_home(config_home)
        .sqlite_path("session")
        .map(Path::to_path_buf)
        .unwrap_or_else(|| config_home.join("sessions.db"));
    SetupItem {
        id: "session",
        label: "Session",
        status: if db_path.exists() { "ready" } else { "warn" },
        summary: if db_path.exists() {
            format!("SQLite session store exists at {}", db_path.display())
        } else {
            "SQLite session store will be created on first session use".to_string()
        },
        next: None,
    }
}

fn setup_permission_item(config: &runtime::RuntimeConfig) -> SetupItem {
    let mode = match config.permission_mode() {
        Some(ResolvedPermissionMode::ReadOnly) => "read-only",
        Some(ResolvedPermissionMode::WorkspaceWrite) => "workspace-write",
        Some(ResolvedPermissionMode::DangerFullAccess) => "danger-full-access",
        None => "default",
    };
    SetupItem {
        id: "permission",
        label: "Permission",
        status: "ready",
        summary: format!("Permission mode is {mode}; --solo/--yolo remain explicit overrides"),
        next: None,
    }
}

fn json_str(value: Option<&JsonValue>) -> Option<&str> {
    value.and_then(JsonValue::as_str)
}

fn json_i64(value: Option<&JsonValue>) -> Option<i64> {
    value.and_then(JsonValue::as_i64)
}

pub(crate) fn render_config_report(
    section: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let discovered = loader.discover();
    let runtime_config = loader.load()?;

    let mut lines = vec![
        format!(
            "Config
  Working directory {}
  Loaded files      {}
  Merged keys       {}",
            cwd.display(),
            runtime_config.loaded_entries().len(),
            runtime_config.merged().len()
        ),
        "Discovered files".to_string(),
    ];
    for entry in discovered {
        let source = match entry.source {
            ConfigSource::User => "user",
            ConfigSource::Project => "project",
            ConfigSource::Local => "local",
            ConfigSource::Environment => "env",
            ConfigSource::Cli => "cli",
        };
        let status = if runtime_config
            .loaded_entries()
            .iter()
            .any(|loaded_entry| loaded_entry.path == entry.path)
        {
            "loaded"
        } else {
            "missing"
        };
        lines.push(format!(
            "  {source:<7} {status:<7} {}",
            entry.path.display()
        ));
    }

    if let Some(section) = section {
        lines.push(format!("Merged section: {section}"));
        let value = match section {
            "env" => runtime_config.get("env"),
            "hooks" => runtime_config.get("hooks"),
            "model" => runtime_config.get("model"),
            "plugins" => runtime_config
                .get("plugins")
                .or_else(|| runtime_config.get("enabledPlugins")),
            other => {
                lines.push(format!(
                    "  Unsupported config section '{other}'. Use env, hooks, model, or plugins."
                ));
                return Ok(lines.join("\n"));
            }
        };
        lines.push(format!(
            "  {}",
            match value {
                Some(value) => value.render(),
                None => "<unset>".to_string(),
            }
        ));
        return Ok(lines.join("\n"));
    }

    lines.push("Merged JSON".to_string());
    lines.push(format!("  {}", runtime_config.as_json().render()));
    Ok(lines.join("\n"))
}

pub(crate) fn render_config_json(
    _section: Option<&str>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let discovered = loader.discover();
    let runtime_config = loader.load()?;

    let loaded_paths: Vec<_> = runtime_config
        .loaded_entries()
        .iter()
        .map(|e| e.path.display().to_string())
        .collect();

    let files: Vec<_> = discovered
        .iter()
        .map(|e| {
            let source = match e.source {
                ConfigSource::User => "user",
                ConfigSource::Project => "project",
                ConfigSource::Local => "local",
                ConfigSource::Environment => "env",
                ConfigSource::Cli => "cli",
            };
            let is_loaded = runtime_config
                .loaded_entries()
                .iter()
                .any(|le| le.path == e.path);
            json!({
                "path": e.path.display().to_string(),
                "source": source,
                "loaded": is_loaded,
            })
        })
        .collect();

    Ok(json!({
        "kind": "config",
        "cwd": cwd.display().to_string(),
        "loaded_files": loaded_paths.len(),
        "merged_keys": runtime_config.merged().len(),
        "files": files,
    }))
}

pub(crate) fn render_memory_report() -> Result<String, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let project_context = ProjectContext::discover(&cwd, DEFAULT_DATE)?;
    let mut lines = vec![format!(
        "Memory
  Working directory {}
  Instruction files {}",
        cwd.display(),
        project_context.instruction_files.len()
    )];
    if project_context.instruction_files.is_empty() {
        lines.push("Discovered files".to_string());
        lines.push(
            "  No instruction files discovered in the current directory ancestry.".to_string(),
        );
    } else {
        lines.push("Discovered files".to_string());
        for (index, file) in project_context.instruction_files.iter().enumerate() {
            let preview = file.content.lines().next().unwrap_or("").trim();
            let preview = if preview.is_empty() {
                "<empty>"
            } else {
                preview
            };
            lines.push(format!("  {}. {}", index + 1, file.path.display(),));
            lines.push(format!(
                "     lines={} preview={}",
                file.content.lines().count(),
                preview
            ));
        }
    }
    Ok(lines.join("\n"))
}

pub(crate) fn render_memory_json() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let project_context = ProjectContext::discover(&cwd, DEFAULT_DATE)?;
    let files: Vec<_> = project_context
        .instruction_files
        .iter()
        .map(|f| {
            json!({
                "path": f.path.display().to_string(),
                "lines": f.content.lines().count(),
                "preview": f.content.lines().next().unwrap_or("").trim(),
            })
        })
        .collect();
    Ok(json!({
        "kind": "memory",
        "cwd": cwd.display().to_string(),
        "instruction_files": files.len(),
        "files": files,
    }))
}

pub(crate) fn render_diff_report() -> Result<String, Box<dyn std::error::Error>> {
    render_diff_report_for(&env::current_dir()?)
}

pub(crate) fn render_diff_report_for(cwd: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let in_git_repo = Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(cwd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !in_git_repo {
        return Ok(format!(
            "Diff\n  Result           no git repository\n  Detail           {} is not inside a git project",
            cwd.display()
        ));
    }
    let staged = run_git_diff_command_in(cwd, &["diff", "--cached"])?;
    let unstaged = run_git_diff_command_in(cwd, &["diff"])?;
    if staged.trim().is_empty() && unstaged.trim().is_empty() {
        return Ok(
            "Diff\n  Result           clean working tree\n  Detail           no current changes"
                .to_string(),
        );
    }

    let mut sections = Vec::new();
    if !staged.trim().is_empty() {
        sections.push(format!("Staged changes:\n{}", staged.trim_end()));
    }
    if !unstaged.trim().is_empty() {
        sections.push(format!("Unstaged changes:\n{}", unstaged.trim_end()));
    }

    Ok(format!("Diff\n\n{}", sections.join("\n\n")))
}

pub(crate) fn render_diff_json_for(
    cwd: &Path,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let in_git_repo = Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(cwd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !in_git_repo {
        return Ok(json!({
            "kind": "diff",
            "result": "no_git_repo",
            "detail": format!("{} is not inside a git project", cwd.display()),
        }));
    }
    let staged = run_git_diff_command_in(cwd, &["diff", "--cached"])?;
    let unstaged = run_git_diff_command_in(cwd, &["diff"])?;
    Ok(json!({
        "kind": "diff",
        "result": if staged.trim().is_empty() && unstaged.trim().is_empty() { "clean" } else { "changes" },
        "staged": staged.trim(),
        "unstaged": unstaged.trim(),
    }))
}

fn run_git_diff_command_in(
    cwd: &Path,
    args: &[&str],
) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git").args(args).current_dir(cwd).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!("git {} failed: {stderr}", args.join(" ")).into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
