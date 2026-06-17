#![allow(
    clippy::unneeded_struct_pattern,
    clippy::unnecessary_wraps,
    clippy::unused_self,
    dead_code
)]
#![deny(deprecated)]
mod api_routes;
mod bootstrap;
mod checks;
mod cli;
mod daemon;
mod doctor;
mod event_bus;
mod gateway;
mod gateway_health;
mod gateway_service;
mod gateway_static;
mod init;
mod logging;
mod mcp_serve;
mod render;
mod runtime_boundary;
mod runtime_protocol;
mod runtime_service;
mod server;
mod session_kernel;
mod session_lifecycle_kernel;
mod suggestions;
mod task_kernel;
mod tui;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::ops::{Deref, DerefMut};
use std::os::unix::io::FromRawFd;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, UNIX_EPOCH};

use provider::{
    detect_provider_kind, resolve_startup_auth_source, AuthSource, CachedProviderClient,
    ContentBlockDelta, InputContentBlock, InputMessage, MessageRequest, MessageResponse,
    OutputContentBlock, PromptCache, ProviderClient as ApiProviderClient, ProviderKind,
    StreamEvent as ApiStreamEvent, ToolChoice, ToolDefinition, ToolResultContentBlock,
};

#[cfg(test)]
use commands::resume_supported_slash_commands;
use commands::{
    classify_skills_slash_command, handle_agents_slash_command, handle_agents_slash_command_json,
    handle_mcp_slash_command, handle_mcp_slash_command_json, handle_plugins_slash_command,
    handle_skills_slash_command, handle_skills_slash_command_json,
    render_slash_command_help_filtered, resolve_skill_invocation, slash_command_specs,
    SkillRegistry, SkillSlashDispatch, SlashCommand,
};
use compat_harness::{extract_manifest, UpstreamPaths};
use init::initialize_repo;
use plugins::{PluginHooks, PluginManager, PluginManagerConfig, PluginRegistry};
use render::{MarkdownStreamState, Spinner, TerminalRenderer};
use runtime::ContextProfile;
use runtime::{
    check_base_commit, format_stale_base_warning, load_system_prompt, resolve_expected_base,
    resolve_sandbox_status, ApiClient, ApiRequest, AssistantEvent, CompactionConfig, ConfigLoader,
    ConfigSource, ContentBlock, ConversationMessage, ConversationRuntime, JsonValue,
    McpServerManager, McpTool, MessageRole, PermissionMode, PermissionPolicy, ProjectContext,
    PromptCacheEvent, ResolvedPermissionMode, ResumeContextPacket, ResumeContextSource,
    RuntimeError, Session, TokenUsage, ToolError, ToolExecutor, UsageTracker,
};
use serde::Deserialize;
use serde_json::json;
use tools::{GlobalToolRegistry, RuntimeToolDefinition};

use futures::StreamExt;
use tui::state::TuiState;

impl tui::app::ToolRegistry for GlobalToolRegistry {
    fn enable_tool(&self, name: &str) {
        // GlobalToolRegistry is read-only at this layer;
        // enable/disable is tracked in SkillsPanel entries.
        // Validation: check the tool name is registered.
        let _ = name;
    }

    fn disable_tool(&self, name: &str) {
        let _ = name;
    }
}

pub(crate) const DEFAULT_MODEL: &str = "claude-sonnet-4-6";
fn max_tokens_for_model(model: &str) -> u32 {
    provider::max_tokens_for_model(model)
}
/// Global list of daemon child processes that must be reaped.
/// Children are adopted (stored here) instead of dropping the handle,
/// which prevents zombie processes when the daemon exits.
static DAEMON_CHILDREN: LazyLock<Mutex<Vec<Child>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Adopt a daemon child process — store it so the handle is not dropped.
/// Returns the child's PID.
fn adopt_daemon_child(child: Child) -> u32 {
    let pid = child.id();
    if let Ok(mut children) = DAEMON_CHILDREN.lock() {
        children.push(child);
    }
    pid
}

/// Keep daemon-child setup local to tracked child handles.
///
/// Do not install `SIGCHLD = SIG_IGN`: that makes unrelated tool subprocesses
/// impossible to `wait` reliably and breaks bash/tool execution in one-shot
/// runs. Zombie prevention is handled by retaining daemon handles and calling
/// `reap_daemon_children`.
#[cfg(unix)]
fn setup_sigchld_handler() {
    tracing::debug!("daemon child reaping uses retained child handles");
}

/// Try to reap any exited daemon children. Called periodically.
fn reap_daemon_children() {
    if let Ok(mut children) = DAEMON_CHILDREN.lock() {
        children.retain_mut(|child| match child.try_wait() {
            Ok(Some(status)) => {
                tracing::debug!(
                    pid = child.id(),
                    code = status.code(),
                    "daemon child reaped"
                );
                false
            }
            Ok(None) => true, // still running
            Err(e) => {
                tracing::warn!(pid = child.id(), error = %e, "failed to wait on daemon child");
                false
            }
        });
    }
}

fn gateway_daemon_log_file() -> Result<std::fs::File, Box<dyn std::error::Error>> {
    let log_dir = runtime::cowd_dirs::config_home_dir().join("logs");
    std::fs::create_dir_all(&log_dir)?;
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("gateway-daemon.log"))?)
}

fn spawn_gateway_daemon(exe: &Path) -> Result<Child, Box<dyn std::error::Error>> {
    let stdout = gateway_daemon_log_file()?;
    let stderr = stdout.try_clone()?;
    let mut command = std::process::Command::new(exe);
    command
        .arg("gateway")
        .arg("run")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(stdout))
        .stderr(std::process::Stdio::from(stderr));
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    command
        .spawn()
        .map_err(|e| format!("failed to start gateway daemon: {e}").into())
}

fn wait_for_gateway_start(
    child: &mut Child,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Some(status) = child.try_wait()? {
            return Err(format!("gateway daemon exited during startup: {status}").into());
        }
        if server::get_server_status()
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("gateway daemon did not become ready before timeout".into())
}

static SHARED_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("shared tokio runtime")
});
// Build-time constants injected by build.rs (fall back to static values when
// build.rs hasn't run, e.g. in doc-test or unusual toolchain environments).
const DEFAULT_DATE: &str = match option_env!("BUILD_DATE") {
    Some(d) => d,
    None => "unknown",
};
const DEFAULT_OAUTH_CALLBACK_PORT: u16 = 4545;
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const BUILD_TARGET: Option<&str> = option_env!("TARGET");
pub(crate) const GIT_SHA: Option<&str> = option_env!("GIT_SHA");
const INTERNAL_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);
const POST_TOOL_STALL_TIMEOUT: Duration = Duration::from_secs(10);
const OFFICIAL_REPO_URL: &str = "https://github.com/ultraworkers/cowd";
const OFFICIAL_REPO_SLUG: &str = "ultraworkers/cowd";
const DEPRECATED_INSTALL_COMMAND: &str = "cargo install cowd";
const LATEST_SESSION_REFERENCE: &str = "latest";
const REMOVED_PROMPT_SUBCOMMAND: &str = "prompt";
const SESSION_REFERENCE_ALIASES: &[&str] = &[LATEST_SESSION_REFERENCE, "last", "recent"];

type AllowedToolSet = BTreeSet<String>;
type RuntimePluginStateBuildOutput = (
    Option<Arc<Mutex<RuntimeMcpState>>>,
    Vec<RuntimeToolDefinition>,
);

/// Expand `~` at the start of a path to the user's home directory.
fn expand_home(path: &std::path::Path) -> std::path::PathBuf {
    if path.starts_with("~") {
        if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
            return std::path::PathBuf::from(path.to_string_lossy().replacen("~", &home, 1));
        }
    }
    path.to_path_buf()
}

/// Convert `runtime::MemoryConfig` → `memory::MemoryConfig`.
/// Returns None if memory is disabled in the config.
fn build_memory_config(
    src: &runtime::MemoryConfig,
    _cwd: &std::path::Path,
) -> Option<memory::MemoryConfig> {
    if !src.enabled {
        return None;
    }
    let store_path = src
        .store_path
        .as_ref()
        .map(|p| expand_home(p))
        .unwrap_or_else(|| runtime::cowd_dirs::config_home_dir().join("memory"));

    // Ensure the store directory exists before SQLite tries to open the database.
    if let Err(e) = std::fs::create_dir_all(&store_path) {
        tracing::warn!("failed to create memory store dir {:?}: {e}", store_path);
    }

    let mut mc = memory::MemoryConfig::default();
    mc.store.sqlite_path = store_path.join("memory.db");
    mc.store.blob_dir = store_path.join("blobs");
    mc.store.enable_vector_index = src.vector.enabled;
    mc.store.vector.enabled = src.vector.enabled;
    mc.store.vector.model = src.vector.model.clone();
    mc.store.vector.api_url = src.vector.api_url.clone();
    mc.store.vector.api_key = src.vector.api_key.clone();
    mc.store.vector.dimension = src.vector.dimension as usize;
    mc.store.vector.timeout_secs = src.vector.timeout_secs;
    mc.store.vector.batch_size = src.vector.batch_size;
    Some(mc)
}

/// Convert `runtime::GatewayConfig` → `Vec<runtime::platform::PlatformConfig>`.
/// Filters out `api_server` (handled by serve itself) and disabled platforms.
fn build_platform_configs(gw: &runtime::GatewayConfig) -> Vec<runtime::platform::PlatformConfig> {
    if !gw.enabled {
        return Vec::new();
    }
    let mut configs: Vec<_> = gw
        .platforms
        .iter()
        .filter(|p| p.enabled && p.platform_type != "api_server")
        .map(|p| {
            let mut pc = runtime::platform::PlatformConfig::new(&p.platform_type);
            pc.enabled = p.enabled;
            for (k, v) in &p.extra {
                pc = pc.with_setting(k, json_value_to_serde(v));
            }
            pc
        })
        .collect();

    let has_wechat = configs
        .iter()
        .any(|p| matches!(p.platform_type.as_str(), "wechat_ilink" | "wechat"));
    if !has_wechat {
        if let Ok(accounts) = runtime::platform::wechat_ilink::list_wechat_qr_accounts(None) {
            if let Some(account) = accounts.first() {
                tracing::info!(
                    "wechat_ilink: auto-detected QR account {} (saved at {})",
                    account.account_id,
                    account.saved_at
                );
                let mut pc = runtime::platform::PlatformConfig::new("wechat_ilink");
                pc = pc
                    .with_setting("credential_source", "qr_account")
                    .with_setting("account_id", account.account_id.clone());
                configs.push(pc);
            }
        }
    }

    configs
}

/// Convert `runtime::JsonValue` → `serde_json::Value` for use with
/// `runtime::platform::PlatformConfig::with_setting()`.
fn json_value_to_serde(v: &runtime::JsonValue) -> serde_json::Value {
    match v {
        runtime::JsonValue::Null => serde_json::Value::Null,
        runtime::JsonValue::Bool(b) => serde_json::Value::Bool(*b),
        runtime::JsonValue::Number(n) => serde_json::json!(*n),
        runtime::JsonValue::String(s) => serde_json::Value::String(s.clone()),
        runtime::JsonValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(json_value_to_serde).collect())
        }
        runtime::JsonValue::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), json_value_to_serde(v)))
                .collect(),
        ),
    }
}

fn main() {
    if let Err(error) = run() {
        let message = error.to_string();
        // When --output-format json is active, emit errors as JSON so downstream
        // tools can parse failures the same way they parse successes (ROADMAP #42).
        let argv: Vec<String> = std::env::args().collect();
        let json_output = argv
            .windows(2)
            .any(|w| w[0] == "--output-format" && w[1] == "json")
            || argv.iter().any(|a| a == "--output-format=json");
        if json_output {
            eprintln!(
                "{}",
                serde_json::json!({
                    "type": "error",
                    "error": message,
                })
            );
        } else if message.contains("`cowd --help`") {
            eprintln!("error: {message}");
        } else {
            eprintln!(
                "error: {message}

Run `cowd --help` for usage."
            );
        }
        std::process::exit(1);
    }
}

/// Merge a piped stdin payload into a prompt argument.
///
/// When `stdin_content` is `None` or empty after trimming, the prompt is
/// returned unchanged. Otherwise the trimmed stdin content is appended to the
/// prompt separated by a blank line so the model sees the prompt first and the
/// piped context immediately after it.
fn merge_prompt_with_stdin(prompt: &str, stdin_content: Option<&str>) -> String {
    let Some(raw) = stdin_content else {
        return prompt.to_string();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return prompt.to_string();
    }
    if prompt.is_empty() {
        return trimmed.to_string();
    }
    format!("{prompt}\n\n{trimmed}")
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    logging::init_logging(VERSION);

    // Set up SIGCHLD handler to auto-reap daemon child processes
    setup_sigchld_handler();

    let args: Vec<String> = env::args().skip(1).collect();
    let action = parse_args(&args)?;

    // 检查是否需要引导配置
    if should_bootstrap_for_action(&action) && bootstrap::needs_bootstrap() {
        bootstrap::run_bootstrap()?;
        // 引导完成后询问是否继续启动
        print!("按 Enter 键启动 Cowd 或 Ctrl+C 退出... ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).ok();
    }

    // Force-initialize SHARED_RT on main thread where no tokio runtime exists yet
    let _ = SHARED_RT.handle();

    match action {
        CliAction::DumpManifests {
            output_format,
            manifests_dir,
        } => dump_manifests(manifests_dir.as_deref(), output_format)?,
        CliAction::BootstrapPlan { output_format } => print_bootstrap_plan(output_format)?,
        CliAction::Agents {
            args,
            output_format,
        } => LiveCli::print_agents(args.as_deref(), output_format)?,
        CliAction::Mcp {
            args,
            output_format,
        } => LiveCli::print_mcp(args.as_deref(), output_format)?,
        CliAction::Skills {
            args,
            output_format,
        } => LiveCli::print_skills(args.as_deref(), output_format)?,
        CliAction::Plugins {
            action,
            target,
            output_format,
        } => LiveCli::print_plugins(action.as_deref(), target.as_deref(), output_format)?,
        CliAction::PrintSystemPrompt {
            cwd,
            date,
            output_format,
        } => print_system_prompt(cwd, date, output_format)?,
        CliAction::Version { output_format } => print_version(output_format)?,
        CliAction::ResumeSession {
            session_path,
            commands,
            output_format,
        } => resume_session(&session_path, &commands, output_format),
        CliAction::Status {
            model,
            permission_mode,
            output_format,
        } => print_status_snapshot(&model, permission_mode, output_format)?,
        CliAction::Sandbox { output_format } => print_sandbox_status_snapshot(output_format)?,

        CliAction::Doctor { output_format } => doctor::run_doctor(output_format)?,
        CliAction::Setup { output_format } => print_setup(output_format)?,
        CliAction::State { output_format } => mcp_serve::run_worker_state(output_format)?,
        CliAction::Init { output_format } => run_init(output_format)?,
        CliAction::Export {
            session_reference,
            output_path,
            output_format,
        } => run_export(&session_reference, output_path.as_deref(), output_format)?,
        CliAction::ImportSession {
            path,
            output_format,
        } => run_import_session(&path, output_format)?,
        CliAction::Repl {
            model,
            session_id,
            allowed_tools,
            permission_mode,
            base_commit,
            reasoning_effort,
            allow_broad_cwd,
            yolo_mode,
        } => {
            // Auto-start daemon if not already running
            let sock_path = daemon_socket_path();
            let sock = sock_path.as_path();
            let daemon_autostart_disabled = std::env::var("COWD_DISABLE_DAEMON_AUTOSTART").is_ok();
            if !sock.exists() && !daemon_autostart_disabled {
                tracing::info!("daemon not running, auto-starting...");
                setup_sigchld_handler();
                if let Ok(exe) = std::env::current_exe() {
                    match std::process::Command::new(&exe)
                        .arg("gateway")
                        .arg("run")
                        .stdin(std::process::Stdio::null())
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .spawn()
                    {
                        Ok(child) => {
                            let pid = adopt_daemon_child(child);
                            tracing::info!(pid, "daemon auto-started for REPL");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to auto-start daemon");
                        }
                    }
                    // Wait for socket to appear (max 5 seconds)
                    for _ in 0..50 {
                        if sock.exists() {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
            } else if daemon_autostart_disabled {
                tracing::debug!("daemon auto-start disabled by COWD_DISABLE_DAEMON_AUTOSTART");
            }
            tracing::debug!("starting TUI REPL");
            run_repl(
                model,
                session_id,
                allowed_tools,
                permission_mode,
                base_commit,
                reasoning_effort,
                allow_broad_cwd,
                yolo_mode,
            )?;
        }
        CliAction::Gateway {
            action,
            output_format,
        } => run_gateway_action(&action, output_format)?,
        CliAction::Install { systemd, path } => run_install(systemd, path.as_deref())?,
        CliAction::HelpTopic(topic) => print_help_topic(topic),
        CliAction::Help { output_format } => print_help(output_format)?,
    }
    Ok(())
}

fn run_gateway_action(
    action: &GatewayAction,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        GatewayAction::Start => {
            let status = server::get_server_status().map_err(|e| e.to_string())?;
            if status.is_some() {
                tracing::info!("gateway start: already running");
                println!("Gateway is already running");
                return Ok(());
            }
            setup_sigchld_handler();
            let exe =
                std::env::current_exe().map_err(|e| format!("cannot find own binary: {e}"))?;
            tracing::info!(binary = %exe.display(), "gateway start: spawning daemon");
            let mut child = spawn_gateway_daemon(&exe)?;
            wait_for_gateway_start(&mut child, Duration::from_secs(5))?;
            let pid = adopt_daemon_child(child);
            println!("Gateway started (pid: {pid})");
            tracing::info!(pid, "gateway daemon spawned");
            Ok(())
        }
        GatewayAction::Stop => {
            server::stop_server().map_err(|e| e.to_string())?;
            println!("Gateway stopped");
            tracing::info!("gateway stopped");
            Ok(())
        }
        GatewayAction::Status => {
            let status = server::get_server_status().map_err(|e| e.to_string())?;
            match output_format {
                CliOutputFormat::Text => match status {
                    Some(info) => println!(
                        "Gateway is running (pid: {}, address: {})",
                        info.pid, info.address
                    ),
                    None => println!("Gateway is not running"),
                },
                CliOutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "kind": "gateway-status",
                            "running": status.is_some(),
                            "pid": status.as_ref().map(|s| s.pid),
                            "address": status.as_ref().map(|s| &s.address),
                        })
                    );
                }
            }
            Ok(())
        }
        GatewayAction::Doctor => doctor::run_doctor(output_format),
        GatewayAction::Run => {
            if let Ok(Some(status)) = server::get_server_status() {
                tracing::info!(pid = status.pid, address = %status.address, "gateway foreground: existing gateway is already running");
                println!(
                    "Gateway is already running (pid: {}, address: {})",
                    status.pid, status.address
                );
                return Ok(());
            }
            let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            let loader = runtime::ConfigLoader::default_for(&cwd);
            let runtime_config = match loader.load() {
                Ok(cfg) => cfg,
                Err(e) => {
                    tracing::warn!("failed to load config, using defaults: {e}");
                    runtime::RuntimeConfig::empty()
                }
            };
            let api_server_platform = runtime_config
                .gateway()
                .platforms
                .iter()
                .find(|p| p.platform_type == "api_server" && p.enabled);
            let effective_host = api_server_platform
                .and_then(|p| p.extra.get("host"))
                .and_then(|h| h.as_str())
                .map(String::from)
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let effective_port = api_server_platform
                .and_then(|p| p.extra.get("port"))
                .and_then(|v| v.as_i64())
                .map(|n| n as u16)
                .unwrap_or(8642);
            let memory_config = build_memory_config(runtime_config.memory(), &cwd);
            let platform_configs = build_platform_configs(runtime_config.gateway());
            let runtime_config_json = runtime_config.as_json().as_object().map(|obj| {
                serde_json::Value::Object(
                    obj.iter()
                        .map(|(k, v)| (k.clone(), json_value_to_serde(v)))
                        .collect(),
                )
            });
            let cors_origins: Vec<String> = api_server_platform
                .and_then(|p| p.extra.get("cors_origins"))
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let auth_token: Option<String> =
                api_server_platform.and_then(gateway_auth_token_from_platform);
            // Ensure the unified session store is initialised before the
            // daemon starts so that the OnceLock is populated.
            let _ = get_unified_store();

            let daemon_config = daemon::DaemonConfig {
                http_addr: format!("{effective_host}:{effective_port}"),
                unix_sock_path: daemon_socket_path().display().to_string(),
                memory_config,
                platform_configs,
                runtime_config: runtime_config_json,
                webui_dir: runtime_config.gateway().webui_dir.clone(),
                cors_origins,
                auth_token,
                message_mirror: None,
            };
            let r2 = SHARED_RT.handle().clone();
            r2.block_on(async {
                daemon::run_daemon(daemon_config)
                    .await
                    .map_err(|e| e.to_string())
            })?;
            Ok(())
        }
        GatewayAction::Restart => {
            setup_sigchld_handler();
            server::stop_server().map_err(|e| e.to_string())?;
            tracing::info!("gateway restart: stopped, re-spawning");
            let exe =
                std::env::current_exe().map_err(|e| format!("cannot find own binary: {e}"))?;
            let mut child = spawn_gateway_daemon(&exe)?;
            wait_for_gateway_start(&mut child, Duration::from_secs(5))?;
            let pid = adopt_daemon_child(child);
            println!("Gateway restarted (pid: {pid})");
            tracing::info!(pid, "gateway restarted");
            Ok(())
        }
        GatewayAction::Logs => {
            let path = runtime::cowd_dirs::config_home_dir()
                .join("logs")
                .join("gateway-daemon.log");
            match std::fs::read_to_string(&path) {
                Ok(content) if !content.trim().is_empty() => {
                    let lines: Vec<&str> = content.lines().rev().take(80).collect();
                    for line in lines.into_iter().rev() {
                        println!("{line}");
                    }
                }
                Ok(_) => println!("Gateway log is empty: {}", path.display()),
                Err(error) => println!("Gateway log unavailable at {}: {error}", path.display()),
            }
            Ok(())
        }
        GatewayAction::Repair => {
            server::stop_server().ok();
            let path = runtime::cowd_dirs::config_home_dir()
                .join("logs")
                .join("gateway-daemon.log");
            println!("Gateway repair prepared a clean start state.");
            println!("Next: cowd gateway start");
            println!("Logs: {}", path.display());
            Ok(())
        }
        GatewayAction::Open => {
            let status = server::get_server_status().map_err(|e| e.to_string())?;
            let address = status
                .as_ref()
                .map(|s| s.address.clone())
                .unwrap_or_else(|| "127.0.0.1:8642".to_string());
            println!("Gateway WebUI: http://{address}/");
            Ok(())
        }
        GatewayAction::WechatQr => run_wechat_qr_login(),
    }
}

fn run_wechat_qr_login() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build runtime: {e}"))?;

    println!("WeChat QR login");
    println!("Use personal WeChat to scan and confirm in the mobile app.");

    let qr = rt
        .block_on(runtime::platform::wechat_ilink::request_wechat_qr_login(
            "3",
        ))
        .map_err(|e| format!("failed to create WeChat QR code: {e}"))?;

    println!();
    println!("Scan data:");
    println!("{}", qr.scan_data);
    println!();

    match qrcode::QrCode::new(qr.scan_data.as_bytes()) {
        Ok(code) => {
            let rendered = code
                .render::<qrcode::render::unicode::Dense1x2>()
                .quiet_zone(true)
                .build();
            println!("{rendered}");
        }
        Err(e) => {
            println!("Failed to render terminal QR code: {e}");
        }
    }

    println!("Waiting for scan confirmation...");
    let mut base_url = qr.base_url.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(480);
    while std::time::Instant::now() < deadline {
        let status = rt
            .block_on(runtime::platform::wechat_ilink::poll_wechat_qr_login(
                &qr.qrcode,
                Some(&base_url),
            ))
            .map_err(|e| format!("failed to poll WeChat QR status: {e}"))?;

        match status.status.as_str() {
            "wait" => {
                print!(".");
                let _ = io::stdout().flush();
            }
            "scaned" => {
                println!("\nScanned. Confirm login in WeChat.");
            }
            "scaned_but_redirect" => {
                if let Some(host) = status.redirect_host {
                    base_url = format!("https://{host}");
                    println!("\nRedirected to regional iLink host.");
                }
            }
            "confirmed" => {
                let credentials = status
                    .credentials
                    .ok_or("WeChat confirmed without credentials")?;
                let path =
                    runtime::platform::wechat_ilink::save_wechat_qr_account(&credentials, None)
                        .map_err(|e| format!("failed to save WeChat account: {e}"))?;
                println!();
                println!("WeChat connected.");
                println!("Account          {}", credentials.account_id);
                if let Some(user_id) = credentials.user_id.as_deref() {
                    println!("User             {user_id}");
                }
                println!("Stored           {}", path.display());
                println!(
                    "Gateway          restart with `cowd gateway restart` to activate the channel"
                );
                return Ok(());
            }
            "expired" => {
                return Err("WeChat QR code expired; rerun the WeChat platform setup flow".into());
            }
            other => {
                println!("\nStatus           {other}");
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    Err("WeChat QR login timed out".into())
}

fn should_bootstrap_for_action(action: &CliAction) -> bool {
    matches!(action, CliAction::Repl { .. })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GatewayAction {
    Start,
    Stop,
    Status,
    Doctor,
    Run,
    Restart,
    Logs,
    Repair,
    Open,
    WechatQr,
}

impl GatewayAction {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "status" => Some(Self::Status),
            "doctor" => Some(Self::Doctor),
            "run" => Some(Self::Run),
            "restart" => Some(Self::Restart),
            "logs" => Some(Self::Logs),
            "repair" => Some(Self::Repair),
            "open" => Some(Self::Open),
            "wechat-qr" => Some(Self::WechatQr),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliAction {
    DumpManifests {
        output_format: CliOutputFormat,
        manifests_dir: Option<PathBuf>,
    },
    BootstrapPlan {
        output_format: CliOutputFormat,
    },
    Agents {
        args: Option<String>,
        output_format: CliOutputFormat,
    },
    Mcp {
        args: Option<String>,
        output_format: CliOutputFormat,
    },
    Skills {
        args: Option<String>,
        output_format: CliOutputFormat,
    },
    Plugins {
        action: Option<String>,
        target: Option<String>,
        output_format: CliOutputFormat,
    },
    PrintSystemPrompt {
        cwd: PathBuf,
        date: String,
        output_format: CliOutputFormat,
    },
    Version {
        output_format: CliOutputFormat,
    },
    ResumeSession {
        session_path: PathBuf,
        commands: Vec<String>,
        output_format: CliOutputFormat,
    },
    Status {
        model: String,
        permission_mode: PermissionMode,
        output_format: CliOutputFormat,
    },
    Sandbox {
        output_format: CliOutputFormat,
    },

    Doctor {
        output_format: CliOutputFormat,
    },
    Setup {
        output_format: CliOutputFormat,
    },
    State {
        output_format: CliOutputFormat,
    },
    Init {
        output_format: CliOutputFormat,
    },
    Export {
        session_reference: String,
        output_path: Option<PathBuf>,
        output_format: CliOutputFormat,
    },
    ImportSession {
        path: PathBuf,
        output_format: CliOutputFormat,
    },
    Repl {
        model: String,
        session_id: Option<String>,
        allowed_tools: Option<AllowedToolSet>,
        permission_mode: PermissionMode,
        base_commit: Option<String>,
        reasoning_effort: Option<String>,
        allow_broad_cwd: bool,
        yolo_mode: bool,
    },
    Gateway {
        action: GatewayAction,
        output_format: CliOutputFormat,
    },
    Install {
        systemd: bool,
        path: Option<String>,
    },
    HelpTopic(LocalHelpTopic),
    // prompt-mode formatting is only supported for non-interactive runs
    Help {
        output_format: CliOutputFormat,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalHelpTopic {
    Status,
    Sandbox,
    Doctor,
    Setup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliOutputFormat {
    Text,
    Json,
}

impl CliOutputFormat {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(format!(
                "unsupported value for --output-format: {other} (expected text or json)"
            )),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn parse_args(args: &[String]) -> Result<CliAction, String> {
    let mut model = DEFAULT_MODEL.to_string();
    let mut output_format = CliOutputFormat::Text;
    let mut permission_mode_override = None;
    let mut wants_help = false;
    let mut wants_version = false;
    let mut allowed_tool_values = Vec::new();
    let mut base_commit: Option<String> = None;
    let mut session_id: Option<String> = None;
    let mut reasoning_effort: Option<String> = None;
    let mut allow_broad_cwd = false;
    let mut yolo_mode = false;
    let mut rest: Vec<String> = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--help" | "-h" if rest.is_empty() => {
                wants_help = true;
                index += 1;
            }
            "--help" | "-h"
                if !rest.is_empty()
                    && matches!(
                        rest[0].as_str(),
                        "prompt"
                            | "version"
                            | "state"
                            | "init"
                            | "export"
                            | "commit"
                            | "pr"
                            | "issue"
                    ) =>
            {
                // `--help` following a removed or local subcommand should show
                // top-level help instead. Subcommands that consume their own
                // args (agents, mcp, plugins, skills) and local help-topic
                // subcommands (status, sandbox, doctor) must NOT be intercepted
                // here — they handle --help in their own dispatch paths.
                wants_help = true;
                index += 1;
            }
            "--version" | "-V" => {
                wants_version = true;
                index += 1;
            }
            "--model" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --model".to_string())?;
                model = resolve_model_alias_with_config(value);
                index += 2;
            }
            flag if flag.starts_with("--model=") => {
                model = resolve_model_alias_with_config(&flag[8..]);
                index += 1;
            }
            "--output-format" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --output-format".to_string())?;
                output_format = CliOutputFormat::parse(value)?;
                index += 2;
            }
            "--permission-mode" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --permission-mode".to_string())?;
                permission_mode_override = Some(parse_permission_mode_arg(value)?);
                index += 2;
            }
            flag if flag.starts_with("--output-format=") => {
                output_format = CliOutputFormat::parse(&flag[16..])?;
                index += 1;
            }
            flag if flag.starts_with("--permission-mode=") => {
                permission_mode_override = Some(parse_permission_mode_arg(&flag[18..])?);
                index += 1;
            }
            "--dangerously-skip-permissions" | "--solo" => {
                permission_mode_override = Some(PermissionMode::DangerFullAccess);
                index += 1;
            }
            "--yolo" => {
                permission_mode_override = Some(PermissionMode::DangerFullAccess);
                yolo_mode = true;
                index += 1;
            }
            "--base-commit" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --base-commit".to_string())?;
                base_commit = Some(value.clone());
                index += 2;
            }
            flag if flag.starts_with("--base-commit=") => {
                base_commit = Some(flag[14..].to_string());
                index += 1;
            }
            "--session" if rest.is_empty() => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --session".to_string())?;
                session_id = Some(value.clone());
                index += 2;
            }
            flag if rest.is_empty() && flag.starts_with("--session=") => {
                session_id = Some(flag[10..].to_string());
                index += 1;
            }
            "--reasoning-effort" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --reasoning-effort".to_string())?;
                if !matches!(value.as_str(), "low" | "medium" | "high") {
                    return Err(format!(
                        "invalid value for --reasoning-effort: '{value}'; must be low, medium, or high"
                    ));
                }
                reasoning_effort = Some(value.clone());
                index += 2;
            }
            flag if flag.starts_with("--reasoning-effort=") => {
                let value = &flag[19..];
                if !matches!(value, "low" | "medium" | "high") {
                    return Err(format!(
                        "invalid value for --reasoning-effort: '{value}'; must be low, medium, or high"
                    ));
                }
                reasoning_effort = Some(value.to_string());
                index += 1;
            }
            "--allow-broad-cwd" => {
                allow_broad_cwd = true;
                index += 1;
            }
            "--compact" => {
                return Err(
                    "--compact was only supported by removed one-shot prompt mode; start the TUI with `cowd`."
                        .to_string(),
                );
            }
            "--tui" => {
                index += 1;
            }

            "--print" => {
                // Legacy compat: --print makes output non-interactive
                output_format = CliOutputFormat::Text;
                index += 1;
            }
            "--resume" if rest.is_empty() => {
                rest.push("--resume".to_string());
                index += 1;
            }
            flag if rest.is_empty() && flag.starts_with("--resume=") => {
                rest.push("--resume".to_string());
                rest.push(flag[9..].to_string());
                index += 1;
            }
            "--allowedTools" | "--allowed-tools" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --allowedTools".to_string())?;
                allowed_tool_values.push(value.clone());
                index += 2;
            }
            flag if flag.starts_with("--allowedTools=") => {
                allowed_tool_values.push(flag[15..].to_string());
                index += 1;
            }
            flag if flag.starts_with("--allowed-tools=") => {
                allowed_tool_values.push(flag[16..].to_string());
                index += 1;
            }
            other if rest.is_empty() && other.starts_with('-') => {
                return Err(suggestions::format_unknown_option(other));
            }
            other => {
                rest.push(other.to_string());
                index += 1;
            }
        }
    }

    if wants_help {
        return Ok(CliAction::Help { output_format });
    }

    if wants_version {
        return Ok(CliAction::Version { output_format });
    }

    let allowed_tools = normalize_allowed_tools(&allowed_tool_values)?;

    if rest.is_empty() {
        let permission_mode = permission_mode_override.unwrap_or_else(default_permission_mode);
        // When stdin is not a terminal (pipe/redirect) and no prompt is given on the
        // command line, read stdin as the prompt and dispatch as a one-shot Prompt
        // rather than starting the interactive REPL (which would consume the pipe and
        // print the startup banner, then exit without sending anything to the API).
        if !std::io::stdin().is_terminal() {
            let mut buf = String::new();
            let _ = std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf);
            let piped = buf.trim().to_string();
            if !piped.is_empty() {
                tracing::warn!(
                    "piped stdin ignored — v0.6.2 removed one-shot CLI prompt; use TUI instead"
                );
            }
        }
        return Ok(CliAction::Repl {
            model,
            session_id,
            allowed_tools,
            permission_mode,
            base_commit,
            reasoning_effort: reasoning_effort.clone(),
            allow_broad_cwd,
            yolo_mode,
        });
    }
    if rest.first().map(String::as_str) == Some("--resume") {
        let permission_mode = permission_mode_override.unwrap_or_else(default_permission_mode);
        return parse_resume_args(
            &rest[1..],
            output_format,
            model,
            allowed_tools,
            permission_mode,
            base_commit,
            reasoning_effort.clone(),
            allow_broad_cwd,
            yolo_mode,
        );
    }
    if let Some(action) = parse_local_help_action(&rest) {
        return action;
    }
    if let Some(action) =
        parse_single_word_command_alias(&rest, &model, permission_mode_override, output_format)
    {
        return action;
    }

    let permission_mode = permission_mode_override.unwrap_or_else(default_permission_mode);

    match rest[0].as_str() {
        "dump-manifests" => parse_dump_manifests_args(&rest[1..], output_format),
        "bootstrap-plan" => Ok(CliAction::BootstrapPlan { output_format }),
        "agents" => Ok(CliAction::Agents {
            args: join_optional_args(&rest[1..]),
            output_format,
        }),
        "mcp" => parse_mcp_args(&rest[1..], output_format),
        "skills" => {
            let args = join_optional_args(&rest[1..]);
            match classify_skills_slash_command(args.as_deref()) {
                SkillSlashDispatch::Invoke(_prompt) => Ok(CliAction::Repl {
                    model,
                    session_id: session_id.clone(),
                    allowed_tools,
                    permission_mode,
                    base_commit,
                    reasoning_effort: reasoning_effort.clone(),
                    allow_broad_cwd,
                    yolo_mode,
                }),
                SkillSlashDispatch::Local => Ok(CliAction::Skills {
                    args,
                    output_format,
                }),
            }
        }
        "system-prompt" => parse_system_prompt_args(&rest[1..], output_format),
        "login" | "logout" => Err(removed_auth_surface_error(rest[0].as_str())),
        "setup" => {
            if rest.len() > 1 {
                return Err("unexpected arguments for setup. Usage: cowd setup".to_string());
            }
            Ok(CliAction::Setup { output_format })
        }
        "init" => Ok(CliAction::Init { output_format }),
        "export" => parse_export_args(&rest[1..], output_format),
        "import-session" => {
            let path = rest.get(1).ok_or_else(|| {
                "missing session file. Usage: cowd import-session <path>".to_string()
            })?;
            if rest.len() > 2 {
                return Err(
                    "unexpected arguments for import-session. Usage: cowd import-session <path>"
                        .to_string(),
                );
            }
            Ok(CliAction::ImportSession {
                path: PathBuf::from(path),
                output_format,
            })
        }
        "install" => parse_install_args(&rest[1..], output_format),
        "gateway" => parse_gateway_args(&rest[1..], output_format),
        removed if removed == REMOVED_PROMPT_SUBCOMMAND => Err(
            "one-shot text mode was removed. Start the TUI with `cowd` or use Gateway/WebUI for chat."
                .to_string(),
        ),

        other if other.starts_with('/') => Err(
            "top-level slash commands were removed. Start the TUI with `cowd` and use slash commands there."
                .to_string(),
        ),
        _other => Ok(CliAction::Repl {
            model,
            session_id,
            allowed_tools,
            permission_mode,
            base_commit,
            reasoning_effort: reasoning_effort.clone(),
            allow_broad_cwd,
            yolo_mode,
        }),
    }
}

fn parse_local_help_action(rest: &[String]) -> Option<Result<CliAction, String>> {
    if rest.len() != 2 || !is_help_flag(&rest[1]) {
        return None;
    }

    let topic = match rest[0].as_str() {
        "status" => LocalHelpTopic::Status,
        "sandbox" => LocalHelpTopic::Sandbox,
        "doctor" => LocalHelpTopic::Doctor,
        "setup" => LocalHelpTopic::Setup,
        _ => return None,
    };
    Some(Ok(CliAction::HelpTopic(topic)))
}

fn is_help_flag(value: &str) -> bool {
    matches!(value, "--help" | "-h")
}

fn parse_single_word_command_alias(
    rest: &[String],
    model: &str,
    permission_mode_override: Option<PermissionMode>,
    output_format: CliOutputFormat,
) -> Option<Result<CliAction, String>> {
    if rest.len() != 1 {
        return None;
    }

    match rest[0].as_str() {
        "help" => Some(Ok(CliAction::Help { output_format })),
        "version" => Some(Ok(CliAction::Version { output_format })),
        "status" => Some(Ok(CliAction::Status {
            model: model.to_string(),
            permission_mode: permission_mode_override.unwrap_or_else(default_permission_mode),
            output_format,
        })),
        "sandbox" => Some(Ok(CliAction::Sandbox { output_format })),
        "doctor" => Some(Ok(CliAction::Doctor { output_format })),
        "setup" => Some(Ok(CliAction::Setup { output_format })),
        "state" => Some(Ok(CliAction::State { output_format })),
        other => bare_slash_command_guidance(other).map(Err),
    }
}

fn bare_slash_command_guidance(command_name: &str) -> Option<String> {
    if matches!(
        command_name,
        "dump-manifests"
            | "bootstrap-plan"
            | "agents"
            | "mcp"
            | "skills"
            | "system-prompt"
            | "init"
            | "prompt"
            | "export"
    ) {
        return None;
    }
    let slash_command = slash_command_specs()
        .iter()
        .find(|spec| spec.name == command_name)?;
    let session_hint = if slash_command.resume_supported {
        " Use `cowd --resume <session-id|latest>` first when you need a saved session."
    } else {
        ""
    };
    let guidance = format!(
        "`cowd {command_name}` is a slash command. Start `cowd` and run `/{command_name}` inside the TUI.{session_hint}"
    );
    Some(guidance)
}

fn removed_auth_surface_error(command_name: &str) -> String {
    format!(
        "`cowd {command_name}` has been removed. Set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN instead."
    )
}

fn try_resolve_bare_skill_prompt(cwd: &Path, trimmed: &str) -> Option<String> {
    let bare_first_token = trimmed.split_whitespace().next().unwrap_or_default();
    let looks_like_skill_name = !bare_first_token.is_empty()
        && !bare_first_token.starts_with('/')
        && bare_first_token
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_');
    if !looks_like_skill_name {
        return None;
    }
    match resolve_skill_invocation(cwd, Some(trimmed)) {
        Ok(SkillSlashDispatch::Invoke(prompt)) => Some(prompt),
        _ => None,
    }
}

fn join_optional_args(args: &[String]) -> Option<String> {
    let joined = args.join(" ");
    let trimmed = joined.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn resolve_model_alias_with_config(model: &str) -> String {
    let trimmed = model.trim();
    let config_aliases = config_aliases_for_current_dir();
    let resolver = runtime::ModelResolver::new(config_aliases);
    resolver.resolve(trimmed)
}

fn config_aliases_for_current_dir() -> std::collections::HashMap<String, String> {
    let cwd = match env::current_dir() {
        Ok(cwd) => cwd,
        Err(_) => return std::collections::HashMap::new(),
    };
    let loader = ConfigLoader::default_for(&cwd);
    match loader.load() {
        Ok(config) => config.aliases().clone().into_iter().collect(),
        Err(_) => std::collections::HashMap::new(),
    }
}

fn daemon_socket_path() -> PathBuf {
    std::env::var_os("COWD_DAEMON_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/cowd.sock"))
}

fn normalize_allowed_tools(values: &[String]) -> Result<Option<AllowedToolSet>, String> {
    if values.is_empty() {
        return Ok(None);
    }
    current_tool_registry()?.normalize_allowed_tools(values)
}

fn current_tool_registry() -> Result<GlobalToolRegistry, String> {
    let cwd = env::current_dir().map_err(|error| error.to_string())?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load().map_err(|error| error.to_string())?;
    let state = build_runtime_plugin_state_with_loader(&cwd, &loader, &runtime_config)
        .map_err(|error| error.to_string())?;
    let registry = state.tool_registry.clone();
    if let Some(mcp_state) = state.mcp_state {
        mcp_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shutdown()
            .map_err(|error| error.to_string())?;
    }
    Ok(registry)
}

fn parse_permission_mode_arg(value: &str) -> Result<PermissionMode, String> {
    normalize_permission_mode(value)
        .ok_or_else(|| {
            format!(
                "unsupported permission mode '{value}'. Use read-only, workspace-write, or danger-full-access."
            )
        })
        .map(permission_mode_from_label)
}

fn permission_mode_from_label(mode: &str) -> PermissionMode {
    cli::permission_mode_from_label(mode)
}

fn permission_mode_from_resolved(mode: ResolvedPermissionMode) -> PermissionMode {
    cli::permission_mode_from_resolved(mode)
}

fn default_permission_mode() -> PermissionMode {
    env::var("COWD_PERMISSION_MODE")
        .ok()
        .as_deref()
        .and_then(normalize_permission_mode)
        .map(permission_mode_from_label)
        .or_else(config_permission_mode_for_current_dir)
        .unwrap_or(PermissionMode::WorkspaceWrite)
}

fn config_permission_mode_for_current_dir() -> Option<PermissionMode> {
    let cwd = env::current_dir().ok()?;
    let loader = ConfigLoader::default_for(&cwd);
    loader
        .load()
        .ok()?
        .permission_mode()
        .map(permission_mode_from_resolved)
}

fn config_model_for_current_dir() -> Option<String> {
    let cwd = env::current_dir().ok()?;
    let loader = ConfigLoader::default_for(&cwd);
    loader.load().ok()?.model().map(ToOwned::to_owned)
}

fn resolve_repl_model(cli_model: String) -> String {
    if cli_model != DEFAULT_MODEL {
        return cli_model;
    }
    // Config file takes priority over environment variables.
    if let Some(config_model) = config_model_for_current_dir() {
        return resolve_model_alias_with_config(&config_model);
    }
    // Environment variables serve as fallback: COWD_MODEL (new) or ANTHROPIC_MODEL (legacy).
    if let Some(env_model) = env::var("COWD_MODEL")
        .ok()
        .or_else(|| env::var("ANTHROPIC_MODEL").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return resolve_model_alias_with_config(&env_model);
    }
    cli_model
}

fn provider_label(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Anthropic => "anthropic",
        ProviderKind::Xai => "xai",
        ProviderKind::OpenAi => "openai",
    }
}

fn format_connected_line(model: &str) -> String {
    let provider = provider_label(detect_provider_kind(model));
    format!("Connected: {model} via {provider}")
}

fn filter_tool_specs(
    tool_registry: &GlobalToolRegistry,
    allowed_tools: Option<&AllowedToolSet>,
) -> Vec<ToolDefinition> {
    tool_registry.definitions(allowed_tools)
}

fn parse_system_prompt_args(
    args: &[String],
    output_format: CliOutputFormat,
) -> Result<CliAction, String> {
    let mut cwd = env::current_dir().map_err(|error| error.to_string())?;
    let mut date = DEFAULT_DATE.to_string();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--cwd" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --cwd".to_string())?;
                cwd = PathBuf::from(value);
                index += 2;
            }
            "--date" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --date".to_string())?;
                date.clone_from(value);
                index += 2;
            }
            other => return Err(format!("unknown system-prompt option: {other}")),
        }
    }

    Ok(CliAction::PrintSystemPrompt {
        cwd,
        date,
        output_format,
    })
}

fn parse_install_args(
    args: &[String],
    _output_format: CliOutputFormat,
) -> Result<CliAction, String> {
    let mut systemd = false;
    let mut path = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--systemd" => {
                systemd = true;
                i += 1;
            }
            "--path" if i + 1 < args.len() => {
                path = Some(args[i + 1].clone());
                i += 2;
            }
            other => return Err(format!("unknown install flag: {other}")),
        }
    }
    Ok(CliAction::Install { systemd, path })
}

fn parse_export_args(args: &[String], output_format: CliOutputFormat) -> Result<CliAction, String> {
    let mut session_reference = LATEST_SESSION_REFERENCE.to_string();
    let mut output_path: Option<PathBuf> = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--session" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --session".to_string())?;
                session_reference.clone_from(value);
                index += 2;
            }
            flag if flag.starts_with("--session=") => {
                session_reference = flag[10..].to_string();
                index += 1;
            }
            "--output" | "-o" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| format!("missing value for {}", args[index]))?;
                output_path = Some(PathBuf::from(value));
                index += 2;
            }
            flag if flag.starts_with("--output=") => {
                output_path = Some(PathBuf::from(&flag[9..]));
                index += 1;
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown export option: {other}"));
            }
            other if output_path.is_none() => {
                output_path = Some(PathBuf::from(other));
                index += 1;
            }
            other => {
                return Err(format!("unexpected export argument: {other}"));
            }
        }
    }

    Ok(CliAction::Export {
        session_reference,
        output_path,
        output_format,
    })
}

fn parse_gateway_args(
    args: &[String],
    output_format: CliOutputFormat,
) -> Result<CliAction, String> {
    let action_str = args.first().ok_or_else(|| {
        "gateway requires a subcommand: start, stop, restart, status, doctor, logs, repair, or open".to_string()
    })?;
    let action = GatewayAction::from_str(action_str).ok_or_else(|| {
        format!(
            "unknown gateway subcommand: {action_str}. Expected start, stop, restart, status, doctor, logs, repair, or open"
        )
    })?;
    Ok(CliAction::Gateway {
        action,
        output_format,
    })
}

fn parse_mcp_args(args: &[String], output_format: CliOutputFormat) -> Result<CliAction, String> {
    if matches!(args.first().map(String::as_str), Some("serve")) {
        return Err(
            "`cowd mcp serve` was removed from the CLI surface. Start `cowd gateway start` and manage MCP through Gateway/WebUI or the TUI."
                .to_string(),
        );
    }
    Ok(CliAction::Mcp {
        args: join_optional_args(args),
        output_format,
    })
}

fn parse_dump_manifests_args(
    args: &[String],
    output_format: CliOutputFormat,
) -> Result<CliAction, String> {
    let mut manifests_dir: Option<PathBuf> = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--manifests-dir" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| String::from("--manifests-dir requires a path"))?;
            manifests_dir = Some(PathBuf::from(value));
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--manifests-dir=") {
            if value.is_empty() {
                return Err(String::from("--manifests-dir requires a path"));
            }
            manifests_dir = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        return Err(format!("unknown dump-manifests option: {arg}"));
    }

    Ok(CliAction::DumpManifests {
        output_format,
        manifests_dir,
    })
}

#[allow(clippy::too_many_arguments)]
fn parse_resume_args(
    args: &[String],
    output_format: CliOutputFormat,
    model: String,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    base_commit: Option<String>,
    reasoning_effort: Option<String>,
    allow_broad_cwd: bool,
    yolo_mode: bool,
) -> Result<CliAction, String> {
    let removed_resume_commands = "`cowd --resume ... /command` was removed from the CLI surface. Start `cowd --resume <session-id|latest>` and run slash commands inside the TUI.";
    let session_path = match args.first() {
        None => PathBuf::from(LATEST_SESSION_REFERENCE),
        Some(first) if looks_like_slash_command_token(first) => {
            return Err(removed_resume_commands.to_string());
        }
        Some(first) => {
            if args.len() > 1 {
                return Err(removed_resume_commands.to_string());
            }
            PathBuf::from(first)
        }
    };

    if output_format != CliOutputFormat::Text {
        return Err(
            "`--output-format` is not supported with `--resume`; start the TUI with `cowd --resume <session-id|latest>`."
                .to_string(),
        );
    }

    Ok(CliAction::Repl {
        model,
        session_id: Some(session_path.display().to_string()),
        allowed_tools,
        permission_mode,
        base_commit,
        reasoning_effort,
        allow_broad_cwd,
        yolo_mode,
    })
}

fn looks_like_slash_command_token(token: &str) -> bool {
    let trimmed = token.trim_start();
    let Some(name) = trimmed.strip_prefix('/').and_then(|value| {
        value
            .split_whitespace()
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }) else {
        return false;
    };

    slash_command_specs()
        .iter()
        .any(|spec| spec.name == name || spec.aliases.contains(&name))
}

fn dump_manifests(
    manifests_dir: Option<&Path>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    dump_manifests_at_path(&workspace_dir, manifests_dir, output_format)
}

const DUMP_MANIFESTS_OVERRIDE_HINT: &str = "Hint: set COWD_UPSTREAM=/path/to/upstream or pass `cowd dump-manifests --manifests-dir /path/to/upstream`.";

// Internal function for testing that accepts a workspace directory path.
fn dump_manifests_at_path(
    workspace_dir: &std::path::Path,
    manifests_dir: Option<&Path>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let paths = if let Some(dir) = manifests_dir {
        let resolved = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
        UpstreamPaths::from_repo_root(resolved)
    } else {
        // Surface the resolved path in the error so users can diagnose missing
        // manifest files without guessing what path the binary expected.
        let resolved = workspace_dir
            .canonicalize()
            .unwrap_or_else(|_| workspace_dir.to_path_buf());
        UpstreamPaths::from_workspace_dir(&resolved)
    };

    let source_root = paths.repo_root();
    if !source_root.exists() {
        return Err(format!(
            "Manifest source directory does not exist.\n  looked in: {}\n  {DUMP_MANIFESTS_OVERRIDE_HINT}",
            source_root.display(),
        )
        .into());
    }

    let required_paths = [
        ("src/commands.ts", paths.commands_path()),
        ("src/tools.ts", paths.tools_path()),
        ("src/entrypoints/cli.tsx", paths.cli_path()),
    ];
    let missing = required_paths
        .iter()
        .filter_map(|(label, path)| (!path.is_file()).then_some(*label))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "Manifest source files are missing.\n  repo root: {}\n  missing: {}\n  {DUMP_MANIFESTS_OVERRIDE_HINT}",
            source_root.display(),
            missing.join(", "),
        )
        .into());
    }

    match extract_manifest(&paths) {
        Ok(manifest) => {
            match output_format {
                CliOutputFormat::Text => {
                    println!("commands: {}", manifest.commands.entries().len());
                    println!("tools: {}", manifest.tools.entries().len());
                    println!("bootstrap phases: {}", manifest.bootstrap.phases().len());
                }
                CliOutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "kind": "dump-manifests",
                        "commands": manifest.commands.entries().len(),
                        "tools": manifest.tools.entries().len(),
                        "bootstrap_phases": manifest.bootstrap.phases().len(),
                    }))?
                ),
            }
            Ok(())
        }
        Err(error) => Err(format!(
            "failed to extract manifests: {error}\n  looked in: {path}\n  {DUMP_MANIFESTS_OVERRIDE_HINT}",
            path = paths.repo_root().display()
        )
        .into()),
    }
}

fn print_bootstrap_plan(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let phases = runtime::BootstrapPlan::claude_code_default()
        .phases()
        .iter()
        .map(|phase| format!("{phase:?}"))
        .collect::<Vec<_>>();
    match output_format {
        CliOutputFormat::Text => {
            for phase in &phases {
                println!("- {phase}");
            }
        }
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "kind": "bootstrap-plan",
                "phases": phases,
            }))?
        ),
    }
    Ok(())
}

fn print_system_prompt(
    cwd: PathBuf,
    date: String,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let sections = load_system_prompt(cwd, date, env::consts::OS, "unknown")?;
    let message = sections.join(
        "

",
    );
    match output_format {
        CliOutputFormat::Text => println!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "kind": "system-prompt",
                "message": message,
                "sections": sections,
            }))?
        ),
    }
    Ok(())
}

fn print_version(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    match output_format {
        CliOutputFormat::Text => println!("{}", render_version_report()),
        CliOutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&version_json_value())?);
        }
    }
    Ok(())
}

fn version_json_value() -> serde_json::Value {
    json!({
        "kind": "version",
        "message": render_version_report(),
        "version": VERSION,
        "git_sha": GIT_SHA,
        "target": BUILD_TARGET,
    })
}

#[allow(clippy::too_many_lines)]
fn resume_session(session_path: &Path, commands: &[String], output_format: CliOutputFormat) {
    let session_reference = session_path.display().to_string();
    let (handle, session) = match load_session_reference(&session_reference) {
        Ok(loaded) => loaded,
        Err(error) => {
            if output_format == CliOutputFormat::Json {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "type": "error",
                        "error": format!("failed to restore session: {error}"),
                    })
                );
            } else {
                eprintln!("failed to restore session: {error}");
            }
            std::process::exit(1);
        }
    };
    let mut resolved_path = handle.path.clone();

    if commands.is_empty() {
        if output_format == CliOutputFormat::Json {
            println!(
                "{}",
                serde_json::json!({
                    "kind": "restored",
                    "session_id": session.session_id,
                    "path": handle.path.display().to_string(),
                    "message_count": session.messages.len(),
                })
            );
        } else {
            println!(
                "Restored session from {} ({} messages).",
                handle.path.display(),
                session.messages.len()
            );
        }
        return;
    }

    let mut session = session;
    for raw_command in commands {
        // Intercept spec commands that have no parse arm before calling
        // SlashCommand::parse — they return Err(SlashCommandParseError) which
        // formats as the confusing circular "Did you mean /X?" message.
        // STUB_COMMANDS covers both completions-filtered stubs and parse-less
        // spec entries; treat both as unsupported in resume mode.
        {
            let cmd_root = raw_command
                .trim_start_matches('/')
                .split_whitespace()
                .next()
                .unwrap_or("");
            if STUB_COMMANDS.contains(&cmd_root) {
                if output_format == CliOutputFormat::Json {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "type": "error",
                            "error": format!("/{cmd_root} is not yet implemented in this build"),
                            "command": raw_command,
                        })
                    );
                } else {
                    eprintln!("/{cmd_root} is not yet implemented in this build");
                }
                std::process::exit(2);
            }
        }
        let command = match SlashCommand::parse(raw_command) {
            Ok(Some(command)) => command,
            Ok(None) => {
                if output_format == CliOutputFormat::Json {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "type": "error",
                            "error": format!("unsupported resumed command: {raw_command}"),
                            "command": raw_command,
                        })
                    );
                } else {
                    eprintln!("unsupported resumed command: {raw_command}");
                }
                std::process::exit(2);
            }
            Err(error) => {
                if output_format == CliOutputFormat::Json {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "type": "error",
                            "error": error.to_string(),
                            "command": raw_command,
                        })
                    );
                } else {
                    eprintln!("{error}");
                }
                std::process::exit(2);
            }
        };
        match run_resume_command(&resolved_path, &session, &command) {
            Ok(ResumeCommandOutcome {
                session: next_session,
                session_path,
                message,
                json,
            }) => {
                session = next_session;
                if let Some(path) = session_path {
                    resolved_path = path;
                }
                if let Ok(store) = get_unified_store() {
                    if let Err(error) = sync_cli_session_to_unified_store(
                        store,
                        &handle,
                        session.model.as_deref(),
                        &session,
                    ) {
                        tracing::warn!(%error, "failed to sync resumed session to SQLite");
                    }
                }
                if output_format == CliOutputFormat::Json {
                    if let Some(value) = json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&value)
                                .expect("resume command json output")
                        );
                    } else if let Some(message) = message {
                        println!("{message}");
                    }
                } else if let Some(message) = message {
                    println!("{message}");
                }
            }
            Err(error) => {
                if output_format == CliOutputFormat::Json {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "type": "error",
                            "error": error.to_string(),
                            "command": raw_command,
                        })
                    );
                } else {
                    eprintln!("{error}");
                }
                std::process::exit(2);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResumeCommandOutcome {
    session: Session,
    session_path: Option<PathBuf>,
    message: Option<String>,
    json: Option<serde_json::Value>,
}

impl ResumeCommandOutcome {
    fn new(session: Session, message: Option<String>, json: Option<serde_json::Value>) -> Self {
        Self {
            session,
            session_path: None,
            message,
            json,
        }
    }

    fn with_session_path(mut self, session_path: PathBuf) -> Self {
        self.session_path = Some(session_path);
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StatusContext {
    cwd: PathBuf,
    session_path: Option<PathBuf>,
    session_id: Option<String>,
    session_store: String,
    loaded_config_files: usize,
    discovered_config_files: usize,
    memory_file_count: usize,
    project_root: Option<PathBuf>,
    git_branch: Option<String>,
    git_summary: GitWorkspaceSummary,
    sandbox_status: runtime::SandboxStatus,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StatusUsage {
    message_count: usize,
    turns: u32,
    latest: TokenUsage,
    cumulative: TokenUsage,
    estimated_tokens: usize,
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct GitWorkspaceSummary {
    changed_files: usize,
    staged_files: usize,
    unstaged_files: usize,
    untracked_files: usize,
    conflicted_files: usize,
}

impl GitWorkspaceSummary {
    fn is_clean(self) -> bool {
        self.changed_files == 0
    }

    fn headline(self) -> String {
        if self.is_clean() {
            "clean".to_string()
        } else {
            let mut details = Vec::new();
            if self.staged_files > 0 {
                details.push(format!("{} staged", self.staged_files));
            }
            if self.unstaged_files > 0 {
                details.push(format!("{} unstaged", self.unstaged_files));
            }
            if self.untracked_files > 0 {
                details.push(format!("{} untracked", self.untracked_files));
            }
            if self.conflicted_files > 0 {
                details.push(format!("{} conflicted", self.conflicted_files));
            }
            format!(
                "dirty · {} files · {}",
                self.changed_files,
                details.join(", ")
            )
        }
    }
}

#[cfg(test)]
fn format_unknown_slash_command_message(name: &str) -> String {
    let suggestions = suggestions::suggest_slash_commands(name);
    let mut message = format!("unknown slash command: /{name}.");
    if !suggestions.is_empty() {
        message.push_str(" Did you mean ");
        message.push_str(&suggestions.join(", "));
        message.push('?');
    }
    if let Some(note) = suggestions::omc_compatibility_note_for_unknown_slash_command(name) {
        message.push(' ');
        message.push_str(note);
    }
    message.push_str(" Use /help to list available commands.");
    message
}

fn format_model_report(model: &str, message_count: usize, turns: u32) -> String {
    format!(
        "Model
  Current model    {model}
  Session messages {message_count}
  Session turns    {turns}

Usage
  Inspect current model with /model
  Switch models with /model <name>"
    )
}

fn format_model_switch_report(previous: &str, next: &str, message_count: usize) -> String {
    format!(
        "Model updated
  Previous         {previous}
  Current          {next}
  Preserved msgs   {message_count}"
    )
}

fn format_permissions_report(mode: &str) -> String {
    let modes = [
        ("read-only", "Read/search tools only", mode == "read-only"),
        (
            "workspace-write",
            "Edit files inside the workspace",
            mode == "workspace-write",
        ),
        (
            "danger-full-access",
            "Unrestricted tool access",
            mode == "danger-full-access",
        ),
    ]
    .into_iter()
    .map(|(name, description, is_current)| {
        let marker = if is_current {
            "● current"
        } else {
            "○ available"
        };
        format!("  {name:<18} {marker:<11} {description}")
    })
    .collect::<Vec<_>>()
    .join(
        "
",
    );

    format!(
        "Permissions
  Active mode      {mode}
  Mode status      live session default

Modes
{modes}

Usage
  Inspect current mode with /permissions
  Switch modes with /permissions <mode>"
    )
}

fn format_permissions_switch_report(previous: &str, next: &str) -> String {
    format!(
        "Permissions updated
  Result           mode switched
  Previous mode    {previous}
  Active mode      {next}
  Applies to       subsequent tool calls
  Usage            /permissions to inspect current mode"
    )
}

fn format_cost_report(usage: TokenUsage) -> String {
    format!(
        "Cost
  Input tokens     {}
  Output tokens    {}
  Cache create     {}
  Cache read       {}
  Total tokens     {}",
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        usage.total_tokens(),
    )
}

fn format_resume_report(session_path: &str, message_count: usize, turns: u32) -> String {
    format!(
        "Session resumed
  Session          {session_path}
  Messages         {message_count}
  Turns            {turns}"
    )
}

fn render_resume_usage() -> String {
    format!(
        "Resume
  Usage            /resume <session-id|{LATEST_SESSION_REFERENCE}>
  Store            SQLite session store
  Import           cowd import-session <local.jsonl>
  Tip              use /session list to inspect saved sessions and local import candidates"
    )
}

fn format_compact_report(removed: usize, resulting_messages: usize, skipped: bool) -> String {
    if skipped {
        format!(
            "Compact
  Result           skipped
  Reason           session below compaction threshold
  Messages kept    {resulting_messages}"
        )
    } else {
        format!(
            "Compact
  Result           compacted
  Messages removed {removed}
  Messages kept    {resulting_messages}"
        )
    }
}

fn format_auto_compaction_notice(removed: usize) -> String {
    format!("[auto-compacted: removed {removed} messages]")
}

fn parse_git_status_metadata(status: Option<&str>) -> (Option<PathBuf>, Option<String>) {
    parse_git_status_metadata_for(
        &env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        status,
    )
}

fn parse_git_status_branch(status: Option<&str>) -> Option<String> {
    let status = status?;
    let first_line = status.lines().next()?;
    let line = first_line.strip_prefix("## ")?;
    if line.starts_with("HEAD") {
        return Some("detached HEAD".to_string());
    }
    let branch = line.split(['.', ' ']).next().unwrap_or_default().trim();
    if branch.is_empty() {
        None
    } else {
        Some(branch.to_string())
    }
}

fn parse_git_workspace_summary(status: Option<&str>) -> GitWorkspaceSummary {
    let mut summary = GitWorkspaceSummary::default();
    let Some(status) = status else {
        return summary;
    };

    for line in status.lines() {
        if line.starts_with("## ") || line.trim().is_empty() {
            continue;
        }

        summary.changed_files += 1;
        let mut chars = line.chars();
        let index_status = chars.next().unwrap_or(' ');
        let worktree_status = chars.next().unwrap_or(' ');

        if index_status == '?' && worktree_status == '?' {
            summary.untracked_files += 1;
            continue;
        }

        if index_status != ' ' {
            summary.staged_files += 1;
        }
        if worktree_status != ' ' {
            summary.unstaged_files += 1;
        }
        if (matches!(index_status, 'U' | 'A') && matches!(worktree_status, 'U' | 'A'))
            || index_status == 'U'
            || worktree_status == 'U'
        {
            summary.conflicted_files += 1;
        }
    }

    summary
}

fn resolve_git_branch_for(cwd: &Path) -> Option<String> {
    let branch = run_git_capture_in(cwd, &["branch", "--show-current"])?;
    let branch = branch.trim();
    if !branch.is_empty() {
        return Some(branch.to_string());
    }

    let fallback = run_git_capture_in(cwd, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let fallback = fallback.trim();
    if fallback.is_empty() {
        None
    } else if fallback == "HEAD" {
        Some("detached HEAD".to_string())
    } else {
        Some(fallback.to_string())
    }
}

fn run_git_capture_in(cwd: &Path, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn find_git_root_in(cwd: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Err("not a git repository".into());
    }
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    if path.is_empty() {
        return Err("empty git root".into());
    }
    Ok(PathBuf::from(path))
}

fn parse_git_status_metadata_for(
    cwd: &Path,
    status: Option<&str>,
) -> (Option<PathBuf>, Option<String>) {
    let branch = resolve_git_branch_for(cwd).or_else(|| parse_git_status_branch(status));
    let project_root = find_git_root_in(cwd).ok();
    (project_root, branch)
}

#[allow(clippy::too_many_lines)]
fn run_resume_command(
    session_path: &Path,
    session: &Session,
    command: &SlashCommand,
) -> Result<ResumeCommandOutcome, Box<dyn std::error::Error>> {
    match command {
        SlashCommand::Help => Ok(ResumeCommandOutcome {
            session: session.clone(),
            session_path: None,
            message: Some(render_repl_help()),
            json: Some(serde_json::json!({ "kind": "help", "text": render_repl_help() })),
        }),
        SlashCommand::Compact => {
            let result = runtime::compact_session(
                session,
                CompactionConfig {
                    max_estimated_tokens: 0,
                    ..CompactionConfig::default()
                },
            );
            let removed = result.removed_message_count;
            let kept = result.compacted_session.messages.len();
            let skipped = removed == 0;
            Ok(ResumeCommandOutcome {
                session: result.compacted_session,
                session_path: None,
                message: Some(format_compact_report(removed, kept, skipped)),
                json: Some(serde_json::json!({
                    "kind": "compact",
                    "skipped": skipped,
                    "removed_messages": removed,
                    "kept_messages": kept,
                })),
            })
        }
        SlashCommand::Clear { confirm } => {
            if !confirm {
                return Ok(ResumeCommandOutcome {
                    session: session.clone(),
                    session_path: None,
                    message: Some(
                        "clear: confirmation required; rerun with /clear --confirm".to_string(),
                    ),
                    json: Some(serde_json::json!({
                        "kind": "error",
                        "error": "confirmation required",
                        "hint": "rerun with /clear --confirm",
                    })),
                });
            }
            let backup_path = write_session_clear_backup(session, session_path)?;
            let previous_session_id = session.session_id.clone();
            let cleared = new_cli_session()?;
            let new_session_id = cleared.session_id.clone();
            Ok(ResumeCommandOutcome {
                session: cleared,
                session_path: None,
                message: Some(format!(
                    "Session cleared\n  Mode             resumed session reset\n  Previous session {previous_session_id}\n  Backup export    {}\n  Resume previous  cowd import-session {}\n  New session      {new_session_id}\n  Store            SQLite session store",
                    backup_path.display(),
                    backup_path.display(),
                )),
                json: Some(serde_json::json!({
                    "kind": "clear",
                    "previous_session_id": previous_session_id,
                    "new_session_id": new_session_id,
                    "backup": backup_path.display().to_string(),
                    "store": session_db_path(),
                })),
            })
        }
        SlashCommand::Status => {
            let tracker = UsageTracker::from_session(session);
            let usage = tracker.cumulative_usage();
            let context =
                status_context_for_session(Some(session_path), Some(session.session_id.as_str()))?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(format_status_report(
                    session.model.as_deref().unwrap_or("restored-session"),
                    StatusUsage {
                        message_count: session.messages.len(),
                        turns: tracker.turns(),
                        latest: tracker.current_turn_usage(),
                        cumulative: usage,
                        estimated_tokens: 0,
                    },
                    default_permission_mode().as_str(),
                    "standard",
                    &context,
                )),
                json: Some(status_json_value(
                    session.model.as_deref(),
                    StatusUsage {
                        message_count: session.messages.len(),
                        turns: tracker.turns(),
                        latest: tracker.current_turn_usage(),
                        cumulative: usage,
                        estimated_tokens: 0,
                    },
                    default_permission_mode().as_str(),
                    &context,
                )),
            })
        }
        SlashCommand::Sandbox => {
            let cwd = env::current_dir()?;
            let loader = ConfigLoader::default_for(&cwd);
            let runtime_config = loader.load()?;
            let status = resolve_sandbox_status(runtime_config.sandbox(), &cwd);
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(format_sandbox_report(&status)),
                json: Some(sandbox_json_value(&status)),
            })
        }
        SlashCommand::Cost => {
            let usage = UsageTracker::from_session(session).cumulative_usage();
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(format_cost_report(usage)),
                json: Some(serde_json::json!({
                    "kind": "cost",
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                    "total_tokens": usage.total_tokens(),
                })),
            })
        }
        SlashCommand::Config { section } => {
            let message = render_config_report(section.as_deref())?;
            let json = render_config_json(section.as_deref())?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(message),
                json: Some(json),
            })
        }
        SlashCommand::Setup => {
            let message = render_setup_report()?;
            let json = render_setup_json()?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(message),
                json: Some(json),
            })
        }
        SlashCommand::Mcp { action, target } => {
            let cwd = env::current_dir()?;
            let args = match (action.as_deref(), target.as_deref()) {
                (None, None) => None,
                (Some(action), None) => Some(action.to_string()),
                (Some(action), Some(target)) => Some(format!("{action} {target}")),
                (None, Some(target)) => Some(target.to_string()),
            };
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(handle_mcp_slash_command(args.as_deref(), &cwd)?),
                json: Some(handle_mcp_slash_command_json(args.as_deref(), &cwd)?),
            })
        }
        SlashCommand::Memory => Ok(ResumeCommandOutcome {
            session: session.clone(),
            session_path: None,
            message: Some(render_memory_report()?),
            json: Some(render_memory_json()?),
        }),
        SlashCommand::Init => {
            let message = init_claude_md()?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(message.clone()),
                json: Some(init_json_value(&message)),
            })
        }
        SlashCommand::Diff => {
            let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            let message = render_diff_report_for(&cwd)?;
            let json = render_diff_json_for(&cwd)?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(message),
                json: Some(json),
            })
        }
        SlashCommand::Version => Ok(ResumeCommandOutcome {
            session: session.clone(),
            session_path: None,
            message: Some(render_version_report()),
            json: Some(version_json_value()),
        }),
        SlashCommand::Export { path } => {
            let export_path = resolve_export_path(path.as_deref(), session)?;
            fs::write(&export_path, render_export_text(session))?;
            let msg_count = session.messages.len();
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(format!(
                    "Export\n  Result           wrote transcript\n  File             {}\n  Messages         {}",
                    export_path.display(),
                    msg_count,
                )),
                json: Some(serde_json::json!({
                    "kind": "export",
                    "file": export_path.display().to_string(),
                    "message_count": msg_count,
                })),
            })
        }
        SlashCommand::Agents { args } => {
            let cwd = env::current_dir()?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(handle_agents_slash_command(args.as_deref(), &cwd)?),
                json: Some(serde_json::json!({
                    "kind": "agents",
                    "text": handle_agents_slash_command(args.as_deref(), &cwd)?,
                })),
            })
        }
        SlashCommand::Skills { args } => {
            if let SkillSlashDispatch::Invoke(_) = classify_skills_slash_command(args.as_deref()) {
                return Err(
                    "resumed /skills invocations are interactive-only; start `cowd` and run `/skills <skill>` in the REPL".into(),
                );
            }
            let cwd = env::current_dir()?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(handle_skills_slash_command(args.as_deref(), &cwd)?),
                json: Some(handle_skills_slash_command_json(args.as_deref(), &cwd)?),
            })
        }
        SlashCommand::Doctor => {
            let report = doctor::render_doctor_report()?;
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(report.render()),
                json: Some(report.json_value()),
            })
        }
        SlashCommand::Stats => {
            let usage = UsageTracker::from_session(session).cumulative_usage();
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(format_cost_report(usage)),
                json: Some(serde_json::json!({
                    "kind": "stats",
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                    "total_tokens": usage.total_tokens(),
                })),
            })
        }
        SlashCommand::History { count } => {
            let limit = parse_history_count(count.as_deref())
                .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
            let entries = collect_session_prompt_history(session);
            let shown: Vec<_> = entries.iter().rev().take(limit).rev().collect();
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(render_prompt_history_report(&entries, limit)),
                json: Some(serde_json::json!({
                    "kind": "history",
                    "total": entries.len(),
                    "showing": shown.len(),
                    "entries": shown.iter().map(|e| serde_json::json!({
                        "timestamp_ms": e.timestamp_ms,
                        "text": e.text,
                    })).collect::<Vec<_>>(),
                })),
            })
        }
        SlashCommand::Unknown(name) => Err(suggestions::format_unknown_slash_command(name).into()),
        // /session list can be served from the sessions directory without a live session.
        SlashCommand::Session {
            action: Some(ref act),
            ..
        } if act == "list" => {
            let sessions = list_managed_sessions().unwrap_or_default();
            let session_ids: Vec<String> = sessions.iter().map(|s| s.id.clone()).collect();
            let active_id = session.session_id.clone();
            let text = render_session_list(&active_id).unwrap_or_else(|e| format!("error: {e}"));
            Ok(ResumeCommandOutcome {
                session: session.clone(),
                session_path: None,
                message: Some(text),
                json: Some(serde_json::json!({
                    "kind": "session_list",
                    "sessions": session_ids,
                    "active": active_id,
                })),
            })
        }
        SlashCommand::Session {
            action: Some(ref act),
            target: Some(target),
        } if act == "switch" => {
            let (handle, switched) = load_session_reference(target)?;
            let message_count = switched.messages.len();
            let session_id = switched.session_id.clone();
            Ok(ResumeCommandOutcome::new(
                switched,
                Some(format!(
                    "Session switched\n  Active session   {}\n  File             {}\n  Messages         {}",
                    session_id,
                    handle.path.display(),
                    message_count,
                )),
                Some(serde_json::json!({
                    "kind": "session_switch",
                    "active_session": session_id,
                    "file": handle.path.display().to_string(),
                    "message_count": message_count,
                })),
            )
            .with_session_path(handle.path))
        }
        SlashCommand::Bughunter { .. }
        | SlashCommand::Commit { .. }
        | SlashCommand::Pr { .. }
        | SlashCommand::Issue { .. }
        | SlashCommand::Ultraplan { .. }
        | SlashCommand::Teleport { .. }
        | SlashCommand::DebugToolCall { .. }
        | SlashCommand::Resume { .. }
        | SlashCommand::Model { .. }
        | SlashCommand::Permissions { .. }
        | SlashCommand::Session { .. }
        | SlashCommand::Plugins { .. }
        | SlashCommand::Login
        | SlashCommand::Logout
        | SlashCommand::Vim
        | SlashCommand::Upgrade
        | SlashCommand::Share
        | SlashCommand::Feedback
        | SlashCommand::Files
        | SlashCommand::Fast
        | SlashCommand::Exit
        | SlashCommand::Summary
        | SlashCommand::Desktop
        | SlashCommand::Brief
        | SlashCommand::Advisor
        | SlashCommand::Stickers
        | SlashCommand::Insights
        | SlashCommand::Thinkback
        | SlashCommand::ReleaseNotes
        | SlashCommand::SecurityReview
        | SlashCommand::Keybindings
        | SlashCommand::PrivacySettings
        | SlashCommand::Plan { .. }
        | SlashCommand::Review { .. }
        | SlashCommand::Tasks { .. }
        | SlashCommand::Approvals { .. }
        | SlashCommand::CrossPlane { .. }
        | SlashCommand::Theme { .. }
        | SlashCommand::Voice { .. }
        | SlashCommand::Usage { .. }
        | SlashCommand::Rename { .. }
        | SlashCommand::Copy { .. }
        | SlashCommand::Hooks { .. }
        | SlashCommand::Context { .. }
        | SlashCommand::Color { .. }
        | SlashCommand::Effort { .. }
        | SlashCommand::Branch { .. }
        | SlashCommand::Rewind { .. }
        | SlashCommand::Ide { .. }
        | SlashCommand::Tag { .. }
        | SlashCommand::OutputStyle { .. }
        | SlashCommand::AddDir { .. }
        | SlashCommand::AgentProfile { .. }
        | SlashCommand::Handoff { .. }
        | SlashCommand::SubAgent { .. }
        | SlashCommand::Pipeline { .. }
        | SlashCommand::Closet { .. }
        | SlashCommand::SandboxSearch { .. }
        | SlashCommand::Retry
        | SlashCommand::Undo
        | SlashCommand::NewSession
        | SlashCommand::Title { .. }
        | SlashCommand::Compress
        | SlashCommand::Solve { .. }
        | SlashCommand::State => Err("unsupported resumed slash command".into()),
    }
}

/// Detect if the current working directory is "broad" (home directory or
/// filesystem root). Returns the cwd path if broad, None otherwise.
fn detect_broad_cwd() -> Option<PathBuf> {
    let Ok(cwd) = env::current_dir() else {
        return None;
    };
    let is_home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .is_some_and(|h| Path::new(&h) == cwd);
    let is_root = cwd.parent().is_none();
    if is_home || is_root {
        Some(cwd)
    } else {
        None
    }
}

/// Enforce the broad-CWD policy: when running from home or root, either
/// require the --allow-broad-cwd flag, or prompt for confirmation (interactive),
/// or exit with an error (non-interactive).
fn enforce_broad_cwd_policy(
    allow_broad_cwd: bool,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    if allow_broad_cwd {
        return Ok(());
    }
    let Some(cwd) = detect_broad_cwd() else {
        return Ok(());
    };

    let is_interactive = io::stdin().is_terminal();

    if is_interactive {
        // Interactive mode: print warning and ask for confirmation
        eprintln!(
            "Warning: cowd is running from a very broad directory ({}).\n\
             The agent can read and search everything under this path.\n\
             Consider running from inside your project: cd /path/to/project && cowd",
            cwd.display()
        );
        eprint!("Continue anyway? [y/N]: ");
        io::stderr().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_lowercase();
        if trimmed != "y" && trimmed != "yes" {
            eprintln!("Aborted.");
            std::process::exit(0);
        }
        Ok(())
    } else {
        // Non-interactive mode: exit with error (JSON or text)
        let message = format!(
            "cowd is running from a very broad directory ({}). \
             The agent can read and search everything under this path. \
             Use --allow-broad-cwd to proceed anyway, \
             or run from inside your project: cd /path/to/project && cowd",
            cwd.display()
        );
        match output_format {
            CliOutputFormat::Json => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "type": "error",
                        "error": message,
                    })
                );
            }
            CliOutputFormat::Text => {
                eprintln!("error: {message}");
            }
        }
        std::process::exit(1);
    }
}

fn run_stale_base_preflight(flag_value: Option<&str>) {
    let Ok(cwd) = env::current_dir() else {
        return;
    };
    let source = resolve_expected_base(flag_value, &cwd);
    let state = check_base_commit(&cwd, source.as_ref());
    if let Some(warning) = format_stale_base_warning(&state) {
        eprintln!("{warning}");
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_repl(
    model: String,
    session_id: Option<String>,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    base_commit: Option<String>,
    reasoning_effort: Option<String>,
    allow_broad_cwd: bool,
    yolo_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("run_repl: enforcing cwd policy");
    enforce_broad_cwd_policy(allow_broad_cwd, CliOutputFormat::Text)?;
    tracing::debug!("run_repl: checking base preflight");
    run_stale_base_preflight(base_commit.as_deref());
    tracing::debug!("run_repl: resolving model");
    let resolved_model = resolve_repl_model(model);
    tracing::debug!(model = %resolved_model, "run_repl: creating LiveCli");
    let mut cli = LiveCli::new(
        resolved_model,
        session_id,
        true,
        allowed_tools,
        permission_mode,
        yolo_mode,
    )?;
    tracing::debug!("run_repl: applying reasoning effort");
    cli.set_reasoning_effort(reasoning_effort);
    match ensure_yolo_task(
        yolo_mode,
        format!("Interactive YOLO session {}", cli.session.id),
    ) {
        Ok(task) => {
            cli.yolo_task = task;
        }
        Err(error) => {
            tracing::warn!(%error, "failed to initialize yolo task state");
        }
    }

    let workspace = std::env::current_dir().unwrap_or_default();
    tracing::debug!(workspace = %workspace.display(), "run_repl: entering TUI");
    run_tui_repl(cli, workspace)
}

fn list_workspace_files(workspace: &PathBuf) -> Vec<tui::FileEntry> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(workspace) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if name.starts_with('.') {
                continue;
            }
            files.push(tui::FileEntry {
                name,
                is_dir: path.is_dir(),
                size: if path.is_dir() {
                    0
                } else {
                    path.metadata().map(|m| m.len()).unwrap_or(0)
                },
            });
        }
    }
    files.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then(a.name.cmp(&b.name)));
    files
}

fn load_session_history(app: &mut tui::App, session: &runtime::Session) {
    use runtime::{ContentBlock, CowdEvent, MessageRole};

    for msg in &session.messages {
        match msg.role {
            MessageRole::User => {
                for block in &msg.blocks {
                    if let ContentBlock::Text { text } = block {
                        app.add_message("user", text);
                    }
                }
            }
            MessageRole::Assistant => {
                let mut text_parts = Vec::new();
                for block in &msg.blocks {
                    match block {
                        ContentBlock::Text { text } => text_parts.push(text.clone()),
                        ContentBlock::ToolUse { id, name, input } => {
                            if !text_parts.is_empty() {
                                app.add_message("assistant", &text_parts.join(""));
                                text_parts.clear();
                            }
                            // Create a collapsed tool card via ToolStart event
                            let preview = if input.chars().count() > 100 {
                                format!("{}...", input.chars().take(100).collect::<String>())
                            } else {
                                input.clone()
                            };
                            app.apply_event(CowdEvent::ToolStart {
                                id: id.clone(),
                                name: name.clone(),
                                preview,
                            });
                        }
                        ContentBlock::ToolResult {
                            tool_use_id,
                            output,
                            is_error,
                            ..
                        } => {
                            let exit = if *is_error { Some(1) } else { Some(0) };
                            app.apply_event(CowdEvent::ToolComplete {
                                id: tool_use_id.clone(),
                                name: String::new(),
                                summary: output.clone(),
                                exit_code: exit,
                            });
                        }
                        _ => {}
                    }
                }
                if !text_parts.is_empty() {
                    app.add_message("assistant", &text_parts.join(""));
                }
            }
            _ => {}
        }
    }
}

fn refresh_panels(app: &mut tui::App, workspace: &PathBuf, runtime: &BuiltRuntime) {
    app.file_entries = list_workspace_files(workspace);

    app.delegate_tasks.clear();
    let handoff = SHARED_RT.block_on(runtime.create_memory_handoff());
    if let Some(handoff) = handoff {
        tracing::debug!(has_handoff = true, "memory handoff");
        for task in &handoff.task_states {
            app.delegate_tasks.push(tui::DelegateTask {
                id: task.task_id.clone(),
                description: task.last_checkpoint.clone(),
                status: format!("{}%", task.progress_percent),
            });
        }
        for item in &handoff.work_items {
            app.delegate_tasks.push(tui::DelegateTask {
                id: item.id.clone(),
                description: item.title.clone(),
                status: format!("{:?}", item.status).to_lowercase(),
            });
        }
    }

    app.memory_entries.clear();
    let handoff = SHARED_RT.block_on(runtime.create_memory_handoff());
    if let Some(handoff) = handoff {
        if !handoff.summary.is_empty() {
            app.memory_entries.push(tui::MemoryEntry {
                layer: "handoff".to_string(),
                content: handoff.summary.clone(),
                priority: "high".to_string(),
                ..Default::default()
            });
        }
    }
    // Session resume via BM25 – surfaces relevant prior-session entries
    if let Some(mgr) = runtime.memory_manager() {
        if let Some(resume) = mgr.session_resume() {
            let session_id = runtime.session().session_id;
            if let Ok(results) =
                SHARED_RT
                    .handle()
                    .block_on(resume.resume_recent(&session_id, None, 10))
            {
                tracing::info!(resumed = results.len(), "session resume");
                for entry in results {
                    app.memory_entries.push(tui::MemoryEntry {
                        layer: "resume".to_string(),
                        content: entry.content,
                        priority: format!("{:.2}", entry.confidence),
                        ..Default::default()
                    });
                }
            }
        }
    }

    // P1: Skills data pipeline, aligned with the WebUI unified skill catalog.
    app.skill_list.clear();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for skill in cowd_app_mfg::server_manufacturing_skill_pack() {
        let risk = if skill
            .output_actions
            .iter()
            .any(|action| action.contains("dispatch") || action.contains("escalation"))
        {
            "controlled"
        } else if skill.tools.iter().any(|tool| tool.contains("cross_plane")) {
            "governed"
        } else {
            "review"
        };
        app.skill_list.push(tui::SkillSummary {
            name: skill.skill_id,
            description: skill.role,
            installed: true,
            category: skill.domain.clone(),
            source: "mfg".to_string(),
            status: "ready".to_string(),
            risk: risk.to_string(),
            tags: vec![skill.domain, "mfg".to_string()],
        });
    }
    match SkillRegistry::discover(&cwd).list() {
        Ok(skills) => {
            for skill in skills {
                app.skill_list.push(tui::SkillSummary {
                    name: skill.name,
                    description: skill.description.unwrap_or_default(),
                    installed: skill.shadowed_by.is_none(),
                    category: "local".to_string(),
                    source: format!("{:?}", skill.source),
                    status: if skill.shadowed_by.is_some() {
                        "shadowed".to_string()
                    } else {
                        "ready".to_string()
                    },
                    risk: "operator_review".to_string(),
                    tags: skill.tags,
                });
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to load skill registry for TUI");
        }
    }

    app.mcp_count = runtime.mcp_state.as_ref().map_or(0, |mcp| {
        mcp.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .server_names()
            .len()
    });
    app.lsp_available = 0;
    app.permission_count = 0;
}

fn run_tui_repl(mut cli: LiveCli, workspace: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("run_tui_repl: start");
    use crossterm::{
        event::{DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind},
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use ratatui::backend::CrosstermBackend;
    use ratatui::Terminal;
    use std::io;
    use std::time::Duration;
    use tui::error_recovery;
    use tui::state::{ProcessedKey, TuiState};

    // ── Install custom panic hook for crash recovery ──
    tracing::debug!("run_tui_repl: installing panic hook");
    error_recovery::install_tui_panic_hook();

    // ── Run config migration (skin.yaml → theme.yaml) ──
    tracing::debug!("run_tui_repl: running config migration");
    let migration_report = tui::config_migration::run_startup_migration();

    // ── Check for accessibility flag ──
    tracing::debug!("run_tui_repl: checking accessibility");
    let accessibility_enabled = std::env::var("COWD_TUI_ACCESSIBILITY")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    let raw_mode_enabled = std::env::var("COWD_TUI_SKIP_RAW_MODE").is_err();
    let mouse_capture_enabled = std::env::var("COWD_TUI_MOUSE")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if raw_mode_enabled {
        tracing::debug!("run_tui_repl: enabling raw mode");
        enable_raw_mode()?;
    } else {
        tracing::debug!("run_tui_repl: raw mode skipped by COWD_TUI_SKIP_RAW_MODE");
    }
    let mut stdout = io::stdout();
    tracing::debug!("run_tui_repl: entering alternate screen");
    execute!(stdout, EnterAlternateScreen)?;
    if mouse_capture_enabled {
        execute!(stdout, EnableMouseCapture)?;
    }
    let backend = CrosstermBackend::new(stdout);
    tracing::debug!("run_tui_repl: creating terminal");
    let mut terminal = Terminal::new(backend)?;

    tracing::debug!("run_tui_repl: creating event channel");
    let (tui_tx, tui_rx) = tui::cowd_event_channel();

    tracing::debug!("run_tui_repl: creating state");
    let session_id = cli.session.id.clone();
    let mut state = TuiState::new(&cli.model, &session_id);
    state.app.yolo_mode = cli.yolo_mode;
    state.app.current_task = cli.yolo_task.as_ref().map(current_task_summary_from_record);
    state.add_message("system", &strip_ansi_for_tui(&cli.startup_banner()));
    state.add_message("system", &format_connected_line(&cli.model));
    let daemon_client = tui::control_client::DaemonControlClient::default_local();
    let mut daemon_session_ids: Vec<String> = Vec::new();
    let mut daemon_session_attached = false;
    let daemon_actor_id = format!("tui:{}", std::process::id());
    let mut daemon_lease_owner: Option<String> = None;
    let mut daemon_session_lease: Option<tui::control_client::DaemonSessionLease> = None;
    match SHARED_RT.block_on(daemon_client.status()) {
        Ok(status) => {
            state.app.server_running = true;
            state.app.active_api_sessions = status.active_sessions;
            state.app.server_uptime_secs = Some(status.uptime_secs);
            state.add_message(
                "system",
                &format!(
                    "Daemon control connected: {} active sessions, uptime {}s",
                    status.active_sessions, status.uptime_secs
                ),
            );
            match SHARED_RT.block_on(daemon_client.ensure_session(&session_id, &cli.model)) {
                Ok(ensured) => {
                    state.app.active_api_sessions = ensured.active_sessions;
                    daemon_session_attached = true;
                    let action = if ensured.created {
                        "created"
                    } else {
                        "attached"
                    };
                    state.add_message(
                        "system",
                        &format!("Daemon session {action}: {}", ensured.session_id),
                    );
                    match SHARED_RT.block_on(daemon_client.attach_session(
                        &ensured.session_id,
                        &daemon_actor_id,
                        "tui",
                        Some("writer"),
                    )) {
                        Ok(attached) => {
                            state.add_message(
                                "system",
                                &format!(
                                    "Daemon lifecycle attached: state={}, seq={}",
                                    attached.event.state, attached.event.sequence
                                ),
                            );
                            match SHARED_RT.block_on(daemon_client.replay_session(
                                &ensured.session_id,
                                0,
                                100,
                            )) {
                                Ok(replay) => state.add_message(
                                    "system",
                                    &format!(
                                        "Daemon replay ready: total={}, next_seq={}",
                                        replay.total, replay.next_sequence
                                    ),
                                ),
                                Err(err) => state.add_message(
                                    "system",
                                    &format!("Daemon replay unavailable: {err}"),
                                ),
                            }
                        }
                        Err(err) => state.add_message(
                            "system",
                            &format!("Daemon lifecycle attach unavailable: {err}"),
                        ),
                    }
                    let lease_owner = daemon_actor_id.clone();
                    match SHARED_RT.block_on(daemon_client.acquire_session_lease(
                        &ensured.session_id,
                        &lease_owner,
                        "collaborative",
                    )) {
                        Ok(lease) => {
                            daemon_lease_owner = Some(lease.owner.clone());
                            daemon_session_lease = Some(lease.clone());
                            state.app.daemon_lease_owner = Some(lease.owner.clone());
                            state.app.daemon_lease_mode = Some(lease.mode.clone());
                            state.add_message(
                                "system",
                                &format!(
                                    "Daemon session lease acquired: owner={}, mode={}",
                                    lease.owner, lease.mode
                                ),
                            );
                        }
                        Err(err) => state.add_message(
                            "system",
                            &format!("Daemon session lease unavailable: {err}"),
                        ),
                    }
                }
                Err(err) => {
                    state.add_message(
                        "system",
                        &format!(
                            "Daemon session attach failed; local runtime remains active: {err}"
                        ),
                    );
                }
            }
            let projection =
                match tui::projection_client::DaemonProjectionClient::from_running_gateway_with_retry(
                    daemon_projection_auth_token(),
                ) {
                    Ok(client) => client,
                    Err(err) => {
                        state.add_message(
                            "system",
                            &format!("Daemon projection client unavailable: {err}"),
                        );
                        None
                    }
                };
            let mut snapshot = SHARED_RT.block_on(
                tui::runtime_control_store::refresh_runtime_control_snapshot(
                    &daemon_client,
                    projection.as_ref(),
                    Some(&session_id),
                ),
            );
            if let Some(lease) = daemon_session_lease.as_ref() {
                snapshot.apply_lease(lease);
            }
            daemon_session_ids = snapshot.session_ids.clone();
            let readiness = snapshot.runtime_readiness.clone();
            let components = snapshot.runtime_components.unwrap_or_default();
            let degraded_reasons = snapshot.degraded_reasons.clone();
            snapshot.apply_to_app(&mut state.app);
            if let Some(readiness) = readiness {
                state.add_message(
                    "system",
                    &format!(
                        "Daemon runtime projection connected: readiness={readiness}, components={components}"
                    ),
                );
            }
            for reason in degraded_reasons.into_iter().take(3) {
                state.add_message("system", &format!("Daemon projection degraded: {reason}"));
            }
            if daemon_session_attached {
                let event_client = daemon_client.clone();
                let event_session_id = session_id.clone();
                let event_tx = tui_tx.clone();
                let _event_bridge = SHARED_RT.spawn(async move {
                    if let Err(err) = event_client
                        .subscribe_session_events(&event_session_id, event_tx.clone())
                        .await
                    {
                        let _ = event_tx.send(runtime::CowdEvent::TurnError {
                            error: format!("Daemon event bridge stopped: {err}"),
                        });
                    }
                });
                state.add_message("system", "Daemon event bridge subscribed for this session");
            }
        }
        Err(err) => {
            state.app.server_running = false;
            state.app.active_api_sessions = 0;
            state.app.server_uptime_secs = None;
            tracing::debug!("daemon control unavailable; local TUI runtime fallback: {err}");
            state.add_message("system", "Daemon unavailable; local TUI runtime active.");
        }
    }
    terminal.draw(|f| state.render(f))?;

    tracing::debug!("tui init: wiring memory manager");
    if let Some(mgr) = cli.runtime.memory_manager() {
        state.set_memory_manager(std::sync::Arc::clone(mgr));
    }

    // ── Wire tool registry to SkillsPanel (T27) ──
    tracing::debug!("tui init: loading tool registry");
    if let Ok(registry) = current_tool_registry() {
        state.set_tool_registry(std::sync::Arc::new(registry));
    }

    // ── Wire TUI into ActiveSessions (T6) ──
    tracing::debug!("tui init: wiring active sessions");
    use crate::gateway::ActiveSessions;
    let active_sessions = Arc::new(ActiveSessions::new());
    {
        // Build a registry runtime from the cli session data
        let session = cli.runtime.session();
        let registry_runtime = build_runtime(
            session,
            &cli.session.id,
            cli.model.clone(),
            cli.system_prompt.clone(),
            true,
            true,
            cli.allowed_tools.clone(),
            cli.permission_mode,
            None,
            None,
        )?;
        if let Err(e) = active_sessions.register(cli.session.id.clone(), registry_runtime) {
            tracing::warn!("failed to register TUI session: {e}");
        }
    }
    cli.set_active_sessions(active_sessions.clone());
    state.set_active_sessions(active_sessions);

    // Enable accessibility mode if flag is set
    if accessibility_enabled {
        state.accessibility = tui::accessibility::AccessibilityMode::full();
        // Apply high contrast theme
        let hc_theme = tui::accessibility::high_contrast_theme(true);
        state.theme_engine = tui::theme::ThemeEngine::new(hc_theme);
    }

    // Show migration report if anything was migrated
    if !migration_report.contains("nothing to migrate") {
        state.add_message("system", &migration_report);
    }
    tracing::debug!("tui init: loading session history");
    load_session_history(&mut state, &cli.runtime.session());
    tracing::debug!("tui init: refreshing panels");
    refresh_panels(&mut state, &workspace, &cli.runtime);

    // ── Populate TUI session list from the unified SQLite store ──
    {
        let sessions = match get_unified_store() {
            Ok(store) => SHARED_RT
                .block_on(store.list_sessions())
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let mut session_list: Vec<(String, String, String)> = sessions
            .iter()
            .map(|r| {
                let name = format!("cli [{}]", &r.session_id[..r.session_id.len().min(8)]);
                (r.session_id.clone(), name, r.created_at.clone())
            })
            .collect();
        for id in daemon_session_ids {
            if session_list.iter().any(|(existing, _, _)| existing == &id) {
                continue;
            }
            let short = id[..id.len().min(8)].to_string();
            session_list.push((id, format!("daemon [{short}]"), "live".to_string()));
        }
        let _ = tui_tx.send(runtime::CowdEvent::SessionList {
            sessions: session_list,
        });
    }

    // Startup phase: ready after init completes.
    // If init <500ms the overlay never shows. If init >500ms,
    // "Loading..." → "Finishing..." → Done (min 3s display).
    let startup_ready = true;
    tracing::debug!("tui init: entering event loop");

    let mut turn_handle: Option<std::thread::JoinHandle<()>> = None;
    let mut abort_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>> = None;
    let mut abort_monitor: Option<HookAbortMonitor> = None;
    let mut abort_signal_for_turn: Option<runtime::HookAbortSignal> = None;

    let res = SHARED_RT.block_on(async {
        let mut reader = crossterm::event::EventStream::new();
        loop {
            tokio::select! {
                Some(Ok(event)) = reader.next() => {
                    // Mouse scroll handling
                    if let Event::Mouse(mouse) = &event {
                        if matches!(mouse.kind, crossterm::event::MouseEventKind::ScrollDown) {
                            state.handle_mouse_scroll_at(true, mouse.column, mouse.row);
                            continue;
                        }
                        if matches!(mouse.kind, crossterm::event::MouseEventKind::ScrollUp) {
                            state.handle_mouse_scroll_at(false, mouse.column, mouse.row);
                            continue;
                        }
                    }
                    if let Event::Key(key) = event {
                        if key.kind == KeyEventKind::Press {
                            // Route picker/approval to dialogs
                            if state.picker_active {
                                state.open_session_picker_dialog();
                            }
                            if state.approval.is_some() && state.dialog_manager.is_empty() {
                                state.open_approval_dialog();
                            }

                            match state.process_raw_key(key) {
                                ProcessedKey::Submit(text) => {
                                    if text.is_empty() { continue; }
                                    if matches!(text.as_str(), "/exit" | "/quit") { break; }
                                    if text.starts_with('/') {
                                        let parsed = SlashCommand::parse(&text)
                                            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                                        match parsed {
                                            Some(cmd) => {
                                                let cmd_name = text.strip_prefix('/')
                                                    .and_then(|s| s.split_whitespace().next())
                                                    .unwrap_or(&text)
                                                    .to_string();
                                                let model_switch_requested = matches!(
                                                    cmd,
                                                    SlashCommand::Model { model: Some(_) }
                                                );
                                                let output = tokio::task::block_in_place(|| {
                                                    capture_stdout(|| cli.handle_repl_command(cmd))
                                                });
                                                match output {
                                                    Ok((true, captured)) => {
                                                        cli.persist_session()?;
                                                        if model_switch_requested {
                                                            state.app.model = cli.model.clone();
                                                            state.app.model_dirty = true;
                                                        }
                                                        state.add_slash_output(&cmd_name, &captured);
                                                        state.open_surface_for_slash_result(&cmd_name);
                                                    }
                                                    Ok((false, captured)) => {
                                                        state.add_slash_output(&cmd_name, &captured);
                                                        state.open_surface_for_slash_result(&cmd_name);
                                                    }
                                                    Err(e) => {
                                                        state.add_message("system", &format!("Error: {e}"));
                                                    }
                                                }
                                                continue;
                                            }
                                            None => {}
                                        }
                                    }
                                    if turn_handle.is_some() {
                                        state.add_message("system", "Already processing, please wait...");
                                        continue;
                                    }
                                    state.add_message("user", &text);
                                    state.is_loading = true;
                                    if daemon_session_attached {
                                        let event_client = daemon_client.clone();
                                        let event_session_id = session_id.clone();
                                        let event_tx = tui_tx.clone();
                                        SHARED_RT.spawn(async move {
                                            if let Err(err) = event_client
                                                .chat_session(&event_session_id, &text)
                                                .await
                                            {
                                                let _ = event_tx.send(runtime::CowdEvent::TurnError {
                                                    error: format!("Daemon chat failed: {err}"),
                                                });
                                            }
                                        });
                                        continue;
                                    }

                                    let callback: std::sync::Arc<dyn runtime::ToolCallback> =
                                        std::sync::Arc::new(tui::TuiToolCallback::new(
                                            tui_tx.clone(),
                                            state.memory_orchestrator.clone(),
                                        ));
                                    let (mut prepared, monitor, abort_signal) =
                                        cli.prepare_turn_runtime(false, Some(callback), Some(tui_tx.clone()))?;
                                    prepared.set_memory_callback(
                                        std::sync::Arc::new(tui::TuiMemoryCallback::new(tui_tx.clone())),
                                    );
                                    abort_monitor = Some(monitor);
                                    abort_signal_for_turn = Some(abort_signal);

                                    let tx = tui_tx.clone();
                                    let rt_handle = SHARED_RT.handle().clone();
                                    let abort_signal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                                    abort_flag = Some(abort_signal.clone());
                                    let task = std::thread::spawn(move || {
                                        tracing::debug!("TUI turn started");
                                        let _ = tx.send(runtime::CowdEvent::TurnStarted);
                                        if abort_signal.load(std::sync::atomic::Ordering::Relaxed) { return; }
                                        rt_handle.block_on(async move {
                                            match prepared.run_turn_async(&text, &runtime::permissions::SharedPrompter::none()).await {
                                                Ok(summary) => {
                                                    let final_text = final_assistant_text(&summary);
                                                    if let Some(collaboration_result) =
                                                        prepared.last_collaboration_result()
                                                    {
                                                        let _ = tx.send(runtime::CowdEvent::WorkGraphSummary {
                                                            summary: runtime::RuntimeWorkGraphSummary::from_review(
                                                                &collaboration_result.work_graph,
                                                                &collaboration_result.review_packet,
                                                            ),
                                                        });
                                                    }
                                                    tracing::info!(text_len = final_text.len(), iterations = summary.iterations, "TUI turn complete");
                                                    let _ = tx.send(runtime::CowdEvent::TurnComplete {
                                                        assistant_text: final_text.clone(),
                                                        iterations: summary.iterations as u32,
                                                    });
                                                }
                                                Err(e) => {
                                                    tracing::error!(error = %e, "TUI turn error");
                                                    let _ = tx.send(runtime::CowdEvent::TurnError { error: e.to_string() });
                                                }
                                            }
                                        });
                                    });
                                    turn_handle = Some(task);
                                }
                                ProcessedKey::Exit => break,
                                ProcessedKey::Cancel => {
                                    if let Some(signal) = abort_signal_for_turn.take() {
                                        signal.abort();
                                    }
                                    if let Some(monitor) = abort_monitor.take() {
                                        monitor.stop();
                                        state.add_message("system", "Interrupted");
                                    }
                                    if let Some(flag) = abort_flag.take() {
                                        flag.store(true, std::sync::atomic::Ordering::SeqCst);
                                    }
                                }
                                ProcessedKey::Nothing => {}
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(16)) => {
                    drain_cowd_events_state(&tui_rx, &mut state);
                    if turn_handle.as_ref().is_some_and(|h| h.is_finished()) {
                        turn_handle = None;
                        state.is_loading = false;
                    }
                    state.update_startup_phase(startup_ready);
                    if state.turn_active {
                        state.tick();
                    }
                    // ── T1 FIX: consume SessionSidebar pending actions ──
                    consume_session_sidebar_actions(&mut state, &mut cli, &workspace);
                }
            }
            terminal.draw(|f| state.render(f))?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    });

    if let Some(owner) = daemon_lease_owner.as_deref() {
        if let Err(err) =
            SHARED_RT.block_on(daemon_client.release_session_lease(&session_id, owner))
        {
            tracing::debug!(error = %err, session_id = %session_id, owner, "best-effort daemon session lease release failed");
        }
    }
    if daemon_session_attached {
        if let Err(err) =
            SHARED_RT.block_on(daemon_client.detach_session(&session_id, &daemon_actor_id))
        {
            tracing::debug!(error = %err, session_id = %session_id, actor = %daemon_actor_id, "best-effort daemon session lifecycle detach failed");
        }
    }
    cli.persist_session()?;
    if raw_mode_enabled {
        disable_raw_mode()?;
    }
    if mouse_capture_enabled {
        execute!(terminal.backend_mut(), DisableMouseCapture)?;
    }
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}

/// Process all pending CowdEvents from the channel without blocking,
/// routing through TuiState::apply_event for EventBus bridging.
fn drain_cowd_events_state(rx: &tui::CowdEventReceiver, state: &mut tui::state::TuiState) {
    let mut count = 0;
    let limit = if state.turn_active { 64 } else { 256 };
    while let Ok(event) = rx.try_recv() {
        state.apply_event(event);
        count += 1;
        if count >= limit {
            break;
        }
    }
}

/// Consume pending session sidebar actions (switch/delete/rename/new/fork/export).
/// Called every 16ms tick in the TUI main loop.
fn consume_session_sidebar_actions(state: &mut TuiState, cli: &mut LiveCli, workspace: &PathBuf) {
    use crate::tui::app::SessionSummary;

    // 1. Session switch
    if let Some(idx) = state.session_sidebar.pending_switch_idx.take() {
        let sessions: Vec<_> = state.session_sidebar.sessions().to_vec();
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if target_id != state.session_id {
                let _ = cli.persist_session();
                match switch_live_cli_session(cli, &target_id) {
                    Ok(report) => {
                        state
                            .session_sidebar
                            .set_current_session(&report.session_id);
                        state.session_id = report.session_id.clone();
                        state.app.session_id = report.session_id.clone();
                        load_session_history(&mut state.app, &cli.runtime.session());
                        refresh_panels(&mut state.app, workspace, &cli.runtime);
                        state.add_message(
                            "system",
                            &format!(
                                "Switched to session {} · messages {}",
                                &report.session_id[..8.min(report.session_id.len())],
                                report.message_count,
                            ),
                        );
                    }
                    Err(error) => {
                        state.add_message("system", &format!("Switch failed: {error}"));
                    }
                }
            }
        }
    }

    // 2. Session delete
    if let Some(idx) = state.session_sidebar.pending_delete_idx.take() {
        let sessions: Vec<_> = state.session_sidebar.sessions().to_vec();
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if let Ok(store) = get_unified_store() {
                let _ = SHARED_RT.block_on(store.delete_session(&target_id));
                state.add_message(
                    "system",
                    &format!("Deleted session {}", &target_id[..8.min(target_id.len())]),
                );

                if let Ok(records) = SHARED_RT.block_on(store.list_sessions()) {
                    let summaries: Vec<SessionSummary> = records
                        .iter()
                        .map(|r| SessionSummary {
                            id: r.session_id.clone(),
                            path: r.chat_id.clone(),
                            updated_at_ms: 0,
                            message_count: r.message_count as usize,
                        })
                        .collect();
                    state.picker_sessions = summaries.clone();
                    state.session_sidebar.load(summaries);
                }
            }
        }
    }

    // 3. Session rename
    if let Some((idx, new_name)) = state.session_sidebar.pending_rename.take() {
        let sessions: Vec<_> = state.session_sidebar.sessions().to_vec();
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if let Ok(store) = get_unified_store() {
                if let Ok(Some(mut record)) = SHARED_RT.block_on(store.get_session(&target_id)) {
                    record.chat_id = new_name.clone();
                    let _ = SHARED_RT.block_on(store.update_session(&record));
                    state.add_message("system", &format!("Renamed to {}", new_name));
                }
            }
        }
    }

    // 4. New session
    if state.session_sidebar.pending_new_session {
        state.session_sidebar.pending_new_session = false;
        let _ = cli.persist_session();
        let result = (|| -> Result<SessionSwitchReport, Box<dyn std::error::Error>> {
            let new_session = new_cli_session()?;
            let handle = create_managed_session_handle(&new_session.session_id)?;
            let report = activate_live_cli_session(cli, handle, new_session, "new")?;
            cli.persist_session()?;
            Ok(report)
        })();
        match result {
            Ok(report) => {
                state
                    .session_sidebar
                    .set_current_session(&report.session_id);
                state.session_id = report.session_id.clone();
                state.app.session_id = report.session_id.clone();
                state.app.timeline_pages.clear();
                state.app.total_entries = 0;
                state.app.timeline_cursor = 0;
                let mut ta = tui_textarea::TextArea::default();
                ta.set_block(
                    ratatui::widgets::Block::default()
                        .borders(ratatui::widgets::Borders::ALL)
                        .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
                );
                ta.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
                state.app.input = ta;
                state.add_message(
                    "system",
                    &format!(
                        "New session created: {}",
                        &report.session_id[..8.min(report.session_id.len())]
                    ),
                );
            }
            Err(error) => {
                state.add_message("system", &format!("New session failed: {error}"));
            }
        }
    }

    // 5. Fork
    if state.session_sidebar.pending_fork {
        state.session_sidebar.pending_fork = false;
        let _fork_at = state.session_sidebar.pending_fork_at.take();
        let _ = cli.persist_session();
        let result = (|| -> Result<SessionSwitchReport, Box<dyn std::error::Error>> {
            let forked = cli.runtime.session().fork(Some("fork".to_string()));
            let handle = create_managed_session_handle(&forked.session_id)?;
            let report = activate_live_cli_session(cli, handle, forked, "forked")?;
            cli.persist_session()?;
            Ok(report)
        })();
        match result {
            Ok(report) => {
                state
                    .session_sidebar
                    .set_current_session(&report.session_id);
                state.session_id = report.session_id.clone();
                state.app.session_id = report.session_id.clone();
                load_session_history(&mut state.app, &cli.runtime.session());
                refresh_panels(&mut state.app, workspace, &cli.runtime);
                state.add_message(
                    "system",
                    &format!(
                        "Session forked: {}",
                        &report.session_id[..8.min(report.session_id.len())]
                    ),
                );
            }
            Err(error) => {
                state.add_message("system", &format!("Session fork failed: {error}"));
            }
        }
    }

    // 6. Export — open dialog
    if state.session_sidebar.pending_export {
        state.session_sidebar.pending_export = false;
        state.export_dialog_active = true;
    }

    // 7. Export — write file after dialog confirms
    if let Some(options) = state.pending_export_options.take() {
        let export_path = std::path::Path::new(&options.filename);
        let export_path = if export_path.is_absolute() {
            export_path.to_path_buf()
        } else {
            workspace.join(&options.filename)
        };
        let text = render_export_text(&cli.runtime.session());
        let _ = std::fs::write(&export_path, text);
        state.toast_manager.push(
            crate::tui::components::toast::ToastVariant::Success,
            Some("Export".into()),
            format!("Exported to {}", export_path.display()),
            3000,
        );
    }
}

struct SessionSwitchReport {
    session_id: String,
    session_path: PathBuf,
    message_count: usize,
}

fn activate_live_cli_session(
    cli: &mut LiveCli,
    handle: SessionHandle,
    session: Session,
    action: &str,
) -> Result<SessionSwitchReport, Box<dyn std::error::Error>> {
    let message_count = session.messages.len();
    let session_id = session.session_id.clone();
    let registry_session = session.clone();
    let runtime = build_runtime(
        session,
        &handle.id,
        cli.model.clone(),
        cli.system_prompt.clone(),
        true,
        true,
        cli.allowed_tools.clone(),
        cli.permission_mode,
        None,
        None,
    )?;
    if let Some(ref active_sessions) = cli.active_sessions {
        let registry_runtime = build_runtime(
            registry_session,
            &handle.id,
            cli.model.clone(),
            cli.system_prompt.clone(),
            true,
            true,
            cli.allowed_tools.clone(),
            cli.permission_mode,
            None,
            None,
        )?;
        if let Err(e) = active_sessions.register(session_id.clone(), registry_runtime) {
            tracing::warn!("failed to register {action} session in ActiveSessions: {e}");
        }
    }
    cli.replace_runtime(runtime)?;
    cli.session = SessionHandle {
        id: session_id.clone(),
        path: handle.path.clone(),
    };
    Ok(SessionSwitchReport {
        session_id,
        session_path: handle.path,
        message_count,
    })
}

fn switch_live_cli_session(
    cli: &mut LiveCli,
    target: &str,
) -> Result<SessionSwitchReport, Box<dyn std::error::Error>> {
    let (handle, session) = load_session_reference(target)?;
    activate_live_cli_session(cli, handle, session, "switched")
}

/// Process all pending CowdEvents from the channel without blocking.
/// This is called at the top of the TUI render loop to keep the display
/// in sync with the background turn runner.
fn capture_stdout<F, R>(f: F) -> Result<(R, String), Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let mut pipe_fds = [-1i32; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err("pipe failed".into());
    }
    let read_fd = pipe_fds[0];
    let write_fd = pipe_fds[1];
    let saved = unsafe { libc::dup(1) };
    if unsafe { libc::dup2(write_fd, 1) } < 0 {
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return Err("dup2 failed".into());
    }
    let result = f();
    unsafe {
        libc::dup2(saved, 1);
        libc::close(saved);
        libc::close(write_fd);
    }
    let mut buf = String::new();
    std::io::Read::read_to_string(
        &mut unsafe { std::fs::File::from_raw_fd(read_fd) },
        &mut buf,
    )?;
    Ok((result?, buf))
}

fn drain_cowd_events(rx: &tui::CowdEventReceiver, app: &mut tui::App) {
    let mut count = 0;
    let limit = if app.turn_active { 64 } else { 256 };
    while let Ok(event) = rx.try_recv() {
        app.apply_event(event);
        count += 1;
        if count >= limit {
            break;
        }
    }
}

#[derive(Debug, Clone)]
struct SessionHandle {
    id: String,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct ManagedSessionSummary {
    id: String,
    path: PathBuf,
    updated_at_ms: u64,
    modified_epoch_millis: u128,
    message_count: usize,
    parent_session_id: Option<String>,
    branch_name: Option<String>,
}

pub(crate) struct LiveCli {
    model: String,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    yolo_mode: bool,
    yolo_task: Option<task_kernel::TaskRecord>,
    next_turn_resume_profile: bool,
    system_prompt: Vec<String>,
    runtime: BuiltRuntime,
    session: SessionHandle,
    prompt_history: Vec<PromptHistoryEntry>,
    active_sessions: Option<Arc<crate::gateway::ActiveSessions>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptHistoryEntry {
    timestamp_ms: u64,
    text: String,
}

struct RuntimePluginState {
    feature_config: runtime::RuntimeFeatureConfig,
    tool_registry: GlobalToolRegistry,
    plugin_registry: PluginRegistry,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
}

struct RuntimeMcpState {
    runtime: tokio::runtime::Runtime,
    manager: McpServerManager,
    pending_servers: Vec<String>,
    degraded_report: Option<runtime::McpDegradedReport>,
}

pub(crate) struct BuiltRuntime {
    runtime: Option<ConversationRuntime<AnthropicRuntimeClient, CliToolExecutor>>,
    plugin_registry: PluginRegistry,
    plugins_active: bool,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    mcp_active: bool,
    resume_context_loaded: bool,
}

impl BuiltRuntime {
    fn new(
        runtime: ConversationRuntime<AnthropicRuntimeClient, CliToolExecutor>,
        plugin_registry: PluginRegistry,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
        resume_context_loaded: bool,
    ) -> Self {
        Self {
            runtime: Some(runtime),
            plugin_registry,
            plugins_active: true,
            mcp_state,
            mcp_active: true,
            resume_context_loaded,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_placeholder() -> Self {
        Self {
            runtime: None,
            plugin_registry: PluginRegistry::default(),
            plugins_active: false,
            mcp_state: None,
            mcp_active: false,
            resume_context_loaded: false,
        }
    }

    fn with_hook_abort_signal(mut self, hook_abort_signal: runtime::HookAbortSignal) -> Self {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing hook abort signal");
        self.runtime = Some(runtime.with_hook_abort_signal(hook_abort_signal));
        self
    }

    fn shutdown_plugins(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.plugins_active {
            self.plugin_registry.shutdown()?;
            self.plugins_active = false;
        }
        Ok(())
    }

    fn shutdown_mcp(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.mcp_active {
            if let Some(mcp_state) = &self.mcp_state {
                mcp_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .shutdown()?;
            }
            self.mcp_active = false;
        }
        Ok(())
    }

    fn resume_context_loaded(&self) -> bool {
        self.resume_context_loaded
    }
}

fn cli_turn_context_profile(
    yolo_mode: bool,
    permission_mode: PermissionMode,
    resume_context: bool,
    review_context: bool,
) -> ContextProfile {
    if review_context {
        ContextProfile::Review
    } else if resume_context {
        ContextProfile::Resume
    } else if yolo_mode {
        ContextProfile::YoloGoal
    } else if permission_mode == PermissionMode::DangerFullAccess {
        ContextProfile::SoloGoal
    } else {
        ContextProfile::MainTurn
    }
}

fn apply_cli_turn_context_profile(
    runtime: &BuiltRuntime,
    yolo_mode: bool,
    permission_mode: PermissionMode,
    resume_context: bool,
    review_context: bool,
) {
    runtime.set_context_profile(cli_turn_context_profile(
        yolo_mode,
        permission_mode,
        resume_context,
        review_context,
    ));
}

impl Deref for BuiltRuntime {
    type Target = ConversationRuntime<AnthropicRuntimeClient, CliToolExecutor>;

    fn deref(&self) -> &Self::Target {
        self.runtime
            .as_ref()
            .expect("runtime should exist while built runtime is alive")
    }
}

impl DerefMut for BuiltRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime
            .as_mut()
            .expect("runtime should exist while built runtime is alive")
    }
}

impl Drop for BuiltRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown_mcp();
        let _ = self.shutdown_plugins();
    }
}

#[derive(Debug, Deserialize)]
struct ToolSearchRequest {
    query: String,
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct McpToolRequest {
    #[serde(rename = "qualifiedName")]
    qualified_name: Option<String>,
    tool: Option<String>,
    arguments: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ListMcpResourcesRequest {
    server: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadMcpResourceRequest {
    server: String,
    uri: String,
}

impl RuntimeMcpState {
    fn new(
        runtime_config: &runtime::RuntimeConfig,
    ) -> Result<Option<(Self, runtime::McpToolDiscoveryReport)>, Box<dyn std::error::Error>> {
        let mut manager = McpServerManager::from_runtime_config(runtime_config);
        if manager.server_names().is_empty() && manager.unsupported_servers().is_empty() {
            return Ok(None);
        }

        // Avoid nested-runtime crash: skip MCP discovery if already inside a runtime
        if tokio::runtime::Handle::try_current().is_ok() {
            return Ok(None);
        }
        let runtime = tokio::runtime::Runtime::new()?;
        let discovery = runtime.block_on(manager.discover_tools_best_effort());
        let pending_servers = discovery
            .failed_servers
            .iter()
            .map(|failure| failure.server_name.clone())
            .chain(
                discovery
                    .unsupported_servers
                    .iter()
                    .map(|server| server.server_name.clone()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let available_tools = discovery
            .tools
            .iter()
            .map(|tool| tool.qualified_name.clone())
            .collect::<Vec<_>>();
        let failed_server_names = pending_servers.iter().cloned().collect::<BTreeSet<_>>();
        let working_servers = manager
            .server_names()
            .into_iter()
            .filter(|server_name| !failed_server_names.contains(server_name))
            .collect::<Vec<_>>();
        let failed_servers =
            discovery
                .failed_servers
                .iter()
                .map(|failure| runtime::McpFailedServer {
                    server_name: failure.server_name.clone(),
                    phase: runtime::McpLifecyclePhase::ToolDiscovery,
                    error: runtime::McpErrorSurface::new(
                        runtime::McpLifecyclePhase::ToolDiscovery,
                        Some(failure.server_name.clone()),
                        failure.error.clone(),
                        std::collections::BTreeMap::new(),
                        true,
                    ),
                })
                .chain(discovery.unsupported_servers.iter().map(|server| {
                    runtime::McpFailedServer {
                        server_name: server.server_name.clone(),
                        phase: runtime::McpLifecyclePhase::ServerRegistration,
                        error: runtime::McpErrorSurface::new(
                            runtime::McpLifecyclePhase::ServerRegistration,
                            Some(server.server_name.clone()),
                            server.reason.clone(),
                            std::collections::BTreeMap::from([(
                                "transport".to_string(),
                                format!("{:?}", server.transport).to_ascii_lowercase(),
                            )]),
                            false,
                        ),
                    }
                }))
                .collect::<Vec<_>>();
        let degraded_report = (!failed_servers.is_empty()).then(|| {
            runtime::McpDegradedReport::new(
                working_servers,
                failed_servers,
                available_tools.clone(),
                available_tools,
            )
        });

        Ok(Some((
            Self {
                runtime,
                manager,
                pending_servers,
                degraded_report,
            },
            discovery,
        )))
    }

    fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.runtime.block_on(self.manager.shutdown())?;
        Ok(())
    }

    fn pending_servers(&self) -> Option<Vec<String>> {
        (!self.pending_servers.is_empty()).then(|| self.pending_servers.clone())
    }

    fn degraded_report(&self) -> Option<runtime::McpDegradedReport> {
        self.degraded_report.clone()
    }

    fn server_names(&self) -> Vec<String> {
        self.manager.server_names()
    }

    fn call_tool(
        &mut self,
        qualified_tool_name: &str,
        arguments: Option<serde_json::Value>,
    ) -> Result<String, ToolError> {
        let response = self
            .runtime
            .block_on(self.manager.call_tool(qualified_tool_name, arguments))
            .map_err(|error| ToolError::new(error.to_string()))?;
        if let Some(error) = response.error {
            return Err(ToolError::new(format!(
                "MCP tool `{qualified_tool_name}` returned JSON-RPC error: {} ({})",
                error.message, error.code
            )));
        }

        let result = response.result.ok_or_else(|| {
            ToolError::new(format!(
                "MCP tool `{qualified_tool_name}` returned no result payload"
            ))
        })?;
        serde_json::to_string_pretty(&result).map_err(|error| ToolError::new(error.to_string()))
    }

    fn list_resources_for_server(&mut self, server_name: &str) -> Result<String, ToolError> {
        let result = self
            .runtime
            .block_on(self.manager.list_resources(server_name))
            .map_err(|error| ToolError::new(error.to_string()))?;
        serde_json::to_string_pretty(&json!({
            "server": server_name,
            "resources": result.resources,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    fn list_resources_for_all_servers(&mut self) -> Result<String, ToolError> {
        let mut resources = Vec::new();
        let mut failures = Vec::new();

        for server_name in self.server_names() {
            match self
                .runtime
                .block_on(self.manager.list_resources(&server_name))
            {
                Ok(result) => resources.push(json!({
                    "server": server_name,
                    "resources": result.resources,
                })),
                Err(error) => failures.push(json!({
                    "server": server_name,
                    "error": error.to_string(),
                })),
            }
        }

        if resources.is_empty() && !failures.is_empty() {
            let message = failures
                .iter()
                .filter_map(|failure| failure.get("error").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ToolError::new(message));
        }

        serde_json::to_string_pretty(&json!({
            "resources": resources,
            "failures": failures,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    fn read_resource(&mut self, server_name: &str, uri: &str) -> Result<String, ToolError> {
        let result = self
            .runtime
            .block_on(self.manager.read_resource(server_name, uri))
            .map_err(|error| ToolError::new(error.to_string()))?;
        serde_json::to_string_pretty(&json!({
            "server": server_name,
            "contents": result.contents,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}

fn build_runtime_mcp_state(
    runtime_config: &runtime::RuntimeConfig,
) -> Result<RuntimePluginStateBuildOutput, Box<dyn std::error::Error>> {
    let Some((mcp_state, discovery)) = RuntimeMcpState::new(runtime_config)? else {
        return Ok((None, Vec::new()));
    };

    let mut runtime_tools = discovery
        .tools
        .iter()
        .map(mcp_runtime_tool_definition)
        .collect::<Vec<_>>();
    if !mcp_state.server_names().is_empty() {
        runtime_tools.extend(mcp_wrapper_tool_definitions());
    }

    Ok((Some(Arc::new(Mutex::new(mcp_state))), runtime_tools))
}

fn mcp_runtime_tool_definition(tool: &runtime::ManagedMcpTool) -> RuntimeToolDefinition {
    RuntimeToolDefinition {
        name: tool.qualified_name.clone(),
        description: Some(
            tool.tool
                .description
                .clone()
                .unwrap_or_else(|| format!("Invoke MCP tool `{}`.", tool.qualified_name)),
        ),
        input_schema: tool
            .tool
            .input_schema
            .clone()
            .unwrap_or_else(|| json!({ "type": "object", "additionalProperties": true })),
        required_permission: permission_mode_for_mcp_tool(&tool.tool),
    }
}

fn mcp_wrapper_tool_definitions() -> Vec<RuntimeToolDefinition> {
    vec![
        RuntimeToolDefinition {
            name: "MCPTool".to_string(),
            description: Some(
                "Call a configured MCP tool by its qualified name and JSON arguments.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "qualifiedName": { "type": "string" },
                    "arguments": {}
                },
                "required": ["qualifiedName"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        RuntimeToolDefinition {
            name: "ListMcpResourcesTool".to_string(),
            description: Some(
                "List MCP resources from one configured server or from every connected server."
                    .to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        RuntimeToolDefinition {
            name: "ReadMcpResourceTool".to_string(),
            description: Some("Read a specific MCP resource from a configured server.".to_string()),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" },
                    "uri": { "type": "string" }
                },
                "required": ["server", "uri"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
    ]
}

fn permission_mode_for_mcp_tool(tool: &McpTool) -> PermissionMode {
    let read_only = mcp_annotation_flag(tool, "readOnlyHint");
    let destructive = mcp_annotation_flag(tool, "destructiveHint");
    let open_world = mcp_annotation_flag(tool, "openWorldHint");

    if read_only && !destructive && !open_world {
        PermissionMode::ReadOnly
    } else if destructive || open_world {
        PermissionMode::DangerFullAccess
    } else {
        PermissionMode::WorkspaceWrite
    }
}

fn mcp_annotation_flag(tool: &McpTool, key: &str) -> bool {
    tool.annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DaemonTaskSlashCommand {
    List,
    Start { objective: String, yolo_mode: bool },
    Cancel { id: String },
    Complete { id: String },
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DaemonApprovalSlashCommand {
    List,
    Respond {
        id: String,
        approved: bool,
        persistence: Option<String>,
        reason: Option<String>,
    },
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DaemonContextSlashCommand {
    Current,
    Runtime,
    Config,
    Memory,
    CrossPlane,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DaemonCrossPlaneSlashCommand {
    Summary,
    Preflight(String),
    Execute(String),
    Help,
}

fn parse_daemon_task_slash_command(args: Option<&str>) -> Result<DaemonTaskSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "list" | "status") {
        return Ok(DaemonTaskSlashCommand::List);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(DaemonTaskSlashCommand::Help);
    }

    let mut parts = raw.split_whitespace();
    let Some(action) = parts.next() else {
        return Ok(DaemonTaskSlashCommand::List);
    };

    match action {
        "start" => {
            let mut yolo_mode = false;
            let mut objective = Vec::new();
            for part in parts {
                if part == "--yolo" {
                    yolo_mode = true;
                } else {
                    objective.push(part);
                }
            }
            let objective = objective.join(" ").trim().to_string();
            if objective.is_empty() {
                return Err("usage: /tasks start [--yolo] <objective>".to_string());
            }
            Ok(DaemonTaskSlashCommand::Start {
                objective,
                yolo_mode,
            })
        }
        "cancel" => {
            let id = parts.next().unwrap_or_default().trim().to_string();
            if id.is_empty() {
                return Err("usage: /tasks cancel <task-id>".to_string());
            }
            Ok(DaemonTaskSlashCommand::Cancel { id })
        }
        "complete" => {
            let id = parts.next().unwrap_or_default().trim().to_string();
            if id.is_empty() {
                return Err("usage: /tasks complete <task-id>".to_string());
            }
            Ok(DaemonTaskSlashCommand::Complete { id })
        }
        other => Err(format!(
            "unknown /tasks action `{other}`; use /tasks --help"
        )),
    }
}

fn parse_daemon_approval_slash_command(
    args: Option<&str>,
) -> Result<DaemonApprovalSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "list" | "pending" | "status") {
        return Ok(DaemonApprovalSlashCommand::List);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(DaemonApprovalSlashCommand::Help);
    }

    let mut parts = raw.split_whitespace();
    let action = parts.next().unwrap_or_default();
    let approved = match action {
        "approve" | "allow" => true,
        "reject" | "deny" => false,
        other => {
            return Err(format!(
                "unknown /approvals action `{other}`; use /approvals --help"
            ));
        }
    };
    let id = parts.next().unwrap_or_default().trim().to_string();
    if id.is_empty() {
        return Err("usage: /approvals approve|reject <request-id>".to_string());
    }

    let mut persistence = None;
    let mut reason = None;
    let mut rest = parts.peekable();
    while let Some(part) = rest.next() {
        match part {
            "--persist" | "--persistence" => {
                let Some(value) = rest.next() else {
                    return Err("usage: --persist <once|session|forever>".to_string());
                };
                persistence = Some(value.to_string());
            }
            "--reason" => {
                let value = rest.collect::<Vec<_>>().join(" ");
                if !value.trim().is_empty() {
                    reason = Some(value);
                }
                break;
            }
            other => {
                return Err(format!(
                    "unknown /approvals option `{other}`; use /approvals --help"
                ));
            }
        }
    }

    Ok(DaemonApprovalSlashCommand::Respond {
        id,
        approved,
        persistence,
        reason,
    })
}

fn parse_daemon_context_slash_command(
    args: Option<&str>,
) -> Result<DaemonContextSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "current" | "status") {
        return Ok(DaemonContextSlashCommand::Current);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(DaemonContextSlashCommand::Help);
    }
    match raw {
        "runtime" | "control-plane" => Ok(DaemonContextSlashCommand::Runtime),
        "config" | "effective-config" => Ok(DaemonContextSlashCommand::Config),
        "memory" => Ok(DaemonContextSlashCommand::Memory),
        "cross-plane" | "channels" => Ok(DaemonContextSlashCommand::CrossPlane),
        other => Err(format!(
            "unknown /context action `{other}`; use /context --help"
        )),
    }
}

fn parse_daemon_cross_plane_slash_command(
    args: Option<&str>,
) -> Result<DaemonCrossPlaneSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "summary" | "status") {
        return Ok(DaemonCrossPlaneSlashCommand::Summary);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(DaemonCrossPlaneSlashCommand::Help);
    }

    let Some(split_at) = raw.find(char::is_whitespace) else {
        return Err("usage: /cross-plane preflight|execute <json>".to_string());
    };
    let (action, payload) = raw.split_at(split_at);
    let payload = payload.trim();
    if payload.is_empty() {
        return Err("usage: /cross-plane preflight|execute <json>".to_string());
    }
    match action {
        "preflight" => Ok(DaemonCrossPlaneSlashCommand::Preflight(payload.to_string())),
        "execute" => Ok(DaemonCrossPlaneSlashCommand::Execute(payload.to_string())),
        other => Err(format!(
            "unknown /cross-plane action `{other}`; use /cross-plane --help"
        )),
    }
}

fn daemon_projection_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
}

fn running_daemon_projection_client(
) -> Result<tui::projection_client::DaemonProjectionClient, Box<dyn std::error::Error>> {
    let Some(client) = tui::projection_client::DaemonProjectionClient::from_running_gateway(
        daemon_projection_auth_token(),
    )?
    else {
        return Err("daemon gateway is not running; start cowd daemon first".into());
    };
    Ok(client)
}

fn print_daemon_task_status(value: &serde_json::Value) {
    println!("## Daemon Tasks");
    let Some(tasks) = value.get("tasks").and_then(serde_json::Value::as_array) else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
        return;
    };
    if tasks.is_empty() {
        println!("No active daemon tasks.");
        return;
    }
    for task in tasks {
        let id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let status = task
            .get("status")
            .or_else(|| task.get("phase"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let objective = task
            .get("objective")
            .or_else(|| task.get("title"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if objective.is_empty() {
            println!("- {id}: {status}");
        } else {
            println!("- {id}: {status} - {objective}");
        }
    }
}

fn print_daemon_approval_status(value: &serde_json::Value) {
    println!("## Pending Approvals");
    let approvals = value
        .as_array()
        .or_else(|| value.get("approvals").and_then(serde_json::Value::as_array))
        .or_else(|| value.get("pending").and_then(serde_json::Value::as_array));
    let Some(approvals) = approvals else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
        return;
    };
    if approvals.is_empty() {
        println!("No pending approvals.");
        return;
    }
    for approval in approvals {
        let id = approval
            .get("id")
            .or_else(|| approval.get("request_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let capability = approval
            .get("capability")
            .or_else(|| approval.get("operation"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("approval");
        let summary = approval
            .get("summary")
            .or_else(|| approval.get("reason"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if summary.is_empty() {
            println!("- {id}: {capability}");
        } else {
            println!("- {id}: {capability} - {summary}");
        }
    }
}

fn print_daemon_projection_response(title: &str, value: &serde_json::Value) {
    println!("## {title}");
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

struct HookAbortMonitor {
    stop_tx: Option<Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl HookAbortMonitor {
    fn spawn(abort_signal: runtime::HookAbortSignal) -> Self {
        Self::spawn_with_waiter(abort_signal, move |stop_rx, abort_signal| {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };

            runtime.block_on(async move {
                let wait_for_stop = tokio::task::spawn_blocking(move || {
                    let _ = stop_rx.recv();
                });

                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if result.is_ok() {
                            abort_signal.abort();
                        }
                    }
                    _ = wait_for_stop => {}
                }
            });
        })
    }

    fn spawn_with_waiter<F>(abort_signal: runtime::HookAbortSignal, wait_for_interrupt: F) -> Self
    where
        F: FnOnce(Receiver<()>, runtime::HookAbortSignal) + Send + 'static,
    {
        let (stop_tx, stop_rx) = mpsc::channel();
        let join_handle = thread::spawn(move || wait_for_interrupt(stop_rx, abort_signal));

        Self {
            stop_tx: Some(stop_tx),
            join_handle: Some(join_handle),
        }
    }

    fn stop(mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

fn format_startup_banner(model: &str, yolo_mode: bool, session_id: &str) -> String {
    format_startup_banner_with_task(model, yolo_mode, session_id, None)
}

fn strip_ansi_for_tui(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        out.push(ch);
    }
    out
}

fn format_startup_banner_with_task(
    model: &str,
    yolo_mode: bool,
    session_id: &str,
    task: Option<&task_kernel::TaskRecord>,
) -> String {
    let status = status_context(None).ok();
    let git_branch = status
        .as_ref()
        .and_then(|context| context.git_branch.as_deref())
        .unwrap_or("unknown");
    let workspace = status.as_ref().map_or_else(
        || "unknown".to_string(),
        |context| context.git_summary.headline(),
    );
    let task_line = task.map_or_else(String::new, |task| {
        let short_id: String = task.id.chars().take(8).collect();
        let objective = truncate_for_banner(&task.objective, 72);
        let phase = current_task_phase_for_display(task)
            .map(|phase| format!(" · phase {}:{}", phase.name, phase.status.as_str()))
            .unwrap_or_default();
        format!(
            "   \x1b[2mTask\x1b[0m        {} {}{} · {}\n",
            task.status.as_str(),
            short_id,
            phase,
            objective
        )
    });
    let short_session = truncate_for_banner(session_id, 18);
    format!(
        "\x1b[1;31mCOWD v{VERSION}\x1b[0m  \x1b[2m{}\x1b[0m\n\
\x1b[2mBranch\x1b[0m {}  \x1b[2mMode\x1b[0m {}  \x1b[2mSession\x1b[0m {}\n\
\x1b[2mWorkspace\x1b[0m {}\n{}\
\x1b[1m/help\x1b[0m · \x1b[1m/status\x1b[0m · \x1b[2mTab\x1b[0m sidebar · \x1b[2mSpace\x1b[0m shortcuts",
        model,
        git_branch,
        if yolo_mode { "yolo" } else { "standard" },
        short_session,
        workspace,
        task_line,
    )
}

fn truncate_for_banner(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn current_task_phase_for_display(
    task: &task_kernel::TaskRecord,
) -> Option<&task_kernel::TaskPhaseRecord> {
    task.current_phase
        .as_deref()
        .and_then(|phase| {
            task.phases
                .iter()
                .rev()
                .find(|candidate| candidate.name == phase)
        })
        .or_else(|| task.phases.last())
}

fn current_task_summary_from_record(
    task: &task_kernel::TaskRecord,
) -> tui::app::CurrentTaskSummary {
    let phase = current_task_phase_for_display(task);
    tui::app::CurrentTaskSummary {
        id: task.id.clone(),
        objective: task.objective.clone(),
        status: task.status.as_str().to_string(),
        current_phase: phase.map(|phase| phase.name.clone()),
        phase_status: phase.map(|phase| phase.status.as_str().to_string()),
        review_result: phase.and_then(|phase| phase.review_result.clone()),
        artifact_count: phase.map_or(0, |phase| phase.artifacts.len()),
        blocker_reason: task.blocker_reason.clone(),
    }
}

impl LiveCli {
    fn new(
        model: String,
        session_id: Option<String>,
        enable_tools: bool,
        allowed_tools: Option<AllowedToolSet>,
        permission_mode: PermissionMode,
        yolo_mode: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let system_prompt = build_system_prompt_for_mode(yolo_mode)?;
        let (session, session_state) = load_or_create_live_session(session_id)?;
        let cwd = std::env::current_dir()?;
        init_runtime_providers_for_cwd(&cwd);
        let runtime = build_runtime(
            session_state,
            &session.id,
            model.clone(),
            system_prompt.clone(),
            enable_tools,
            true,
            allowed_tools.clone(),
            permission_mode,
            None,
            None,
        )?;
        let cli = Self {
            model,
            allowed_tools,
            permission_mode,
            yolo_mode,
            yolo_task: None,
            next_turn_resume_profile: false,
            system_prompt,
            runtime,
            session,
            prompt_history: Vec::new(),
            active_sessions: None,
        };
        cli.persist_session()?;
        Ok(cli)
    }

    fn set_reasoning_effort(&mut self, effort: Option<String>) {
        if let Some(rt) = self.runtime.runtime.as_mut() {
            rt.api_client_mut().set_reasoning_effort(effort);
        }
    }

    /// Set the shared ActiveSessions registry for session tracking.
    pub(crate) fn set_active_sessions(
        &mut self,
        active_sessions: Arc<crate::gateway::ActiveSessions>,
    ) {
        self.active_sessions = Some(active_sessions);
    }

    fn startup_banner(&self) -> String {
        format_startup_banner_with_task(
            &self.model,
            self.yolo_mode,
            &self.session.id,
            self.yolo_task.as_ref(),
        )
    }

    fn repl_completion_candidates(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(slash_command_completion_candidates_with_sessions(
            &self.model,
            Some(&self.session.id),
            list_managed_sessions()?
                .into_iter()
                .map(|session| session.id)
                .collect(),
        ))
    }

    fn prepare_turn_runtime(
        &self,
        emit_output: bool,
        tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
        stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    ) -> Result<
        (BuiltRuntime, HookAbortMonitor, runtime::HookAbortSignal),
        Box<dyn std::error::Error>,
    > {
        let hook_abort_signal = runtime::HookAbortSignal::new();
        let abort_for_caller = hook_abort_signal.clone();
        let runtime = build_runtime(
            self.runtime.session().without_persistence(),
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            emit_output,
            self.allowed_tools.clone(),
            self.permission_mode,
            tool_callback,
            stream_callback,
        )?
        .with_hook_abort_signal(hook_abort_signal.clone());
        let hook_abort_monitor = HookAbortMonitor::spawn(hook_abort_signal);

        Ok((runtime, hook_abort_monitor, abort_for_caller))
    }

    fn replace_runtime(&mut self, runtime: BuiltRuntime) -> Result<(), Box<dyn std::error::Error>> {
        self.runtime.shutdown_plugins()?;
        self.runtime = runtime;
        Ok(())
    }

    fn run_turn(&mut self, input: &str) -> Result<(), Box<dyn std::error::Error>> {
        let (mut runtime, hook_abort_monitor, _) = self.prepare_turn_runtime(true, None, None)?;
        let resume_context = std::mem::take(&mut self.next_turn_resume_profile);
        apply_cli_turn_context_profile(
            &runtime,
            self.yolo_mode,
            self.permission_mode,
            resume_context,
            false,
        );
        let mut spinner = Spinner::new();
        let mut stdout = io::stdout();
        spinner.tick(
            "🦀 Thinking...",
            TerminalRenderer::new().color_theme(),
            &mut stdout,
        )?;
        let prompter = runtime::permissions::SharedPrompter::new(Box::new(
            CliPermissionPrompter::new(self.permission_mode),
        ));
        let handle =
            tokio::runtime::Handle::try_current().unwrap_or_else(|_| SHARED_RT.handle().clone());
        let result = handle.block_on(runtime.run_turn_async(input, &prompter));
        hook_abort_monitor.stop();
        match result {
            Ok(summary) => {
                self.replace_runtime(runtime)?;
                spinner.finish(
                    "✨ Done",
                    TerminalRenderer::new().color_theme(),
                    &mut stdout,
                )?;
                println!();
                if let Some(event) = summary.auto_compaction {
                    println!(
                        "{}",
                        format_auto_compaction_notice(event.removed_message_count)
                    );
                }
                self.persist_session()?;
                Ok(())
            }
            Err(error) => {
                runtime.shutdown_plugins()?;
                spinner.fail(
                    "❌ Request failed",
                    TerminalRenderer::new().color_theme(),
                    &mut stdout,
                )?;
                Err(Box::new(error))
            }
        }
    }

    fn handle_daemon_tasks_command(
        &mut self,
        args: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let command = parse_daemon_task_slash_command(args)?;
        if command == DaemonTaskSlashCommand::Help {
            println!("## Daemon Tasks");
            println!("/tasks");
            println!("/tasks start [--yolo] <objective>");
            println!("/tasks cancel <task-id>");
            println!("/tasks complete <task-id>");
            return Ok(());
        }

        let client = running_daemon_projection_client()?;
        match command {
            DaemonTaskSlashCommand::List => {
                let value = SHARED_RT.block_on(client.task_status())?;
                print_daemon_task_status(&value);
            }
            DaemonTaskSlashCommand::Start {
                objective,
                yolo_mode,
            } => {
                let value = SHARED_RT.block_on(client.start_task(&objective, yolo_mode))?;
                print_daemon_projection_response("Task Started", &value);
            }
            DaemonTaskSlashCommand::Cancel { id } => {
                let value = SHARED_RT.block_on(client.cancel_task(&id))?;
                print_daemon_projection_response("Task Cancelled", &value);
            }
            DaemonTaskSlashCommand::Complete { id } => {
                let value = SHARED_RT.block_on(client.complete_task(&id))?;
                print_daemon_projection_response("Task Completed", &value);
            }
            DaemonTaskSlashCommand::Help => {}
        }
        Ok(())
    }

    fn handle_daemon_approvals_command(
        &mut self,
        args: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let command = parse_daemon_approval_slash_command(args)?;
        if command == DaemonApprovalSlashCommand::Help {
            println!("## Daemon Approvals");
            println!("/approvals");
            println!(
                "/approvals approve <request-id> [--persist once|session|forever] [--reason text]"
            );
            println!("/approvals reject <request-id> [--reason text]");
            return Ok(());
        }

        let client = running_daemon_projection_client()?;
        match command {
            DaemonApprovalSlashCommand::List => {
                let value = SHARED_RT.block_on(client.pending_approvals())?;
                print_daemon_approval_status(&value);
            }
            DaemonApprovalSlashCommand::Respond {
                id,
                approved,
                persistence,
                reason,
            } => {
                let value = SHARED_RT.block_on(client.respond_approval(
                    &id,
                    approved,
                    persistence.as_deref(),
                    reason.as_deref(),
                ))?;
                print_daemon_projection_response("Approval Responded", &value);
            }
            DaemonApprovalSlashCommand::Help => {}
        }
        Ok(())
    }

    fn handle_daemon_context_command(
        &mut self,
        args: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let command = parse_daemon_context_slash_command(args)?;
        if command == DaemonContextSlashCommand::Help {
            println!("## Daemon Context");
            println!("/context");
            println!("/context runtime");
            println!("/context config");
            println!("/context memory");
            println!("/context cross-plane");
            return Ok(());
        }

        let client = running_daemon_projection_client()?;
        let (title, value) = match command {
            DaemonContextSlashCommand::Current => (
                "Current Context",
                SHARED_RT.block_on(client.current_context(Some(&self.session.id)))?,
            ),
            DaemonContextSlashCommand::Runtime => (
                "Runtime Control Plane",
                SHARED_RT.block_on(client.runtime_control_plane())?,
            ),
            DaemonContextSlashCommand::Config => (
                "Runtime Effective Config",
                SHARED_RT.block_on(client.runtime_effective_config())?,
            ),
            DaemonContextSlashCommand::Memory => {
                ("Memory Status", SHARED_RT.block_on(client.memory_status())?)
            }
            DaemonContextSlashCommand::CrossPlane => (
                "Cross-Plane Summary",
                SHARED_RT.block_on(client.cross_plane_summary())?,
            ),
            DaemonContextSlashCommand::Help => unreachable!("help returned above"),
        };
        print_daemon_projection_response(title, &value);
        Ok(())
    }

    fn handle_daemon_cross_plane_command(
        &mut self,
        args: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let command = parse_daemon_cross_plane_slash_command(args)?;
        if command == DaemonCrossPlaneSlashCommand::Help {
            println!("## Cross-Plane");
            println!("/cross-plane");
            println!("/cross-plane preflight <json>");
            println!("/cross-plane execute <json>");
            return Ok(());
        }

        let client = running_daemon_projection_client()?;
        match command {
            DaemonCrossPlaneSlashCommand::Summary => {
                let value = SHARED_RT.block_on(client.cross_plane_summary())?;
                print_daemon_projection_response("Cross-Plane Summary", &value);
            }
            DaemonCrossPlaneSlashCommand::Preflight(payload) => {
                let request: serde_json::Value = serde_json::from_str(&payload)?;
                let value = SHARED_RT.block_on(client.preflight_cross_plane_action(request))?;
                print_daemon_projection_response("Cross-Plane Preflight", &value);
            }
            DaemonCrossPlaneSlashCommand::Execute(payload) => {
                let request: serde_json::Value = serde_json::from_str(&payload)?;
                let value = SHARED_RT.block_on(client.execute_cross_plane_action(request))?;
                print_daemon_projection_response("Cross-Plane Execute", &value);
            }
            DaemonCrossPlaneSlashCommand::Help => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn handle_repl_command(
        &mut self,
        command: SlashCommand,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(match command {
            SlashCommand::Help => {
                println!("## Cowd Commands\n");
                println!(
                    "**Session**: /status /cost /resume /session /new /clear /compact /retry /undo"
                );
                println!("**Memory**: /memory /closet /sandbox");
                println!("**Agent**: /subagent /pipeline /agents");
                println!("**Daemon**: /tasks /approvals /context /cross-plane");
                println!("**Project**: /state /diff /commit /init /config /title");
                println!("**Model**: use --model or config aliases (main/fast/coder/reasoning)");
                println!("Type /<command> --help for details.");
                false
            }
            SlashCommand::Status => {
                self.print_status();
                false
            }
            SlashCommand::Bughunter { scope } => {
                self.run_bughunter(scope.as_deref())?;
                false
            }
            SlashCommand::Commit => {
                self.run_commit(None)?;
                false
            }
            SlashCommand::Pr { context } => {
                self.run_pr(context.as_deref())?;
                false
            }
            SlashCommand::Issue { context } => {
                self.run_issue(context.as_deref())?;
                false
            }
            SlashCommand::Ultraplan { task } => {
                self.run_ultraplan(task.as_deref())?;
                false
            }
            SlashCommand::Teleport { target } => {
                Self::run_teleport(target.as_deref())?;
                false
            }
            SlashCommand::DebugToolCall => {
                self.run_debug_tool_call(None)?;
                false
            }
            SlashCommand::Sandbox => {
                Self::print_sandbox_status();
                false
            }
            SlashCommand::Compact => {
                self.compact()?;
                false
            }
            SlashCommand::Model { model } => self.set_model(model)?,
            SlashCommand::Permissions { mode } => self.set_permissions(mode)?,
            SlashCommand::Clear { confirm } => self.clear_session(confirm)?,
            SlashCommand::Cost => {
                self.print_cost();
                false
            }
            SlashCommand::Resume { session_path } => self.resume_session(session_path)?,
            SlashCommand::Config { section } => {
                Self::print_config(section.as_deref())?;
                false
            }
            SlashCommand::Setup => {
                Self::print_setup()?;
                false
            }
            SlashCommand::Mcp { action, target } => {
                let args = match (action.as_deref(), target.as_deref()) {
                    (None, None) => None,
                    (Some(action), None) => Some(action.to_string()),
                    (Some(action), Some(target)) => Some(format!("{action} {target}")),
                    (None, Some(target)) => Some(target.to_string()),
                };
                Self::print_mcp(args.as_deref(), CliOutputFormat::Text)?;
                false
            }
            SlashCommand::Memory => {
                Self::print_memory()?;
                false
            }
            SlashCommand::Init => {
                run_init(CliOutputFormat::Text)?;
                false
            }
            SlashCommand::Diff => {
                Self::print_diff()?;
                false
            }
            SlashCommand::Version => {
                Self::print_version(CliOutputFormat::Text);
                false
            }
            SlashCommand::Export { path } => {
                self.export_session(path.as_deref())?;
                false
            }
            SlashCommand::Session { action, target } => {
                self.handle_session_command(action.as_deref(), target.as_deref())?
            }
            SlashCommand::Plugins { action, target } => {
                self.handle_plugins_command(action.as_deref(), target.as_deref())?
            }
            SlashCommand::Agents { args } => {
                Self::print_agents(args.as_deref(), CliOutputFormat::Text)?;
                false
            }
            SlashCommand::Skills { args } => {
                match classify_skills_slash_command(args.as_deref()) {
                    SkillSlashDispatch::Invoke(prompt) => self.run_turn(&prompt)?,
                    SkillSlashDispatch::Local => {
                        Self::print_skills(args.as_deref(), CliOutputFormat::Text)?;
                    }
                }
                false
            }
            SlashCommand::Doctor => {
                println!("{}", doctor::render_doctor_report()?.render());
                false
            }
            SlashCommand::History { count } => {
                self.print_prompt_history(count.as_deref());
                false
            }
            SlashCommand::Tasks { args } => {
                self.handle_daemon_tasks_command(args.as_deref())?;
                false
            }
            SlashCommand::Approvals { args } => {
                self.handle_daemon_approvals_command(args.as_deref())?;
                false
            }
            SlashCommand::Context { action } => {
                self.handle_daemon_context_command(action.as_deref())?;
                false
            }
            SlashCommand::CrossPlane { args } => {
                self.handle_daemon_cross_plane_command(args.as_deref())?;
                false
            }
            SlashCommand::Stats => {
                let usage = UsageTracker::from_session(&self.runtime.session()).cumulative_usage();
                println!("{}", format_cost_report(usage));
                false
            }
            SlashCommand::Login
            | SlashCommand::Logout
            | SlashCommand::Vim
            | SlashCommand::Upgrade
            | SlashCommand::Share
            | SlashCommand::Feedback
            | SlashCommand::Files
            | SlashCommand::Fast
            | SlashCommand::Exit
            | SlashCommand::Summary
            | SlashCommand::Desktop
            | SlashCommand::Brief
            | SlashCommand::Advisor
            | SlashCommand::Stickers
            | SlashCommand::Insights
            | SlashCommand::Thinkback
            | SlashCommand::ReleaseNotes
            | SlashCommand::SecurityReview
            | SlashCommand::Keybindings
            | SlashCommand::PrivacySettings
            | SlashCommand::Plan { .. }
            | SlashCommand::Review { .. }
            | SlashCommand::Theme { .. }
            | SlashCommand::Voice { .. }
            | SlashCommand::Usage { .. }
            | SlashCommand::Rename { .. }
            | SlashCommand::Copy { .. }
            | SlashCommand::Hooks { .. }
            | SlashCommand::Color { .. }
            | SlashCommand::Effort { .. }
            | SlashCommand::Branch { .. }
            | SlashCommand::Rewind { .. }
            | SlashCommand::Ide { .. }
            | SlashCommand::Tag { .. }
            | SlashCommand::OutputStyle { .. }
            | SlashCommand::AddDir { .. }
            | SlashCommand::Handoff { .. }
            | SlashCommand::AgentProfile { .. } => {
                eprintln!("{} not yet implemented.", command.slash_name());
                false
            }
            SlashCommand::Closet { topic } => {
                let q = topic.unwrap_or_default();
                println!("## Closet: {q}\nUse /memory for full management.");
                false
            }
            SlashCommand::SandboxSearch { query } => {
                let q = query.unwrap_or_default();
                println!("## Sandbox: {q}\nUse /sandbox <query> to search tool outputs.");
                false
            }
            SlashCommand::Retry => {
                println!("Retry: resend last message.");
                false
            }
            SlashCommand::Undo => {
                println!("Undo: remove last exchange.");
                false
            }
            SlashCommand::NewSession => self.handle_session_command(Some("new"), None)?,
            SlashCommand::Title { name } => {
                println!("Title: {}", name.unwrap_or_default());
                false
            }
            SlashCommand::Compress => {
                println!("Compacting...");
                false
            }
            SlashCommand::State => {
                println!("## Project State\nCtrl+T toggles theme. /state for status.");
                false
            }
            SlashCommand::Pipeline { task } => {
                let t = task.unwrap_or_default();
                if !t.is_empty() {
                    println!("## Pipeline: Reasoner→Executor→Reviewer");
                    let _ = self.run_turn(&format!(
                        "[Reasoner] Analyze this task and propose approach: {t}"
                    ));
                    let _ = self.run_turn(
                        "[Executor] Implement the proposed approach. Write code and commit.",
                    );
                    let _ = self.run_turn(
                        "[Reviewer] Review the implementation. Check correctness and completeness.",
                    );
                } else {
                    println!("Usage: /pipeline <task description>");
                }
                false
            }
            SlashCommand::Solve { problem } => {
                let p = problem.as_deref().unwrap_or("");
                if p.is_empty() {
                    println!("Usage: /solve \"problem description\"");
                    println!("Runs the 7-phase Joint Problem Solving protocol (P8.3).");
                } else {
                    println!("## Joint Problem Solving (P8.3)");
                    println!("Problem: {p}");
                    println!(
                        "Phases: ProblemFraming → SolutionBrainstorming → SolutionMerger → Evaluation → Selection → Execution → Review"
                    );
                    let prompt = format!(
                        "Solve the following problem using the Joint Problem Solving protocol (P8.3).\n\n\
                         ## Problem\n{p}\n\n\
                         Follow the 7-phase protocol:\n\
                         1. ProblemFraming - Analyze and frame the problem\n\
                         2. SolutionBrainstorming - Propose concrete solutions\n\
                         3. SolutionMerger - Deduplicate and merge similar solutions\n\
                         4. Evaluation - Score all solutions on clarity, feasibility, novelty, impact, efficiency\n\
                         5. Selection - Select the best solution\n\
                         6. Execution - Implement the selected solution\n\
                         7. Review - Review the results\n\n\
                         Begin with Phase 1: ProblemFraming."
                    );
                    if let Err(e) = self.run_turn(&prompt) {
                        eprintln!("Solve error: {e}");
                    }
                }
                false
            }
            SlashCommand::SubAgent { role, task } => {
                let r = role.as_deref().unwrap_or("executor");
                let t = task.as_deref().unwrap_or("");
                if t.is_empty() {
                    println!("Usage: /subagent <role> <task>");
                    println!("Roles: reasoner, executor, reviewer");
                } else {
                    let prefix = match r {
                        "reasoner" => "You are a Reasoner. Analyze, don't execute. ",
                        "executor" => "You are an Executor. Implement the plan. ",
                        "reviewer" => "You are a Reviewer. Check quality. ",
                        _ => "",
                    };
                    if let Err(e) = self.run_turn(&format!("{prefix}Task: {t}")) {
                        eprintln!("SubAgent error: {e}");
                    }
                }
                false
            }
            SlashCommand::Unknown(name) => {
                eprintln!("{}", suggestions::format_unknown_slash_command(&name));
                false
            }
        })
    }

    fn persist_session(&self) -> Result<(), Box<dyn std::error::Error>> {
        let session = self.runtime.session();
        if let Ok(store) = get_unified_store() {
            sync_cli_session_to_unified_store(
                store,
                &self.session,
                Some(self.model.as_str()),
                &session,
            )?;
        }
        Ok(())
    }

    fn print_status(&self) {
        let cumulative = self.runtime.usage().cumulative_usage();
        let latest = self.runtime.usage().current_turn_usage();
        println!(
            "{}",
            format_status_report(
                &self.model,
                StatusUsage {
                    message_count: self.runtime.session().messages.len(),
                    turns: self.runtime.usage().turns(),
                    latest,
                    cumulative,
                    estimated_tokens: self.runtime.estimated_tokens(),
                },
                self.permission_mode.as_str(),
                if self.yolo_mode { "yolo" } else { "standard" },
                &status_context_for_session(
                    Some(&self.session.path),
                    Some(self.session.id.as_str())
                )
                .expect("status context should load"),
            )
        );
    }

    fn record_prompt_history(&mut self, prompt: &str) {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map_or(self.runtime.session().updated_at_ms, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });
        let entry = PromptHistoryEntry {
            timestamp_ms,
            text: prompt.to_string(),
        };
        self.prompt_history.push(entry);
        if let Err(error) = self.runtime.session_mut().push_prompt_entry(prompt) {
            eprintln!("warning: failed to persist prompt history: {error}");
        }
    }

    fn print_prompt_history(&self, count: Option<&str>) {
        let limit = match parse_history_count(count) {
            Ok(limit) => limit,
            Err(message) => {
                eprintln!("{message}");
                return;
            }
        };
        let session_entries = &self.runtime.session().prompt_history;
        let entries = if session_entries.is_empty() {
            if self.prompt_history.is_empty() {
                collect_session_prompt_history(&self.runtime.session())
            } else {
                self.prompt_history
                    .iter()
                    .map(|entry| PromptHistoryEntry {
                        timestamp_ms: entry.timestamp_ms,
                        text: entry.text.clone(),
                    })
                    .collect()
            }
        } else {
            session_entries
                .iter()
                .map(|entry| PromptHistoryEntry {
                    timestamp_ms: entry.timestamp_ms,
                    text: entry.text.clone(),
                })
                .collect()
        };
        println!("{}", render_prompt_history_report(&entries, limit));
    }

    fn print_sandbox_status() {
        let cwd = env::current_dir().expect("current dir");
        let loader = ConfigLoader::default_for(&cwd);
        let runtime_config = loader
            .load()
            .unwrap_or_else(|_| runtime::RuntimeConfig::empty());
        println!(
            "{}",
            format_sandbox_report(&resolve_sandbox_status(runtime_config.sandbox(), &cwd))
        );
    }

    fn set_model(&mut self, model: Option<String>) -> Result<bool, Box<dyn std::error::Error>> {
        let Some(model) = model else {
            println!(
                "{}",
                format_model_report(
                    &self.model,
                    self.runtime.session().messages.len(),
                    self.runtime.usage().turns(),
                )
            );
            return Ok(false);
        };

        let model = resolve_model_alias_with_config(&model);

        if model == self.model {
            println!(
                "{}",
                format_model_report(
                    &self.model,
                    self.runtime.session().messages.len(),
                    self.runtime.usage().turns(),
                )
            );
            return Ok(false);
        }

        let previous = self.model.clone();
        let message_count = self.runtime.session().messages.len();
        if let Some(rt) = self.runtime.runtime.as_mut() {
            rt.api_client_mut().switch_model(&model)?;
        }
        self.model.clone_from(&model);
        println!(
            "{}",
            format_model_switch_report(&previous, &model, message_count)
        );
        Ok(true)
    }

    fn set_permissions(
        &mut self,
        mode: Option<String>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let Some(mode) = mode else {
            println!(
                "{}",
                format_permissions_report(self.permission_mode.as_str())
            );
            return Ok(false);
        };

        let normalized = normalize_permission_mode(&mode).ok_or_else(|| {
            format!(
                "unsupported permission mode '{mode}'. Use read-only, workspace-write, or danger-full-access."
            )
        })?;

        if normalized == self.permission_mode.as_str() {
            println!("{}", format_permissions_report(normalized));
            return Ok(false);
        }

        let previous = self.permission_mode.as_str().to_string();
        let session = self.runtime.session().without_persistence();
        self.permission_mode = permission_mode_from_label(normalized);
        let runtime = build_runtime(
            session,
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            true,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        self.replace_runtime(runtime)?;
        self.persist_session()?;
        println!(
            "{}",
            format_permissions_switch_report(&previous, normalized)
        );
        Ok(true)
    }

    fn clear_session(&mut self, confirm: bool) -> Result<bool, Box<dyn std::error::Error>> {
        if !confirm {
            println!(
                "clear: confirmation required; run /clear --confirm to start a fresh session."
            );
            return Ok(false);
        }

        let previous_session = self.session.clone();
        let session_state = new_cli_session()?;
        self.session = create_managed_session_handle(&session_state.session_id)?;
        let runtime = build_runtime(
            session_state,
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            true,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        self.replace_runtime(runtime)?;
        self.persist_session()?;
        println!(
            "Session cleared\n  Mode             fresh session\n  Previous session {}\n  Resume previous  /resume {}\n  Preserved model  {}\n  Permission mode  {}\n  New session      {}\n  Store            SQLite session store",
            previous_session.id,
            previous_session.id,
            self.model,
            self.permission_mode.as_str(),
            self.session.id,
        );
        Ok(true)
    }

    fn print_cost(&self) {
        let cumulative = self.runtime.usage().cumulative_usage();
        println!("{}", format_cost_report(cumulative));
    }

    fn resume_session(
        &mut self,
        session_path: Option<String>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let Some(session_ref) = session_path else {
            println!("{}", render_resume_usage());
            return Ok(false);
        };

        let (handle, session) = load_session_reference(&session_ref)?;
        let message_count = session.messages.len();
        let session_id = session.session_id.clone();
        let runtime = build_runtime(
            session,
            &handle.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            true,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        self.replace_runtime(runtime)?;
        self.session = SessionHandle {
            id: session_id,
            path: handle.path,
        };
        self.next_turn_resume_profile = true;
        println!(
            "{}",
            format_resume_report(
                &self.session.path.display().to_string(),
                message_count,
                self.runtime.usage().turns(),
            )
        );
        Ok(true)
    }

    fn print_config(section: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", render_config_report(section)?);
        Ok(())
    }

    fn print_setup() -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", render_setup_report()?);
        Ok(())
    }

    fn print_memory() -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", render_memory_report()?);
        Ok(())
    }

    fn print_agents(
        args: Option<&str>,
        output_format: CliOutputFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cwd = env::current_dir()?;
        match output_format {
            CliOutputFormat::Text => println!("{}", handle_agents_slash_command(args, &cwd)?),
            CliOutputFormat::Json => println!(
                "{}",
                serde_json::to_string_pretty(&handle_agents_slash_command_json(args, &cwd)?)?
            ),
        }
        Ok(())
    }

    fn print_mcp(
        args: Option<&str>,
        output_format: CliOutputFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cwd = env::current_dir()?;
        match output_format {
            CliOutputFormat::Text => println!("{}", handle_mcp_slash_command(args, &cwd)?),
            CliOutputFormat::Json => println!(
                "{}",
                serde_json::to_string_pretty(&handle_mcp_slash_command_json(args, &cwd)?)?
            ),
        }
        Ok(())
    }

    fn print_skills(
        args: Option<&str>,
        output_format: CliOutputFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cwd = env::current_dir()?;
        match output_format {
            CliOutputFormat::Text => println!("{}", handle_skills_slash_command(args, &cwd)?),
            CliOutputFormat::Json => println!(
                "{}",
                serde_json::to_string_pretty(&handle_skills_slash_command_json(args, &cwd)?)?
            ),
        }
        Ok(())
    }

    fn print_plugins(
        action: Option<&str>,
        target: Option<&str>,
        output_format: CliOutputFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cwd = env::current_dir()?;
        let loader = ConfigLoader::default_for(&cwd);
        let runtime_config = loader.load()?;
        let mut manager = build_plugin_manager(&cwd, &loader, &runtime_config);
        let result = handle_plugins_slash_command(action, target, &mut manager)?;
        match output_format {
            CliOutputFormat::Text => println!("{}", result.message),
            CliOutputFormat::Json => println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "kind": "plugin",
                    "action": action.unwrap_or("list"),
                    "target": target,
                    "message": result.message,
                    "reload_runtime": result.reload_runtime,
                }))?
            ),
        }
        Ok(())
    }

    fn print_diff() -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", render_diff_report()?);
        Ok(())
    }

    fn print_version(output_format: CliOutputFormat) {
        let _ = crate::print_version(output_format);
    }

    fn export_session(
        &self,
        requested_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let export_path = resolve_export_path(requested_path, &self.runtime.session())?;
        fs::write(&export_path, render_export_text(&self.runtime.session()))?;
        println!(
            "Export\n  Result           wrote transcript\n  File             {}\n  Messages         {}",
            export_path.display(),
            self.runtime.session().messages.len(),
        );
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn handle_session_command(
        &mut self,
        action: Option<&str>,
        target: Option<&str>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match action {
            None | Some("list") => {
                println!("{}", render_session_list(&self.session.id)?);
                Ok(false)
            }
            Some("switch") => {
                let Some(target) = target else {
                    println!("Usage: /session switch <session-id>");
                    return Ok(false);
                };
                let report = switch_live_cli_session(self, target)?;
                println!(
                    "Session switched\n  Active session   {}\n  File             {}\n  Messages         {}",
                    report.session_id,
                    report.session_path.display(),
                    report.message_count,
                );
                Ok(true)
            }
            Some("new") => {
                // Create a fresh session
                let new_session = new_cli_session()?;
                let session_id = new_session.session_id.clone();
                let handle = create_managed_session_handle(&session_id)?;
                let message_count = new_session.messages.len();
                let registry_session = new_session.clone();
                let runtime = build_runtime(
                    new_session,
                    &handle.id,
                    self.model.clone(),
                    self.system_prompt.clone(),
                    true,
                    true,
                    self.allowed_tools.clone(),
                    self.permission_mode,
                    None,
                    None,
                )?;
                // Register in ActiveSessions for API access
                if let Some(ref as2) = self.active_sessions {
                    let registry_runtime = build_runtime(
                        registry_session,
                        &handle.id,
                        self.model.clone(),
                        self.system_prompt.clone(),
                        true,
                        true,
                        self.allowed_tools.clone(),
                        self.permission_mode,
                        None,
                        None,
                    )?;
                    if let Err(e) = as2.register(session_id.clone(), registry_runtime) {
                        tracing::warn!("failed to register new session in ActiveSessions: {e}");
                    }
                }
                self.replace_runtime(runtime)?;
                self.session = handle;
                let _ = self.persist_session();
                println!(
                    "New session created\n  Active session   {}\n  File             {}\n  Messages         {}",
                    self.session.id,
                    self.session.path.display(),
                    message_count,
                );
                Ok(true)
            }
            Some("fork") => {
                let forked = self.runtime.fork_session(target.map(ToOwned::to_owned));
                let parent_session_id = self.session.id.clone();
                let handle = create_managed_session_handle(&forked.session_id)?;
                let branch_name = forked
                    .fork
                    .as_ref()
                    .and_then(|fork| fork.branch_name.clone());
                let message_count = forked.messages.len();
                activate_live_cli_session(self, handle, forked, "forked")?;
                self.persist_session()?;
                println!(
                    "Session forked\n  Parent session   {}\n  Active session   {}\n  Branch           {}\n  File             {}\n  Messages         {}",
                    parent_session_id,
                    self.session.id,
                    branch_name.as_deref().unwrap_or("(unnamed)"),
                    self.session.path.display(),
                    message_count,
                );
                Ok(true)
            }
            Some("delete") => {
                let Some(target) = target else {
                    println!("Usage: /session delete <session-id> [--force]");
                    return Ok(false);
                };
                let handle = resolve_session_reference(target)?;
                if handle.id == self.session.id {
                    println!(
                        "delete: refusing to delete the active session '{}'.\nSwitch to another session first with /session switch <session-id>.",
                        handle.id
                    );
                    return Ok(false);
                }
                if !confirm_session_deletion(&handle.id) {
                    println!("delete: cancelled.");
                    return Ok(false);
                }
                delete_managed_session(&handle.id)?;
                println!(
                    "Session deleted\n  Deleted session  {}\n  Store            {}",
                    handle.id,
                    session_db_path().display(),
                );
                Ok(false)
            }
            Some("delete-force") => {
                let Some(target) = target else {
                    println!("Usage: /session delete <session-id> [--force]");
                    return Ok(false);
                };
                let handle = resolve_session_reference(target)?;
                if handle.id == self.session.id {
                    println!(
                        "delete: refusing to delete the active session '{}'.\nSwitch to another session first with /session switch <session-id>.",
                        handle.id
                    );
                    return Ok(false);
                }
                delete_managed_session(&handle.id)?;
                println!(
                    "Session deleted\n  Deleted session  {}\n  Store            {}",
                    handle.id,
                    session_db_path().display(),
                );
                Ok(false)
            }
            Some(other) => {
                println!(
                    "Unknown /session action '{other}'. Use /session list, /session switch <session-id>, /session fork [branch-name], or /session delete <session-id> [--force]."
                );
                Ok(false)
            }
        }
    }

    fn handle_plugins_command(
        &mut self,
        action: Option<&str>,
        target: Option<&str>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let cwd = env::current_dir()?;
        let loader = ConfigLoader::default_for(&cwd);
        let runtime_config = loader.load()?;
        let mut manager = build_plugin_manager(&cwd, &loader, &runtime_config);
        let result = handle_plugins_slash_command(action, target, &mut manager)?;
        println!("{}", result.message);
        if result.reload_runtime {
            self.reload_runtime_features()?;
        }
        Ok(false)
    }

    fn reload_runtime_features(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let runtime = build_runtime(
            self.runtime.session().without_persistence(),
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            true,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        self.replace_runtime(runtime)?;
        self.persist_session()
    }

    fn compact(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let result = self.runtime.compact(CompactionConfig::default());
        let removed = result.removed_message_count;
        let kept = result.compacted_session.messages.len();
        let skipped = removed == 0;
        let runtime = build_runtime(
            result.compacted_session,
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            true,
            true,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        self.replace_runtime(runtime)?;
        self.persist_session()?;
        println!("{}", format_compact_report(removed, kept, skipped));
        Ok(())
    }

    fn run_internal_prompt_text(
        &self,
        prompt: &str,
        enable_tools: bool,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let session = self.runtime.session().without_persistence();
        let mut runtime = build_runtime(
            session,
            &self.session.id,
            self.model.clone(),
            self.system_prompt.clone(),
            enable_tools,
            false,
            self.allowed_tools.clone(),
            self.permission_mode,
            None,
            None,
        )?;
        apply_cli_turn_context_profile(&runtime, self.yolo_mode, self.permission_mode, false, true);
        let prompter = runtime::permissions::SharedPrompter::new(Box::new(
            CliPermissionPrompter::new(self.permission_mode),
        ));
        let handle =
            tokio::runtime::Handle::try_current().unwrap_or_else(|_| SHARED_RT.handle().clone());
        let summary = handle.block_on(runtime.run_turn_async(prompt, &prompter))?;
        let text = final_assistant_text(&summary).trim().to_string();
        runtime.shutdown_plugins()?;
        Ok(text)
    }

    fn run_bughunter(&self, scope: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", format_bughunter_report(scope));
        Ok(())
    }

    fn run_ultraplan(&self, task: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", format_ultraplan_report(task));
        Ok(())
    }

    fn run_teleport(target: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let Some(target) = target.map(str::trim).filter(|value| !value.is_empty()) else {
            println!("Usage: /teleport <symbol-or-path>");
            return Ok(());
        };

        println!("{}", render_teleport_report(target)?);
        Ok(())
    }

    fn run_debug_tool_call(&self, args: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        validate_no_args("/debug-tool-call", args)?;
        println!(
            "{}",
            render_last_tool_debug_report(&self.runtime.session())?
        );
        Ok(())
    }

    fn run_commit(&mut self, args: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        validate_no_args("/commit", args)?;
        let status = git_output(&["status", "--short", "--branch"])?;
        let summary = parse_git_workspace_summary(Some(&status));
        let branch = parse_git_status_branch(Some(&status));
        if summary.is_clean() {
            println!("{}", format_commit_skipped_report());
            return Ok(());
        }

        println!(
            "{}",
            format_commit_preflight_report(branch.as_deref(), summary)
        );
        Ok(())
    }

    fn run_pr(&self, context: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let branch =
            resolve_git_branch_for(&env::current_dir()?).unwrap_or_else(|| "unknown".to_string());
        println!("{}", format_pr_report(&branch, context));
        Ok(())
    }

    fn run_issue(&self, context: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", format_issue_report(context));
        Ok(())
    }
}

// ── Unified Session Store (DB-backed session source of truth) ─────────────────
//
// TUI sessions use the same SQLite-backed `UnifiedSessionStore` as the HTTP
// server. SQLite is the canonical session store; JSONL is only an explicit
// import/export format.

static UNIFIED_STORE: std::sync::OnceLock<memory::UnifiedSessionStore> = std::sync::OnceLock::new();

/// Return the global unified session store, lazily initialised on first call.
fn get_unified_store() -> Result<&'static memory::UnifiedSessionStore, Box<dyn std::error::Error>> {
    if let Some(store) = UNIFIED_STORE.get() {
        return Ok(store);
    }

    let db_path = runtime::cowd_dirs::config_home_dir().join("sessions.db");
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let store = memory::UnifiedSessionStore::open(&db_path).map_err(|e| {
        let msg = format!("failed to open unified session store at {:?}: {e}", db_path);
        Box::<dyn std::error::Error>::from(msg)
    })?;

    // set() fails if another thread already initialised — either way get() works.
    UNIFIED_STORE.set(store).unwrap_or_else(|_| {});
    Ok(UNIFIED_STORE.get().unwrap())
}

/// Flat directory where JSONL session content files live.
fn jsonl_sessions_dir() -> PathBuf {
    runtime::cowd_dirs::config_home_dir().join("sessions")
}

fn session_db_path() -> PathBuf {
    runtime::cowd_dirs::config_home_dir().join("sessions.db")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSessionImportCandidate {
    path: PathBuf,
    session_id: String,
}

/// Discover local legacy session files without importing them.
///
/// JSONL is no longer part of the automatic session lifecycle. Discovery is
/// passive so startup remains deterministic and users can choose whether a
/// local file should be imported.
fn discover_local_session_import_candidates() -> Vec<LocalSessionImportCandidate> {
    let base = jsonl_sessions_dir();
    let mut roots = vec![base.clone(), base.join("global"), base.join("projects")];
    let mut candidates = Vec::new();

    while let Some(root) = roots.pop() {
        let entries = match std::fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                roots.push(path);
                continue;
            }
            let ext = path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            if ext != "jsonl" && ext != "json" {
                continue;
            }
            let Some(session_id) = path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(std::string::ToString::to_string)
            else {
                continue;
            };
            candidates.push(LocalSessionImportCandidate { path, session_id });
        }
    }
    candidates.sort_by(|a, b| a.path.cmp(&b.path));
    candidates
}

/// Stream JSONL session file line-by-line, converting each message to
/// [`SessionMessage`] and batch-inserting into SQLite.
/// Avoids loading the entire session into memory.
fn migrate_session_messages(
    store: &memory::UnifiedSessionStore,
    session_id: &str,
    jsonl_path: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(jsonl_path)?;
    let reader = BufReader::new(file);
    let mut batch = Vec::with_capacity(100);
    let mut total = 0usize;
    let mut sequence = 0usize;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        // Skip metadata/compaction records (not message records)
        if line.contains(r#""type":"session_meta""#) || line.contains(r#""type":"compaction""#) {
            continue;
        }

        // Parse as JSONL message record {"type":"message","message":{...}}
        if let Ok(value) = JsonValue::parse(&line) {
            if let Some(message_val) = value.as_object().and_then(|obj| obj.get("message")) {
                if let Ok(msg) = ConversationMessage::from_json(message_val) {
                    let record = msg.to_session_message(session_id, sequence);
                    batch.push(record);
                    sequence += 1;
                    total += 1;
                }
            }
        }

        // Batch insert every 100 messages
        if batch.len() >= 100 {
            SHARED_RT.block_on(store.insert_messages_batch(&batch))?;
            batch.clear();
        }
    }

    // Final flush
    if !batch.is_empty() {
        SHARED_RT.block_on(store.insert_messages_batch(&batch))?;
    }

    tracing::info!(
        session_id,
        count = total,
        "migrated session messages to SQLite"
    );
    Ok(total)
}

fn import_local_session_file(
    store: &memory::UnifiedSessionStore,
    path: &Path,
) -> Result<(String, usize), Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!("session file not found: {}", path.display()).into());
    }
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    if ext != "jsonl" && ext != "json" {
        return Err(format!(
            "unsupported session import format: {} (expected .jsonl or .json)",
            path.display()
        )
        .into());
    }

    let session = Session::load_from_path(path)?;
    let record = session_to_record(&session, path);
    let session_id = record.session_id.clone();
    SHARED_RT.block_on(async {
        if store.get_session(&session_id).await?.is_some() {
            store.update_session(&record).await?;
            store.delete_messages_from(&session_id, 0).await?;
            store
                .delete_events_by_type_from(&session_id, "message_appended", 0)
                .await?;
        } else {
            store.create_session(&record).await?;
        }
        Ok::<(), memory::MemoryError>(())
    })?;
    let imported_messages = migrate_session_messages(store, &session_id, path)?;
    Ok((session_id, imported_messages))
}

fn run_import_session(
    path: &Path,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    let (session_id, imported_messages) = import_local_session_file(store, path)?;
    match output_format {
        CliOutputFormat::Text => {
            println!(
                "Session imported\n  Session          {session_id}\n  Messages         {imported_messages}\n  Store            {}",
                session_db_path().display()
            );
        }
        CliOutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "kind": "session-import",
                    "session_id": session_id,
                    "messages": imported_messages,
                    "store": session_db_path(),
                }))?
            );
        }
    }
    Ok(())
}

/// CLI handler for `cowd migrate-sessions`.
///
/// Scans legacy `~/.cowd/sessions/projects/*/` and `global/` directories for
/// .jsonl / .json files and imports them into the UnifiedSessionStore SQLite
/// database.  Sessions that already exist in the store are counted as skipped.
/// Build a [`memory::SessionRecord`] from a loaded `Session` + its file path.
fn session_to_record(session: &Session, path: &Path) -> memory::store::session::SessionRecord {
    use memory::store::session::SessionRecord;

    let id = session.session_id.clone();
    let now = chrono::Utc::now().to_rfc3339();

    let metadata = serde_json::json!({
        "workspace_root": session.workspace_root().map(|p| p.display().to_string()),
        "parent_session_id": session.fork.as_ref().map(|f| f.parent_session_id.clone()),
        "branch_name": session.fork.as_ref().and_then(|f| f.branch_name.clone()),
        "legacy_path": path.display().to_string(),
    });

    SessionRecord {
        session_id: id,
        platform: "cli".to_string(),
        chat_id: path.display().to_string(),
        user_id: None,
        model: session.model.clone(),
        created_at: now.clone(),
        last_activity: now,
        message_count: session.messages.len() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata.to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

fn sync_cli_session_to_unified_store(
    store: &memory::UnifiedSessionStore,
    handle: &SessionHandle,
    model: Option<&str>,
    session: &Session,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now().to_rfc3339();
    let existing = SHARED_RT.block_on(store.get_session(&session.session_id))?;
    let created_at = existing
        .as_ref()
        .map(|record| record.created_at.clone())
        .unwrap_or_else(|| now.clone());
    let metadata = serde_json::json!({
        "workspace_root": session.workspace_root().map(|p| p.display().to_string()),
        "parent_session_id": session.fork.as_ref().map(|f| f.parent_session_id.clone()),
        "branch_name": session.fork.as_ref().and_then(|f| f.branch_name.clone()),
        "session_path": handle.path.display().to_string(),
    });

    let record = memory::store::session::SessionRecord {
        session_id: session.session_id.clone(),
        platform: "cli".to_string(),
        chat_id: session.session_id.clone(),
        user_id: None,
        model: session
            .model
            .clone()
            .or_else(|| model.map(std::string::ToString::to_string)),
        created_at,
        last_activity: now,
        message_count: session.messages.len() as i64,
        reset_policy: existing
            .as_ref()
            .map(|record| record.reset_policy.clone())
            .unwrap_or_else(|| "none".to_string()),
        metadata_json: Some(metadata.to_string()),
        input_tokens: session
            .messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| i64::from(usage.input_tokens))
            .sum(),
        output_tokens: session
            .messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| i64::from(usage.output_tokens))
            .sum(),
        estimated_cost_usd: existing
            .as_ref()
            .map(|record| record.estimated_cost_usd)
            .unwrap_or(0.0),
        status: "active".to_string(),
    };

    let existed = existing.is_some();
    SHARED_RT.block_on(async {
        if existed {
            store.update_session(&record).await?;
        } else {
            store.create_session(&record).await?;
        }
        store.delete_messages_from(&session.session_id, 0).await?;
        store
            .delete_events_by_type_from(&session.session_id, "message_appended", 0)
            .await?;

        let messages = session
            .messages
            .iter()
            .enumerate()
            .map(|(sequence, message)| message.to_session_message(&session.session_id, sequence))
            .collect::<Vec<_>>();
        if !messages.is_empty() {
            store.insert_messages_batch(&messages).await?;
        }

        for (sequence, message) in session.messages.iter().enumerate() {
            let message_json =
                serde_json::from_str::<serde_json::Value>(&message.to_json().render())
                    .unwrap_or(serde_json::Value::Null);
            let event = memory::SessionEvent {
                session_id: session.session_id.clone(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": sequence,
                    "role": message.role.role_str(),
                    "message": message_json,
                })
                .to_string(),
                sequence,
                created_at_ms: messages
                    .get(sequence)
                    .map(|message| message.created_at_ms)
                    .unwrap_or(0),
            };
            store.append_event(&event).await?;
        }

        Ok::<(), memory::MemoryError>(())
    })?;

    Ok(())
}

fn hydrate_session_from_unified_store(
    store: &memory::UnifiedSessionStore,
    handle: &SessionHandle,
) -> Result<Option<Session>, Box<dyn std::error::Error>> {
    let Some(record) = SHARED_RT.block_on(store.get_session(&handle.id))? else {
        return Ok(None);
    };
    let stored_messages = SHARED_RT.block_on(store.get_all_messages(&record.session_id))?;
    let mut messages = Vec::with_capacity(stored_messages.len());

    for stored in stored_messages {
        let blocks = JsonValue::parse(&stored.content_json)?;
        let mut object = BTreeMap::new();
        object.insert("role".to_string(), JsonValue::String(stored.role.clone()));
        object.insert("blocks".to_string(), blocks);
        if let Some(usage_json) = stored.token_usage_json.as_deref() {
            object.insert("usage".to_string(), JsonValue::parse(usage_json)?);
        }
        messages.push(ConversationMessage::from_json(&JsonValue::Object(object))?);
    }

    let metadata = record
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let workspace_root = metadata
        .as_ref()
        .and_then(|value| value.get("workspace_root"))
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from);
    let parent_session_id = metadata
        .as_ref()
        .and_then(|value| value.get("parent_session_id"))
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string);
    let branch_name = metadata
        .as_ref()
        .and_then(|value| value.get("branch_name"))
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string);

    let mut session = Session::new();
    session.session_id = record.session_id;
    session.model = record.model;
    session.messages = messages;
    session.workspace_root = workspace_root;
    session.fork = parent_session_id.map(|parent_session_id| runtime::SessionFork {
        parent_session_id,
        branch_name,
    });
    session.closed = record.status.eq_ignore_ascii_case("closed");

    Ok(Some(session))
}

fn sessions_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(session_db_path())
}

pub(crate) fn new_cli_session() -> Result<Session, Box<dyn std::error::Error>> {
    Ok(Session::new().with_workspace_root(env::current_dir()?))
}

fn load_or_create_live_session(
    session_id: Option<String>,
) -> Result<(SessionHandle, Session), Box<dyn std::error::Error>> {
    let Some(session_id) = session_id else {
        let session_state = new_cli_session()?;
        let handle = create_managed_session_handle(&session_state.session_id)?;
        return Ok((handle, session_state));
    };

    match load_session_reference(&session_id) {
        Ok((handle, session)) => Ok((handle, session)),
        Err(error) if error.to_string().contains("session not found") => {
            let mut session_state = new_cli_session()?;
            session_state.session_id = session_id.clone();
            let handle = create_managed_session_handle(&session_id)?;
            Ok((handle, session_state))
        }
        Err(error) => Err(error),
    }
}

/// Create a managed session handle and register its metadata in SQLite.
fn create_managed_session_handle(
    session_id: &str,
) -> Result<SessionHandle, Box<dyn std::error::Error>> {
    let path = session_db_path();
    let workspace_root = env::current_dir()?;

    // Register metadata in SQLite (idempotent via INSERT OR IGNORE).
    if let Ok(store) = get_unified_store() {
        let now = chrono::Utc::now().to_rfc3339();
        let metadata = serde_json::json!({
            "workspace_root": workspace_root.display().to_string(),
        });
        let record = memory::store::session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "cli".to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "none".to_string(),
            metadata_json: Some(metadata.to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let _ = SHARED_RT.block_on(store.create_session(&record));
        // Also bump last_activity so it sorts near the top
        let _ = SHARED_RT.block_on(store.upsert_session(&record));
    }

    Ok(SessionHandle {
        id: session_id.to_string(),
        path,
    })
}

fn resolve_session_reference(reference: &str) -> Result<SessionHandle, Box<dyn std::error::Error>> {
    // 1. Aliases ("latest", "last", "recent") → most-recent SQLite record.
    if reference.eq_ignore_ascii_case("latest")
        || reference.eq_ignore_ascii_case("last")
        || reference.eq_ignore_ascii_case("recent")
    {
        let store = get_unified_store()?;
        let workspace_records = list_workspace_session_records(store)?;
        let record = workspace_records
            .iter()
            .find(|record| record.message_count > 0)
            .cloned()
            .or_else(|| workspace_records.into_iter().next())
            .or_else(|| {
                SHARED_RT
                    .block_on(store.list_sessions())
                    .ok()
                    .and_then(|records| {
                        records
                            .iter()
                            .find(|record| record.message_count > 0)
                            .cloned()
                            .or_else(|| records.into_iter().next())
                    })
            })
            .ok_or_else(|| -> Box<dyn std::error::Error> { "no managed sessions found".into() })?;
        return Ok(SessionHandle {
            id: record.session_id,
            path: session_db_path(),
        });
    }

    // 2. Path-based reference (backward-compat: absolute/relative paths).
    let direct = PathBuf::from(reference);
    let candidate = if direct.is_absolute() {
        direct.clone()
    } else {
        env::current_dir()?.join(&direct)
    };
    let looks_like_path = direct.extension().is_some() || direct.components().count() > 1;

    if candidate.exists() {
        let id = candidate
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(reference)
            .to_string();
        return Ok(SessionHandle {
            id,
            path: candidate,
        });
    }

    if looks_like_path {
        return Err(format!("session file not found: {reference}").into());
    }

    // 3. Session-ID → SQLite lookup, return a DB-backed handle.
    let path = resolve_managed_session_path(reference)?;
    let id = if path == session_db_path() {
        reference.to_string()
    } else {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(reference)
            .to_string()
    };
    Ok(SessionHandle { id, path })
}

fn resolve_managed_session_path(session_id: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Check if the session is registered in SQLite.
    if let Ok(store) = get_unified_store() {
        if let Ok(Some(_record)) = SHARED_RT.block_on(store.get_session(session_id)) {
            return Ok(session_db_path());
        }
    }

    Err(format!("session not found: {session_id}").into())
}

fn list_managed_sessions() -> Result<Vec<ManagedSessionSummary>, Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    let records = list_workspace_session_records(store)?;
    Ok(records.into_iter().map(record_to_summary).collect())
}

fn list_workspace_session_records(
    store: &memory::UnifiedSessionStore,
) -> Result<Vec<memory::store::session::SessionRecord>, Box<dyn std::error::Error>> {
    let workspace_root = env::current_dir()?;
    SHARED_RT
        .block_on(
            store.list_sessions_by_workspace_root(workspace_root.display().to_string().as_str()),
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("failed to list workspace sessions: {e}").into()
        })
}

/// Convert a SQLite [`memory::SessionRecord`] into the TUI's summary struct.
fn record_to_summary(record: memory::store::session::SessionRecord) -> ManagedSessionSummary {
    let path = session_db_path();

    // Parse last_activity from ISO 8601 → epoch millis for sorting.
    let last_activity_ms = chrono::DateTime::parse_from_rfc3339(&record.last_activity)
        .ok()
        .map(|dt| dt.timestamp_millis().max(0) as u64)
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(&record.last_activity, "%Y-%m-%dT%H:%M:%S%.fZ")
                .ok()
                .map(|dt| dt.and_utc().timestamp_millis().max(0) as u64)
        })
        .unwrap_or(0);

    let (parent_session_id, branch_name) = record
        .metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .map(|v| {
            (
                v.get("parent_session_id")
                    .and_then(|s| s.as_str())
                    .map(String::from),
                v.get("branch_name")
                    .and_then(|s| s.as_str())
                    .map(String::from),
            )
        })
        .unwrap_or((None, None));

    ManagedSessionSummary {
        id: record.session_id,
        path,
        updated_at_ms: last_activity_ms,
        modified_epoch_millis: u128::from(last_activity_ms),
        message_count: record.message_count.max(0) as usize,
        parent_session_id,
        branch_name,
    }
}

fn latest_managed_session() -> Result<ManagedSessionSummary, Box<dyn std::error::Error>> {
    list_managed_sessions()?
        .into_iter()
        .next()
        .ok_or_else(|| -> Box<dyn std::error::Error> { "no managed sessions found".into() })
}

fn load_session_reference(
    reference: &str,
) -> Result<(SessionHandle, Session), Box<dyn std::error::Error>> {
    let handle = resolve_session_reference(reference)?;
    let session = if let Ok(store) = get_unified_store() {
        if let Some(hydrated) = hydrate_session_from_unified_store(store, &handle)? {
            hydrated
        } else if handle.path.exists() {
            return Err(format!(
                "local session file is not imported: {}. Import it explicitly before resume.",
                handle.path.display()
            )
            .into());
        } else {
            return Err(format!("session not found: {}", handle.id).into());
        }
    } else if handle.path.exists() {
        return Err(format!(
            "local session file is not imported: {}. Import it explicitly before resume.",
            handle.path.display()
        )
        .into());
    } else {
        return Err(format!("session not found: {}", handle.id).into());
    };

    // Check workspace mismatch
    if let Some(ref session_workspace) = session.workspace_root {
        let current_dir = std::env::current_dir()?;
        if *session_workspace != current_dir {
            tracing::warn!(
                session_workspace = %session_workspace.display(),
                current_workspace = %current_dir.display(),
                session_id = %session.session_id,
                "session workspace mismatch: session was created in '{}' but current workspace is '{}'",
                session_workspace.display(),
                current_dir.display()
            );
        }
    }

    Ok((handle, session))
}

fn delete_managed_session(session_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    SHARED_RT.block_on(store.delete_session(session_id))?;
    Ok(())
}

fn confirm_session_deletion(session_id: &str) -> bool {
    print!("Delete session '{session_id}'? This cannot be undone. [y/N]: ");
    io::stdout().flush().unwrap_or(());
    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() {
        return false;
    }
    matches!(answer.trim(), "y" | "Y" | "yes" | "Yes" | "YES")
}

fn render_session_list(active_session_id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let sessions = list_managed_sessions()?;
    let import_candidates = discover_local_session_import_candidates();
    let mut lines = vec![
        "Sessions".to_string(),
        format!("  Store             {}", session_db_path().display()),
    ];
    if !import_candidates.is_empty() {
        lines.push(format!(
            "  Local imports     {} legacy session file(s) available; import explicitly to use them.",
            import_candidates.len()
        ));
    }
    if sessions.is_empty() {
        lines.push("  No managed sessions saved yet.".to_string());
        return Ok(lines.join("\n"));
    }
    for session in sessions {
        let marker = if session.id == active_session_id {
            "● current"
        } else {
            "○ saved"
        };
        let lineage = match (
            session.branch_name.as_deref(),
            session.parent_session_id.as_deref(),
        ) {
            (Some(branch_name), Some(parent_session_id)) => {
                format!(" branch={branch_name} from={parent_session_id}")
            }
            (None, Some(parent_session_id)) => format!(" from={parent_session_id}"),
            (Some(branch_name), None) => format!(" branch={branch_name}"),
            (None, None) => String::new(),
        };
        lines.push(format!(
            "  {id:<20} {marker:<10} msgs={msgs:<4} updated={modified}{lineage} store={path}",
            id = session.id,
            msgs = session.message_count,
            modified = format_session_modified_age(session.modified_epoch_millis),
            lineage = lineage,
            path = session.path.display(),
        ));
    }
    Ok(lines.join("\n"))
}

fn format_session_modified_age(modified_epoch_millis: u128) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map_or(modified_epoch_millis, |duration| duration.as_millis());
    let delta_seconds = now
        .saturating_sub(modified_epoch_millis)
        .checked_div(1_000)
        .unwrap_or_default();
    match delta_seconds {
        0..=4 => "just-now".to_string(),
        5..=59 => format!("{delta_seconds}s-ago"),
        60..=3_599 => format!("{}m-ago", delta_seconds / 60),
        3_600..=86_399 => format!("{}h-ago", delta_seconds / 3_600),
        _ => format!("{}d-ago", delta_seconds / 86_400),
    }
}

fn write_session_clear_backup(
    session: &Session,
    session_path: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let backup_path = session_clear_backup_path(session_path);
    if let Some(parent) = backup_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&backup_path, session.export_jsonl()?)?;
    Ok(backup_path)
}

fn session_clear_backup_path(session_path: &Path) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map_or(0, |duration| duration.as_millis());
    let file_name = session_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("session");
    session_path.with_file_name(format!("{file_name}.before-clear-{timestamp}.jsonl"))
}

fn render_repl_help() -> String {
    [
        "REPL".to_string(),
        "  /exit                Quit the REPL".to_string(),
        "  /quit                Quit the REPL".to_string(),
        "  Up/Down              Navigate prompt history".to_string(),
        "  Ctrl-R               Reverse-search prompt history".to_string(),
        "  Tab                  Complete commands, modes, and recent sessions".to_string(),
        "  Ctrl-C               Clear input (or exit on empty prompt)".to_string(),
        "  Shift+Enter/Ctrl+J   Insert a newline".to_string(),
        "  Auto-save            SQLite session store".to_string(),
        "  Resume latest        /resume latest".to_string(),
        "  Browse sessions      /session list".to_string(),
        "  Show prompt history  /history [count]".to_string(),
        "  Daemon tasks         /tasks [start|cancel|complete]".to_string(),
        "  Daemon approvals     /approvals [approve|reject]".to_string(),
        "  Daemon context       /context [runtime|config|memory|cross-plane]".to_string(),
        "  Cross-plane action   /cross-plane [preflight|execute] <json>".to_string(),
        String::new(),
        render_slash_command_help_filtered(STUB_COMMANDS),
    ]
    .join(
        "
",
    )
}

fn print_status_snapshot(
    model: &str,
    permission_mode: PermissionMode,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let usage = StatusUsage {
        message_count: 0,
        turns: 0,
        latest: TokenUsage::default(),
        cumulative: TokenUsage::default(),
        estimated_tokens: 0,
    };
    let context = status_context(None)?;
    match output_format {
        CliOutputFormat::Text => println!(
            "{}",
            format_status_report(model, usage, permission_mode.as_str(), "standard", &context)
        ),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&status_json_value(
                Some(model),
                usage,
                permission_mode.as_str(),
                &context,
            ))?
        ),
    }
    Ok(())
}

fn status_json_value(
    model: Option<&str>,
    usage: StatusUsage,
    permission_mode: &str,
    context: &StatusContext,
) -> serde_json::Value {
    json!({
        "kind": "status",
        "model": model,
        "permission_mode": permission_mode,
        "usage": {
            "messages": usage.message_count,
            "turns": usage.turns,
            "latest_total": usage.latest.total_tokens(),
            "cumulative_input": usage.cumulative.input_tokens,
            "cumulative_output": usage.cumulative.output_tokens,
            "cumulative_total": usage.cumulative.total_tokens(),
            "estimated_tokens": usage.estimated_tokens,
        },
        "workspace": {
            "cwd": context.cwd,
            "project_root": context.project_root,
            "git_branch": context.git_branch,
            "git_state": context.git_summary.headline(),
            "changed_files": context.git_summary.changed_files,
            "staged_files": context.git_summary.staged_files,
            "unstaged_files": context.git_summary.unstaged_files,
            "untracked_files": context.git_summary.untracked_files,
            "session": context.session_path.as_ref().map_or_else(|| "live-repl".to_string(), |path| path.display().to_string()),
            "session_id": context.session_id.as_deref(),
            "session_store": context.session_store.as_str(),
            "loaded_config_files": context.loaded_config_files,
            "discovered_config_files": context.discovered_config_files,
            "memory_file_count": context.memory_file_count,
        },
        "sandbox": {
            "enabled": context.sandbox_status.enabled,
            "active": context.sandbox_status.active,
            "supported": context.sandbox_status.supported,
            "in_container": context.sandbox_status.in_container,
            "requested_namespace": context.sandbox_status.requested.namespace_restrictions,
            "active_namespace": context.sandbox_status.namespace_active,
            "requested_network": context.sandbox_status.requested.network_isolation,
            "active_network": context.sandbox_status.network_active,
            "filesystem_mode": context.sandbox_status.filesystem_mode.as_str(),
            "filesystem_active": context.sandbox_status.filesystem_active,
            "allowed_mounts": context.sandbox_status.allowed_mounts,
            "markers": context.sandbox_status.container_markers,
            "fallback_reason": context.sandbox_status.fallback_reason,
        }
    })
}

fn status_context(
    session_path: Option<&Path>,
) -> Result<StatusContext, Box<dyn std::error::Error>> {
    status_context_for_session(session_path, None)
}

fn status_context_for_session(
    session_path: Option<&Path>,
    session_id: Option<&str>,
) -> Result<StatusContext, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let discovered_config_files = loader.discover().len();
    let runtime_config = loader.load()?;
    let project_context = ProjectContext::discover_with_git(&cwd, DEFAULT_DATE)?;
    let (project_root, git_branch) =
        parse_git_status_metadata(project_context.git_status.as_deref());
    let git_summary = parse_git_workspace_summary(project_context.git_status.as_deref());
    let sandbox_status = resolve_sandbox_status(runtime_config.sandbox(), &cwd);
    Ok(StatusContext {
        cwd,
        session_path: session_path.map(Path::to_path_buf),
        session_id: session_id.map(ToOwned::to_owned).or_else(|| {
            session_path.and_then(|path| {
                if path.file_name().and_then(|n| n.to_str()) == Some("sessions.db") {
                    None
                } else {
                    path.file_stem().map(|n| n.to_string_lossy().into_owned())
                }
            })
        }),
        session_store: session_path.map_or_else(
            || "live-repl".to_string(),
            |path| {
                if path.file_name().and_then(|n| n.to_str()) == Some("sessions.db") {
                    "SQLite session store".to_string()
                } else {
                    "local import/export file".to_string()
                }
            },
        ),
        loaded_config_files: runtime_config.loaded_entries().len(),
        discovered_config_files,
        memory_file_count: project_context.instruction_files.len(),
        project_root,
        git_branch,
        git_summary,
        sandbox_status,
    })
}

fn format_status_report(
    model: &str,
    usage: StatusUsage,
    permission_mode: &str,
    execution_mode: &str,
    context: &StatusContext,
) -> String {
    [
        format!(
            "Status
  Model            {model}
  Permission mode  {permission_mode}
  Execution mode   {execution_mode}
  Messages         {}
  Turns            {}
  Estimated tokens {}",
            usage.message_count, usage.turns, usage.estimated_tokens,
        ),
        format!(
            "Usage
  Latest total     {}
  Cumulative input {}
  Cumulative output {}
  Cumulative total {}",
            usage.latest.total_tokens(),
            usage.cumulative.input_tokens,
            usage.cumulative.output_tokens,
            usage.cumulative.total_tokens(),
        ),
        format!(
            "Workspace
  Cwd              {}
  Project root     {}
  Git branch       {}
  Git state        {}
  Changed files    {}
  Staged           {}
  Unstaged         {}
  Untracked        {}
  Session          {}
  Session id       {}
  Session store    {}
  Config files     loaded {}/{}
  Memory files     {}
  Suggested flow   /status → /diff → /commit",
            context.cwd.display(),
            context
                .project_root
                .as_ref()
                .map_or_else(|| "unknown".to_string(), |path| path.display().to_string()),
            context.git_branch.as_deref().unwrap_or("unknown"),
            context.git_summary.headline(),
            context.git_summary.changed_files,
            context.git_summary.staged_files,
            context.git_summary.unstaged_files,
            context.git_summary.untracked_files,
            context.session_path.as_ref().map_or_else(
                || "live-repl".to_string(),
                |path| path.display().to_string()
            ),
            context.session_id.as_deref().unwrap_or("live-repl"),
            context.session_store.as_str(),
            context.loaded_config_files,
            context.discovered_config_files,
            context.memory_file_count,
        ),
        format_sandbox_report(&context.sandbox_status),
    ]
    .join(
        "

",
    )
}

fn format_sandbox_report(status: &runtime::SandboxStatus) -> String {
    format!(
        "Sandbox
  Enabled           {}
  Active            {}
  Supported         {}
  In container      {}
  Requested ns      {}
  Active ns         {}
  Requested net     {}
  Active net        {}
  Filesystem mode   {}
  Filesystem active {}
  Allowed mounts    {}
  Markers           {}
  Fallback reason   {}",
        status.enabled,
        status.active,
        status.supported,
        status.in_container,
        status.requested.namespace_restrictions,
        status.namespace_active,
        status.requested.network_isolation,
        status.network_active,
        status.filesystem_mode.as_str(),
        status.filesystem_active,
        if status.allowed_mounts.is_empty() {
            "<none>".to_string()
        } else {
            status.allowed_mounts.join(", ")
        },
        if status.container_markers.is_empty() {
            "<none>".to_string()
        } else {
            status.container_markers.join(", ")
        },
        status
            .fallback_reason
            .clone()
            .unwrap_or_else(|| "<none>".to_string()),
    )
}

fn format_commit_preflight_report(branch: Option<&str>, summary: GitWorkspaceSummary) -> String {
    format!(
        "Commit
  Result           ready
  Branch           {}
  Workspace        {}
  Changed files    {}
  Action           create a git commit from the current workspace changes",
        branch.unwrap_or("unknown"),
        summary.headline(),
        summary.changed_files,
    )
}

fn format_commit_skipped_report() -> String {
    "Commit
  Result           skipped
  Reason           no workspace changes
  Action           create a git commit from the current workspace changes
  Next             /status to inspect context · /diff to inspect repo changes"
        .to_string()
}

fn print_sandbox_status_snapshot(
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader
        .load()
        .unwrap_or_else(|_| runtime::RuntimeConfig::empty());
    let status = resolve_sandbox_status(runtime_config.sandbox(), &cwd);
    match output_format {
        CliOutputFormat::Text => println!("{}", format_sandbox_report(&status)),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&sandbox_json_value(&status))?
        ),
    }
    Ok(())
}

fn sandbox_json_value(status: &runtime::SandboxStatus) -> serde_json::Value {
    json!({
        "kind": "sandbox",
        "enabled": status.enabled,
        "active": status.active,
        "supported": status.supported,
        "in_container": status.in_container,
        "requested_namespace": status.requested.namespace_restrictions,
        "active_namespace": status.namespace_active,
        "requested_network": status.requested.network_isolation,
        "active_network": status.network_active,
        "filesystem_mode": status.filesystem_mode.as_str(),
        "filesystem_active": status.filesystem_active,
        "allowed_mounts": status.allowed_mounts,
        "markers": status.container_markers,
        "fallback_reason": status.fallback_reason,
    })
}

#[derive(Debug, Clone)]
struct SetupItem {
    id: &'static str,
    label: &'static str,
    status: &'static str,
    summary: String,
    next: Option<String>,
}

#[derive(Debug, Clone)]
struct SetupSnapshot {
    cwd: PathBuf,
    config_home: PathBuf,
    loaded_files: Vec<String>,
    gateway_running: bool,
    items: Vec<SetupItem>,
}

impl SetupSnapshot {
    fn overall_status(&self) -> &'static str {
        if self.items.iter().any(|item| item.status == "action") {
            "action"
        } else if self.items.iter().any(|item| item.status == "warn") {
            "warn"
        } else {
            "ready"
        }
    }

    fn next_action(&self) -> String {
        self.items
            .iter()
            .filter(|item| item.status == "action")
            .find_map(|item| item.next.clone())
            .or_else(|| self.items.iter().find_map(|item| item.next.clone()))
            .unwrap_or_else(|| "Start Cowd: cowd --yolo, or inspect runtime: /status".to_string())
    }
}

fn print_setup(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    match output_format {
        CliOutputFormat::Text => println!("{}", render_setup_report()?),
        CliOutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&render_setup_json()?)?)
        }
    }
    Ok(())
}

fn render_setup_report() -> Result<String, Box<dyn std::error::Error>> {
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

fn render_setup_json() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
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
    let gateway_running = server::get_server_status().ok().flatten().is_some();
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
    let db_path = config_home.join("sessions.db");
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

fn render_help_topic(topic: LocalHelpTopic) -> String {
    match topic {
        LocalHelpTopic::Status => "Status
  Usage            cowd status
  Purpose          show the local workspace snapshot without entering the TUI
  Output           model, permissions, git state, config files, and sandbox status
  Related          /status inside TUI · cowd --resume latest"
            .to_string(),
        LocalHelpTopic::Sandbox => "Sandbox
  Usage            cowd sandbox
  Purpose          inspect the resolved sandbox and isolation state for the current directory
  Output           namespace, network, filesystem, and fallback details
  Related          /sandbox · cowd status"
            .to_string(),
        LocalHelpTopic::Doctor => "Doctor
  Usage            cowd doctor
  Purpose          diagnose local auth, config, workspace, sandbox, and build metadata
  Output           local-only health report; no provider request or session resume required
  Related          /doctor inside TUI · cowd --resume latest"
            .to_string(),
        LocalHelpTopic::Setup => "Setup
  Usage            cowd setup
  Purpose          check local setup, channels, gateway, memory, sessions, and permissions
  Output           safe readiness report with no secrets
  Related          /setup · cowd gateway status · cowd gateway open"
            .to_string(),
    }
}

fn print_help_topic(topic: LocalHelpTopic) {
    println!("{}", render_help_topic(topic));
}

fn render_config_report(section: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
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
                return Ok(lines.join(
                    "
",
                ));
            }
        };
        lines.push(format!(
            "  {}",
            match value {
                Some(value) => value.render(),
                None => "<unset>".to_string(),
            }
        ));
        return Ok(lines.join(
            "
",
        ));
    }

    lines.push("Merged JSON".to_string());
    lines.push(format!("  {}", runtime_config.as_json().render()));
    Ok(lines.join(
        "
",
    ))
}

fn render_config_json(
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
            serde_json::json!({
                "path": e.path.display().to_string(),
                "source": source,
                "loaded": is_loaded,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "kind": "config",
        "cwd": cwd.display().to_string(),
        "loaded_files": loaded_paths.len(),
        "merged_keys": runtime_config.merged().len(),
        "files": files,
    }))
}

fn render_memory_report() -> Result<String, Box<dyn std::error::Error>> {
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
    Ok(lines.join(
        "
",
    ))
}

fn render_memory_json() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
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

fn init_claude_md() -> Result<String, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    Ok(initialize_repo(&cwd)?.render())
}

fn run_init(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let message = init_claude_md()?;
    match output_format {
        CliOutputFormat::Text => println!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&init_json_value(&message))?
        ),
    }
    Ok(())
}

fn init_json_value(message: &str) -> serde_json::Value {
    json!({
        "kind": "init",
        "message": message,
    })
}

fn normalize_permission_mode(mode: &str) -> Option<&'static str> {
    match mode.trim() {
        "read-only" => Some("read-only"),
        "workspace-write" => Some("workspace-write"),
        "danger-full-access" => Some("danger-full-access"),
        _ => None,
    }
}

fn render_diff_report() -> Result<String, Box<dyn std::error::Error>> {
    render_diff_report_for(&env::current_dir()?)
}

fn render_diff_report_for(cwd: &Path) -> Result<String, Box<dyn std::error::Error>> {
    // Verify we are inside a git repository before calling `git diff`.
    // Running `git diff --cached` outside a git tree produces a misleading
    // "unknown option `cached`" error because git falls back to --no-index mode.
    let in_git_repo = std::process::Command::new("git")
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

fn render_diff_json_for(cwd: &Path) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let in_git_repo = std::process::Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(cwd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !in_git_repo {
        return Ok(serde_json::json!({
            "kind": "diff",
            "result": "no_git_repo",
            "detail": format!("{} is not inside a git project", cwd.display()),
        }));
    }
    let staged = run_git_diff_command_in(cwd, &["diff", "--cached"])?;
    let unstaged = run_git_diff_command_in(cwd, &["diff"])?;
    Ok(serde_json::json!({
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
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!("git {} failed: {stderr}", args.join(" ")).into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn render_teleport_report(target: &str) -> Result<String, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;

    let file_list = Command::new("rg")
        .args(["--files"])
        .current_dir(&cwd)
        .output()?;
    let file_matches = if file_list.status.success() {
        String::from_utf8(file_list.stdout)?
            .lines()
            .filter(|line| line.contains(target))
            .take(10)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let content_output = Command::new("rg")
        .args(["-n", "-S", "--color", "never", target, "."])
        .current_dir(&cwd)
        .output()?;

    let mut lines = vec![
        "Teleport".to_string(),
        format!("  Target           {target}"),
        "  Action           search workspace files and content for the target".to_string(),
    ];
    if !file_matches.is_empty() {
        lines.push(String::new());
        lines.push("File matches".to_string());
        lines.extend(file_matches.into_iter().map(|path| format!("  {path}")));
    }

    if content_output.status.success() {
        let matches = String::from_utf8(content_output.stdout)?;
        if !matches.trim().is_empty() {
            lines.push(String::new());
            lines.push("Content matches".to_string());
            lines.push(truncate_for_prompt(&matches, 4_000));
        }
    }

    if lines.len() == 1 {
        lines.push("  Result           no matches found".to_string());
    }

    Ok(lines.join("\n"))
}

fn render_last_tool_debug_report(session: &Session) -> Result<String, Box<dyn std::error::Error>> {
    let last_tool_use = session
        .messages
        .iter()
        .rev()
        .find_map(|message| {
            message.blocks.iter().rev().find_map(|block| match block {
                ContentBlock::ToolUse { id, name, input } => {
                    Some((id.clone(), name.clone(), input.clone()))
                }
                _ => None,
            })
        })
        .ok_or_else(|| "no prior tool call found in session".to_string())?;

    let tool_result = session.messages.iter().rev().find_map(|message| {
        message.blocks.iter().rev().find_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } if tool_use_id == &last_tool_use.0 => {
                Some((tool_name.clone(), output.clone(), *is_error))
            }
            _ => None,
        })
    });

    let mut lines = vec![
        "Debug tool call".to_string(),
        "  Action           inspect the last recorded tool call and its result".to_string(),
        format!("  Tool id          {}", last_tool_use.0),
        format!("  Tool name        {}", last_tool_use.1),
        "  Input".to_string(),
        indent_block(&last_tool_use.2, 4),
    ];

    match tool_result {
        Some((tool_name, output, is_error)) => {
            lines.push("  Result".to_string());
            lines.push(format!("    name           {tool_name}"));
            lines.push(format!(
                "    status         {}",
                if is_error { "error" } else { "ok" }
            ));
            lines.push(indent_block(&output, 4));
        }
        None => lines.push("  Result           missing tool result".to_string()),
    }

    Ok(lines.join("\n"))
}

fn indent_block(value: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    value
        .lines()
        .map(|line| format!("{indent}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_no_args(
    command_name: &str,
    args: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(args) = args.map(str::trim).filter(|value| !value.is_empty()) {
        return Err(format!(
            "{command_name} does not accept arguments. Received: {args}\nUsage: {command_name}"
        )
        .into());
    }
    Ok(())
}

fn format_bughunter_report(scope: Option<&str>) -> String {
    format!(
        "Bughunter
  Scope            {}
  Action           inspect the selected code for likely bugs and correctness issues
  Output           findings should include file paths, severity, and suggested fixes",
        scope.unwrap_or("the current repository")
    )
}

fn format_ultraplan_report(task: Option<&str>) -> String {
    format!(
        "Ultraplan
  Task             {}
  Action           break work into a multi-step execution plan
  Output           plan should cover goals, risks, sequencing, verification, and rollback",
        task.unwrap_or("the current repo work")
    )
}

fn format_pr_report(branch: &str, context: Option<&str>) -> String {
    format!(
        "PR
  Branch           {branch}
  Context          {}
  Action           draft or create a pull request for the current branch
  Output           title and markdown body suitable for GitHub",
        context.unwrap_or("none")
    )
}

fn format_issue_report(context: Option<&str>) -> String {
    format!(
        "Issue
  Context          {}
  Action           draft or create a GitHub issue from the current context
  Output           title and markdown body suitable for GitHub",
        context.unwrap_or("none")
    )
}

fn git_output(args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env::current_dir()?)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!("git {} failed: {stderr}", args.join(" ")).into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn git_status_ok(args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env::current_dir()?)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!("git {} failed: {stderr}", args.join(" ")).into());
    }
    Ok(())
}

fn command_exists(name: &str) -> bool {
    // Safety: validate input to prevent path traversal and command injection.
    // Only alphanumeric characters, hyphens, underscores, and dots are allowed
    // to prevent passing arbitrary arguments to external commands.
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return false;
    }
    // Search PATH directories for the executable (no shell invocation).
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
}

fn write_temp_text_file(
    filename: &str,
    contents: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = env::temp_dir().join(filename);
    fs::write(&path, contents)?;
    Ok(path)
}

const DEFAULT_HISTORY_LIMIT: usize = 20;

fn parse_history_count(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_HISTORY_LIMIT);
    };
    let parsed: usize = raw
        .parse()
        .map_err(|_| format!("history: invalid count '{raw}'. Expected a positive integer."))?;
    if parsed == 0 {
        return Err("history: count must be greater than 0.".to_string());
    }
    Ok(parsed)
}

fn format_history_timestamp(timestamp_ms: u64) -> String {
    let secs = timestamp_ms / 1_000;
    let subsec_ms = timestamp_ms % 1_000;
    let days_since_epoch = secs / 86_400;
    let seconds_of_day = secs % 86_400;
    let hours = seconds_of_day / 3_600;
    let minutes = (seconds_of_day % 3_600) / 60;
    let seconds = seconds_of_day % 60;

    let (year, month, day) = civil_from_days(i64::try_from(days_since_epoch).unwrap_or(0));
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{subsec_ms:03}Z")
}

// Computes civil (Gregorian) year/month/day from days since the Unix epoch
// (1970-01-01) using Howard Hinnant's `civil_from_days` algorithm.
#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u64; // [0, 146_096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = y + i64::from(m <= 2);
    (y as i32, m as u32, d as u32)
}

fn render_prompt_history_report(entries: &[PromptHistoryEntry], limit: usize) -> String {
    if entries.is_empty() {
        return "Prompt history\n  Result           no prompts recorded yet".to_string();
    }

    let total = entries.len();
    let start = total.saturating_sub(limit);
    let shown = &entries[start..];
    let mut lines = vec![
        "Prompt history".to_string(),
        format!("  Total            {total}"),
        format!("  Showing          {} most recent", shown.len()),
        format!("  Reverse search   Ctrl-R in the REPL"),
        String::new(),
    ];
    for (offset, entry) in shown.iter().enumerate() {
        let absolute_index = start + offset + 1;
        let timestamp = format_history_timestamp(entry.timestamp_ms);
        let first_line = entry.text.lines().next().unwrap_or("").trim();
        let display = if first_line.chars().count() > 80 {
            let truncated: String = first_line.chars().take(77).collect();
            format!("{truncated}...")
        } else {
            first_line.to_string()
        };
        lines.push(format!("  {absolute_index:>3}. [{timestamp}] {display}"));
    }
    lines.join("\n")
}

fn collect_session_prompt_history(session: &Session) -> Vec<PromptHistoryEntry> {
    if !session.prompt_history.is_empty() {
        return session
            .prompt_history
            .iter()
            .map(|entry| PromptHistoryEntry {
                timestamp_ms: entry.timestamp_ms,
                text: entry.text.clone(),
            })
            .collect();
    }
    let timestamp_ms = session.updated_at_ms;
    session
        .messages
        .iter()
        .filter(|message| message.role == MessageRole::User)
        .filter_map(|message| {
            message.blocks.iter().find_map(|block| match block {
                ContentBlock::Text { text } => Some(PromptHistoryEntry {
                    timestamp_ms,
                    text: text.clone(),
                }),
                _ => None,
            })
        })
        .collect()
}

fn recent_user_context(session: &Session, limit: usize) -> String {
    let requests = session
        .messages
        .iter()
        .filter(|message| message.role == MessageRole::User)
        .filter_map(|message| {
            message.blocks.iter().find_map(|block| match block {
                ContentBlock::Text { text } => Some(text.trim().to_string()),
                _ => None,
            })
        })
        .rev()
        .take(limit)
        .collect::<Vec<_>>();

    if requests.is_empty() {
        "<no prior user messages>".to_string()
    } else {
        requests
            .into_iter()
            .rev()
            .enumerate()
            .map(|(index, text)| format!("{}. {}", index + 1, text))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn truncate_for_prompt(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        value.trim().to_string()
    } else {
        let truncated = value.chars().take(limit).collect::<String>();
        format!("{}\n…[truncated]", truncated.trim_end())
    }
}

fn sanitize_generated_message(value: &str) -> String {
    value.trim().trim_matches('`').trim().replace("\r\n", "\n")
}

fn parse_titled_body(value: &str) -> Option<(String, String)> {
    let normalized = sanitize_generated_message(value);
    let title = normalized
        .lines()
        .find_map(|line| line.strip_prefix("TITLE:").map(str::trim))?;
    let body_start = normalized.find("BODY:")?;
    let body = normalized[body_start + "BODY:".len()..].trim();
    Some((title.to_string(), body.to_string()))
}

fn render_version_report() -> String {
    let git_sha = GIT_SHA.unwrap_or("unknown");
    let target = BUILD_TARGET.unwrap_or("unknown");
    format!(
        "Cowd\n  Version          {VERSION}\n  Git SHA          {git_sha}\n  Target           {target}\n  Build date       {DEFAULT_DATE}"
    )
}

fn render_export_text(session: &Session) -> String {
    let mut lines = vec!["# Conversation Export".to_string(), String::new()];
    for (index, message) in session.messages.iter().enumerate() {
        let role = match message.role {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        };
        lines.push(format!("## {}. {role}", index + 1));
        for block in &message.blocks {
            match block {
                ContentBlock::Text { text } => lines.push(text.clone()),
                ContentBlock::Thinking { thinking, .. } => {
                    lines.push(format!("[thinking] {thinking}"))
                }
                ContentBlock::ToolUse { id, name, input } => {
                    lines.push(format!("[tool_use id={id} name={name}] {input}"));
                }
                ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    is_error,
                } => {
                    lines.push(format!(
                        "[tool_result id={tool_use_id} name={tool_name} error={is_error}] {output}"
                    ));
                }
            }
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

fn default_export_filename(session: &Session) -> String {
    let stem = session
        .messages
        .iter()
        .find_map(|message| match message.role {
            MessageRole::User => message.blocks.iter().find_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .map_or("conversation", |text| {
            text.lines().next().unwrap_or("conversation")
        })
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join("-");
    let fallback = if stem.is_empty() {
        "conversation"
    } else {
        &stem
    };
    format!("{fallback}.txt")
}

fn resolve_export_path(
    requested_path: Option<&str>,
    session: &Session,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let file_name =
        requested_path.map_or_else(|| default_export_filename(session), ToOwned::to_owned);
    let final_name = if Path::new(&file_name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("txt"))
    {
        file_name
    } else {
        format!("{file_name}.txt")
    };
    Ok(cwd.join(final_name))
}

const SESSION_MARKDOWN_TOOL_SUMMARY_LIMIT: usize = 280;

fn summarize_tool_payload_for_markdown(payload: &str) -> String {
    let compact = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(value) => value.to_string(),
        Err(_) => payload.split_whitespace().collect::<Vec<_>>().join(" "),
    };
    if compact.is_empty() {
        return String::new();
    }
    truncate_for_summary(&compact, SESSION_MARKDOWN_TOOL_SUMMARY_LIMIT)
}

/// Fallback: load providers directly from the active Cowd config home.
/// when ConfigLoader merge loses them.
fn fallback_init_providers_from_user_config() {
    let user_cfg = runtime::cowd_dirs::config_home_dir().join("config.yaml");
    if !user_cfg.exists() {
        return;
    }
    let raw = match std::fs::read_to_string(&user_cfg) {
        Ok(s) => s,
        Err(_) => return,
    };
    let yaml_val: serde_yaml::Value = match serde_yaml::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return,
    };
    let providers_yaml = match yaml_val.get("providers") {
        Some(v) => v,
        None => return,
    };
    let providers_map = match providers_yaml.as_mapping() {
        Some(m) => m,
        None => return,
    };

    let mut providers = std::collections::HashMap::new();
    for (key, value) in providers_map {
        let name = match key.as_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let entry = match value.as_mapping() {
            Some(m) => m,
            None => continue,
        };
        let base_url = entry
            .get("base_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let api_key = entry
            .get("api_key")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let models: Vec<String> = entry
            .get("models")
            .and_then(|v| v.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let protocol = entry
            .get("protocol")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        providers.insert(
            name.clone(),
            runtime::ProviderConfig {
                name: name.clone(),
                base_url,
                api_key,
                models,
                protocol,
            },
        );
    }

    runtime::init_global_providers(runtime::ProvidersConfig { providers });
    tracing::warn!(
        path = %user_cfg.display(),
        "[init] fallback: loaded {} providers from Cowd config home",
        runtime::list_all_providers().len()
    );
}

fn init_runtime_providers_for_cwd(cwd: &Path) {
    let loader = runtime::ConfigLoader::default_for(cwd);
    match loader.load() {
        Ok(cfg) => {
            let providers = cfg.providers().clone();
            tracing::debug!(
                "[init] merged providers count: {}",
                providers.providers.len()
            );
            if !providers.is_empty() {
                runtime::init_global_providers(providers);
            } else {
                fallback_init_providers_from_user_config();
            }
        }
        Err(e) => {
            tracing::warn!("failed to load config for provider registry: {e}");
            fallback_init_providers_from_user_config();
        }
    }
}

fn run_prompt(
    text: &str,
    model: String,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    base_commit: Option<String>,
    reasoning_effort: Option<String>,
    allow_broad_cwd: bool,
    yolo_mode: bool,
    compact: bool,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    enforce_broad_cwd_policy(allow_broad_cwd, output_format)?;
    run_stale_base_preflight(base_commit.as_deref());
    let resolved_model = resolve_repl_model(model);
    let system_prompt = build_system_prompt_for_mode(yolo_mode)?;
    let session_state = new_cli_session()?;
    let session = create_managed_session_handle(&session_state.session_id)?;
    let cwd = std::env::current_dir()?;
    init_runtime_providers_for_cwd(&cwd);
    let mut runtime = build_runtime(
        session_state,
        &session.id,
        resolved_model,
        system_prompt,
        true,
        false,
        allowed_tools,
        permission_mode,
        None,
        None,
    )?;
    apply_cli_turn_context_profile(&runtime, yolo_mode, permission_mode, false, false);
    if let Some(effort) = reasoning_effort {
        if let Some(rt) = runtime.runtime.as_mut() {
            rt.api_client_mut().set_reasoning_effort(Some(effort));
        }
    }
    let prompter = runtime::permissions::SharedPrompter::new(Box::new(CliPermissionPrompter::new(
        permission_mode,
    )));
    let handle =
        tokio::runtime::Handle::try_current().unwrap_or_else(|_| SHARED_RT.handle().clone());
    let summary = handle.block_on(runtime.run_turn_async(text, &prompter))?;
    runtime.shutdown_plugins()?;
    let final_text = final_assistant_text(&summary).trim().to_string();
    match output_format {
        CliOutputFormat::Text => {
            if compact {
                // --compact: print only the final assistant text
                println!("{final_text}");
            } else {
                // Full output: print text (includes any thinking blocks)
                println!("{final_text}");
            }
        }
        CliOutputFormat::Json => {
            let cost = summary.usage.estimate_cost_usd().total_cost_usd();
            let response = serde_json::json!({
                "message": final_text,
                "text": final_text,
                "iterations": summary.iterations,
                "tool_uses": collect_tool_uses(&summary),
                "tool_results": collect_tool_results(&summary),
                "prompt_cache_events": collect_prompt_cache_events(&summary),
                "auto_compaction": summary.auto_compaction.as_ref().map(|event| json!({
                    "removed_message_count": event.removed_message_count,
                })),
                "usage": {
                    "input_tokens": summary.usage.input_tokens,
                    "output_tokens": summary.usage.output_tokens,
                    "cache_creation_input_tokens": summary.usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": summary.usage.cache_read_input_tokens,
                    "total_tokens": summary.usage.total_tokens(),
                },
                "estimated_cost": runtime::format_usd(cost),
                "compact": compact,
            });
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
    }
    Ok(())
}

fn run_install(systemd: bool, path: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let raw_install_dir = path
        .map(PathBuf::from)
        .unwrap_or_else(runtime::cowd_dirs::config_home_dir);
    let install_dir = if raw_install_dir.file_name().and_then(|name| name.to_str()) == Some("bin") {
        raw_install_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or(raw_install_dir)
    } else {
        raw_install_dir
    };
    let bin_dir = install_dir.join("bin");
    std::fs::create_dir_all(&bin_dir)?;

    let current_exe = std::env::current_exe()?;
    let target = bin_dir.join("cowd");
    std::fs::copy(&current_exe, &target)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))?;
    }

    println!("Installed cowd to {}", target.display());
    println!("WebUI assets are optional; configure gateway.webui_dir to enable browser UI.");

    if systemd {
        let unit = format!(
            r#"[Unit]
Description=COWD Gateway Daemon
After=network.target

[Service]
ExecStart={} gateway start
Restart=always
RestartSec=5
Environment=RUST_LOG=warn

[Install]
WantedBy=default.target
"#,
            target.display()
        );
        let home_dir = std::env::var("HOME").unwrap_or_else(|_| "~".to_string());
        let unit_path = PathBuf::from(&home_dir)
            .join(".config")
            .join("systemd")
            .join("user")
            .join("cowd-gateway.service");
        if let Some(parent) = unit_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&unit_path, &unit)?;
        println!("Created systemd unit at {}", unit_path.display());
        println!(
            "To enable: systemctl --user enable --now {}",
            unit_path.display()
        );
    }
    Ok(())
}

fn gateway_auth_token_from_platform(platform: &runtime::GatewayPlatformConfig) -> Option<String> {
    // Prefer flat auth_token key (legacy format).
    let flat = platform.extra.get("auth_token").and_then(|v| v.as_str());

    // Fallback: nested auth.token (current config format).
    let nested = platform
        .extra
        .get("auth")
        .and_then(|v| v.as_object())
        .and_then(|auth_obj| auth_obj.get("token"))
        .and_then(|v| v.as_str());

    flat.or(nested)
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(String::from)
}

fn run_export(
    session_reference: &str,
    output_path: Option<&Path>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let (handle, session) = load_session_reference(session_reference)?;
    let markdown = render_session_markdown(&session, &handle.id, &handle.path);

    if let Some(path) = output_path {
        fs::write(path, &markdown)?;
        let report = format!(
            "Export\n  Result           wrote markdown transcript\n  File             {}\n  Session          {}\n  Messages         {}",
            path.display(),
            handle.id,
            session.messages.len(),
        );
        match output_format {
            CliOutputFormat::Text => println!("{report}"),
            CliOutputFormat::Json => println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "kind": "export",
                    "message": report,
                    "session_id": handle.id,
                    "file": path.display().to_string(),
                    "messages": session.messages.len(),
                }))?
            ),
        }
        return Ok(());
    }

    match output_format {
        CliOutputFormat::Text => {
            print!("{markdown}");
            if !markdown.ends_with('\n') {
                println!();
            }
        }
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "kind": "export",
                "session_id": handle.id,
                "file": handle.path.display().to_string(),
                "messages": session.messages.len(),
                "markdown": markdown,
            }))?
        ),
    }
    Ok(())
}

fn render_session_markdown(session: &Session, session_id: &str, session_path: &Path) -> String {
    let mut lines = vec![
        "# Conversation Export".to_string(),
        String::new(),
        format!("- **Session**: `{session_id}`"),
        format!("- **File**: `{}`", session_path.display()),
        format!("- **Messages**: {}", session.messages.len()),
    ];
    if let Some(workspace_root) = session.workspace_root() {
        lines.push(format!("- **Workspace**: `{}`", workspace_root.display()));
    }
    if let Some(fork) = &session.fork {
        let branch = fork.branch_name.as_deref().unwrap_or("(unnamed)");
        lines.push(format!(
            "- **Forked from**: `{}` (branch `{branch}`)",
            fork.parent_session_id
        ));
    }
    if let Some(compaction) = &session.compaction {
        lines.push(format!(
            "- **Compactions**: {} (last removed {} messages)",
            compaction.count, compaction.removed_message_count
        ));
    }
    lines.push(String::new());
    lines.push("---".to_string());
    lines.push(String::new());

    for (index, message) in session.messages.iter().enumerate() {
        let role = match message.role {
            MessageRole::System => "System",
            MessageRole::User => "User",
            MessageRole::Assistant => "Assistant",
            MessageRole::Tool => "Tool",
        };
        lines.push(format!("## {}. {role}", index + 1));
        lines.push(String::new());
        for block in &message.blocks {
            match block {
                ContentBlock::Text { text } => {
                    let trimmed = text.trim_end();
                    if !trimmed.is_empty() {
                        lines.push(trimmed.to_string());
                        lines.push(String::new());
                    }
                }
                ContentBlock::Thinking { thinking, .. } => {
                    lines.push(format!(
                        "> **Thinking:** {}",
                        thinking.chars().take(200).collect::<String>()
                    ));
                    lines.push(String::new());
                }
                ContentBlock::ToolUse { id, name, input } => {
                    lines.push(format!(
                        "**Tool call** `{name}` _(id `{}`)_",
                        short_tool_id(id)
                    ));
                    let summary = summarize_tool_payload_for_markdown(input);
                    if !summary.is_empty() {
                        lines.push(format!("> {summary}"));
                    }
                    lines.push(String::new());
                }
                ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    is_error,
                } => {
                    let status = if *is_error { "error" } else { "ok" };
                    lines.push(format!(
                        "**Tool result** `{tool_name}` _(id `{}`, {status})_",
                        short_tool_id(tool_use_id)
                    ));
                    let summary = summarize_tool_payload_for_markdown(output);
                    if !summary.is_empty() {
                        lines.push(format!("> {summary}"));
                    }
                    lines.push(String::new());
                }
            }
        }
        if let Some(usage) = message.usage {
            lines.push(format!(
                "_tokens: in={} out={} cache_create={} cache_read={}_",
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
            ));
            lines.push(String::new());
        }
    }
    lines.join("\n")
}

fn short_tool_id(id: &str) -> String {
    let char_count = id.chars().count();
    if char_count <= 12 {
        return id.to_string();
    }
    let prefix: String = id.chars().take(12).collect();
    format!("{prefix}…")
}

pub(crate) fn build_system_prompt() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    build_system_prompt_for_mode(false)
}

pub(crate) fn build_system_prompt_for_mode(
    yolo_mode: bool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut sections = load_system_prompt(
        env::current_dir()?,
        DEFAULT_DATE,
        env::consts::OS,
        "unknown",
    )?;
    if yolo_mode {
        sections.push(yolo_mode_system_instruction().to_string());
    }
    Ok(sections)
}

fn yolo_mode_system_instruction() -> &'static str {
    "YOLO continuous execution mode is active.\n\
Treat the user's objective as a persistent goal: decompose it, implement it, verify it, review it, and continue without waiting for extra confirmation until the goal is complete or a concrete external blocker makes further progress impossible.\n\
Use the full tool surface allowed by danger-full-access, but keep edits scoped, preserve user changes, avoid destructive git operations, run relevant automated and scenario tests, monitor logs or services when needed, and clean up temporary services/tmux sessions before reporting.\n\
After each major phase, self-review against correctness, stability, interaction quality, and performance; then continue to the next highest-impact gap."
}

pub(crate) fn ensure_yolo_task(
    yolo_mode: bool,
    objective: impl Into<String>,
) -> Result<Option<task_kernel::TaskRecord>, String> {
    if !yolo_mode {
        return Ok(None);
    }
    let kernel =
        task_kernel::TaskKernel::open(runtime::cowd_dirs::config_home_dir().join("tasks.db"))?;
    if let Some(current) = kernel.current() {
        return Ok(Some(current));
    }
    kernel.start_goal(objective, true).map(Some)
}

fn build_runtime_plugin_state() -> Result<RuntimePluginState, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load()?;
    build_runtime_plugin_state_with_loader(&cwd, &loader, &runtime_config)
}

fn build_runtime_plugin_state_with_loader(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> Result<RuntimePluginState, Box<dyn std::error::Error>> {
    let plugin_manager = build_plugin_manager(cwd, loader, runtime_config);
    let plugin_registry = plugin_manager.plugin_registry()?;
    let plugin_hook_config =
        runtime_hook_config_from_plugin_hooks(plugin_registry.aggregated_hooks()?);
    let feature_config = runtime_config
        .feature_config()
        .clone()
        .with_hooks(runtime_config.hooks().merged(&plugin_hook_config));
    let (mcp_state, runtime_tools) = build_runtime_mcp_state(runtime_config)?;
    let tool_registry = GlobalToolRegistry::with_plugin_tools(plugin_registry.aggregated_tools()?)?
        .with_runtime_tools(runtime_tools)?;
    Ok(RuntimePluginState {
        feature_config,
        tool_registry,
        plugin_registry,
        mcp_state,
    })
}

fn build_plugin_manager(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> PluginManager {
    let plugin_settings = runtime_config.plugins();
    let mut plugin_config = PluginManagerConfig::new(loader.config_home().to_path_buf());
    // Start with config.yaml's enabled_plugins (user-defined defaults)
    plugin_config.enabled_plugins = plugin_settings.enabled_plugins().clone();
    // Merge plugin-state.json runtime overrides (take precedence over config.yaml)
    let state_path = runtime::cowd_dirs::config_home_dir().join("plugin-state.json");
    if let Ok(content) = std::fs::read_to_string(&state_path) {
        if !content.trim().is_empty() {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(map) = val.get("enabledPlugins").and_then(|v| v.as_object()) {
                    for (k, v) in map {
                        if let Some(enabled) = v.as_bool() {
                            plugin_config.enabled_plugins.insert(k.clone(), enabled);
                        }
                    }
                }
            }
        }
    }
    plugin_config.external_dirs = plugin_settings
        .external_directories()
        .iter()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path))
        .collect();
    plugin_config.install_root = plugin_settings
        .install_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.registry_path = plugin_settings
        .registry_path()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.bundled_root = plugin_settings
        .bundled_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    PluginManager::new(plugin_config)
}

fn resolve_plugin_path(cwd: &Path, config_home: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else if value.starts_with('.') {
        cwd.join(path)
    } else {
        config_home.join(path)
    }
}

fn runtime_hook_config_from_plugin_hooks(hooks: PluginHooks) -> runtime::RuntimeHookConfig {
    runtime::RuntimeHookConfig::new(
        hooks.pre_tool_use,
        hooks.post_tool_use,
        hooks.post_tool_use_failure,
    )
}

fn compact_message_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            ContentBlock::Thinking { thinking, .. } => Some(thinking.as_str()),
            ContentBlock::ToolUse { name, .. } => Some(name.as_str()),
            ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
        })
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn session_db_resume_context_packet(session: &Session) -> Option<ResumeContextPacket> {
    if session.messages.is_empty()
        && session.compaction.is_none()
        && session.fork.is_none()
        && session.prompt_history.is_empty()
    {
        return None;
    }

    let recent_turns = session
        .messages
        .iter()
        .rev()
        .take(4)
        .filter_map(|message| {
            let text = compact_message_text(message);
            (!text.is_empty()).then(|| {
                format!(
                    "{}: {}",
                    message.role.role_str(),
                    text.chars().take(240).collect::<String>()
                )
            })
        })
        .collect::<Vec<_>>();

    let mut recent_decisions = Vec::new();
    if let Some(compaction) = &session.compaction {
        recent_decisions.push(format!(
            "compaction#{} removed {} messages: {}",
            compaction.count, compaction.removed_message_count, compaction.summary
        ));
    }
    if let Some(fork) = &session.fork {
        recent_decisions.push(format!(
            "forked from {}{}",
            fork.parent_session_id,
            fork.branch_name
                .as_ref()
                .map(|name| format!(" branch={name}"))
                .unwrap_or_default()
        ));
    }
    if let Some(last_prompt) = session.prompt_history.last() {
        recent_decisions.push(format!(
            "last prompt: {}",
            last_prompt
                .text
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }

    Some(ResumeContextPacket {
        session_id: session.session_id.clone(),
        handoff_summary: session
            .compaction
            .as_ref()
            .map(|compaction| compaction.summary.clone()),
        active_task: (!recent_turns.is_empty()).then(|| recent_turns.join("\n")),
        recent_decisions,
        blockers: Vec::new(),
        source: ResumeContextSource::SessionDb,
    })
}

fn handoff_resume_context_packet(handoff: &memory::HandoffData) -> ResumeContextPacket {
    let active_task = handoff.task_states.first().map(|task| {
        format!(
            "task={} progress={} checkpoint={} context={}",
            task.task_id, task.progress_percent, task.last_checkpoint, task.context
        )
    });
    let mut recent_decisions = handoff
        .decisions
        .iter()
        .take(6)
        .map(|decision| format!("{}: {}", decision.summary, decision.rationale))
        .collect::<Vec<_>>();
    recent_decisions.extend(handoff.work_items.iter().take(4).map(|item| {
        format!(
            "work {:?}: {} - {}",
            item.status, item.title, item.description
        )
    }));
    let blockers = handoff
        .blockers
        .iter()
        .take(6)
        .map(|blocker| {
            blocker
                .resolution_hint
                .as_ref()
                .map(|hint| format!("{}; hint: {hint}", blocker.description))
                .unwrap_or_else(|| blocker.description.clone())
        })
        .collect::<Vec<_>>();

    ResumeContextPacket {
        session_id: handoff.session_id.clone(),
        handoff_summary: (!handoff.summary.is_empty()).then(|| handoff.summary.clone()),
        active_task,
        recent_decisions,
        blockers,
        source: ResumeContextSource::Handoff,
    }
}

fn inject_auto_resume_context(
    runtime: &ConversationRuntime<AnthropicRuntimeClient, CliToolExecutor>,
    session_packet: Option<ResumeContextPacket>,
    session_id: &str,
) -> bool {
    let mut injected = false;
    if let Some(packet) = session_packet {
        runtime.inject_resume_context(packet);
        injected = true;
    }
    let manager = memory::HandoffManager::new();
    let handoff = manager
        .load(session_id)
        .unwrap_or_else(|err| {
            tracing::debug!(%session_id, error = %err, "failed to load exact handoff");
            None
        })
        .or_else(|| {
            manager.load_latest().unwrap_or_else(|err| {
                tracing::debug!(%session_id, error = %err, "failed to load latest handoff");
                None
            })
        });
    if let Some(handoff) = handoff {
        runtime.inject_resume_context(handoff_resume_context_packet(&handoff));
        injected = true;
    }
    injected
}

fn workspace_context_item(session: &Session, model_ctx: u32) -> runtime::ContextItem {
    let root_path = session
        .workspace_root
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let git_snapshot = workspace_git_snapshot(&root_path, 16);
    let mut project_notes = vec![format!("model_context_window={model_ctx}")];
    if let Some(branch) = git_snapshot.branch {
        project_notes.push(format!("git_branch={branch}"));
    }
    if git_snapshot.touched_files_truncated {
        project_notes.push("changed_files_truncated=true".to_string());
    }
    let hot_symbols = workspace_hot_symbols(&root_path, &git_snapshot.touched_files, 8);
    let token_estimate =
        64 + (git_snapshot.touched_files.len() as u64 * 16) + (hot_symbols.len() as u64 * 18);
    let packet = runtime::WorkspacePacket {
        root: root_path.display().to_string(),
        touched_files: git_snapshot.touched_files,
        hot_symbols,
        project_notes,
        token_estimate,
    };
    runtime::ContextRuntimeKernel::workspace_item(&packet)
}

struct WorkspaceGitSnapshot {
    branch: Option<String>,
    touched_files: Vec<String>,
    touched_files_truncated: bool,
}

fn workspace_git_snapshot(root: &Path, file_limit: usize) -> WorkspaceGitSnapshot {
    let branch = git_current_branch(root);
    let (touched_files, touched_files_truncated) = git_changed_files(root, file_limit);
    WorkspaceGitSnapshot {
        branch,
        touched_files,
        touched_files_truncated,
    }
}

fn git_current_branch(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["branch", "--show-current"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!branch.is_empty()).then_some(branch)
}

fn git_changed_files(root: &Path, file_limit: usize) -> (Vec<String>, bool) {
    let output = match Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["status", "--short", "--untracked-files=no"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return (Vec::new(), false),
    };

    let mut files = Vec::new();
    let mut truncated = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some(path) = parse_git_status_path(line) else {
            continue;
        };
        if files.len() >= file_limit {
            truncated = true;
            break;
        }
        files.push(path);
    }
    (files, truncated)
}

fn parse_git_status_path(line: &str) -> Option<String> {
    let path = line.get(3..)?.trim();
    if path.is_empty() {
        return None;
    }
    let path = path
        .rsplit_once(" -> ")
        .map(|(_, target)| target)
        .unwrap_or(path)
        .trim_matches('"')
        .to_string();
    (!path.is_empty()).then_some(path)
}

fn workspace_hot_symbols(
    root: &Path,
    touched_files: &[String],
    symbol_limit: usize,
) -> Vec<String> {
    const FILE_LIMIT: usize = 4;
    const MAX_BYTES: u64 = 256 * 1024;

    if touched_files.is_empty() || symbol_limit == 0 {
        return Vec::new();
    }
    let mut indexer = match memory::CodeIndexer::new(root) {
        Ok(indexer) => indexer,
        Err(err) => {
            tracing::debug!(error = %err, "failed to initialise workspace code indexer");
            return Vec::new();
        }
    };

    let mut hot_symbols = Vec::new();
    for relative in touched_files.iter().take(FILE_LIMIT) {
        if hot_symbols.len() >= symbol_limit {
            break;
        }
        let path = root.join(relative);
        if !memory::IndexLanguage::is_indexable(&path) {
            continue;
        }
        let Ok(metadata) = std::fs::metadata(&path) else {
            continue;
        };
        if !metadata.is_file() || metadata.len() > MAX_BYTES {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok((symbols, _edges)) = indexer.index_content(&content, &path) else {
            continue;
        };
        for symbol in symbols {
            if hot_symbols.len() >= symbol_limit {
                break;
            }
            hot_symbols.push(format!(
                "{}:{}:{}:{}",
                symbol.file_path,
                symbol.line,
                symbol.kind.as_str(),
                symbol.name
            ));
        }
    }
    hot_symbols
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_runtime(
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<BuiltRuntime, Box<dyn std::error::Error>> {
    let runtime_plugin_state = build_runtime_plugin_state()?;
    build_runtime_with_plugin_state(
        None,
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_runtime_with_session_store(
    session_store: Arc<memory::session_store::UnifiedSessionStore>,
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<BuiltRuntime, Box<dyn std::error::Error>> {
    let runtime_plugin_state = build_runtime_plugin_state()?;
    build_runtime_with_plugin_state(
        Some(session_store),
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

/// Production executor for sub-agent tasks in the CLI.

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
fn build_runtime_with_plugin_state(
    session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    mut session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    runtime_plugin_state: RuntimePluginState,
) -> Result<BuiltRuntime, Box<dyn std::error::Error>> {
    // Persist the model in session metadata so resumed sessions can report it.
    if session.model.is_none() {
        session.model = Some(model.clone());
    }
    let session_resume_packet = session_db_resume_context_packet(&session);
    let RuntimePluginState {
        feature_config,
        tool_registry,
        plugin_registry,
        mcp_state,
    } = runtime_plugin_state;
    plugin_registry.initialize()?;
    let policy = permission_policy(permission_mode, &feature_config, &tool_registry)
        .map_err(std::io::Error::other)?;
    let overrides = feature_config.model_context_windows();
    let model_ctx = provider::model_context_window_with_overrides(&model, Some(&overrides));
    let workspace_item = workspace_context_item(&session, model_ctx);
    // Clone model for sub-agent usage before it's consumed by the main runtime.
    let subagent_model = model.clone();
    // Shared tool executor — used by both the main runtime and sub-agent factory.
    let subagent_tool_executor = std::sync::Arc::new(CliToolExecutor::new(
        allowed_tools.clone(),
        emit_output,
        tool_registry.clone(),
        mcp_state.clone(),
    ));
    let mut runtime = ConversationRuntime::new_with_features(
        session,
        AnthropicRuntimeClient::new(
            session_id,
            model,
            enable_tools,
            emit_output,
            allowed_tools.clone(),
            tool_registry.clone(),
            stream_callback.clone(),
        )?,
        subagent_tool_executor.clone(),
        policy,
        system_prompt,
        &feature_config,
    );
    runtime = runtime.with_model_context_window(model_ctx);
    if let Some(store) = session_store {
        runtime = runtime.with_session_store(store);
    }
    if let Some(ref tx) = stream_callback {
        let _ = tx.try_send(runtime::CowdEvent::ContextWindow(model_ctx as u64));
    }
    if let Some(callback) = tool_callback {
        runtime = runtime.with_tool_callback(callback);
    }
    if emit_output {
        runtime = runtime.with_hook_progress_reporter(Box::new(CliHookProgressReporter));
    }
    let cowd_bus = runtime::CowdEventBus::new();
    runtime = runtime.with_cowd_event_bus(cowd_bus);
    runtime.push_external_context_item(workspace_item);
    let resume_context_loaded =
        inject_auto_resume_context(&runtime, session_resume_packet, session_id);
    // Wire the production sub-agent executor so the collaboration pipeline
    // can delegate real work to sub-agents.
    {
        let session_id_owned = session_id.to_string();
        let allowed_tools_clone = allowed_tools.clone();
        let tool_registry_clone = tool_registry.clone();
        let executor = runtime::agent::ProductionExecutor::new(
            move || {
                AnthropicRuntimeClient::new(
                    &session_id_owned,
                    subagent_model.clone(),
                    true,  // sub-agents need tool access
                    false, // no TUI streaming for sub-agents
                    allowed_tools_clone.clone(),
                    tool_registry_clone.clone(),
                    None, // no stream callback for sub-agents
                )
                .expect("sub-agent API client creation failed")
            },
            subagent_tool_executor.clone(),
        );
        let executor_arc = std::sync::Arc::new(executor);
        runtime = runtime.with_collaboration(runtime::agent_collaboration::new_boxed(
            executor_arc.clone(),
        ));
        // Wire JPS pipeline for complex task routing
        let jps_pipeline = runtime::joint_problem_solving::new_boxed::<
            runtime::agent::ProductionExecutor<AnthropicRuntimeClient, CliToolExecutor>,
        >(executor_arc);
        runtime = runtime.with_jps_pipeline(jps_pipeline);
    }
    Ok(BuiltRuntime::new(
        runtime,
        plugin_registry,
        mcp_state,
        resume_context_loaded,
    ))
}

struct CliHookProgressReporter;

impl runtime::HookProgressReporter for CliHookProgressReporter {
    fn on_event(&mut self, event: &runtime::HookProgressEvent) {
        match event {
            runtime::HookProgressEvent::Started {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Completed {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook done {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Cancelled {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook cancelled {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
        }
    }
}

pub(crate) struct CliPermissionPrompter {
    current_mode: PermissionMode,
}

impl CliPermissionPrompter {
    fn new(current_mode: PermissionMode) -> Self {
        Self { current_mode }
    }
}

impl runtime::PermissionPrompter for CliPermissionPrompter {
    fn decide(
        &mut self,
        request: &runtime::PermissionRequest,
    ) -> runtime::PermissionPromptDecision {
        println!();
        println!("Permission approval required");
        println!("  Tool             {}", request.tool_name);
        println!("  Current mode     {}", self.current_mode.as_str());
        println!("  Required mode    {}", request.required_mode.as_str());
        if let Some(reason) = &request.reason {
            println!("  Reason           {reason}");
        }
        println!("  Input            {}", request.input);
        print!("Approve this tool call? [y/N]: ");
        let _ = io::stdout().flush();

        let mut response = String::new();
        match io::stdin().read_line(&mut response) {
            Ok(_) => {
                let normalized = response.trim().to_ascii_lowercase();
                if matches!(normalized.as_str(), "y" | "yes") {
                    runtime::PermissionPromptDecision::Allow
                } else {
                    runtime::PermissionPromptDecision::Deny {
                        reason: format!(
                            "tool '{}' denied by user approval prompt",
                            request.tool_name
                        ),
                    }
                }
            }
            Err(error) => runtime::PermissionPromptDecision::Deny {
                reason: format!("permission approval failed: {error}"),
            },
        }
    }
}

// NOTE: Despite the historical name `AnthropicRuntimeClient`, this struct
// now holds an `ApiProviderClient` which dispatches to Anthropic, xAI,
// OpenAI, or DashScope at construction time based on
// `detect_provider_kind(&model)`. The struct name is kept to avoid
// churning `BuiltRuntime` and every Deref/DerefMut site that references
// it. See ROADMAP #29 for the provider-dispatch routing fix.
struct AnthropicRuntimeClient {
    client: ApiProviderClient,
    cached_client: CachedProviderClient,
    session_id: String,
    model: String,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_registry: GlobalToolRegistry,
    reasoning_effort: Option<String>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
}

impl AnthropicRuntimeClient {
    fn new(
        session_id: &str,
        model: String,
        enable_tools: bool,
        emit_output: bool,
        allowed_tools: Option<AllowedToolSet>,
        tool_registry: GlobalToolRegistry,
        stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Dispatch to the correct provider at construction time.
        // `ApiProviderClient` (exposed by the api crate as
        // `ProviderClient`) is an enum over Anthropic / xAI / OpenAI
        // variants, where xAI and OpenAI both use the OpenAI-compat
        // wire format under the hood. We consult
        // `detect_provider_kind(&resolved_model)` so model-name prefix
        // routing (`openai/`, `gpt-`, `grok`, `qwen/`) wins over
        // env-var presence.
        //
        // For Anthropic we build the client directly instead of going
        // through `ApiProviderClient::from_model_with_anthropic_auth`
        // so we can explicitly apply `provider::read_base_url()` — that
        // reads `ANTHROPIC_BASE_URL` and lets configured test or
        // staging endpoints exercise the same provider path as
        // production. We also attach a session-scoped prompt cache on
        // the Anthropic path; the prompt cache is Anthropic-only so
        // non-Anthropic variants skip it.
        let resolved_model = model.trim().to_string();

        let provider = runtime::resolve_global_provider(&resolved_model).ok_or_else(|| {
            provider::ApiError::NoProviderConfigured {
                model: resolved_model.clone(),
            }
        })?;

        let mut client = ApiProviderClient::from_config(&provider)?;

        if provider.protocol.as_deref() == Some("anthropic") {
            client = client.with_prompt_cache(PromptCache::new(session_id));
        }

        let cached_client = CachedProviderClient::new(client.clone(), session_id);
        Ok(Self {
            client,
            cached_client,
            session_id: session_id.to_string(),
            model,
            enable_tools,
            emit_output,
            allowed_tools,
            tool_registry,
            reasoning_effort: None,
            stream_callback,
        })
    }

    fn set_reasoning_effort(&mut self, effort: Option<String>) {
        self.reasoning_effort = effort;
    }

    /// 运行时切换模型（不改配置文件）。重建内部 ProviderClient 和 CachedProviderClient
    pub fn switch_model(&mut self, new_model: &str) -> Result<(), Box<dyn std::error::Error>> {
        let provider = runtime::resolve_global_provider(new_model).ok_or_else(|| {
            provider::ApiError::NoProviderConfigured {
                model: new_model.to_string(),
            }
        })?;

        let mut client = ApiProviderClient::from_config(&provider)?;

        if provider.protocol.as_deref() == Some("anthropic") {
            client = client.with_prompt_cache(PromptCache::new(&self.session_id));
        }

        self.client = client.clone();
        self.cached_client = CachedProviderClient::new(client, &self.session_id);
        self.model = new_model.to_string();

        Ok(())
    }
}

fn resolve_cli_auth_source_for_cwd() -> Result<AuthSource, provider::ApiError> {
    resolve_startup_auth_source(|| Ok(None))
}

impl ApiClient for AnthropicRuntimeClient {
    #[allow(clippy::too_many_lines)]
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        match self.stream_collect(request) {
            Ok(events) => Box::pin(futures::stream::iter(events.into_iter().map(Ok))),
            Err(e) => Box::pin(futures::stream::iter(std::iter::once(Err(e)))),
        }
    }
}

impl AnthropicRuntimeClient {
    #[allow(clippy::too_many_lines)]
    fn stream_collect(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
        let is_post_tool = request_ends_with_tool_result(&request);
        let message_request = MessageRequest {
            model: self.model.clone(),
            max_tokens: max_tokens_for_model(&self.model),
            messages: convert_messages(&request.messages),
            system: (!request.system_prompt.is_empty()).then(|| request.system_prompt.join("\n\n")),
            tools: self
                .enable_tools
                .then(|| filter_tool_specs(&self.tool_registry, self.allowed_tools.as_ref())),
            tool_choice: self.enable_tools.then_some(ToolChoice::Auto),
            stream: true,
            reasoning_effort: self.reasoning_effort.clone(),
            ..Default::default()
        };

        let max_attempts: usize = if is_post_tool { 2 } else { 1 };

        // Clone fields needed for standalone execution when inside a runtime.
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let emit_output = self.emit_output;
        let stream_callback = self.stream_callback.clone();

        // When resuming after tool execution, apply a stall timeout on the
        // first stream event.  If the model does not respond within the
        // deadline we drop the stalled connection and re-send the request as
        // a continuation nudge (one retry only).

        // If we are already inside a tokio runtime, calling block_on again
        // will panic with "nested enter_runtime". Spawn a dedicated OS thread
        // with its own single-threaded runtime to avoid the nesting.
        match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let (tx, rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("stream_collect rt");
                    let result = rt.block_on(async {
                        for attempt in 1..=max_attempts {
                            let apply_stall = is_post_tool && attempt == 1;
                            let result = consume_stream_standalone(
                                client.clone(),
                                session_id.clone(),
                                emit_output,
                                stream_callback.clone(),
                                message_request.clone(),
                                apply_stall,
                            )
                            .await;
                            match result {
                                Ok(events) => return Ok(events),
                                Err(error)
                                    if error.to_string().contains("post-tool stall")
                                        && attempt < max_attempts =>
                                {
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        Err(RuntimeError::new("post-tool continuation nudge exhausted"))
                    });
                    let _ = tx.send(result);
                });
                rx.recv()
                    .map_err(|_| RuntimeError::new("stream thread panicked"))?
            }
            Err(_) => {
                // Not inside a runtime — use SHARED_RT directly (original path).
                SHARED_RT.block_on(async {
                    for attempt in 1..=max_attempts {
                        let apply_stall = is_post_tool && attempt == 1;
                        let result = self.consume_stream(&message_request, apply_stall).await;
                        match result {
                            Ok(events) => return Ok(events),
                            Err(error)
                                if error.to_string().contains("post-tool stall")
                                    && attempt < max_attempts =>
                            {
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(RuntimeError::new("post-tool continuation nudge exhausted"))
                })
            }
        }
    }
}

impl AnthropicRuntimeClient {
    /// Consume a single streaming response, optionally applying a stall
    /// timeout on the first event for post-tool continuations.
    #[allow(clippy::too_many_lines)]
    async fn consume_stream(
        &self,
        message_request: &MessageRequest,
        apply_stall_timeout: bool,
    ) -> Result<Vec<AssistantEvent>, RuntimeError> {
        let mut stream = self
            .client
            .stream_message(message_request)
            .await
            .map_err(|error| {
                RuntimeError::new(format_user_visible_api_error(&self.session_id, &error))
            })?;
        let mut stdout = io::stdout();
        let mut sink = io::sink();
        let out: &mut dyn Write = if self.emit_output {
            &mut stdout
        } else {
            &mut sink
        };
        let renderer = TerminalRenderer::new();
        let mut markdown_stream = MarkdownStreamState::default();
        let mut events = Vec::new();
        let mut pending_tool: Option<(String, String, String)> = None;
        let mut block_has_thinking_summary = false;
        let mut saw_stop = false;
        let mut received_any_event = false;

        loop {
            let next = if apply_stall_timeout && !received_any_event {
                match tokio::time::timeout(POST_TOOL_STALL_TIMEOUT, stream.next_event()).await {
                    Ok(inner) => inner.map_err(|error| {
                        RuntimeError::new(format_user_visible_api_error(&self.session_id, &error))
                    })?,
                    Err(_elapsed) => {
                        return Err(RuntimeError::new(
                            "post-tool stall: model did not respond within timeout",
                        ));
                    }
                }
            } else {
                stream.next_event().await.map_err(|error| {
                    RuntimeError::new(format_user_visible_api_error(&self.session_id, &error))
                })?
            };

            let Some(event) = next else {
                break;
            };
            received_any_event = true;

            match event {
                ApiStreamEvent::MessageStart(start) => {
                    for block in start.message.content {
                        push_output_block(
                            block,
                            out,
                            &mut events,
                            &mut pending_tool,
                            true,
                            &mut block_has_thinking_summary,
                        )?;
                    }
                }
                ApiStreamEvent::ContentBlockStart(start) => {
                    push_output_block(
                        start.content_block,
                        out,
                        &mut events,
                        &mut pending_tool,
                        true,
                        &mut block_has_thinking_summary,
                    )?;
                }
                ApiStreamEvent::ContentBlockDelta(delta) => match delta.delta {
                    ContentBlockDelta::TextDelta { text } => {
                        if !text.is_empty() {
                            if let Some(rendered) = markdown_stream.push(&renderer, &text) {
                                write!(out, "{rendered}")
                                    .and_then(|()| out.flush())
                                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                            }
                            events.push(AssistantEvent::TextDelta(text.clone()));
                            if let Some(ref cb) = self.stream_callback {
                                let _ = cb.try_send(runtime::CowdEvent::TextDelta { text });
                            }
                        }
                    }
                    ContentBlockDelta::InputJsonDelta { partial_json } => {
                        if let Some((_, _, input)) = &mut pending_tool {
                            input.push_str(&partial_json);
                        }
                    }
                    ContentBlockDelta::ThinkingDelta { thinking } => {
                        if !block_has_thinking_summary {
                            render_thinking_block_summary(out, None, false)?;
                            block_has_thinking_summary = true;
                        }
                        events.push(AssistantEvent::ThinkingDelta(thinking.clone()));
                        if let Some(ref cb) = self.stream_callback {
                            let _ = cb.try_send(runtime::CowdEvent::ThinkingDelta { thinking });
                        }
                    }
                    ContentBlockDelta::SignatureDelta { signature } => {
                        tracing::debug!("signature delta received");
                        events.push(AssistantEvent::SignatureDelta(signature));
                    }
                },
                ApiStreamEvent::ContentBlockStop(_) => {
                    block_has_thinking_summary = false;
                    if let Some(rendered) = markdown_stream.flush(&renderer) {
                        write!(out, "{rendered}")
                            .and_then(|()| out.flush())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                    }
                    if let Some((id, name, input)) = pending_tool.take() {
                        // Display tool call now that input is fully accumulated
                        writeln!(out, "\n{}", format_tool_call_start(&name, &input))
                            .and_then(|()| out.flush())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                        events.push(AssistantEvent::ToolUse { id, name, input });
                    }
                }
                ApiStreamEvent::MessageDelta(delta) => {
                    events.push(AssistantEvent::Usage(delta.usage.token_usage()));
                }
                ApiStreamEvent::MessageStop(_) => {
                    saw_stop = true;
                    if let Some(rendered) = markdown_stream.flush(&renderer) {
                        write!(out, "{rendered}")
                            .and_then(|()| out.flush())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                    }
                    events.push(AssistantEvent::MessageStop);
                }
            }
        }

        push_prompt_cache_record(&self.client, &mut events);

        if !saw_stop
            && events.iter().any(|event| {
                matches!(event, AssistantEvent::TextDelta(text) if !text.is_empty())
                    || matches!(event, AssistantEvent::ToolUse { .. })
            })
        {
            events.push(AssistantEvent::MessageStop);
        }

        if events
            .iter()
            .any(|event| matches!(event, AssistantEvent::MessageStop))
        {
            return Ok(events);
        }

        let (response, cache_record) = self
            .cached_client
            .send_message(&MessageRequest {
                stream: false,
                ..message_request.clone()
            })
            .await
            .map_err(|error| {
                RuntimeError::new(format_user_visible_api_error(&self.session_id, &error))
            })?;
        let mut events = response_to_events(response, out)?;
        // Forward cache-break record from CachedProviderClient if present.
        if let Some(record) = cache_record {
            if let Some(event) = prompt_cache_record_to_runtime_event(record) {
                events.push(AssistantEvent::PromptCache(event));
            }
        }
        push_prompt_cache_record(&self.client, &mut events);
        Ok(events)
    }
}

/// Standalone version of `consume_stream` that takes owned parameters
/// instead of `&self`. Used from `stream_collect` when we are already
/// inside a tokio runtime and must spawn a dedicated OS thread with its
/// own single-threaded runtime to avoid nested `enter_runtime` panics.
#[allow(clippy::too_many_lines)]
async fn consume_stream_standalone(
    client: ApiProviderClient,
    session_id: String,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    message_request: MessageRequest,
    apply_stall_timeout: bool,
) -> Result<Vec<AssistantEvent>, RuntimeError> {
    let mut stream = client
        .stream_message(&message_request)
        .await
        .map_err(|error| RuntimeError::new(format_user_visible_api_error(&session_id, &error)))?;
    let mut stdout = io::stdout();
    let mut sink = io::sink();
    let out: &mut dyn Write = if emit_output { &mut stdout } else { &mut sink };
    let renderer = TerminalRenderer::new();
    let mut markdown_stream = MarkdownStreamState::default();
    let mut events = Vec::new();
    let mut pending_tool: Option<(String, String, String)> = None;
    let mut block_has_thinking_summary = false;
    let mut saw_stop = false;
    let mut received_any_event = false;

    loop {
        let next = if apply_stall_timeout && !received_any_event {
            match tokio::time::timeout(POST_TOOL_STALL_TIMEOUT, stream.next_event()).await {
                Ok(inner) => inner.map_err(|error| {
                    RuntimeError::new(format_user_visible_api_error(&session_id, &error))
                })?,
                Err(_elapsed) => {
                    return Err(RuntimeError::new(
                        "post-tool stall: model did not respond within timeout",
                    ));
                }
            }
        } else {
            stream.next_event().await.map_err(|error| {
                RuntimeError::new(format_user_visible_api_error(&session_id, &error))
            })?
        };

        let Some(event) = next else {
            break;
        };
        received_any_event = true;

        match event {
            ApiStreamEvent::MessageStart(start) => {
                for block in start.message.content {
                    push_output_block(
                        block,
                        out,
                        &mut events,
                        &mut pending_tool,
                        true,
                        &mut block_has_thinking_summary,
                    )?;
                }
            }
            ApiStreamEvent::ContentBlockStart(start) => {
                push_output_block(
                    start.content_block,
                    out,
                    &mut events,
                    &mut pending_tool,
                    true,
                    &mut block_has_thinking_summary,
                )?;
            }
            ApiStreamEvent::ContentBlockDelta(delta) => match delta.delta {
                ContentBlockDelta::TextDelta { text } => {
                    if !text.is_empty() {
                        if let Some(rendered) = markdown_stream.push(&renderer, &text) {
                            write!(out, "{rendered}")
                                .and_then(|()| out.flush())
                                .map_err(|error| RuntimeError::new(error.to_string()))?;
                        }
                        events.push(AssistantEvent::TextDelta(text.clone()));
                        if let Some(ref cb) = stream_callback {
                            let _ = cb.try_send(runtime::CowdEvent::TextDelta { text });
                        }
                    }
                }
                ContentBlockDelta::InputJsonDelta { partial_json } => {
                    if let Some((_, _, input)) = &mut pending_tool {
                        input.push_str(&partial_json);
                    }
                }
                ContentBlockDelta::ThinkingDelta { thinking } => {
                    if !block_has_thinking_summary {
                        render_thinking_block_summary(out, None, false)?;
                        block_has_thinking_summary = true;
                    }
                    events.push(AssistantEvent::ThinkingDelta(thinking.clone()));
                    if let Some(ref cb) = stream_callback {
                        let _ = cb.try_send(runtime::CowdEvent::ThinkingDelta { thinking });
                    }
                }
                ContentBlockDelta::SignatureDelta { signature } => {
                    tracing::debug!("signature delta received");
                    events.push(AssistantEvent::SignatureDelta(signature));
                }
            },
            ApiStreamEvent::ContentBlockStop(_) => {
                block_has_thinking_summary = false;
                if let Some(rendered) = markdown_stream.flush(&renderer) {
                    write!(out, "{rendered}")
                        .and_then(|()| out.flush())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                }
                if let Some((id, name, input)) = pending_tool.take() {
                    writeln!(out, "\n{}", format_tool_call_start(&name, &input))
                        .and_then(|()| out.flush())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    events.push(AssistantEvent::ToolUse { id, name, input });
                }
            }
            ApiStreamEvent::MessageDelta(delta) => {
                events.push(AssistantEvent::Usage(delta.usage.token_usage()));
            }
            ApiStreamEvent::MessageStop(_) => {
                saw_stop = true;
                if let Some(rendered) = markdown_stream.flush(&renderer) {
                    write!(out, "{rendered}")
                        .and_then(|()| out.flush())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                }
                events.push(AssistantEvent::MessageStop);
            }
        }
    }

    push_prompt_cache_record(&client, &mut events);

    if !saw_stop
        && events.iter().any(|event| {
            matches!(event, AssistantEvent::TextDelta(text) if !text.is_empty())
                || matches!(event, AssistantEvent::ToolUse { .. })
        })
    {
        events.push(AssistantEvent::MessageStop);
    }

    if events
        .iter()
        .any(|event| matches!(event, AssistantEvent::MessageStop))
    {
        return Ok(events);
    }

    let response = client
        .send_message(&MessageRequest {
            stream: false,
            ..message_request.clone()
        })
        .await
        .map_err(|error| RuntimeError::new(format_user_visible_api_error(&session_id, &error)))?;
    let mut events = response_to_events(response, out)?;
    push_prompt_cache_record(&client, &mut events);
    Ok(events)
}

/// Returns `true` when the conversation ends with a tool-result message,
/// meaning the model is expected to continue after tool execution.
fn request_ends_with_tool_result(request: &ApiRequest) -> bool {
    request
        .messages
        .last()
        .is_some_and(|message| message.role == MessageRole::Tool)
}

fn format_user_visible_api_error(session_id: &str, error: &provider::ApiError) -> String {
    if error.is_context_window_failure() {
        format_context_window_error(session_id, error)
    } else if error.is_generic_fatal_wrapper() {
        let mut qualifiers = vec![format!("session {session_id}")];
        if let Some(request_id) = error.request_id() {
            qualifiers.push(format!("trace {request_id}"));
        }
        format!(
            "{} ({}): {}",
            error.safe_failure_class(),
            qualifiers.join(", "),
            error
        )
    } else {
        error.to_string()
    }
}

fn format_context_window_error(session_id: &str, error: &provider::ApiError) -> String {
    let mut lines: Vec<String> = vec!["context_window_blocked".to_string(), String::new()];

    match error {
        provider::ApiError::ContextWindowExceeded {
            model,
            estimated_input_tokens,
            requested_output_tokens: _,
            estimated_total_tokens,
            context_window_tokens: _,
        } => {
            lines.push(format!("Context window blocked for {model}"));
            lines.push(String::new());
            lines.push(format!("{:<17}{session_id}", "Session"));
            lines.push(format!("{:<17}{model}", "Model"));
            lines.push(format!(
                "{:<17}~{estimated_input_tokens} tokens (heuristic)",
                "Input estimate"
            ));
            lines.push(format!(
                "{:<17}~{estimated_total_tokens} tokens (heuristic)",
                "Total estimate"
            ));
            lines.push(String::new());
            lines.push(format!("{:<17}/compact", "Compact"));
            lines.push(format!("{:<17}cowd --resume {session_id}", "Resume TUI"));
            lines.push(format!("{:<17}/clear --confirm", "Fresh session"));
            lines.push(format!(
                "{:<17}reduce output tokens or break into smaller requests",
                "Reduce scope"
            ));
            lines.push(format!("{:<17}rerun", "Retry"));
        }
        provider::ApiError::Api {
            message,
            request_id,
            ..
        } => {
            if let Some(ref rid) = request_id {
                lines.push(format!("{:<17}{rid}", "Trace"));
            }
            if let Some(ref msg) = message {
                lines.push(format!("{:<17}{msg}", "Detail"));
            }
            lines.push(String::new());
            lines.push(format!("{:<17}/compact", "Compact"));
            lines.push(format!("{:<17}/clear --confirm", "Fresh session"));
        }
        provider::ApiError::RetriesExhausted {
            attempts,
            last_error,
        } => {
            lines.push("Context window blocked".to_string());
            lines.push(format!("api failed after {attempts} attempts"));
            lines.push(String::new());
            if let Some(rid) = last_error.request_id() {
                lines.push(format!("{:<17}{rid}", "Trace"));
            }
            if let provider::ApiError::Api {
                message: Some(ref msg),
                ..
            } = **last_error
            {
                lines.push(format!("{:<17}{msg}", "Detail"));
            }
            lines.push(String::new());
            lines.push(format!("{:<17}/compact", "Compact"));
            lines.push(format!("{:<17}cowd --resume {session_id}", "Resume TUI"));
        }
        _ => {
            lines.push(error.to_string());
        }
    }

    lines.join("\n")
}

fn final_assistant_text(summary: &runtime::TurnSummary) -> String {
    summary
        .assistant_messages
        .last()
        .map(|message| {
            message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn collect_tool_uses(summary: &runtime::TurnSummary) -> Vec<serde_json::Value> {
    summary
        .assistant_messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolUse { id, name, input } => Some(json!({
                "id": id,
                "name": name,
                "input": input,
            })),
            _ => None,
        })
        .collect()
}

fn collect_tool_results(summary: &runtime::TurnSummary) -> Vec<serde_json::Value> {
    summary
        .tool_results
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } => Some(json!({
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "output": output,
                "is_error": is_error,
            })),
            _ => None,
        })
        .collect()
}

fn collect_prompt_cache_events(summary: &runtime::TurnSummary) -> Vec<serde_json::Value> {
    summary
        .prompt_cache_events
        .iter()
        .map(|event| {
            json!({
                "unexpected": event.unexpected,
                "reason": event.reason,
                "previous_cache_read_input_tokens": event.previous_cache_read_input_tokens,
                "current_cache_read_input_tokens": event.current_cache_read_input_tokens,
                "token_drop": event.token_drop,
            })
        })
        .collect()
}

/// Slash commands that are registered in the spec list but not yet implemented
/// in this build. Used to filter both REPL completions and help output so the
/// discovery surface only shows commands that actually work (ROADMAP #39).
const STUB_COMMANDS: &[&str] = &[
    "login",
    "logout",
    "upgrade",
    "share",
    "feedback",
    "files",
    "fast",
    "exit",
    "insights",
    "thinkback",
    "release-notes",
    "security-review",
    "keybindings",
    "privacy-settings",
    "plan",
    "tasks",
    "theme",
    "usage",
    "rename",
    "copy",
    "hooks",
    "color",
    "effort",
    "rewind",
    "ide",
    "tag",
    "output-style",
    "add-dir",
    // Spec entries with no parse arm — produce circular "Did you mean" error
    // without this guard. Adding here routes them to the proper unsupported
    // message and excludes them from REPL completions / help.
    // NOTE: do NOT add "stats", "tokens", "cache" — they are implemented.
    "allowed-tools",
    "bookmarks",
    "workspace",
    "reasoning",
    "budget",
    "rate-limit",
    "changelog",
    "diagnostics",
    "metrics",
    "tool-details",
    "focus",
    "unfocus",
    "pin",
    "unpin",
    "language",
    "profile",
    "max-tokens",
    "temperature",
    "system-prompt",
    "notifications",
    "telemetry",
    "env",
    "project",
    "terminal-setup",
    "api-key",
    "reset",
    "undo",
    "stop",
    "retry",
    "paste",
    "screenshot",
    "image",
    "cron",
    "team",
    "benchmark",
    "migrate",
    "templates",
    "chat",
    "map",
    "symbols",
    "references",
    "definition",
    "hover",
    "autofix",
    "multi",
    "macro",
    "alias",
    "parallel",
    "subagent",
    "agent",
];

fn slash_command_completion_candidates_with_sessions(
    model: &str,
    active_session_id: Option<&str>,
    recent_session_ids: Vec<String>,
) -> Vec<String> {
    let mut completions = BTreeSet::new();

    for spec in slash_command_specs() {
        if STUB_COMMANDS.contains(&spec.name) {
            continue;
        }
        completions.insert(format!("/{}", spec.name));
        for alias in spec.aliases {
            if !STUB_COMMANDS.contains(alias) {
                completions.insert(format!("/{alias}"));
            }
        }
    }

    for candidate in [
        "/bughunter ",
        "/clear --confirm",
        "/config ",
        "/config env",
        "/config hooks",
        "/config model",
        "/config plugins",
        "/mcp ",
        "/mcp list",
        "/mcp show ",
        "/export ",
        "/issue ",
        "/model ",
        "/model opus",
        "/model sonnet",
        "/model haiku",
        "/permissions ",
        "/permissions read-only",
        "/permissions workspace-write",
        "/permissions danger-full-access",
        "/plugin list",
        "/plugin install ",
        "/plugin enable ",
        "/plugin disable ",
        "/plugin uninstall ",
        "/plugin update ",
        "/plugins list",
        "/pr ",
        "/resume ",
        "/session list",
        "/session switch ",
        "/session fork ",
        "/teleport ",
        "/ultraplan ",
        "/agents help",
        "/mcp help",
        "/skills help",
    ] {
        completions.insert(candidate.to_string());
    }

    if !model.trim().is_empty() {
        completions.insert(format!("/model {}", resolve_model_alias_with_config(model)));
        completions.insert(format!("/model {model}"));
    }

    if let Some(active_session_id) = active_session_id.filter(|value| !value.trim().is_empty()) {
        completions.insert(format!("/resume {active_session_id}"));
        completions.insert(format!("/session switch {active_session_id}"));
    }

    for session_id in recent_session_ids
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .take(10)
    {
        completions.insert(format!("/resume {session_id}"));
        completions.insert(format!("/session switch {session_id}"));
    }

    completions.into_iter().collect()
}

fn format_tool_call_start(name: &str, input: &str) -> String {
    let parsed: serde_json::Value =
        serde_json::from_str(input).unwrap_or(serde_json::Value::String(input.to_string()));

    let detail = match name {
        "bash" | "Bash" => format_bash_call(&parsed),
        "read_file" | "Read" => {
            let path = extract_tool_path(&parsed);
            format!("\x1b[2m📄 Reading {path}…\x1b[0m")
        }
        "write_file" | "Write" => {
            let path = extract_tool_path(&parsed);
            let lines = parsed
                .get("content")
                .and_then(|value| value.as_str())
                .map_or(0, |content| content.lines().count());
            format!("\x1b[1;32m✏️ Writing {path}\x1b[0m \x1b[2m({lines} lines)\x1b[0m")
        }
        "edit_file" | "Edit" => {
            let path = extract_tool_path(&parsed);
            let old_value = parsed
                .get("old_string")
                .or_else(|| parsed.get("oldString"))
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            let new_value = parsed
                .get("new_string")
                .or_else(|| parsed.get("newString"))
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            format!(
                "\x1b[1;33m📝 Editing {path}\x1b[0m{}",
                format_patch_preview(old_value, new_value)
                    .map(|preview| format!("\n{preview}"))
                    .unwrap_or_default()
            )
        }
        "glob_search" | "Glob" => format_search_start("🔎 Glob", &parsed),
        "grep_search" | "Grep" => format_search_start("🔎 Grep", &parsed),
        "web_search" | "WebSearch" => parsed
            .get("query")
            .and_then(|value| value.as_str())
            .unwrap_or("?")
            .to_string(),
        _ => summarize_tool_payload(input),
    };

    let border = "─".repeat(name.len() + 8);
    format!(
        "\x1b[38;5;245m╭─ \x1b[1;36m{name}\x1b[0;38;5;245m ─╮\x1b[0m\n\x1b[38;5;245m│\x1b[0m {detail}\n\x1b[38;5;245m╰{border}╯\x1b[0m"
    )
}

fn format_tool_result(name: &str, output: &str, is_error: bool) -> String {
    let icon = if is_error {
        "\x1b[1;31m✗\x1b[0m"
    } else {
        "\x1b[1;32m✓\x1b[0m"
    };
    if is_error {
        let summary = truncate_for_summary(output.trim(), 160);
        return if summary.is_empty() {
            format!("{icon} \x1b[38;5;245m{name}\x1b[0m")
        } else {
            format!("{icon} \x1b[38;5;245m{name}\x1b[0m\n\x1b[38;5;203m{summary}\x1b[0m")
        };
    }

    let parsed: serde_json::Value =
        serde_json::from_str(output).unwrap_or(serde_json::Value::String(output.to_string()));
    match name {
        "bash" | "Bash" => format_bash_result(icon, &parsed),
        "read_file" | "Read" => format_read_result(icon, &parsed),
        "write_file" | "Write" => format_write_result(icon, &parsed),
        "edit_file" | "Edit" => format_edit_result(icon, &parsed),
        "glob_search" | "Glob" => format_glob_result(icon, &parsed),
        "grep_search" | "Grep" => format_grep_result(icon, &parsed),
        _ => format_generic_tool_result(icon, name, &parsed),
    }
}

const DISPLAY_TRUNCATION_NOTICE: &str =
    "\x1b[2m… output truncated for display; full result preserved in session.\x1b[0m";
const READ_DISPLAY_MAX_LINES: usize = 80;
const READ_DISPLAY_MAX_CHARS: usize = 6_000;
const TOOL_OUTPUT_DISPLAY_MAX_LINES: usize = 60;
const TOOL_OUTPUT_DISPLAY_MAX_CHARS: usize = 4_000;

fn extract_tool_path(parsed: &serde_json::Value) -> String {
    parsed
        .get("file_path")
        .or_else(|| parsed.get("filePath"))
        .or_else(|| parsed.get("path"))
        .and_then(|value| value.as_str())
        .unwrap_or("?")
        .to_string()
}

fn format_search_start(label: &str, parsed: &serde_json::Value) -> String {
    let pattern = parsed
        .get("pattern")
        .and_then(|value| value.as_str())
        .unwrap_or("?");
    let scope = parsed
        .get("path")
        .and_then(|value| value.as_str())
        .unwrap_or(".");
    format!("{label} {pattern}\n\x1b[2min {scope}\x1b[0m")
}

fn format_patch_preview(old_value: &str, new_value: &str) -> Option<String> {
    if old_value.is_empty() && new_value.is_empty() {
        return None;
    }
    Some(format!(
        "\x1b[38;5;203m- {}\x1b[0m\n\x1b[38;5;70m+ {}\x1b[0m",
        truncate_for_summary(first_visible_line(old_value), 72),
        truncate_for_summary(first_visible_line(new_value), 72)
    ))
}

fn format_bash_call(parsed: &serde_json::Value) -> String {
    let command = parsed
        .get("command")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if command.is_empty() {
        String::new()
    } else {
        format!(
            "\x1b[48;5;236;38;5;255m $ {} \x1b[0m",
            truncate_for_summary(command, 160)
        )
    }
}

fn first_visible_line(text: &str) -> &str {
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(text)
}

fn format_bash_result(icon: &str, parsed: &serde_json::Value) -> String {
    use std::fmt::Write as _;

    let mut lines = vec![format!("{icon} \x1b[38;5;245mbash\x1b[0m")];
    if let Some(task_id) = parsed
        .get("backgroundTaskId")
        .and_then(|value| value.as_str())
    {
        write!(&mut lines[0], " backgrounded ({task_id})").expect("write to string");
    } else if let Some(status) = parsed
        .get("returnCodeInterpretation")
        .and_then(|value| value.as_str())
        .filter(|status| !status.is_empty())
    {
        write!(&mut lines[0], " {status}").expect("write to string");
    }

    if let Some(stdout) = parsed.get("stdout").and_then(|value| value.as_str()) {
        if !stdout.trim().is_empty() {
            lines.push(truncate_output_for_display(
                stdout,
                TOOL_OUTPUT_DISPLAY_MAX_LINES,
                TOOL_OUTPUT_DISPLAY_MAX_CHARS,
            ));
        }
    }
    if let Some(stderr) = parsed.get("stderr").and_then(|value| value.as_str()) {
        if !stderr.trim().is_empty() {
            lines.push(format!(
                "\x1b[38;5;203m{}\x1b[0m",
                truncate_output_for_display(
                    stderr,
                    TOOL_OUTPUT_DISPLAY_MAX_LINES,
                    TOOL_OUTPUT_DISPLAY_MAX_CHARS,
                )
            ));
        }
    }

    lines.join("\n\n")
}

fn format_read_result(icon: &str, parsed: &serde_json::Value) -> String {
    let file = parsed.get("file").unwrap_or(parsed);
    let path = extract_tool_path(file);
    let start_line = file
        .get("startLine")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    let num_lines = file
        .get("numLines")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let total_lines = file
        .get("totalLines")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(num_lines);
    let content = file
        .get("content")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let end_line = start_line.saturating_add(num_lines.saturating_sub(1));

    format!(
        "{icon} \x1b[2m📄 Read {path} (lines {}-{} of {})\x1b[0m\n{}",
        start_line,
        end_line.max(start_line),
        total_lines,
        truncate_output_for_display(content, READ_DISPLAY_MAX_LINES, READ_DISPLAY_MAX_CHARS)
    )
}

fn format_write_result(icon: &str, parsed: &serde_json::Value) -> String {
    let path = extract_tool_path(parsed);
    let kind = parsed
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("write");
    let line_count = parsed
        .get("content")
        .and_then(|value| value.as_str())
        .map_or(0, |content| content.lines().count());
    format!(
        "{icon} \x1b[1;32m✏️ {} {path}\x1b[0m \x1b[2m({line_count} lines)\x1b[0m",
        if kind == "create" { "Wrote" } else { "Updated" },
    )
}

fn format_structured_patch_preview(parsed: &serde_json::Value) -> Option<String> {
    let hunks = parsed.get("structuredPatch")?.as_array()?;
    let mut preview = Vec::new();
    for hunk in hunks.iter().take(2) {
        let lines = hunk.get("lines")?.as_array()?;
        for line in lines.iter().filter_map(|value| value.as_str()).take(6) {
            match line.chars().next() {
                Some('+') => preview.push(format!("\x1b[38;5;70m{line}\x1b[0m")),
                Some('-') => preview.push(format!("\x1b[38;5;203m{line}\x1b[0m")),
                _ => preview.push(line.to_string()),
            }
        }
    }
    if preview.is_empty() {
        None
    } else {
        Some(preview.join("\n"))
    }
}

fn format_edit_result(icon: &str, parsed: &serde_json::Value) -> String {
    let path = extract_tool_path(parsed);
    let suffix = if parsed
        .get("replaceAll")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        " (replace all)"
    } else {
        ""
    };
    let preview = format_structured_patch_preview(parsed).or_else(|| {
        let old_value = parsed
            .get("oldString")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let new_value = parsed
            .get("newString")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        format_patch_preview(old_value, new_value)
    });

    match preview {
        Some(preview) => format!("{icon} \x1b[1;33m📝 Edited {path}{suffix}\x1b[0m\n{preview}"),
        None => format!("{icon} \x1b[1;33m📝 Edited {path}{suffix}\x1b[0m"),
    }
}

fn format_glob_result(icon: &str, parsed: &serde_json::Value) -> String {
    let num_files = parsed
        .get("numFiles")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let filenames = parsed
        .get("filenames")
        .and_then(|value| value.as_array())
        .map(|files| {
            files
                .iter()
                .filter_map(|value| value.as_str())
                .take(8)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if filenames.is_empty() {
        format!("{icon} \x1b[38;5;245mglob_search\x1b[0m matched {num_files} files")
    } else {
        format!("{icon} \x1b[38;5;245mglob_search\x1b[0m matched {num_files} files\n{filenames}")
    }
}

fn format_grep_result(icon: &str, parsed: &serde_json::Value) -> String {
    let num_matches = parsed
        .get("numMatches")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let num_files = parsed
        .get("numFiles")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let content = parsed
        .get("content")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let filenames = parsed
        .get("filenames")
        .and_then(|value| value.as_array())
        .map(|files| {
            files
                .iter()
                .filter_map(|value| value.as_str())
                .take(8)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let summary = format!(
        "{icon} \x1b[38;5;245mgrep_search\x1b[0m {num_matches} matches across {num_files} files"
    );
    if !content.trim().is_empty() {
        format!(
            "{summary}\n{}",
            truncate_output_for_display(
                content,
                TOOL_OUTPUT_DISPLAY_MAX_LINES,
                TOOL_OUTPUT_DISPLAY_MAX_CHARS,
            )
        )
    } else if !filenames.is_empty() {
        format!("{summary}\n{filenames}")
    } else {
        summary
    }
}

fn format_generic_tool_result(icon: &str, name: &str, parsed: &serde_json::Value) -> String {
    let rendered_output = match parsed {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            serde_json::to_string_pretty(parsed).unwrap_or_else(|_| parsed.to_string())
        }
        _ => parsed.to_string(),
    };
    let preview = truncate_output_for_display(
        &rendered_output,
        TOOL_OUTPUT_DISPLAY_MAX_LINES,
        TOOL_OUTPUT_DISPLAY_MAX_CHARS,
    );

    if preview.is_empty() {
        format!("{icon} \x1b[38;5;245m{name}\x1b[0m")
    } else if preview.contains('\n') {
        format!("{icon} \x1b[38;5;245m{name}\x1b[0m\n{preview}")
    } else {
        format!("{icon} \x1b[38;5;245m{name}:\x1b[0m {preview}")
    }
}

fn summarize_tool_payload(payload: &str) -> String {
    let compact = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(value) => value.to_string(),
        Err(_) => payload.trim().to_string(),
    };
    truncate_for_summary(&compact, 96)
}

fn truncate_for_summary(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn truncate_output_for_display(content: &str, max_lines: usize, max_chars: usize) -> String {
    let original = content.trim_end_matches('\n');
    if original.is_empty() {
        return String::new();
    }

    let mut preview_lines = Vec::new();
    let mut used_chars = 0usize;
    let mut truncated = false;

    for (index, line) in original.lines().enumerate() {
        if index >= max_lines {
            truncated = true;
            break;
        }

        let newline_cost = usize::from(!preview_lines.is_empty());
        let available = max_chars.saturating_sub(used_chars + newline_cost);
        if available == 0 {
            truncated = true;
            break;
        }

        let line_chars = line.chars().count();
        if line_chars > available {
            preview_lines.push(line.chars().take(available).collect::<String>());
            truncated = true;
            break;
        }

        preview_lines.push(line.to_string());
        used_chars += newline_cost + line_chars;
    }

    let mut preview = preview_lines.join("\n");
    if truncated {
        if !preview.is_empty() {
            preview.push('\n');
        }
        preview.push_str(DISPLAY_TRUNCATION_NOTICE);
    }
    preview
}

fn render_thinking_block_summary(
    out: &mut (impl Write + ?Sized),
    char_count: Option<usize>,
    redacted: bool,
) -> Result<(), RuntimeError> {
    let summary = if redacted {
        "\n▶ Thinking block hidden by provider\n".to_string()
    } else if let Some(char_count) = char_count {
        format!("\n▶ Thinking ({char_count} chars hidden)\n")
    } else {
        "\n▶ Thinking hidden\n".to_string()
    };
    write!(out, "{summary}")
        .and_then(|()| out.flush())
        .map_err(|error| RuntimeError::new(error.to_string()))
}

fn push_output_block(
    block: OutputContentBlock,
    out: &mut (impl Write + ?Sized),
    events: &mut Vec<AssistantEvent>,
    pending_tool: &mut Option<(String, String, String)>,
    streaming_tool_input: bool,
    block_has_thinking_summary: &mut bool,
) -> Result<(), RuntimeError> {
    match block {
        OutputContentBlock::Text { text } => {
            if !text.is_empty() {
                let rendered = TerminalRenderer::new().markdown_to_ansi(&text);
                write!(out, "{rendered}")
                    .and_then(|()| out.flush())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                events.push(AssistantEvent::TextDelta(text));
            }
        }
        OutputContentBlock::ToolUse { id, name, input } => {
            // During streaming, the initial content_block_start has an empty input ({}).
            // The real input arrives via input_json_delta events. In
            // non-streaming responses, preserve a legitimate empty object.
            let initial_input = if streaming_tool_input
                && input.is_object()
                && input.as_object().is_some_and(serde_json::Map::is_empty)
            {
                String::new()
            } else {
                input.to_string()
            };
            *pending_tool = Some((id, name, initial_input));
        }
        OutputContentBlock::Thinking { thinking, .. } => {
            render_thinking_block_summary(out, Some(thinking.chars().count()), false)?;
            *block_has_thinking_summary = true;
            events.push(AssistantEvent::ThinkingDelta(thinking));
        }
        OutputContentBlock::RedactedThinking { .. } => {
            render_thinking_block_summary(out, None, true)?;
            *block_has_thinking_summary = true;
        }
    }
    Ok(())
}

fn response_to_events(
    response: MessageResponse,
    out: &mut (impl Write + ?Sized),
) -> Result<Vec<AssistantEvent>, RuntimeError> {
    let mut events = Vec::new();
    let mut pending_tool = None;

    for block in response.content {
        let mut block_has_thinking_summary = false;
        push_output_block(
            block,
            out,
            &mut events,
            &mut pending_tool,
            false,
            &mut block_has_thinking_summary,
        )?;
        if let Some((id, name, input)) = pending_tool.take() {
            events.push(AssistantEvent::ToolUse { id, name, input });
        }
    }

    events.push(AssistantEvent::Usage(response.usage.token_usage()));
    events.push(AssistantEvent::MessageStop);
    Ok(events)
}

fn push_prompt_cache_record(client: &ApiProviderClient, events: &mut Vec<AssistantEvent>) {
    // `ApiProviderClient::take_last_prompt_cache_record` is a pass-through
    // to the Anthropic variant and returns `None` for OpenAI-compat /
    // xAI variants, which do not have a prompt cache. So this helper
    // remains a no-op on non-Anthropic providers without any extra
    // branching here.
    if let Some(record) = client.take_last_prompt_cache_record() {
        if let Some(event) = prompt_cache_record_to_runtime_event(record) {
            events.push(AssistantEvent::PromptCache(event));
        }
    }
}

fn prompt_cache_record_to_runtime_event(
    record: provider::PromptCacheRecord,
) -> Option<PromptCacheEvent> {
    let cache_break = record.cache_break?;
    Some(PromptCacheEvent {
        unexpected: cache_break.unexpected,
        reason: cache_break.reason,
        previous_cache_read_input_tokens: cache_break.previous_cache_read_input_tokens,
        current_cache_read_input_tokens: cache_break.current_cache_read_input_tokens,
        token_drop: cache_break.token_drop,
    })
}

pub(crate) struct CliToolExecutor {
    renderer: TerminalRenderer,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_registry: GlobalToolRegistry,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
}

impl CliToolExecutor {
    fn new(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_registry: GlobalToolRegistry,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    ) -> Self {
        Self {
            renderer: TerminalRenderer::new(),
            emit_output,
            allowed_tools,
            tool_registry,
            mcp_state,
        }
    }

    fn execute_search_tool(&self, value: serde_json::Value) -> Result<String, ToolError> {
        let input: ToolSearchRequest = serde_json::from_value(value)
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
        let (pending_mcp_servers, mcp_degraded) =
            self.mcp_state.as_ref().map_or((None, None), |state| {
                let state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (state.pending_servers(), state.degraded_report())
            });
        serde_json::to_string_pretty(&self.tool_registry.search(
            &input.query,
            input.max_results.unwrap_or(5),
            pending_mcp_servers,
            mcp_degraded,
        ))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    fn execute_runtime_tool(
        &self,
        tool_name: &str,
        value: serde_json::Value,
    ) -> Result<String, ToolError> {
        let Some(mcp_state) = &self.mcp_state else {
            return Err(ToolError::new(format!(
                "runtime tool `{tool_name}` is unavailable without configured MCP servers"
            )));
        };
        let mut mcp_state = mcp_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match tool_name {
            "MCPTool" => {
                let input: McpToolRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                let qualified_name = input
                    .qualified_name
                    .or(input.tool)
                    .ok_or_else(|| ToolError::new("missing required field `qualifiedName`"))?;
                mcp_state.call_tool(&qualified_name, input.arguments)
            }
            "ListMcpResourcesTool" => {
                let input: ListMcpResourcesRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                match input.server {
                    Some(server_name) => mcp_state.list_resources_for_server(&server_name),
                    None => mcp_state.list_resources_for_all_servers(),
                }
            }
            "ReadMcpResourceTool" => {
                let input: ReadMcpResourceRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                mcp_state.read_resource(&input.server, &input.uri)
            }
            _ => mcp_state.call_tool(tool_name, Some(value)),
        }
    }
}

impl ToolExecutor for CliToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        if self
            .allowed_tools
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(tool_name))
        {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is not enabled by the current --allowedTools setting"
            )));
        }
        let value = serde_json::from_str(input)
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
        let result = if tool_name == "ToolSearch" {
            self.execute_search_tool(value)
        } else if self.tool_registry.has_runtime_tool(tool_name) {
            self.execute_runtime_tool(tool_name, value)
        } else {
            self.tool_registry
                .execute(tool_name, &value)
                .map_err(ToolError::new)
        };
        match result {
            Ok(output) => {
                if self.emit_output {
                    let markdown = format_tool_result(tool_name, &output, false);
                    self.renderer
                        .stream_markdown(&markdown, &mut io::stdout())
                        .map_err(|error| ToolError::new(error.to_string()))?;
                }
                Ok(output)
            }
            Err(error) => {
                if self.emit_output {
                    let markdown = format_tool_result(tool_name, &error.to_string(), true);
                    self.renderer
                        .stream_markdown(&markdown, &mut io::stdout())
                        .map_err(|stream_error| ToolError::new(stream_error.to_string()))?;
                }
                Err(error)
            }
        }
    }
}

fn permission_policy(
    mode: PermissionMode,
    feature_config: &runtime::RuntimeFeatureConfig,
    tool_registry: &GlobalToolRegistry,
) -> Result<PermissionPolicy, String> {
    Ok(tool_registry.permission_specs(None)?.into_iter().fold(
        PermissionPolicy::new(mode).with_permission_rules(feature_config.permission_rules()),
        |policy, (name, required_permission)| {
            policy.with_tool_requirement(name, required_permission)
        },
    ))
}

fn convert_messages(messages: &[ConversationMessage]) -> Vec<InputMessage> {
    messages
        .iter()
        .filter_map(|message| {
            let role = match message.role {
                MessageRole::System | MessageRole::User | MessageRole::Tool => "user",
                MessageRole::Assistant => "assistant",
            };
            let content = message
                .blocks
                .iter()
                .map(|block| match block {
                    ContentBlock::Text { text } => InputContentBlock::Text { text: text.clone() },
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => InputContentBlock::Thinking {
                        thinking: thinking.clone(),
                        signature: signature.clone(),
                    },
                    ContentBlock::ToolUse { id, name, input } => InputContentBlock::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        input: serde_json::from_str(input)
                            .unwrap_or_else(|_| serde_json::json!({ "raw": input })),
                    },
                    ContentBlock::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                        ..
                    } => InputContentBlock::ToolResult {
                        tool_use_id: tool_use_id.clone(),
                        content: vec![ToolResultContentBlock::Text {
                            text: output.clone(),
                        }],
                        is_error: *is_error,
                    },
                })
                .collect::<Vec<_>>();
            (!content.is_empty()).then(|| InputMessage {
                role: role.to_string(),
                content,
            })
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn print_help_to(out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "cowd v{VERSION}")?;
    writeln!(out)?;
    writeln!(out, "Core commands:")?;
    writeln!(out, "  cowd")?;
    writeln!(out, "      Start the interactive TUI")?;
    writeln!(out, "  cowd --tui")?;
    writeln!(out, "      Explicitly start the interactive TUI")?;
    writeln!(
        out,
        "  cowd gateway start|stop|restart|status|doctor|logs|repair|open"
    )?;
    writeln!(out, "      Control and diagnose the browser WebUI gateway")?;
    writeln!(out, "  cowd status")?;
    writeln!(out, "      Show the current local workspace snapshot")?;
    writeln!(out, "  cowd doctor")?;
    writeln!(
        out,
        "      Diagnose local provider credentials, config, workspace, and sandbox health"
    )?;
    writeln!(out, "  cowd setup")?;
    writeln!(
        out,
        "      Check local setup, channels, gateway/WebUI, and next action"
    )?;
    writeln!(
        out,
        "  cowd export [PATH] [--session SESSION] [--output PATH]"
    )?;
    writeln!(out, "      Dump the latest or named session as markdown")?;
    writeln!(out, "  cowd import-session PATH")?;
    writeln!(out, "      Import a local legacy .jsonl/.json session file")?;
    writeln!(out, "  cowd skills list|show|validate")?;
    writeln!(
        out,
        "      Basic skill inventory and validation; use WebUI/TUI for management"
    )?;
    writeln!(out)?;
    writeln!(out, "WebUI access:")?;
    writeln!(out, "  cowd gateway start")?;
    writeln!(
        out,
        "      Start the gateway daemon that serves the browser console"
    )?;
    writeln!(out, "  cowd gateway status")?;
    writeln!(
        out,
        "      Print the local WebUI/API address when the gateway is running"
    )?;
    writeln!(out)?;
    writeln!(out, "Advanced local tools:")?;
    writeln!(out, "  cowd --resume [session-id|latest]")?;
    writeln!(out, "      Start the TUI attached to a saved session")?;
    writeln!(out, "  cowd sandbox")?;
    writeln!(out, "      Show the current sandbox isolation snapshot")?;
    writeln!(out, "  cowd help | cowd version")?;
    writeln!(out, "      Local help and version aliases")?;
    writeln!(out, "  cowd init")?;
    writeln!(out, "      Initialize local cowd project files")?;
    writeln!(out, "  cowd agents")?;
    writeln!(out, "  cowd mcp")?;
    writeln!(out, "  cowd plugins")?;
    writeln!(out, "  cowd dump-manifests [--manifests-dir PATH]")?;
    writeln!(out, "  cowd bootstrap-plan")?;
    writeln!(out, "  cowd system-prompt [--cwd PATH] [--date YYYY-MM-DD]")?;
    writeln!(out, "  cowd gateway logs|repair|open")?;
    writeln!(
        out,
        "      Compatibility and channel helpers; prefer WebUI/TUI for broad state management"
    )?;
    writeln!(out)?;
    writeln!(out, "Source of truth: {OFFICIAL_REPO_SLUG}")?;
    writeln!(
        out,
        "Warning: do not `{DEPRECATED_INSTALL_COMMAND}` (deprecated stub)"
    )?;
    writeln!(out)?;
    writeln!(out, "Flags:")?;
    writeln!(
        out,
        "  --model MODEL              Override the active model"
    )?;
    writeln!(
        out,
        "  --session SESSION          Start or attach the interactive TUI to a managed session id"
    )?;
    writeln!(
        out,
        "  --output-format FORMAT     Machine-readable output for local diagnostics and export: text or json"
    )?;
    writeln!(
        out,
        "  --permission-mode MODE     Set read-only, workspace-write, or danger-full-access"
    )?;
    writeln!(
        out,
        "  --dangerously-skip-permissions  Skip all permission checks"
    )?;
    writeln!(
        out,
        "  --solo                       Alias for --dangerously-skip-permissions"
    )?;
    writeln!(
        out,
        "  --yolo                       Continuous autonomous mode: danger-full-access plus persistent goal execution"
    )?;
    writeln!(
        out,
        "  --allowedTools TOOLS       Restrict enabled tools (repeatable; comma-separated aliases supported)"
    )?;
    writeln!(
        out,
        "  --version, -V              Print version and build information locally"
    )?;
    writeln!(out)?;
    writeln!(out, "Interactive slash commands:")?;
    writeln!(out, "{}", render_slash_command_help_filtered(STUB_COMMANDS))?;
    writeln!(out)?;
    writeln!(out, "Session shortcuts:")?;
    writeln!(out, "  REPL turns auto-save to the SQLite session store")?;
    writeln!(
        out,
        "  Use `{LATEST_SESSION_REFERENCE}` with --resume, /resume, or /session switch to target the newest saved session"
    )?;
    writeln!(
        out,
        "  Use /session list in the REPL to browse managed sessions"
    )?;
    writeln!(
        out,
        "  Local .jsonl/.json files are never imported automatically; use cowd import-session PATH"
    )?;
    writeln!(out, "Examples:")?;
    writeln!(out, "  cowd --model claude-opus")?;
    writeln!(out, "  cowd gateway start")?;
    writeln!(out, "  cowd gateway status --output-format json")?;
    writeln!(out, "  cowd skills list")?;
    writeln!(out, "  cowd --resume {LATEST_SESSION_REFERENCE}")?;
    writeln!(out, "  cowd agents")?;
    writeln!(out, "  cowd mcp show my-server")?;
    writeln!(out, "  cowd doctor")?;
    writeln!(out, "  source of truth: {OFFICIAL_REPO_URL}")?;
    writeln!(
        out,
        "  do not run `{DEPRECATED_INSTALL_COMMAND}` — it installs a deprecated stub"
    )?;
    writeln!(out, "  cowd init")?;
    writeln!(out, "  cowd export")?;
    writeln!(out, "  cowd export conversation.md")?;
    Ok(())
}

fn print_help(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    print_help_to(&mut buffer)?;
    let message = String::from_utf8(buffer)?;
    match output_format {
        CliOutputFormat::Text => print!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "kind": "help",
                "message": message,
            }))?
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::{
        activate_live_cli_session, build_runtime_plugin_state_with_loader,
        build_runtime_with_plugin_state, build_system_prompt_for_mode, cli_turn_context_profile,
        collect_session_prompt_history, create_managed_session_handle,
        current_task_summary_from_record, discover_local_session_import_candidates,
        ensure_yolo_task, filter_tool_specs, format_bughunter_report,
        format_commit_preflight_report, format_commit_skipped_report, format_compact_report,
        format_connected_line, format_cost_report, format_history_timestamp, format_issue_report,
        format_model_report, format_model_switch_report, format_permissions_report,
        format_permissions_switch_report, format_pr_report, format_resume_report,
        format_startup_banner, format_startup_banner_with_task, format_status_report,
        format_tool_call_start, format_tool_result, format_ultraplan_report,
        format_unknown_slash_command_message, format_user_visible_api_error,
        gateway_auth_token_from_platform, get_unified_store, handoff_resume_context_packet,
        hydrate_session_from_unified_store, import_local_session_file, jsonl_sessions_dir,
        merge_prompt_with_stdin, normalize_permission_mode, parse_args,
        parse_daemon_approval_slash_command, parse_daemon_context_slash_command,
        parse_daemon_cross_plane_slash_command, parse_daemon_task_slash_command, parse_export_args,
        parse_gateway_args, parse_git_status_branch, parse_git_status_metadata_for,
        parse_git_workspace_summary, parse_history_count, permission_policy, print_help_to,
        push_output_block, render_config_report, render_diff_report, render_diff_report_for,
        render_memory_report, render_prompt_history_report, render_repl_help, render_resume_usage,
        render_session_markdown, render_setup_json, render_setup_report,
        resolve_model_alias_with_config, resolve_repl_model, resolve_session_reference,
        response_to_events, resume_supported_slash_commands, run_resume_command, session_db_path,
        session_db_resume_context_packet, short_tool_id,
        slash_command_completion_candidates_with_sessions, status_context, strip_ansi_for_tui,
        suggestions::format_unknown_slash_command, summarize_tool_payload_for_markdown,
        sync_cli_session_to_unified_store, try_resolve_bare_skill_prompt, validate_no_args,
        workspace_context_item, write_mcp_server_fixture, CliAction, CliOutputFormat,
        CliToolExecutor, DaemonApprovalSlashCommand, DaemonContextSlashCommand,
        DaemonCrossPlaneSlashCommand, DaemonTaskSlashCommand, GatewayAction, GitWorkspaceSummary,
        LiveCli, LocalHelpTopic, PromptHistoryEntry, SessionHandle, SlashCommand, StatusUsage,
        DEFAULT_MODEL, LATEST_SESSION_REFERENCE, SHARED_RT, STUB_COMMANDS,
    };
    use crate::task_kernel::{
        TaskPhaseArtifact, TaskPhaseRecord, TaskPhaseStatus, TaskRecord, TaskStatus,
    };
    use plugins::{
        PluginManager, PluginManagerConfig, PluginTool, PluginToolDefinition, PluginToolPermission,
    };
    use provider::{ApiError, MessageResponse, OutputContentBlock, Usage};
    use runtime::{
        load_oauth_credentials, save_oauth_credentials, AssistantEvent, ConfigLoader, ContentBlock,
        ContextProfile, ConversationMessage, GatewayPlatformConfig, JsonValue, MessageRole,
        OAuthConfig, PermissionMode, Session, ToolExecutor,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tools::GlobalToolRegistry;

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

    #[test]
    fn parse_daemon_task_slash_command_maps_core_actions() {
        assert_eq!(
            parse_daemon_task_slash_command(None).unwrap(),
            DaemonTaskSlashCommand::List
        );
        assert_eq!(
            parse_daemon_task_slash_command(Some("status")).unwrap(),
            DaemonTaskSlashCommand::List
        );
        assert_eq!(
            parse_daemon_task_slash_command(Some("start --yolo finish daemon parity")).unwrap(),
            DaemonTaskSlashCommand::Start {
                objective: "finish daemon parity".to_string(),
                yolo_mode: true,
            }
        );
        assert_eq!(
            parse_daemon_task_slash_command(Some("cancel task-1")).unwrap(),
            DaemonTaskSlashCommand::Cancel {
                id: "task-1".to_string(),
            }
        );
        assert!(parse_daemon_task_slash_command(Some("start --yolo")).is_err());
        assert!(parse_daemon_task_slash_command(Some("unknown")).is_err());
    }

    #[test]
    fn parse_daemon_approval_slash_command_maps_core_actions() {
        assert_eq!(
            parse_daemon_approval_slash_command(None).unwrap(),
            DaemonApprovalSlashCommand::List
        );
        assert_eq!(
            parse_daemon_approval_slash_command(Some(
                "approve req-1 --persist session --reason trusted channel"
            ))
            .unwrap(),
            DaemonApprovalSlashCommand::Respond {
                id: "req-1".to_string(),
                approved: true,
                persistence: Some("session".to_string()),
                reason: Some("trusted channel".to_string()),
            }
        );
        assert_eq!(
            parse_daemon_approval_slash_command(Some("reject req-2")).unwrap(),
            DaemonApprovalSlashCommand::Respond {
                id: "req-2".to_string(),
                approved: false,
                persistence: None,
                reason: None,
            }
        );
        assert!(parse_daemon_approval_slash_command(Some("approve")).is_err());
        assert!(parse_daemon_approval_slash_command(Some("maybe req-1")).is_err());
    }

    #[test]
    fn parse_daemon_context_slash_command_maps_core_actions() {
        assert_eq!(
            parse_daemon_context_slash_command(None).unwrap(),
            DaemonContextSlashCommand::Current
        );
        assert_eq!(
            parse_daemon_context_slash_command(Some("runtime")).unwrap(),
            DaemonContextSlashCommand::Runtime
        );
        assert_eq!(
            parse_daemon_context_slash_command(Some("effective-config")).unwrap(),
            DaemonContextSlashCommand::Config
        );
        assert_eq!(
            parse_daemon_context_slash_command(Some("memory")).unwrap(),
            DaemonContextSlashCommand::Memory
        );
        assert_eq!(
            parse_daemon_context_slash_command(Some("channels")).unwrap(),
            DaemonContextSlashCommand::CrossPlane
        );
        assert!(parse_daemon_context_slash_command(Some("unknown")).is_err());
    }

    #[test]
    fn parse_daemon_cross_plane_slash_command_maps_core_actions() {
        assert_eq!(
            parse_daemon_cross_plane_slash_command(None).unwrap(),
            DaemonCrossPlaneSlashCommand::Summary
        );
        assert_eq!(
            parse_daemon_cross_plane_slash_command(Some("preflight {\"operation\":\"send_text\"}"))
                .unwrap(),
            DaemonCrossPlaneSlashCommand::Preflight("{\"operation\":\"send_text\"}".to_string())
        );
        assert_eq!(
            parse_daemon_cross_plane_slash_command(Some("execute {\"id\":\"req-1\"}")).unwrap(),
            DaemonCrossPlaneSlashCommand::Execute("{\"id\":\"req-1\"}".to_string())
        );
        assert!(parse_daemon_cross_plane_slash_command(Some("execute")).is_err());
        assert!(parse_daemon_cross_plane_slash_command(Some("unknown {}")).is_err());
    }

    fn registry_with_plugin_tool() -> GlobalToolRegistry {
        GlobalToolRegistry::with_plugin_tools(vec![PluginTool::new(
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
        assert!(error.contains("`cowd mcp serve` was removed"));
        assert!(error.contains("cowd gateway start"));
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
            suggested_action: None,
        };

        let rendered = format_user_visible_api_error("session-issue-32", &error);
        assert!(rendered.contains("context_window_blocked"), "{rendered}");
        assert!(
            rendered.contains("Trace            req_ctx_456"),
            "{rendered}"
        );
        assert!(
            rendered
                .contains("Detail           This model's maximum context length is 200000 tokens"),
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
            rendered
                .contains("Detail           Request is too large for this model's context window."),
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
        std::env::temp_dir().join(format!("cowd-cli-{nanos}-{unique}"))
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
        original: Option<String>,
    }

    impl ConfigHomeGuard {
        fn new() -> Self {
            let original = std::env::var("COWD_CONFIG_HOME").ok();
            let tmp = std::env::temp_dir().join("cc-test-config-home");
            let _ = fs::create_dir_all(&tmp);
            std::env::set_var("COWD_CONFIG_HOME", &tmp);
            Self { original }
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

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    fn defaults_to_repl_when_no_args() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        std::env::remove_var("COWD_PERMISSION_MODE");
        assert_eq!(
            parse_args(&[]).expect("args should parse"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
            r#"{"permissionMode":"acceptEdits"}"#,
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
            r#"{"permissionMode":"acceptEdits"}"#,
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

        save_oauth_credentials(&runtime::OAuthTokenSet {
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
    fn builtin_aliases_fallback_main_and_fast() {
        let resolver = runtime::ModelResolver::default();
        assert_eq!(resolver.resolve("main"), "claude-sonnet-4-6");
        assert_eq!(resolver.resolve("fast"), "claude-haiku-4-5-20251213");
        // Unknown aliases pass through
        assert_eq!(resolver.resolve("opus"), "opus");
        assert_eq!(resolver.resolve("grok-3"), "grok-3");
    }

    #[test]
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
        let _cfg_guard = ConfigHomeGuard::new();
        let args = vec!["--permission-mode=read-only".to_string()];
        assert_eq!(
            parse_args(&args).expect("args should parse"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
    fn parses_repl_session_flag() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        std::env::remove_var("COWD_PERMISSION_MODE");

        assert_eq!(
            parse_args(&["--session".to_string(), "session-alpha".to_string()])
                .expect("session flag should parse"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
    fn dangerously_skip_permissions_flag_forces_danger_full_access_in_repl() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        std::env::set_var("COWD_PERMISSION_MODE", "read-only");
        let args = vec!["--dangerously-skip-permissions".to_string()];
        let parsed = parse_args(&args).expect("args should parse");
        std::env::remove_var("COWD_PERMISSION_MODE");

        assert_eq!(
            parsed,
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
    fn yolo_flag_forces_danger_full_access_and_marks_repl_mode() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        std::env::set_var("COWD_PERMISSION_MODE", "read-only");
        let args = vec!["--yolo".to_string()];
        let parsed = parse_args(&args).expect("args should parse");
        std::env::remove_var("COWD_PERMISSION_MODE");

        assert_eq!(
            parsed,
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
    fn yolo_mode_creates_and_reuses_durable_task() {
        let _guard = env_lock();
        let config_home = temp_dir();
        fs::create_dir_all(&config_home).expect("config home");
        let original = std::env::var("COWD_CONFIG_HOME").ok();
        std::env::set_var("COWD_CONFIG_HOME", &config_home);

        assert!(ensure_yolo_task(false, "ignored").unwrap().is_none());
        let first = ensure_yolo_task(true, "ship v0.8.10")
            .unwrap()
            .expect("yolo task should create");
        let second = ensure_yolo_task(true, "different objective")
            .unwrap()
            .expect("existing yolo task should restore");

        assert_eq!(first.id, second.id);
        assert_eq!(second.objective, "ship v0.8.10");
        assert!(config_home.join("tasks.db").is_file());

        match original {
            Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
        let _ = fs::remove_dir_all(config_home);
    }

    #[test]
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
    fn parses_allowed_tools_flags_with_aliases_and_lists() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        std::env::remove_var("COWD_PERMISSION_MODE");
        let args = vec![
            "--allowedTools".to_string(),
            "read,glob".to_string(),
            "--allowed-tools=write_file".to_string(),
        ];
        assert_eq!(
            parse_args(&args).expect("args should parse"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
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
    fn rejects_unknown_allowed_tools() {
        let error = parse_args(&["--allowedTools".to_string(), "teleport".to_string()])
            .expect_err("tool should be rejected");
        assert!(error.contains("unsupported tool in --allowedTools: teleport"));
    }

    #[test]
    fn parses_system_prompt_options() {
        let args = vec![
            "system-prompt".to_string(),
            "--cwd".to_string(),
            "/tmp/project".to_string(),
            "--date".to_string(),
            "2026-04-01".to_string(),
        ];
        assert_eq!(
            parse_args(&args).expect("args should parse"),
            CliAction::PrintSystemPrompt {
                cwd: PathBuf::from("/tmp/project"),
                date: "2026-04-01".to_string(),
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn help_prioritizes_minimal_core_surface() {
        let mut out = Vec::new();
        print_help_to(&mut out).expect("help should render");
        let help = String::from_utf8(out).expect("help should be utf8");

        assert!(help.contains("Core commands:"));
        assert!(help.contains("cowd --tui"));
        assert!(help.contains("cowd gateway start|stop|restart|status|doctor|logs|repair|open"));
        assert!(help.contains("cowd status"));
        assert!(help.contains("cowd doctor"));
        assert!(help.contains("cowd export"));
        assert!(help.contains("cowd import-session PATH"));
        assert!(help.contains("cowd skills list|show|validate"));
        assert!(help.contains("Advanced local tools:"));

        let core_start = help.find("Core commands:").expect("core section");
        let advanced_start = help
            .find("Advanced local tools:")
            .expect("advanced section");
        let core = &help[core_start..advanced_start];
        let advanced = &help[advanced_start..];

        for complex in [
            "cowd agents",
            "cowd mcp",
            "cowd plugins",
            "cowd dump-manifests",
            "cowd bootstrap-plan",
            "cowd system-prompt",
        ] {
            assert!(
                !core.contains(complex),
                "{complex} must not be presented as a core CLI command"
            );
            assert!(
                advanced.contains(complex),
                "{complex} must remain documented for compatibility"
            );
        }
    }

    #[test]
    fn gateway_help_keeps_channel_helpers_out_of_core_surface() {
        let mut out = Vec::new();
        print_help_to(&mut out).expect("help should render");
        let help = String::from_utf8(out).expect("help should be utf8");
        let core_start = help.find("Core commands:").expect("core section");
        let advanced_start = help
            .find("Advanced local tools:")
            .expect("advanced section");
        let core = &help[core_start..advanced_start];
        let advanced = &help[advanced_start..];

        assert!(core.contains("gateway start|stop|restart|status|doctor|logs|repair|open"));
        assert!(!core.contains("wechat-qr"));
        assert!(!advanced.contains("wechat-qr"));
    }

    #[test]
    fn removed_login_and_logout_subcommands_error_helpfully() {
        let _cfg_guard = ConfigHomeGuard::new();
        let login = parse_args(&["login".to_string()]).expect_err("login should be removed");
        assert!(login.contains("ANTHROPIC_API_KEY"));
        let logout = parse_args(&["logout".to_string()]).expect_err("logout should be removed");
        assert!(logout.contains("ANTHROPIC_AUTH_TOKEN"));
        assert_eq!(
            parse_args(&["doctor".to_string()]).expect("doctor should parse"),
            CliAction::Doctor {
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["state".to_string()]).expect("state should parse"),
            CliAction::State {
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["setup".to_string()]).expect("setup should parse"),
            CliAction::Setup {
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&[
                "state".to_string(),
                "--output-format".to_string(),
                "json".to_string()
            ])
            .expect("state --output-format json should parse"),
            CliAction::State {
                output_format: CliOutputFormat::Json,
            }
        );
        assert_eq!(
            parse_args(&["init".to_string()]).expect("init should parse"),
            CliAction::Init {
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["agents".to_string()]).expect("agents should parse"),
            CliAction::Agents {
                args: None,
                output_format: CliOutputFormat::Text
            }
        );
        assert_eq!(
            parse_args(&["mcp".to_string()]).expect("mcp should parse"),
            CliAction::Mcp {
                args: None,
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["skills".to_string()]).expect("skills should parse"),
            CliAction::Skills {
                args: None,
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&[
                "skills".to_string(),
                "help".to_string(),
                "overview".to_string()
            ])
            .expect("skills help overview should invoke"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
                session_id: None,
                allowed_tools: None,
                permission_mode: crate::default_permission_mode(),
                base_commit: None,
                reasoning_effort: None,
                allow_broad_cwd: false,
                yolo_mode: false,
            }
        );
        assert_eq!(
            parse_args(&["agents".to_string(), "--help".to_string()])
                .expect("agents help should parse"),
            CliAction::Agents {
                args: Some("--help".to_string()),
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn dump_manifests_subcommand_accepts_explicit_manifest_dir() {
        assert_eq!(
            parse_args(&[
                "dump-manifests".to_string(),
                "--manifests-dir".to_string(),
                "/tmp/upstream".to_string(),
            ])
            .expect("dump-manifests should parse"),
            CliAction::DumpManifests {
                output_format: CliOutputFormat::Text,
                manifests_dir: Some(PathBuf::from("/tmp/upstream")),
            }
        );
        assert_eq!(
            parse_args(&[
                "dump-manifests".to_string(),
                "--manifests-dir=/tmp/upstream".to_string()
            ])
            .expect("inline dump-manifests flag should parse"),
            CliAction::DumpManifests {
                output_format: CliOutputFormat::Text,
                manifests_dir: Some(PathBuf::from("/tmp/upstream")),
            }
        );
    }

    #[test]
    fn local_command_help_flags_stay_on_the_local_parser_path() {
        assert_eq!(
            parse_args(&["status".to_string(), "--help".to_string()])
                .expect("status help should parse"),
            CliAction::HelpTopic(LocalHelpTopic::Status)
        );
        assert_eq!(
            parse_args(&["sandbox".to_string(), "-h".to_string()])
                .expect("sandbox help should parse"),
            CliAction::HelpTopic(LocalHelpTopic::Sandbox)
        );
        assert_eq!(
            parse_args(&["doctor".to_string(), "--help".to_string()])
                .expect("doctor help should parse"),
            CliAction::HelpTopic(LocalHelpTopic::Doctor)
        );
        assert_eq!(
            parse_args(&["setup".to_string(), "--help".to_string()])
                .expect("setup help should parse"),
            CliAction::HelpTopic(LocalHelpTopic::Setup)
        );
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
        assert_eq!(
            parse_args(&["status".to_string()]).expect("status should parse"),
            CliAction::Status {
                model: DEFAULT_MODEL.to_string(),
                permission_mode: PermissionMode::WorkspaceWrite,
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["sandbox".to_string()]).expect("sandbox should parse"),
            CliAction::Sandbox {
                output_format: CliOutputFormat::Text,
            }
        );
        assert_eq!(
            parse_args(&["setup".to_string()]).expect("setup should parse"),
            CliAction::Setup {
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn parses_bare_export_subcommand_targeting_latest_session() {
        // given
        let _guard = env_lock();
        std::env::remove_var("COWD_PERMISSION_MODE");
        let args = vec!["export".to_string()];

        // when
        let parsed = parse_args(&args).expect("bare export should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: LATEST_SESSION_REFERENCE.to_string(),
                output_path: None,
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn parses_export_subcommand_with_positional_output_path() {
        // given
        let args = vec!["export".to_string(), "conversation.md".to_string()];

        // when
        let parsed = parse_args(&args).expect("export with path should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: LATEST_SESSION_REFERENCE.to_string(),
                output_path: Some(PathBuf::from("conversation.md")),
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn parses_export_subcommand_with_session_and_output_flags() {
        // given
        let args = vec![
            "export".to_string(),
            "--session".to_string(),
            "session-alpha".to_string(),
            "--output".to_string(),
            "/tmp/share.md".to_string(),
        ];

        // when
        let parsed = parse_args(&args).expect("export flags should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: "session-alpha".to_string(),
                output_path: Some(PathBuf::from("/tmp/share.md")),
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn parses_export_subcommand_with_inline_flag_values() {
        // given
        let args = vec![
            "export".to_string(),
            "--session=session-beta".to_string(),
            "--output=/tmp/beta.md".to_string(),
        ];

        // when
        let parsed = parse_args(&args).expect("export inline flags should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: "session-beta".to_string(),
                output_path: Some(PathBuf::from("/tmp/beta.md")),
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn parses_export_subcommand_with_json_output_format() {
        // given
        let args = vec![
            "--output-format=json".to_string(),
            "export".to_string(),
            "/tmp/notes.md".to_string(),
        ];

        // when
        let parsed = parse_args(&args).expect("json export should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: LATEST_SESSION_REFERENCE.to_string(),
                output_path: Some(PathBuf::from("/tmp/notes.md")),
                output_format: CliOutputFormat::Json,
            }
        );
    }

    #[test]
    fn rejects_unknown_export_options_with_helpful_message() {
        // given
        let args = vec!["export".to_string(), "--bogus".to_string()];

        // when
        let error = parse_args(&args).expect_err("unknown export option should fail");

        // then
        assert!(error.contains("unknown export option: --bogus"));
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
        assert!(error.contains("unexpected export argument: second.md"));
    }

    #[test]
    fn parse_export_args_helper_defaults_to_latest_reference_and_no_output() {
        // given
        let args: Vec<String> = vec![];

        // when
        let parsed = parse_export_args(&args, CliOutputFormat::Text)
            .expect("empty export args should parse");

        // then
        assert_eq!(
            parsed,
            CliAction::Export {
                session_reference: LATEST_SESSION_REFERENCE.to_string(),
                output_path: None,
                output_format: CliOutputFormat::Text,
            }
        );
    }

    #[test]
    fn render_session_markdown_includes_header_and_summarized_tool_calls() {
        // given
        let mut session = Session::new();
        session.session_id = "session-export-test".to_string();
        session.messages = vec![
            ConversationMessage::user_text("How do I list files?"),
            ConversationMessage::assistant(vec![
                ContentBlock::Text {
                    text: "I'll run a tool.".to_string(),
                },
                ContentBlock::ToolUse {
                    id: "toolu_abcdefghijklmnop".to_string(),
                    name: "bash".to_string(),
                    input: r#"{"command":"ls -la"}"#.to_string(),
                },
            ]),
            ConversationMessage {
                role: MessageRole::Tool,
                blocks: vec![ContentBlock::ToolResult {
                    tool_use_id: "toolu_abcdefghijklmnop".to_string(),
                    tool_name: "bash".to_string(),
                    output: "total 8\ndrwxr-xr-x  2 user staff   64 Apr  7 12:00 .".to_string(),
                    is_error: false,
                }],
                usage: None,
            },
        ];

        // when
        let markdown = render_session_markdown(
            &session,
            "session-export-test",
            std::path::Path::new("/tmp/sessions/session-export-test.jsonl"),
        );

        // then
        assert!(markdown.starts_with("# Conversation Export"));
        assert!(markdown.contains("- **Session**: `session-export-test`"));
        assert!(markdown.contains("- **Messages**: 3"));
        assert!(markdown.contains("## 1. User"));
        assert!(markdown.contains("How do I list files?"));
        assert!(markdown.contains("## 2. Assistant"));
        assert!(markdown.contains("**Tool call** `bash`"));
        assert!(markdown.contains("toolu_abcdef…"));
        assert!(markdown.contains("ls -la"));
        assert!(markdown.contains("## 3. Tool"));
        assert!(markdown.contains("**Tool result** `bash`"));
        assert!(markdown.contains("ok"));
        assert!(markdown.contains("total 8"));
    }

    #[test]
    fn render_session_markdown_marks_tool_errors_and_skips_empty_summaries() {
        // given
        let mut session = Session::new();
        session.session_id = "errs".to_string();
        session.messages = vec![ConversationMessage {
            role: MessageRole::Tool,
            blocks: vec![ContentBlock::ToolResult {
                tool_use_id: "short".to_string(),
                tool_name: "read_file".to_string(),
                output: "   ".to_string(),
                is_error: true,
            }],
            usage: None,
        }];

        // when
        let markdown =
            render_session_markdown(&session, "errs", std::path::Path::new("errs.jsonl"));

        // then
        assert!(markdown.contains("**Tool result** `read_file` _(id `short`, error)_"));
        // an empty summary should not produce a stray blockquote line
        assert!(!markdown.contains("> \n"));
    }

    #[test]
    fn summarize_tool_payload_for_markdown_compacts_json_and_truncates_overflow() {
        // given
        let json_payload = r#"{
            "command":   "ls -la",
            "cwd": "/tmp"
        }"#;
        let long_payload = "a".repeat(600);

        // when
        let compacted = summarize_tool_payload_for_markdown(json_payload);
        let truncated = summarize_tool_payload_for_markdown(&long_payload);

        // then
        assert_eq!(compacted, r#"{"command":"ls -la","cwd":"/tmp"}"#);
        assert!(truncated.ends_with('…'));
        assert!(truncated.chars().count() <= 281);
    }

    #[test]
    fn short_tool_id_truncates_long_identifiers_with_ellipsis() {
        // given
        let long = "toolu_01ABCDEFGHIJKLMN";
        let short = "tool_1";

        // when
        let trimmed_long = short_tool_id(long);
        let trimmed_short = short_tool_id(short);

        // then
        assert_eq!(trimmed_long, "toolu_01ABCD…");
        assert_eq!(trimmed_short, "tool_1");
    }

    #[test]
    fn parses_json_output_for_mcp_and_skills_commands() {
        assert_eq!(
            parse_args(&["--output-format=json".to_string(), "mcp".to_string()])
                .expect("json mcp should parse"),
            CliAction::Mcp {
                args: None,
                output_format: CliOutputFormat::Json,
            }
        );
        assert_eq!(
            parse_args(&[
                "--output-format=json".to_string(),
                "skills".to_string(),
                "help".to_string(),
            ])
            .expect("json skills help should parse"),
            CliAction::Skills {
                args: Some("help".to_string()),
                output_format: CliOutputFormat::Json,
            }
        );
    }

    #[test]
    fn single_word_slash_command_names_return_guidance_instead_of_hitting_prompt_mode() {
        let error = parse_args(&["cost".to_string()]).expect_err("cost should return guidance");
        assert!(error.contains("slash command"));
        assert!(error.contains("/cost"));
    }

    #[test]
    fn direct_slash_commands_return_tui_guidance() {
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
    fn parses_import_session_subcommand_and_rejects_extra_args() {
        assert_eq!(
            parse_args(&["import-session".to_string(), "legacy.jsonl".to_string()])
                .expect("import-session should parse"),
            CliAction::ImportSession {
                path: PathBuf::from("legacy.jsonl"),
                output_format: CliOutputFormat::Text,
            }
        );

        let error = parse_args(&[
            "import-session".to_string(),
            "legacy.jsonl".to_string(),
            "extra".to_string(),
        ])
        .expect_err("extra import-session arguments should fail");
        assert!(error.contains("unexpected arguments for import-session"));
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
    fn rejects_resume_flag_with_single_slash_command() {
        let args = vec![
            "--resume".to_string(),
            "session-123".to_string(),
            "/compact".to_string(),
        ];
        let error = parse_args(&args).expect_err("resume slash commands should be removed");
        assert!(error.contains("was removed from the CLI surface"));
        assert!(error.contains("run slash commands inside the TUI"));
    }

    #[test]
    fn parses_resume_flag_without_path_as_latest_session() {
        assert_eq!(
            parse_args(&["--resume".to_string()]).expect("args should parse"),
            CliAction::Repl {
                model: DEFAULT_MODEL.to_string(),
                session_id: Some("latest".to_string()),
                allowed_tools: None,
                permission_mode: crate::default_permission_mode(),
                base_commit: None,
                reasoning_effort: None,
                allow_broad_cwd: false,
                yolo_mode: false,
            }
        );
        let error = parse_args(&["--resume".to_string(), "/status".to_string()])
            .expect_err("resume slash shortcut should be removed");
        assert!(error.contains("was removed from the CLI surface"));
        assert!(error.contains("run slash commands inside the TUI"));
    }

    #[test]
    fn rejects_resume_flag_with_slash_commands() {
        let args = vec![
            "--resume".to_string(),
            "session-123".to_string(),
            "/status".to_string(),
            "/compact".to_string(),
            "/cost".to_string(),
        ];
        let error = parse_args(&args).expect_err("resume slash commands should be removed");
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
    fn rejects_resume_flag_with_slash_command_arguments() {
        let args = vec![
            "--resume".to_string(),
            "session-123".to_string(),
            "/export".to_string(),
            "notes.txt".to_string(),
            "/clear".to_string(),
            "--confirm".to_string(),
        ];
        let error = parse_args(&args).expect_err("resume slash commands should be removed");
        assert!(error.contains("was removed from the CLI surface"));
        assert!(error.contains("run slash commands inside the TUI"));
    }

    #[test]
    fn rejects_resume_flag_with_absolute_export_path() {
        let args = vec![
            "--resume".to_string(),
            "session-123".to_string(),
            "/export".to_string(),
            "/tmp/notes.txt".to_string(),
            "/status".to_string(),
        ];
        let error = parse_args(&args).expect_err("resume slash commands should be removed");
        assert!(error.contains("was removed from the CLI surface"));
        assert!(error.contains("run slash commands inside the TUI"));
    }

    #[test]
    fn filtered_tool_specs_respect_allowlist() {
        let allowed = ["read_file", "grep_search"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let filtered = filter_tool_specs(&GlobalToolRegistry::builtin(), Some(&allowed));
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
        let help = commands::render_slash_command_help();
        assert!(help.contains("Slash commands"));
        assert!(
            help.contains("[resumed TUI]     available after `cowd --resume <session-id|latest>`")
        );
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
    fn repl_help_includes_shared_commands_and_exit() {
        let help = render_repl_help();
        assert!(help.contains("REPL"));
        assert!(help.contains("/help"));
        assert!(help.contains("Complete commands, modes, and recent sessions"));
        assert!(help.contains("/status"));
        assert!(help.contains("/sandbox"));
        assert!(help.contains("/model [model]"));
        assert!(help.contains("/permissions [read-only|workspace-write|danger-full-access]"));
        assert!(help.contains("/clear [--confirm]"));
        assert!(help.contains("/cost"));
        assert!(help.contains("/resume <session-id|latest>"));
        assert!(help.contains("/config [env|hooks|model|plugins]"));
        assert!(help.contains("/mcp [list|show <server>|help]"));
        assert!(help.contains("/memory"));
        assert!(help.contains("/init"));
        assert!(help.contains("/diff"));
        assert!(help.contains("/version"));
        assert!(help.contains("/export [file]"));
        // Batch 5 added `/session delete`; match on the stable core rather than
        // the trailing bracket so future additions don't re-break this.
        assert!(help.contains("/session [list|switch <session-id>|fork [branch-name]"));
        assert!(help.contains(
            "/plugin [list|install <path>|enable <name>|disable <name>|uninstall <id>|update <id>]"
        ));
        assert!(help.contains("aliases: /plugins, /marketplace"));
        assert!(help.contains("/agents"));
        assert!(help.contains("/skills"));
        assert!(help.contains("/exit"));
        assert!(help.contains("Auto-save            SQLite session store"));
        assert!(help.contains("Resume latest        /resume latest"));
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
    fn startup_banner_mentions_workflow_completions() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");

        let banner = with_current_dir(&root, || {
            format_startup_banner("claude-sonnet-4-6", false, "session-banner-test")
        });

        assert!(banner.contains("Tab"));
        assert!(banner.contains("sidebar"));
        assert!(banner.contains("standard"));

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
        let banner = format_startup_banner("claude-sonnet-4-6", false, "session-banner-test");
        let plain = strip_ansi_for_tui(&banner);

        assert!(plain.contains("COWD"));
        assert!(!plain.contains("\u{1b}["));
    }

    #[test]
    fn cli_turn_context_profile_maps_runtime_modes() {
        assert_eq!(
            cli_turn_context_profile(false, PermissionMode::WorkspaceWrite, false, false),
            ContextProfile::MainTurn
        );
        assert_eq!(
            cli_turn_context_profile(false, PermissionMode::DangerFullAccess, false, false),
            ContextProfile::SoloGoal
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
        let task = TaskRecord {
            id: "task-abcdef123456".to_string(),
            objective: "complete v0.8.10 enterprise AI framework".to_string(),
            status: TaskStatus::Running,
            current_phase: Some("tui-cockpit".to_string()),
            phases: vec![TaskPhaseRecord {
                id: "phase-1".to_string(),
                name: "tui-cockpit".to_string(),
                objective: "surface durable task state in TUI".to_string(),
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
                status: TaskPhaseStatus::Completed,
                created_at_ms: 1,
                updated_at_ms: 1,
            }],
            yolo_mode: true,
            failure_count: 0,
            blocker_reason: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            audit: Vec::new(),
            agent_graph: None,
        };

        let banner = format_startup_banner_with_task(
            "claude-sonnet-4-6",
            true,
            "session-yolo-test",
            Some(&task),
        );

        assert!(banner.contains("Task"));
        assert!(banner.contains("running"));
        assert!(banner.contains("task-abc"));
        assert!(banner.contains("phase tui-cockpit:completed"));
        assert!(banner.contains("complete v0.8.10"));
    }

    #[test]
    fn current_task_summary_includes_phase_review_and_artifacts() {
        let task = TaskRecord {
            id: "task-abcdef123456".to_string(),
            objective: "complete v0.8.10 enterprise AI framework".to_string(),
            status: TaskStatus::Running,
            current_phase: Some("tui-cockpit".to_string()),
            phases: vec![TaskPhaseRecord {
                id: "phase-1".to_string(),
                name: "tui-cockpit".to_string(),
                objective: "surface durable task state in TUI".to_string(),
                plan: Vec::new(),
                acceptance: Vec::new(),
                test_commands: Vec::new(),
                artifacts: vec![
                    TaskPhaseArtifact {
                        kind: "test".to_string(),
                        label: "unit".to_string(),
                        value: "passed".to_string(),
                        created_at_ms: 1,
                    },
                    TaskPhaseArtifact {
                        kind: "smoke".to_string(),
                        label: "tmux".to_string(),
                        value: "passed".to_string(),
                        created_at_ms: 2,
                    },
                ],
                review_result: Some("accepted".to_string()),
                status: TaskPhaseStatus::Completed,
                created_at_ms: 1,
                updated_at_ms: 2,
            }],
            yolo_mode: true,
            failure_count: 0,
            blocker_reason: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            audit: Vec::new(),
            agent_graph: None,
        };

        let summary = current_task_summary_from_record(&task);

        assert_eq!(summary.current_phase.as_deref(), Some("tui-cockpit"));
        assert_eq!(summary.phase_status.as_deref(), Some("completed"));
        assert_eq!(summary.review_result.as_deref(), Some("accepted"));
        assert_eq!(summary.artifact_count, 2);
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
    fn resolve_repl_model_returns_user_supplied_model_unchanged_when_explicit() {
        let user_model = "gpt-4o".to_string();

        let resolved = resolve_repl_model(user_model);

        assert_eq!(resolved, "gpt-4o");
    }

    #[test]
    fn resolve_repl_model_falls_back_to_anthropic_model_env_when_default() {
        let _guard = env_lock();
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");
        let config_home = root.join("config");
        fs::create_dir_all(&config_home).expect("config home dir");
        std::env::set_var("COWD_CONFIG_HOME", &config_home);
        std::env::remove_var("ANTHROPIC_MODEL");
        std::env::set_var("ANTHROPIC_MODEL", "claude-sonnet-4-6");

        let resolved = with_current_dir(&root, || resolve_repl_model(DEFAULT_MODEL.to_string()));

        assert_eq!(resolved, "claude-sonnet-4-6");

        std::env::remove_var("ANTHROPIC_MODEL");
        std::env::remove_var("COWD_CONFIG_HOME");
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn resolve_repl_model_returns_default_when_env_unset_and_no_config() {
        let _guard = env_lock();
        let _cfg_guard = ConfigHomeGuard::new();
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");
        let config_home = root.join("config");
        fs::create_dir_all(&config_home).expect("config home dir");
        std::env::set_var("COWD_CONFIG_HOME", &config_home);
        std::env::remove_var("ANTHROPIC_MODEL");

        let resolved = with_current_dir(&root, || resolve_repl_model(DEFAULT_MODEL.to_string()));

        assert_eq!(resolved, DEFAULT_MODEL);

        std::env::remove_var("COWD_CONFIG_HOME");
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn resume_supported_command_list_matches_expected_surface() {
        let names = resume_supported_slash_commands()
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        // Now with 135+ slash commands, verify minimum resume support
        assert!(
            names.len() >= 39,
            "expected at least 39 resume-supported commands, got {}",
            names.len()
        );
        // Verify key resume commands still exist
        assert!(names.contains(&"help"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"compact"));
    }

    #[test]
    fn resume_report_uses_sectioned_layout() {
        let report = format_resume_report("session-123", 14, 6);
        assert!(report.contains("Session resumed"));
        assert!(report.contains("Session          session-123"));
        assert!(report.contains("Messages         14"));
        assert!(report.contains("Turns            6"));
    }

    #[test]
    fn session_db_resume_packet_summarizes_recent_session_state() {
        let mut session = runtime::Session::new();
        session.session_id = "session-resume-packet".to_string();
        session.messages.push(runtime::ConversationMessage {
            role: runtime::MessageRole::User,
            blocks: vec![runtime::ContentBlock::Text {
                text: "continue the context runtime work".to_string(),
            }],
            usage: None,
        });
        session.messages.push(runtime::ConversationMessage {
            role: runtime::MessageRole::Assistant,
            blocks: vec![runtime::ContentBlock::Text {
                text: "context timeline is persisted".to_string(),
            }],
            usage: None,
        });
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
            .args(["-C"])
            .arg(&root)
            .args(["add", "src/lib.rs"])
            .output()
            .expect("run git add");
        assert!(git_add.status.success());
        let session = runtime::Session::new().with_workspace_root(root.clone());

        let item = workspace_context_item(&session, 200_000);

        assert_eq!(item.source, runtime::ContextSourceKind::Workspace);
        assert!(item.content.contains(&root.display().to_string()));
        assert!(item.content.contains("model_context_window=200000"));
        assert!(item.content.contains("src/lib.rs"));
        assert!(item.content.contains("workspace_context_probe"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn compact_report_uses_structured_output() {
        let compacted = format_compact_report(8, 5, false);
        assert!(compacted.contains("Compact"));
        assert!(compacted.contains("Result           compacted"));
        assert!(compacted.contains("Messages removed 8"));
        let skipped = format_compact_report(0, 3, true);
        assert!(skipped.contains("Result           skipped"));
    }

    #[test]
    fn cost_report_uses_sectioned_layout() {
        let report = format_cost_report(runtime::TokenUsage {
            input_tokens: 20,
            output_tokens: 8,
            cache_creation_input_tokens: 3,
            cache_read_input_tokens: 1,
        });
        assert!(report.contains("Cost"));
        assert!(report.contains("Input tokens     20"));
        assert!(report.contains("Output tokens    8"));
        assert!(report.contains("Cache create     3"));
        assert!(report.contains("Cache read       1"));
        assert!(report.contains("Total tokens     32"));
    }

    #[test]
    fn permissions_report_uses_sectioned_layout() {
        let report = format_permissions_report("workspace-write");
        assert!(report.contains("Permissions"));
        assert!(report.contains("Active mode      workspace-write"));
        assert!(report.contains("Modes"));
        assert!(report.contains("read-only          ○ available Read/search tools only"));
        assert!(report.contains("workspace-write    ● current   Edit files inside the workspace"));
        assert!(report.contains("danger-full-access ○ available Unrestricted tool access"));
    }

    #[test]
    fn permissions_switch_report_is_structured() {
        let report = format_permissions_switch_report("read-only", "workspace-write");
        assert!(report.contains("Permissions updated"));
        assert!(report.contains("Result           mode switched"));
        assert!(report.contains("Previous mode    read-only"));
        assert!(report.contains("Active mode      workspace-write"));
        assert!(report.contains("Applies to       subsequent tool calls"));
    }

    #[test]
    fn help_mentions_minimal_local_commands() {
        let mut help = Vec::new();
        print_help_to(&mut help).expect("help should render");
        let help = String::from_utf8(help).expect("help should be utf8");
        assert!(help.contains("cowd help"));
        assert!(help.contains("cowd version"));
        assert!(help.contains("cowd status"));
        assert!(help.contains("cowd sandbox"));
        assert!(help.contains("cowd init"));
        assert!(help.contains("cowd agents"));
        assert!(help.contains("cowd mcp"));
        assert!(help.contains("cowd skills"));
        assert!(help.contains("cowd skills list"));
        assert!(help.contains("/skills"));
        assert!(!help.contains("cowd /skills"));
        assert!(help.contains("ultraworkers/cowd"));
        assert!(help.contains("cargo install cowd"));
        assert!(!help.contains("login command"));
        assert!(!help.contains("logout command"));
    }

    #[test]
    fn model_report_uses_sectioned_layout() {
        let report = format_model_report("claude-sonnet", 12, 4);
        assert!(report.contains("Model"));
        assert!(report.contains("Current model    claude-sonnet"));
        assert!(report.contains("Session messages 12"));
        assert!(report.contains("Switch models with /model <name>"));
    }

    #[test]
    fn model_switch_report_preserves_context_summary() {
        let report = format_model_switch_report("claude-sonnet", "claude-opus", 9);
        assert!(report.contains("Model updated"));
        assert!(report.contains("Previous         claude-sonnet"));
        assert!(report.contains("Current          claude-opus"));
        assert!(report.contains("Preserved msgs   9"));
    }

    #[test]
    fn status_line_reports_model_and_token_totals() {
        let status = format_status_report(
            "claude-sonnet",
            StatusUsage {
                message_count: 7,
                turns: 3,
                latest: runtime::TokenUsage {
                    input_tokens: 5,
                    output_tokens: 4,
                    cache_creation_input_tokens: 1,
                    cache_read_input_tokens: 0,
                },
                cumulative: runtime::TokenUsage {
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
        assert!(status.contains("Status"));
        assert!(status.contains("Model            claude-sonnet"));
        assert!(status.contains("Permission mode  workspace-write"));
        assert!(status.contains("Execution mode   yolo"));
        assert!(status.contains("Messages         7"));
        assert!(status.contains("Latest total     10"));
        assert!(status.contains("Cumulative total 31"));
        assert!(status.contains("Cwd              /tmp/project"));
        assert!(status.contains("Project root     /tmp"));
        assert!(status.contains("Git branch       main"));
        assert!(
            status.contains("Git state        dirty · 3 files · 1 staged, 1 unstaged, 1 untracked")
        );
        assert!(status.contains("Changed files    3"));
        assert!(status.contains("Staged           1"));
        assert!(status.contains("Unstaged         1"));
        assert!(status.contains("Untracked        1"));
        assert!(status.contains("Session          session.jsonl"));
        assert!(status.contains("Session id       session"));
        assert!(status.contains("Session store    local import/export file"));
        assert!(status.contains("Config files     loaded 2/3"));
        assert!(status.contains("Memory files     4"));
        assert!(status.contains("Suggested flow   /status → /diff → /commit"));
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
        assert!(report
            .contains("Action           create a git commit from the current workspace changes"));
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
        assert!(report.contains("Memory"));
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
        fs::write(root.join("tracked.txt"), "hello\nstaged\nunstaged\n")
            .expect("update file twice");

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
    fn resume_diff_command_renders_report_for_saved_session() {
        let _guard = env_lock();
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");
        git(&["init", "--quiet"], &root);
        git(&["config", "user.email", "tests@example.com"], &root);
        git(&["config", "user.name", "Rusty Claude Tests"], &root);
        fs::write(root.join("tracked.txt"), "hello\n").expect("write tracked");
        git(&["add", "tracked.txt"], &root);
        git(&["commit", "-m", "init", "--quiet"], &root);
        fs::write(root.join("tracked.txt"), "hello\nworld\n").expect("modify tracked");
        let session_path = root.join("session.json");
        Session::new()
            .save_to_path(&session_path)
            .expect("session should save");

        let session = Session::load_from_path(&session_path).expect("session should load");
        let outcome = with_current_dir(&root, || {
            run_resume_command(&session_path, &session, &SlashCommand::Diff)
                .expect("resume diff should work")
        });
        let message = outcome.message.expect("diff message should exist");
        assert!(message.contains("Unstaged changes:"));
        assert!(message.contains("tracked.txt"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn resume_session_switch_updates_outcome_session_and_path() {
        let _guard = env_lock();
        let root = temp_dir();
        let config_home_original = std::env::var("COWD_CONFIG_HOME").ok();
        let config_home = temp_dir();
        std::env::set_var("COWD_CONFIG_HOME", &config_home);
        fs::create_dir_all(&root).expect("root dir");
        let (active_path, active, target_handle) = with_current_dir(&root, || {
            let active_handle =
                create_managed_session_handle("resume-switch-active").expect("active handle");
            let active_path = active_handle.path.clone();
            let active = Session::new()
                .with_workspace_root(root.clone())
                .with_persistence_path(active_path.clone());

            let target_handle =
                create_managed_session_handle("resume-switch-target").expect("target handle");
            let target = Session::new().with_workspace_root(root.clone());
            sync_cli_session_to_unified_store(
                get_unified_store().expect("store should open"),
                &target_handle,
                None,
                &target,
            )
            .expect("target session should sync");
            (active_path, active, target_handle)
        });

        let command = SlashCommand::parse(&format!("/session switch {}", target_handle.id))
            .expect("parse should succeed")
            .expect("command should exist");
        let outcome = with_current_dir(&root, || {
            run_resume_command(&active_path, &active, &command).expect("switch should succeed")
        });

        assert_eq!(outcome.session.session_id, target_handle.id);
        assert_eq!(
            outcome
                .session_path
                .expect("switch should update session path"),
            session_db_path()
        );
        assert!(outcome
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("Session switched"));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(config_home);
        if let Some(v) = config_home_original {
            std::env::set_var("COWD_CONFIG_HOME", v);
        } else {
            std::env::remove_var("COWD_CONFIG_HOME");
        }
    }

    #[test]
    fn hydrates_runtime_session_from_unified_store_messages() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");
        let store = memory::UnifiedSessionStore::open_in_memory().expect("store should open");
        let session_id = "store-only-session";
        let record = memory::store::session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "api_server".to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: "2026-06-05T00:00:00Z".to_string(),
            last_activity: "2026-06-05T00:00:01Z".to_string(),
            message_count: 1,
            reset_policy: "none".to_string(),
            metadata_json: Some(
                serde_json::json!({
                    "workspace_root": root.display().to_string(),
                })
                .to_string(),
            ),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        SHARED_RT
            .block_on(store.upsert_session(&record))
            .expect("record should sync");
        let message = ConversationMessage::user_text("from sqlite");
        SHARED_RT
            .block_on(store.insert_message(&message.to_session_message(session_id, 0)))
            .expect("message should sync");

        let handle = SessionHandle {
            id: session_id.to_string(),
            path: root.join("sessions.db"),
        };
        let hydrated = hydrate_session_from_unified_store(&store, &handle)
            .expect("hydrate should work")
            .expect("session should exist");

        assert_eq!(hydrated.session_id, session_id);
        assert_eq!(hydrated.model.as_deref(), Some("test-model"));
        assert_eq!(hydrated.workspace_root.as_deref(), Some(root.as_path()));
        assert_eq!(hydrated.messages, vec![message]);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn cli_session_sync_replaces_store_messages_and_events() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("root dir");
        let store = memory::UnifiedSessionStore::open_in_memory().expect("store should open");
        let handle = SessionHandle {
            id: "cli-sync".to_string(),
            path: root.join("sessions.db"),
        };
        let mut session = Session::new().with_workspace_root(root.clone());
        session.session_id = "cli-sync".to_string();
        session.messages = vec![
            ConversationMessage::user_text("first"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "second".to_string(),
            }]),
        ];

        sync_cli_session_to_unified_store(&store, &handle, Some("test-model"), &session)
            .expect("initial sync should work");
        let messages = SHARED_RT
            .block_on(store.get_all_messages("cli-sync"))
            .expect("messages should read");
        let events = SHARED_RT
            .block_on(store.get_events("cli-sync", 0))
            .expect("events should read");
        assert_eq!(messages.len(), 2);
        assert_eq!(events.len(), 2);

        session.messages.truncate(1);
        sync_cli_session_to_unified_store(&store, &handle, Some("test-model"), &session)
            .expect("second sync should replace store view");
        let messages = SHARED_RT
            .block_on(store.get_all_messages("cli-sync"))
            .expect("messages should read");
        let events = SHARED_RT
            .block_on(store.get_events("cli-sync", 0))
            .expect("events should read");
        let record = SHARED_RT
            .block_on(store.get_session("cli-sync"))
            .expect("record should read")
            .expect("record should exist");

        assert_eq!(record.message_count, 1);
        let metadata = record
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .expect("record metadata should be valid json");
        let expected_session_path = handle.path.display().to_string();
        assert_eq!(
            metadata
                .get("session_path")
                .and_then(|value| value.as_str()),
            Some(expected_session_path.as_str())
        );
        assert!(
            metadata.get("legacy_path").is_none(),
            "runtime sync metadata should not describe DB-backed sessions as legacy paths"
        );
        assert_eq!(record.chat_id, "cli-sync");
        assert_eq!(messages.len(), 1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 0);

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
    fn help_mentions_db_resume_and_explicit_import() {
        let mut help = Vec::new();
        print_help_to(&mut help).expect("help should render");
        let help = String::from_utf8(help).expect("help should be utf8");
        assert!(help.contains("cowd --resume [session-id|latest]"));
        assert!(help.contains("cowd import-session PATH"));
        assert!(help.contains("Use `latest` with --resume, /resume, or /session switch"));
        assert!(help.contains("cowd --resume latest"));
        assert!(!help.contains("cowd --resume latest /status"));
    }

    #[test]
    fn managed_sessions_default_to_sqlite_and_detect_legacy_imports() {
        let _guard = cwd_lock().lock().unwrap_or_else(|e| e.into_inner());
        let config_home_original = std::env::var("COWD_CONFIG_HOME").ok();
        let workspace = temp_workspace("session-resolution");
        let config_home = temp_workspace("session-resolution-config");
        std::fs::create_dir_all(&workspace).expect("workspace should create");
        std::fs::create_dir_all(&config_home).expect("config home should create");
        let previous = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&workspace).expect("switch cwd");
        std::env::set_var("COWD_CONFIG_HOME", &config_home);

        let legacy_root = jsonl_sessions_dir().join("global");
        std::fs::create_dir_all(&legacy_root).expect("legacy root should create");
        let legacy_path = legacy_root.join("legacy.json");
        Session::new()
            .with_workspace_root(workspace.clone())
            .with_persistence_path(legacy_path.clone())
            .save_to_path(&legacy_path)
            .expect("legacy session should save");

        let candidates = discover_local_session_import_candidates();
        assert!(candidates
            .iter()
            .any(|candidate| candidate.path == legacy_path));
        assert!(
            resolve_session_reference("legacy").is_err(),
            "legacy files must not resolve until explicitly imported"
        );

        let store = memory::UnifiedSessionStore::open_in_memory().expect("store should open");
        let (imported_id, _messages) =
            import_local_session_file(&store, &legacy_path).expect("legacy import should succeed");
        assert!(!imported_id.is_empty());
        assert!(SHARED_RT
            .block_on(store.get_session(&imported_id))
            .expect("session lookup should succeed")
            .is_some());

        std::env::set_current_dir(previous).expect("restore cwd");
        std::fs::remove_dir_all(workspace).expect("workspace should clean up");
        std::fs::remove_dir_all(config_home).expect("config home should clean up");
        if let Some(v) = config_home_original {
            std::env::set_var("COWD_CONFIG_HOME", v);
        } else {
            std::env::remove_var("COWD_CONFIG_HOME");
        }
    }

    #[test]
    #[serial_test::serial(provider_registry)]
    fn tui_sidebar_switch_replaces_live_runtime_session() {
        let _cwd_guard = cwd_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _env_guard = env_lock();
        let config_home_original = std::env::var("COWD_CONFIG_HOME").ok();
        let api_key_original = std::env::var("ANTHROPIC_API_KEY").ok();
        let workspace = temp_workspace("tui-switch-runtime");
        let config_home = temp_workspace("tui-switch-config");
        let previous = std::env::current_dir().expect("cwd");
        std::fs::create_dir_all(&workspace).expect("workspace should create");
        std::fs::create_dir_all(&config_home).expect("config home should create");
        std::env::set_current_dir(&workspace).expect("switch cwd");
        std::env::set_var("COWD_CONFIG_HOME", &config_home);
        std::env::set_var("ANTHROPIC_API_KEY", "test-dummy-key-for-tui-switch");
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: claude-sonnet-4-6
providers:
  test-anthropic:
    base_url: http://127.0.0.1:9
    api_key: test-dummy-key-for-tui-switch
    protocol: anthropic
    models:
      - claude-sonnet-4-6
"#,
        )
        .expect("test provider config should write");
        runtime::init_global_providers(runtime::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "test-anthropic".to_string(),
                runtime::ProviderConfig {
                    name: "test-anthropic".to_string(),
                    base_url: "http://127.0.0.1:9".to_string(),
                    api_key: "test-dummy-key-for-tui-switch".to_string(),
                    models: vec!["claude-sonnet-4-6".to_string()],
                    protocol: Some("anthropic".to_string()),
                },
            )]),
        });

        let mut cli = LiveCli::new(
            "claude-sonnet-4-6".to_string(),
            None,
            true,
            None,
            PermissionMode::DangerFullAccess,
            false,
        )
        .expect("cli should initialize");
        let original_id = cli.runtime.session().session_id.clone();

        let target_handle =
            create_managed_session_handle("session-target").expect("target handle should create");
        let target_session = Session::new().with_workspace_root(workspace.clone());
        let target_session_id = target_session.session_id.clone();
        let store = get_unified_store().expect("store should open");
        sync_cli_session_to_unified_store(
            store,
            &target_handle,
            Some("claude-sonnet-4-6"),
            &target_session,
        )
        .expect("target session should sync");

        let report = crate::switch_live_cli_session(&mut cli, &target_session_id)
            .expect("switch should succeed");

        assert_ne!(original_id, report.session_id);
        assert_eq!(report.session_id, target_session_id);
        assert_eq!(cli.session.id, target_session_id);
        assert_eq!(cli.runtime.session().session_id, target_session_id);
        assert!(cli.session.path.ends_with("sessions.db"));

        std::env::remove_var("COWD_CONFIG_HOME");
        if let Some(v) = config_home_original {
            std::env::set_var("COWD_CONFIG_HOME", v);
        }
        std::env::remove_var("ANTHROPIC_API_KEY");
        if let Some(v) = api_key_original {
            std::env::set_var("ANTHROPIC_API_KEY", v);
        }
        std::env::set_current_dir(previous).expect("restore cwd");
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_home);
    }

    #[test]
    fn latest_session_alias_resolves_most_recent_managed_session() {
        let _guard = cwd_lock().lock().unwrap_or_else(|e| e.into_inner());
        let config_home_original = std::env::var("COWD_CONFIG_HOME").ok();
        let workspace = temp_workspace("latest-session-alias");
        let config_home = temp_workspace("latest-session-alias-config");
        std::env::set_var("COWD_CONFIG_HOME", &config_home);
        std::fs::create_dir_all(&workspace).expect("workspace should create");
        let previous = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&workspace).expect("switch cwd");

        let _older = create_managed_session_handle("session-older").expect("older handle");
        std::thread::sleep(Duration::from_millis(20));
        let newer = create_managed_session_handle("session-newer").expect("newer handle");

        let resolved = resolve_session_reference("latest").expect("latest session should resolve");
        assert_eq!(resolved.id, newer.id);

        std::env::set_current_dir(previous).expect("restore cwd");
        std::fs::remove_dir_all(workspace).expect("workspace should clean up");
        std::fs::remove_dir_all(config_home).expect("config home should clean up");
        if let Some(v) = config_home_original {
            std::env::set_var("COWD_CONFIG_HOME", v);
        } else {
            std::env::remove_var("COWD_CONFIG_HOME");
        }
    }

    #[test]
    fn load_session_reference_rejects_unimported_local_session_file() {
        let _guard = cwd_lock().lock().unwrap_or_else(|e| e.into_inner());
        let workspace_a = temp_workspace("session-mismatch-a");
        let workspace_b = temp_workspace("session-mismatch-b");
        std::fs::create_dir_all(&workspace_a).expect("workspace a should create");
        std::fs::create_dir_all(&workspace_b).expect("workspace b should create");
        let previous = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&workspace_b).expect("switch cwd");

        let session_path = workspace_a.join(".cowd/sessions/legacy-cross.jsonl");
        std::fs::create_dir_all(
            session_path
                .parent()
                .expect("session path should have parent directory"),
        )
        .expect("session dir should exist");
        Session::new()
            .with_workspace_root(workspace_a.clone())
            .with_persistence_path(session_path.clone())
            .save_to_path(&session_path)
            .expect("session should save");

        let error = crate::load_session_reference(&session_path.display().to_string())
            .expect_err("unimported local session file should fail");
        assert!(
            error
                .to_string()
                .contains("local session file is not imported"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("Import it explicitly before resume"),
            "expected import guidance in error: {error}"
        );

        std::env::set_current_dir(previous).expect("restore cwd");
        std::fs::remove_dir_all(workspace_a).expect("workspace a should clean up");
        std::fs::remove_dir_all(workspace_b).expect("workspace b should clean up");
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

    #[test]
    fn resume_usage_mentions_latest_shortcut() {
        let usage = render_resume_usage();
        assert!(usage.contains("/resume <session-id|latest>"));
        assert!(usage.contains("SQLite session store"));
        assert!(usage.contains("cowd import-session <local.jsonl>"));
        assert!(usage.contains("/session list"));
    }

    fn cwd_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn temp_workspace(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("cowd-cli-{label}-{nanos}"))
    }

    #[test]
    fn init_template_mentions_detected_rust_workspace() {
        let _guard = cwd_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let rendered = crate::init::render_init_claude_md(&workspace_root);
        assert!(rendered.contains("# CLAUDE.md"));
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
    fn repl_help_mentions_history_completion_and_multiline() {
        let help = render_repl_help();
        assert!(help.contains("Up/Down"));
        assert!(help.contains("Tab"));
        assert!(help.contains("Shift+Enter/Ctrl+J"));
        assert!(help.contains("Ctrl-R"));
        assert!(help.contains("Reverse-search prompt history"));
        assert!(help.contains("/history [count]"));
        assert!(help.contains("/tasks [start|cancel|complete]"));
        assert!(help.contains("/approvals [approve|reject]"));
        assert!(help.contains("/context [runtime|config|memory|cross-plane]"));
        assert!(help.contains("/cross-plane [preflight|execute] <json>"));
    }

    #[test]
    fn parse_history_count_defaults_to_twenty_when_missing() {
        // given
        let raw: Option<&str> = None;

        // when
        let parsed = parse_history_count(raw);

        // then
        assert_eq!(parsed, Ok(20));
    }

    #[test]
    fn parse_history_count_accepts_positive_integers() {
        // given
        let raw = Some("25");

        // when
        let parsed = parse_history_count(raw);

        // then
        assert_eq!(parsed, Ok(25));
    }

    #[test]
    fn parse_history_count_rejects_zero() {
        // given
        let raw = Some("0");

        // when
        let parsed = parse_history_count(raw);

        // then
        assert!(parsed.is_err());
        assert!(parsed.unwrap_err().contains("greater than 0"));
    }

    #[test]
    fn parse_history_count_rejects_non_numeric() {
        // given
        let raw = Some("abc");

        // when
        let parsed = parse_history_count(raw);

        // then
        assert!(parsed.is_err());
        assert!(parsed.unwrap_err().contains("invalid count 'abc'"));
    }

    #[test]
    fn format_history_timestamp_renders_iso8601_utc() {
        // given
        // 2023-01-15T12:34:56.789Z -> 1673786096789 ms
        let timestamp_ms: u64 = 1_673_786_096_789;

        // when
        let formatted = format_history_timestamp(timestamp_ms);

        // then
        assert_eq!(formatted, "2023-01-15T12:34:56.789Z");
    }

    #[test]
    fn format_history_timestamp_renders_unix_epoch_origin() {
        // given
        let timestamp_ms: u64 = 0;

        // when
        let formatted = format_history_timestamp(timestamp_ms);

        // then
        assert_eq!(formatted, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn render_prompt_history_report_lists_entries_with_timestamps() {
        // given
        let entries = vec![
            PromptHistoryEntry {
                timestamp_ms: 1_673_786_096_000,
                text: "first prompt".to_string(),
            },
            PromptHistoryEntry {
                timestamp_ms: 1_673_786_100_000,
                text: "second prompt".to_string(),
            },
        ];

        // when
        let rendered = render_prompt_history_report(&entries, 10);

        // then
        assert!(rendered.contains("Prompt history"));
        assert!(rendered.contains("Total            2"));
        assert!(rendered.contains("Showing          2 most recent"));
        assert!(rendered.contains("Reverse search   Ctrl-R in the REPL"));
        assert!(rendered.contains("2023-01-15T12:34:56.000Z"));
        assert!(rendered.contains("first prompt"));
        assert!(rendered.contains("second prompt"));
    }

    #[test]
    fn render_prompt_history_report_truncates_to_limit_from_the_tail() {
        // given
        let entries = vec![
            PromptHistoryEntry {
                timestamp_ms: 1_000,
                text: "older".to_string(),
            },
            PromptHistoryEntry {
                timestamp_ms: 2_000,
                text: "middle".to_string(),
            },
            PromptHistoryEntry {
                timestamp_ms: 3_000,
                text: "latest".to_string(),
            },
        ];

        // when
        let rendered = render_prompt_history_report(&entries, 2);

        // then
        assert!(rendered.contains("Total            3"));
        assert!(rendered.contains("Showing          2 most recent"));
        assert!(!rendered.contains("older"));
        assert!(rendered.contains("middle"));
        assert!(rendered.contains("latest"));
    }

    #[test]
    fn render_prompt_history_report_handles_empty_history() {
        // given
        let entries: Vec<PromptHistoryEntry> = Vec::new();

        // when
        let rendered = render_prompt_history_report(&entries, 10);

        // then
        assert!(rendered.contains("no prompts recorded yet"));
    }

    #[test]
    fn collect_session_prompt_history_extracts_user_text_blocks() {
        // given
        let mut session = Session::new();
        session.push_user_text("hello").unwrap();
        session.push_user_text("world").unwrap();

        // when
        let entries = collect_session_prompt_history(&session);

        // then
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].text, "hello");
        assert_eq!(entries[1].text, "world");
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
        assert!(rendered.contains('\u{1b}'));
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
    fn response_to_events_renders_collapsed_thinking_summary() {
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

        // Thinking block produces ThinkingDelta before the text event.
        assert!(
            matches!(
                &events[0],
                AssistantEvent::ThinkingDelta(thinking) if thinking == "step 1"
            ),
            "first event should be ThinkingDelta with the reasoning content"
        );
        assert!(
            matches!(
                &events[1],
                AssistantEvent::TextDelta(text) if text == "Final answer"
            ),
            "second event should be TextDelta with the visible response"
        );
        let rendered = String::from_utf8(out).expect("utf8");
        assert!(rendered.contains("▶ Thinking (6 chars hidden)"));
        // The thinking content is passed to events but NOT leaked into the rendered output.
        assert!(!rendered.contains("step 1"));
    }

    #[ignore]
    #[test]
    fn build_runtime_plugin_state_merges_plugin_hooks_into_runtime_features() {
        let config_home = temp_dir();
        let workspace = temp_dir();
        let source_root = temp_dir();
        fs::create_dir_all(&config_home).expect("config home");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&source_root).expect("source root");
        write_plugin_fixture(&source_root, "hook-runtime-demo", true, false);

        let mut manager = PluginManager::new(PluginManagerConfig::new(&config_home));
        manager
            .install(source_root.to_str().expect("utf8 source path"))
            .expect("plugin install should succeed");
        let loader = ConfigLoader::new(&workspace, &config_home);
        let runtime_config = loader.load().expect("runtime config should load");
        let state = build_runtime_plugin_state_with_loader(&workspace, &loader, &runtime_config)
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
    fn build_runtime_plugin_state_discovers_mcp_tools_and_surfaces_pending_servers() {
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
        let state = build_runtime_plugin_state_with_loader(&workspace, &loader, &runtime_config)
            .expect("runtime plugin state should load");

        let allowed = state
            .tool_registry
            .normalize_allowed_tools(&["mcp__alpha__echo".to_string(), "MCPTool".to_string()])
            .expect("mcp tools should be allow-listable")
            .expect("allow-list should exist");
        assert!(allowed.contains("mcp__alpha__echo"));
        assert!(allowed.contains("MCPTool"));

        let executor = CliToolExecutor::new(
            None,
            false,
            state.tool_registry.clone(),
            state.mcp_state.clone(),
        );

        let tool_output = executor
            .execute("mcp__alpha__echo", r#"{"text":"hello"}"#)
            .expect("discovered mcp tool should execute");
        let tool_json: serde_json::Value =
            serde_json::from_str(&tool_output).expect("tool output should be json");
        assert_eq!(tool_json["structuredContent"]["echoed"], "hello");

        let wrapped_output = executor
            .execute(
                "MCPTool",
                r#"{"qualifiedName":"mcp__alpha__echo","arguments":{"text":"wrapped"}}"#,
            )
            .expect("generic mcp wrapper should execute");
        let wrapped_json: serde_json::Value =
            serde_json::from_str(&wrapped_output).expect("wrapped output should be json");
        assert_eq!(wrapped_json["structuredContent"]["echoed"], "wrapped");

        let search_output = executor
            .execute("ToolSearch", r#"{"query":"alpha echo","max_results":5}"#)
            .expect("tool search should execute");
        let search_json: serde_json::Value =
            serde_json::from_str(&search_output).expect("search output should be json");
        assert_eq!(search_json["matches"][0], "mcp__alpha__echo");
        assert_eq!(search_json["pending_mcp_servers"][0], "broken");
        assert_eq!(
            search_json["mcp_degraded"]["failed_servers"][0]["server_name"],
            "broken"
        );
        assert_eq!(
            search_json["mcp_degraded"]["failed_servers"][0]["phase"],
            "tool_discovery"
        );
        assert_eq!(
            search_json["mcp_degraded"]["available_tools"][0],
            "mcp__alpha__echo"
        );

        let listed = executor
            .execute("ListMcpResourcesTool", r#"{"server":"alpha"}"#)
            .expect("resources should list");
        let listed_json: serde_json::Value =
            serde_json::from_str(&listed).expect("resource output should be json");
        assert_eq!(listed_json["resources"][0]["uri"], "file://guide.txt");

        let read = executor
            .execute(
                "ReadMcpResourceTool",
                r#"{"server":"alpha","uri":"file://guide.txt"}"#,
            )
            .expect("resource should read");
        let read_json: serde_json::Value =
            serde_json::from_str(&read).expect("resource read output should be json");
        assert_eq!(
            read_json["contents"][0]["text"],
            "contents for file://guide.txt"
        );

        if let Some(mcp_state) = state.mcp_state {
            mcp_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shutdown()
                .expect("mcp shutdown should succeed");
        }

        let _ = fs::remove_dir_all(config_home);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn build_runtime_plugin_state_surfaces_unsupported_mcp_servers_structurally() {
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
        let state = build_runtime_plugin_state_with_loader(&workspace, &loader, &runtime_config)
            .expect("runtime plugin state should load");
        let executor = CliToolExecutor::new(
            None,
            false,
            state.tool_registry.clone(),
            state.mcp_state.clone(),
        );

        let search_output = executor
            .execute("ToolSearch", r#"{"query":"remote","max_results":5}"#)
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

    #[ignore]
    #[test]
    fn build_runtime_runs_plugin_lifecycle_init_and_shutdown() {
        // Serialize access to process-wide env vars so parallel tests that
        // set/remove ANTHROPIC_API_KEY do not race with this test.
        let _guard = env_lock();
        let config_home = temp_dir();
        // Inject a dummy API key so runtime construction succeeds without real credentials.
        // This test only exercises plugin lifecycle (init/shutdown), never calls the API.
        std::env::set_var("ANTHROPIC_API_KEY", "test-dummy-key-for-plugin-lifecycle");
        let workspace = temp_dir();
        let source_root = temp_dir();
        fs::create_dir_all(&config_home).expect("config home");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&source_root).expect("source root");
        write_plugin_fixture(&source_root, "lifecycle-runtime-demo", false, true);

        let mut manager = PluginManager::new(PluginManagerConfig::new(&config_home));
        let install = manager
            .install(source_root.to_str().expect("utf8 source path"))
            .expect("plugin install should succeed");
        let log_path = install.install_path.join("lifecycle.log");
        let loader = ConfigLoader::new(&workspace, &config_home);
        let runtime_config = loader.load().expect("runtime config should load");
        let runtime_plugin_state =
            build_runtime_plugin_state_with_loader(&workspace, &loader, &runtime_config)
                .expect("plugin state should load");
        let mut runtime = build_runtime_with_plugin_state(
            None,
            Session::new(),
            "runtime-plugin-lifecycle",
            DEFAULT_MODEL.to_string(),
            vec!["test system prompt".to_string()],
            true,
            false,
            None,
            PermissionMode::DangerFullAccess,
            None,
            None,
            runtime_plugin_state,
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
        std::env::remove_var("ANTHROPIC_API_KEY");
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
        for value in ["low", "medium", "high"] {
            let result = parse_args(&["--reasoning-effort".to_string(), value.to_string()]);
            assert!(
                result.is_ok(),
                "--reasoning-effort {value} should be accepted, got: {result:?}"
            );
            if let Ok(CliAction::Repl {
                reasoning_effort, ..
            }) = result
            {
                assert_eq!(reasoning_effort.as_deref(), Some(value));
            }
        }
    }

    #[test]
    fn stub_commands_absent_from_repl_completions() {
        let candidates =
            slash_command_completion_candidates_with_sessions("claude-3-5-sonnet", None, vec![]);
        for stub in STUB_COMMANDS {
            let with_slash = format!("/{stub}");
            assert!(
                !candidates.contains(&with_slash),
                "stub command {with_slash} should not appear in REPL completions"
            );
        }
    }
}

fn write_mcp_server_fixture(script_path: &Path) {
    let script = [
            "#!/usr/bin/env python3",
            "import json, sys",
            "",
            "def read_message():",
            "    header = b''",
            r"    while not header.endswith(b'\r\n\r\n'):",
            "        chunk = sys.stdin.buffer.read(1)",
            "        if not chunk:",
            "            return None",
            "        header += chunk",
            "    length = 0",
            r"    for line in header.decode().split('\r\n'):",
            r"        if line.lower().startswith('content-length:'):",
            "            length = int(line.split(':', 1)[1].strip())",
            "    payload = sys.stdin.buffer.read(length)",
            "    return json.loads(payload.decode())",
            "",
            "def send_message(message):",
            "    payload = json.dumps(message).encode()",
            r"    sys.stdout.buffer.write(f'Content-Length: {len(payload)}\r\n\r\n'.encode() + payload)",
            "    sys.stdout.buffer.flush()",
            "",
            "while True:",
            "    request = read_message()",
            "    if request is None:",
            "        break",
            "    method = request['method']",
            "    if method == 'initialize':",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'protocolVersion': request['params']['protocolVersion'],",
            "                'capabilities': {'tools': {}, 'resources': {}},",
            "                'serverInfo': {'name': 'fixture', 'version': '1.0.0'}",
            "            }",
            "        })",
            "    elif method == 'tools/list':",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'tools': [",
            "                    {",
            "                        'name': 'echo',",
            "                        'description': 'Echo from MCP fixture',",
            "                        'inputSchema': {",
            "                            'type': 'object',",
            "                            'properties': {'text': {'type': 'string'}},",
            "                            'required': ['text'],",
            "                            'additionalProperties': False",
            "                        },",
            "                        'annotations': {'readOnlyHint': True}",
            "                    }",
            "                ]",
            "            }",
            "        })",
            "    elif method == 'tools/call':",
            "        args = request['params'].get('arguments') or {}",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'content': [{'type': 'text', 'text': f\"echo:{args.get('text', '')}\"}],",
            "                'structuredContent': {'echoed': args.get('text', '')},",
            "                'isError': False",
            "            }",
            "        })",
            "    elif method == 'resources/list':",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'resources': [{'uri': 'file://guide.txt', 'name': 'guide', 'mimeType': 'text/plain'}]",
            "            }",
            "        })",
            "    elif method == 'resources/read':",
            "        uri = request['params']['uri']",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'contents': [{'uri': uri, 'mimeType': 'text/plain', 'text': f'contents for {uri}'}]",
            "            }",
            "        })",
            "    else:",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'error': {'code': -32601, 'message': method}",
            "        })",
            "",
        ]
        .join("\n");
    fs::write(script_path, script).expect("mcp fixture script should write");
}

#[cfg(test)]
mod sandbox_report_tests {
    #![allow(unused_imports)]
    use super::{format_sandbox_report, HookAbortMonitor};
    use runtime::HookAbortSignal;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn sandbox_report_renders_expected_fields() {
        let report = format_sandbox_report(&runtime::SandboxStatus::default());
        assert!(report.contains("Sandbox"));
        assert!(report.contains("Enabled"));
        assert!(report.contains("Filesystem mode"));
        assert!(report.contains("Fallback reason"));
    }

    #[test]
    fn hook_abort_monitor_stops_without_aborting() {
        let abort_signal = HookAbortSignal::new();
        let (ready_tx, ready_rx) = mpsc::channel();
        let monitor = HookAbortMonitor::spawn_with_waiter(
            abort_signal.clone(),
            move |stop_rx, abort_signal| {
                ready_tx.send(()).expect("ready signal");
                let _ = stop_rx.recv();
                assert!(!abort_signal.is_aborted());
            },
        );

        ready_rx.recv().expect("waiter should be ready");
        monitor.stop();

        assert!(!abort_signal.is_aborted());
    }

    #[test]
    fn hook_abort_monitor_propagates_interrupt() {
        let abort_signal = HookAbortSignal::new();
        let (done_tx, done_rx) = mpsc::channel();
        let monitor = HookAbortMonitor::spawn_with_waiter(
            abort_signal.clone(),
            move |_stop_rx, abort_signal| {
                abort_signal.abort();
                done_tx.send(()).expect("done signal");
            },
        );

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("interrupt should complete");
        monitor.stop();

        assert!(abort_signal.is_aborted());
    }
}

#[cfg(test)]
mod dump_manifests_tests {
    #![allow(unused_imports)]
    use super::{dump_manifests_at_path, CliOutputFormat};
    use std::fs;

    #[test]
    fn dump_manifests_shows_helpful_error_when_manifests_missing() {
        let root = std::env::temp_dir().join(format!(
            "cowd_test_missing_manifests_{}",
            std::process::id()
        ));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).expect("failed to create temp workspace");

        let result = dump_manifests_at_path(&workspace, None, CliOutputFormat::Text);
        assert!(
            result.is_err(),
            "expected an error when manifests are missing"
        );

        let error_msg = result.unwrap_err().to_string();

        assert!(
            error_msg.contains("Manifest source files are missing"),
            "error message should mention missing manifest sources: {error_msg}"
        );
        assert!(
            error_msg.contains(&root.display().to_string()),
            "error message should contain the resolved repo root path: {error_msg}"
        );
        assert!(
            error_msg.contains("src/commands.ts"),
            "error message should mention missing commands.ts: {error_msg}"
        );
        assert!(
            error_msg.contains("COWD_UPSTREAM"),
            "error message should explain how to supply the upstream path: {error_msg}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn dump_manifests_uses_explicit_manifest_dir() {
        let root = std::env::temp_dir().join(format!(
            "cowd_test_explicit_manifest_dir_{}",
            std::process::id()
        ));
        let workspace = root.join("workspace");
        let upstream = root.join("upstream");
        fs::create_dir_all(workspace.join("nested")).expect("workspace should exist");
        fs::create_dir_all(upstream.join("src/entrypoints"))
            .expect("upstream fixture should exist");
        fs::write(
            upstream.join("src/commands.ts"),
            "import FooCommand from './commands/foo'\n",
        )
        .expect("commands fixture should write");
        fs::write(
            upstream.join("src/tools.ts"),
            "import ReadTool from './tools/read'\n",
        )
        .expect("tools fixture should write");
        fs::write(
            upstream.join("src/entrypoints/cli.tsx"),
            "startupProfiler()\n",
        )
        .expect("cli fixture should write");

        let result = dump_manifests_at_path(&workspace, Some(&upstream), CliOutputFormat::Text);
        assert!(
            result.is_ok(),
            "explicit manifest dir should succeed: {result:?}"
        );

        let _ = fs::remove_dir_all(&root);
    }
}

#[cfg(test)]
mod skill_pipeline_tests {
    #![allow(unused_imports)]
    use super::tui::{App, SkillSummary};
    use std::path::PathBuf;

    #[test]
    fn skill_summary_fields_populate_correctly() {
        let s = SkillSummary {
            name: "test-skill".to_string(),
            description: "A test skill".to_string(),
            installed: true,
            category: "local".to_string(),
            source: "ProjectCowd".to_string(),
            status: "ready".to_string(),
            risk: "operator_review".to_string(),
            tags: vec!["test".to_string()],
        };
        assert_eq!(s.name, "test-skill");
        assert_eq!(s.description, "A test skill");
        assert!(s.installed);
        assert_eq!(s.status, "ready");
    }

    #[test]
    fn app_skill_list_initializes_empty() {
        let app = App::new("test-model", "test-session");
        assert!(app.skill_list.is_empty());
    }

    #[test]
    fn skill_scan_from_temp_dir() {
        let tmp = std::env::temp_dir().join(format!("cowd_skill_test_{}", std::process::id()));
        let skill_dir = tmp.join("my-skill");
        std::fs::create_dir_all(&skill_dir).expect("create skill dir");
        std::fs::write(skill_dir.join("SKILL.md"), "# My Skill\nDoes things")
            .expect("write SKILL.md");

        let mut app = App::new("test-model", "test-session");
        if let Ok(entries) = std::fs::read_dir(&tmp) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.join("SKILL.md").exists() {
                    if let Ok(content) = std::fs::read_to_string(path.join("SKILL.md")) {
                        let name = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let desc = content
                            .lines()
                            .filter(|l| !l.trim().is_empty() && !l.trim().starts_with("---"))
                            .next()
                            .map(|l| l.trim().to_string())
                            .unwrap_or_default();
                        app.skill_list.push(SkillSummary {
                            name,
                            description: desc,
                            installed: false,
                            category: "local".to_string(),
                            source: "fixture".to_string(),
                            status: "ready".to_string(),
                            risk: "operator_review".to_string(),
                            tags: Vec::new(),
                        });
                    }
                }
            }
        }
        assert_eq!(app.skill_list.len(), 1);
        assert_eq!(app.skill_list[0].name, "my-skill");
        assert_eq!(app.skill_list[0].description, "# My Skill");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
