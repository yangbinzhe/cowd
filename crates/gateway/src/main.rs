#![recursion_limit = "256"]
#![allow(
    clippy::unneeded_struct_pattern,
    clippy::unnecessary_wraps,
    clippy::unused_self,
    dead_code
)]
#![deny(deprecated)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
mod api_routes;
#[path = "core/bootstrap.rs"]
mod bootstrap;
#[path = "core/boundary_policy.rs"]
mod boundary_policy;
#[path = "core/checks.rs"]
mod checks;
mod cli;
mod command;
#[path = "core/compat_manifest.rs"]
mod compat_manifest;
#[path = "core/doctor.rs"]
mod doctor;
mod entry;
#[path = "core/event_bus.rs"]
mod event_bus;
#[path = "core/gateway.rs"]
mod gateway;
#[path = "infrastructure/capacity.rs"]
mod gateway_capacity;
#[path = "infrastructure/gateway_health.rs"]
mod gateway_health;
#[path = "core/gateway_service.rs"]
mod gateway_service;
#[path = "infrastructure/gateway_static.rs"]
mod gateway_static;
#[path = "runtime/gateway_tool_executor.rs"]
mod gateway_tool_executor;
#[path = "core/init.rs"]
mod init;
#[path = "runtime/lark_cli_tool.rs"]
mod lark_cli_tool;
#[path = "core/logging.rs"]
mod logging;
#[path = "infrastructure/matrix_store.rs"]
mod matrix_store;
#[path = "runtime/mcp_serve.rs"]
mod mcp_serve;
#[path = "static/plugin_static.rs"]
mod plugin_static;
#[path = "runtime/runtime_bootstrap.rs"]
mod runtime_bootstrap;
#[path = "runtime/runtime_boundary.rs"]
mod runtime_boundary;
#[path = "runtime/runtime_entry.rs"]
mod runtime_entry;
#[path = "runtime/runtime_factory.rs"]
mod runtime_factory;
mod runtime_host;
#[path = "runtime/runtime_protocol.rs"]
mod runtime_protocol;
#[path = "runtime/runtime_service.rs"]
mod runtime_service;
#[path = "infrastructure/selected_storage.rs"]
mod selected_storage;
mod server;
mod services;
#[path = "runtime/session_runtime_bridge.rs"]
mod session_runtime_bridge;
#[path = "runtime/session_runtime_data_port.rs"]
mod session_runtime_data_port;
#[path = "static/skill_static.rs"]
mod skill_static;
#[path = "infrastructure/storage_cutover.rs"]
mod storage_cutover;
#[path = "core/suggestions.rs"]
mod suggestions;
mod surface_host;

pub use boundary_policy::{GatewayBoundaryPolicy, GatewayResponsibility};

/// Operator-only storage cutover entry used by the thin CLI binary.
pub fn storage_entry(args: &[String]) -> std::process::ExitCode {
    match storage_cutover::run(args) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("storage command failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Hidden process boundary for a Gateway-owned harness evaluation.
///
/// The public CLI dispatches this role before normal command parsing. Running
/// model/tool evaluation in a process gives Gateway a real cancellation and
/// reap boundary even when a provider or native library blocks.
#[doc(hidden)]
pub fn harness_eval_worker_entry(args: &[String]) -> std::process::ExitCode {
    services::harness_eval_service::worker_process_entry(args)
}

/// Feature-gated black-box integration harness. This intentionally exposes
/// only a fully assembled API router, never Gateway internals or mutation
/// shortcuts.
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::api_routes::test_support::GatewayTestHarness;
}

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{self, IsTerminal, Write};
#[cfg(test)]
use std::os::unix::io::FromRawFd;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{LazyLock, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[cfg(test)]
use provider as provider_crate;
#[cfg(test)]
use provider_crate::{
    resolve_startup_auth_source, AuthSource, ImageSource, InputContentBlock, InputMessage,
    MessageResponse, OutputContentBlock, ToolResultContentBlock,
};

#[cfg(test)]
use crate::command::slash::is_executable_slash_command;
use crate::command::slash::{
    render_slash_command_help_filtered, slash_command_specs, NON_EXECUTABLE_SLASH_COMMANDS,
};
#[cfg(test)]
use compat_manifest::{extract_manifest, UpstreamPaths};
use runtime::ContextProfile;
#[cfg(test)]
use runtime::ResolvedPermissionMode;
use runtime::{
    check_base_commit, format_stale_base_warning, load_system_prompt, resolve_expected_base,
    resolve_sandbox_status, ContentBlock, ConversationMessage, MessageRole, PermissionMode,
    PermissionPolicy, ResumeContextPacket, ResumeContextSource, Session,
};
#[cfg(test)]
use runtime::{AssistantEvent, RuntimeError};
use runtime_bootstrap::GatewayToolRegistry;
use runtime_entry::GatewayRuntimeEntry;
use serde_json::json;

#[cfg(test)]
static TEST_PROCESS_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(test)]
pub(crate) fn test_process_env_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_PROCESS_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) use entry::env_entry::resolve_model_alias_with_config;
#[cfg(test)]
pub(crate) use entry::env_entry::resolve_tui_model;
#[cfg(test)]
pub(crate) use entry::env_entry::{default_permission_mode, parse_permission_mode_arg};
#[cfg(test)]
pub(crate) use entry::local_command_entry::print_help_to;
#[cfg(test)]
pub(crate) use entry::local_command_entry::{
    format_bughunter_report, format_issue_report, format_pr_report, format_ultraplan_report,
};
#[cfg(test)]
pub(crate) use entry::skill_entry::try_resolve_bare_skill_prompt;
#[cfg(test)]
pub(crate) use entry::status_entry::parse_git_status_branch;
#[cfg(test)]
pub(crate) use entry::status_entry::parse_git_status_metadata_for;
#[cfg(test)]
pub(crate) use entry::status_entry::{format_sandbox_report, format_status_report, StatusUsage};
pub(crate) use entry::status_entry::{
    parse_git_status_metadata, parse_git_workspace_summary, status_context, GitWorkspaceSummary,
    StatusContext,
};
#[cfg(test)]
pub(crate) use entry::workspace_entry::{
    render_config_report, render_diff_report_for, render_memory_report, render_setup_json,
    render_setup_report,
};
#[cfg(test)]
pub(crate) use entry::workspace_entry::{render_diff_report, SetupItem, SetupSnapshot};
#[cfg(test)]
use gateway_tool_executor::GatewayToolExecutor;

pub(crate) const DEFAULT_MODEL_ALIAS: &str = "main";
/// Global list of gateway child processes that must be reaped.
/// Children are adopted (stored here) instead of dropping the handle,
/// which prevents zombie processes when the gateway process exits.
static DAEMON_CHILDREN: LazyLock<Mutex<Vec<Child>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Adopt a gateway child process — store it so the handle is not dropped.
/// Returns the child's PID.
fn adopt_gateway_child(child: Child) -> u32 {
    let pid = child.id();
    if let Ok(mut children) = DAEMON_CHILDREN.lock() {
        children.push(child);
    }
    pid
}

/// Keep gateway-child setup local to tracked child handles.
///
/// Do not install `SIGCHLD = SIG_IGN`: that makes unrelated tool subprocesses
/// impossible to `wait` reliably and breaks bash/tool execution in one-shot
/// runs. Zombie prevention is handled by retaining gateway process handles and calling
/// `reap_gateway_children`.
#[cfg(unix)]
fn setup_sigchld_handler() {
    tracing::debug!("gateway child reaping uses retained child handles");
}

/// Try to reap any exited gateway children. Called periodically.
fn reap_gateway_children() {
    if let Ok(mut children) = DAEMON_CHILDREN.lock() {
        children.retain_mut(|child| match child.try_wait() {
            Ok(Some(status)) => {
                tracing::debug!(
                    pid = child.id(),
                    code = status.code(),
                    "gateway child reaped"
                );
                false
            }
            Ok(None) => true, // still running
            Err(e) => {
                tracing::warn!(pid = child.id(), error = %e, "failed to wait on gateway child");
                false
            }
        });
    }
}

fn gateway_process_log_file() -> Result<std::fs::File, Box<dyn std::error::Error>> {
    let log_dir = runtime::cowd_dirs::config_home_dir().join("logs");
    std::fs::create_dir_all(&log_dir)?;
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("gateway.log"))?)
}

fn spawn_gateway_process(exe: &Path) -> Result<Child, Box<dyn std::error::Error>> {
    if cfg!(test) {
        return Err("gateway process spawn is disabled under the Rust test harness".into());
    }
    let stdout = gateway_process_log_file()?;
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
        .map_err(|e| format!("failed to start gateway process: {e}").into())
}

fn wait_for_gateway_start(
    child: &mut Child,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    // A production readiness snapshot includes bounded APP, storage and
    // connector probes.  The aggregate commonly exceeds 500 ms even though
    // the Gateway is healthy, so the per-request budget must cover one full
    // snapshot while the outer timeout remains the authoritative startup cap.
    let readiness_client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Some(status) = child.try_wait()? {
            return Err(format!("gateway process exited during startup: {status}").into());
        }
        if let Some(status) = server::get_server_status().map_err(|e| e.to_string())? {
            if status.pid == child.id() {
                let readiness_url = format!("{}/readyz", status.address.trim_end_matches('/'));
                if readiness_client
                    .get(readiness_url)
                    .send()
                    .is_ok_and(|response| response.status().is_success())
                {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    terminate_failed_gateway_start(child);
    Err("gateway process did not become ready before timeout".into())
}

fn terminate_failed_gateway_start(child: &mut Child) {
    #[cfg(unix)]
    {
        if let Ok(process_group) = i32::try_from(child.id()) {
            // The child is created as its own process group. Terminating the
            // group also cleans up startup helpers such as the auth broker.
            let _ = unsafe { libc::kill(-process_group, libc::SIGTERM) };
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while std::time::Instant::now() < deadline {
                if child.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

#[allow(
    clippy::expect_used,
    reason = "a process-wide runtime is required by synchronous CLI adapters; construction failure aborts startup before serving work"
)]
pub(crate) static SHARED_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(gateway_capacity::configured_runtime_workers())
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
pub(crate) const OFFICIAL_REPO_URL: &str = "https://github.com/ultraworkers/cowd";
pub(crate) const OFFICIAL_REPO_SLUG: &str = "ultraworkers/cowd";
pub(crate) const DEPRECATED_INSTALL_COMMAND: &str = "cargo install cowd";
pub(crate) const LATEST_SESSION_REFERENCE: &str = "latest";
const REMOVED_PROMPT_SUBCOMMAND: &str = "prompt";
const SESSION_REFERENCE_ALIASES: &[&str] = &[LATEST_SESSION_REFERENCE, "last", "recent"];

type AllowedToolSet = BTreeSet<String>;
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
    runtime_config: &runtime::RuntimeConfig,
    _cwd: &std::path::Path,
) -> Option<memory::MemoryConfig> {
    let src = runtime_config.memory();
    if !src.enabled {
        return None;
    }
    let config_home = runtime::cowd_dirs::config_home_dir();
    let (sqlite_path, blob_dir) =
        if let Some(store_path) = src.store_path.as_ref().map(|path| expand_home(path)) {
            if let Err(error) = std::fs::create_dir_all(&store_path) {
                tracing::warn!(?store_path, "failed to create memory store dir: {error}");
            }
            (store_path.join("memory.db"), store_path.join("blobs"))
        } else {
            let layout = storage::StorageLayout::default_for_config_home(&config_home);
            let sqlite_path = layout
                .sqlite_path("memory")
                .map(std::path::Path::to_path_buf)
                .unwrap_or_else(|| layout.root.join("memory.sqlite"));
            (sqlite_path, layout.blobs)
        };
    let mut mc = memory::MemoryConfig::default();
    mc.store.sqlite_path = sqlite_path;
    mc.store.blob_dir = blob_dir;
    if let Some(parent) = mc.store.sqlite_path.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            tracing::warn!(?parent, "failed to create memory sqlite dir: {error}");
        }
    }
    if let Err(error) = std::fs::create_dir_all(&mc.store.blob_dir) {
        tracing::warn!(path = %mc.store.blob_dir.display(), "failed to create memory blob dir: {error}");
    }
    mc.store.enable_vector_index = src.vector.enabled;
    mc.store.vector.enabled = src.vector.enabled;
    mc.store.vector.model = src.vector.model.clone();
    mc.store.vector.api_url = src.vector.api_url.clone();
    mc.store.vector.api_key = src.vector.api_key.clone();
    mc.store.vector.dimension = src.vector.dimension;
    mc.store.vector.timeout_secs = src.vector.timeout_secs;
    mc.store.vector.batch_size = src.vector.batch_size;
    if mc.store.vector.enabled
        && !mc.store.vector.model.trim().is_empty()
        && mc.store.vector.api_url.trim().is_empty()
    {
        if let Some(provider) = runtime_config
            .providers()
            .resolve_full(&mc.store.vector.model)
        {
            match model_protocol::provider_config::ProviderProtocol::effective_for_provider(
                provider,
            ) {
                Ok(model_protocol::provider_config::ProviderProtocol::Anthropic) => {
                    tracing::warn!(
                        model = %mc.store.vector.model,
                        "memory embeddings require an OpenAI-compatible provider; configure memory.vector.api_url explicitly"
                    );
                }
                Ok(_) if !provider.base_url.trim().is_empty() => {
                    mc.store.vector.api_url = embeddings_endpoint(&provider.base_url);
                    if mc.store.vector.api_key.trim().is_empty() {
                        mc.store.vector.api_key = provider.api_key.clone();
                    }
                }
                Ok(_) => tracing::warn!(
                    model = %mc.store.vector.model,
                    "memory embedding provider has no base URL"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    model = %mc.store.vector.model,
                    "memory embedding provider protocol is invalid"
                ),
            }
        } else {
            tracing::warn!(
                model = %mc.store.vector.model,
                "memory vector model is not declared by any configured provider"
            );
        }
    }
    mc.layers.l0_enabled = src.layers.l0_enabled;
    mc.layers.l1_max_tokens = src.layers.l1_max_tokens;
    mc.layers.l2_max_tokens = src.layers.l2_max_tokens;
    mc.layers.l3_search_limit = src.layers.l3_search_limit;
    mc.layers.l4_enabled = src.layers.l4_enabled;
    mc.extractor.enabled = src.extraction.auto_extract;
    mc.governance.enabled = src.governance.enabled;
    mc.governance.startup_delay_secs = src.governance.startup_delay_secs;
    mc.governance.deep_scan_hour_local = src.governance.deep_scan_hour_local;
    mc.governance.max_candidates = src.governance.max_candidates;
    mc.governance.stale_threshold_bp = src.governance.stale_threshold_bp;
    mc.governance.low_confidence_threshold_bp = src.governance.low_confidence_threshold_bp;
    mc.compression.enable_deep_compression = runtime_config.compression().deep.enabled;

    let explicit_llm = &runtime_config.compression().llm;
    if explicit_llm.is_configured() {
        mc.compression.llm.enabled = true;
        mc.compression.llm.model = explicit_llm.model.clone();
    } else if src.extraction.auto_extract {
        if let Some(model) = runtime_config.resolved_model() {
            mc.compression.llm.enabled = true;
            mc.compression.llm.model = model;
        }
    }
    Some(mc)
}

fn embeddings_endpoint(base_url: &str) -> String {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.ends_with("/embeddings") {
        base_url.to_string()
    } else {
        format!("{base_url}/embeddings")
    }
}

/// Convert `runtime::GatewayConfig` into external Edge message connector descriptors.
/// Filters out `api_server` because it is the gateway listener itself.
fn build_surface_configs(gw: &runtime::GatewayConfig) -> Vec<surface::SurfaceManifest> {
    if !gw.enabled {
        return Vec::new();
    }
    gw.platforms
        .iter()
        .filter(|p| p.enabled && p.platform_type != "api_server")
        .map(|p| {
            let id = surface::message::normalize_message_connector(&p.platform_type);
            let required = surface::message::message_connector_required_fields(&id)
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>();
            surface::SurfaceManifest {
                schema: surface::SURFACE_PROTOCOL.to_string(),
                id: id.clone(),
                name: format!("{id} message connector"),
                version: env!("CARGO_PKG_VERSION").to_string(),
                kind: surface::SurfaceKind::MessageConnector,
                runtime: Some(surface::SurfaceRuntimeSpec::Managed {
                    artifact: if id == "feishu" {
                        "cowd-edge-open-platform-message".to_string()
                    } else {
                        format!("cowd-edge-{id}-message")
                    },
                    driver_profile: format!("{id}-message"),
                    transport: surface::SurfaceTransport::UdsHttp2,
                    state: if id == "wechat-ilink" {
                        surface::SurfaceStateMode::Persistent
                    } else {
                        surface::SurfaceStateMode::Ephemeral
                    },
                }),
                capabilities: surface::message::message_connector_capabilities(&id)
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                routes: Vec::new(),
                resources: Vec::new(),
                health: surface::SurfaceHealthSpec {
                    mode: surface::SurfaceHealthMode::Jsonl,
                    interval_ms: 30_000,
                    ..Default::default()
                },
                config_schema: serde_json::json!({ "required": required }),
                default_enabled: p.enabled,
            }
        })
        .collect()
}

fn build_surface_runtime_configs(
    gw: &runtime::GatewayConfig,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    if !gw.enabled {
        return std::collections::BTreeMap::new();
    }
    gw.platforms
        .iter()
        .filter(|p| p.enabled && p.platform_type != "api_server")
        .map(|p| {
            // Lark 与 Feishu 共用同一个 open-platform artifact，运行配置
            // 必须使用与清单相同的 canonical surface id，否则 sidecar
            // 虽然可以被发现，却拿不到 app_id/app_secret。
            let id = surface::message::normalize_message_connector(&p.platform_type);
            let mut config = p
                .extra
                .iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "platformType" | "platform_type" | "type" | "enabled"
                    )
                })
                .map(|(key, value)| (key.clone(), json_value_to_serde(value)))
                .collect::<serde_json::Map<_, _>>();
            config.insert(
                "platform_type".to_string(),
                serde_json::Value::String(p.platform_type.clone()),
            );
            (id, serde_json::Value::Object(config))
        })
        .collect()
}

/// Convert `runtime::JsonValue` → `serde_json::Value`.
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

pub fn backend_entry() {
    exit_on_error(run_backend_entry())
}

#[allow(
    clippy::exit,
    reason = "top-level CLI boundary must preserve command exit status for scripts"
)]
fn exit_on_error(result: Result<(), Box<dyn std::error::Error>>) {
    if let Err(error) = result {
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

fn run_backend_entry() -> Result<(), Box<dyn std::error::Error>> {
    logging::init_logging(VERSION);
    setup_sigchld_handler();

    let args: Vec<String> = env::args().skip(1).collect();
    let (action, output_format) = parse_backend_args(&args)?;
    run_gateway_action(&action, output_format)
}

fn parse_backend_args(args: &[String]) -> Result<(GatewayAction, CliOutputFormat), String> {
    let Some((surface, rest)) = args.split_first() else {
        return Err("gateway backend entry requires `cowd gateway <action>`".to_string());
    };
    if surface != "gateway" {
        return Err("gateway backend entry only accepts `cowd gateway ...` commands".to_string());
    }

    let mut output_format = CliOutputFormat::Text;
    let mut action = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--output-format" => {
                let value = rest
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --output-format".to_string())?;
                output_format = CliOutputFormat::parse(value)?;
                index += 2;
            }
            value if value.starts_with("--output-format=") => {
                output_format = CliOutputFormat::parse(&value["--output-format=".len()..])?;
                index += 1;
            }
            value if action.is_none() => {
                action = GatewayAction::from_str(value);
                if action.is_none() {
                    return Err(format!("unknown gateway subcommand: {value}"));
                }
                index += 1;
            }
            value => return Err(format!("unexpected gateway argument: {value}")),
        }
    }
    let action = action.ok_or_else(|| {
        "gateway requires a subcommand: start, stop, restart, status, doctor, run, logs, repair, open, or wechat-qr"
            .to_string()
    })?;
    Ok((action, output_format))
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
            tracing::info!(binary = %exe.display(), "gateway start: spawning gateway process");
            let mut child = spawn_gateway_process(&exe)?;
            wait_for_gateway_start(&mut child, Duration::from_secs(30))?;
            let pid = adopt_gateway_child(child);
            println!("Gateway started (pid: {pid})");
            tracing::info!(pid, "gateway process spawned");
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
                            "discovery_warning": status.as_ref().and_then(|s| s.discovery_warning.as_deref()),
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
            let startup_cwd = std::env::current_dir()
                .map_err(|error| format!("failed to resolve Gateway startup directory: {error}"))?;
            let (workspace_root, runtime_config) =
                resolve_gateway_workspace_and_config(&startup_cwd)?;
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
            let memory_config = build_memory_config(&runtime_config, &workspace_root);
            let surface_configs = build_surface_configs(runtime_config.gateway());
            let surface_runtime_configs = build_surface_runtime_configs(runtime_config.gateway());
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
            let runtime_host_config = runtime_host::RuntimeHostConfig {
                http_addr: format!("{effective_host}:{effective_port}"),
                workspace_root,
                memory_config,
                surface_configs,
                surface_runtime_configs,
                runtime_config: runtime_config_json,
                session_recovery: runtime_config.gateway().recovery,
                webui_dir: runtime_config.gateway().webui_dir.clone(),
                cors_origins,
                auth_token,
            };
            let r2 = SHARED_RT.handle().clone();
            r2.block_on(async {
                runtime_host::run_gateway_runtime(runtime_host_config)
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
            let mut child = spawn_gateway_process(&exe)?;
            wait_for_gateway_start(&mut child, Duration::from_secs(30))?;
            let pid = adopt_gateway_child(child);
            println!("Gateway restarted (pid: {pid})");
            tracing::info!(pid, "gateway restarted");
            Ok(())
        }
        GatewayAction::Logs => {
            let path = runtime::cowd_dirs::config_home_dir()
                .join("logs")
                .join("gateway.log");
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
                .join("gateway.log");
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

fn load_gateway_runtime_config(
    loader: &runtime::ConfigLoader,
) -> Result<runtime::RuntimeConfig, String> {
    loader.load().map_err(|error| {
        format!(
            "Gateway configuration is invalid; refusing to change the selected storage or runtime topology: {error}"
        )
    })
}

fn resolve_gateway_workspace_and_config(
    startup_cwd: &Path,
) -> Result<(PathBuf, runtime::RuntimeConfig), String> {
    let startup_cwd = canonical_workspace_dir(startup_cwd, "Gateway startup directory")?;
    let bootstrap_loader = runtime::ConfigLoader::default_for(&startup_cwd);
    let bootstrap_config = load_gateway_runtime_config(&bootstrap_loader)?;
    let workspace_root = match bootstrap_config.workspace() {
        Some(configured) => resolve_configured_workspace(configured, &startup_cwd)?,
        None => startup_cwd.clone(),
    };

    let final_loader = runtime::ConfigLoader::default_for(&workspace_root);
    let final_config = load_gateway_runtime_config(&final_loader)?;
    if let Some(configured) = final_config.workspace() {
        let reloaded_workspace = resolve_configured_workspace(configured, &startup_cwd)?;
        if reloaded_workspace != workspace_root {
            return Err(format!(
                "Gateway workspace configuration is unstable: bootstrap selected `{}`, \
                 but workspace configuration loaded from that directory selected `{}`",
                workspace_root.display(),
                reloaded_workspace.display()
            ));
        }
    }

    Ok((workspace_root, final_config))
}

fn resolve_configured_workspace(configured: &Path, startup_cwd: &Path) -> Result<PathBuf, String> {
    let candidate = if configured == Path::new("~") {
        configured_workspace_home()?
    } else if let Ok(relative) = configured.strip_prefix("~/") {
        configured_workspace_home()?.join(relative)
    } else if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        startup_cwd.join(configured)
    };
    canonical_workspace_dir(&candidate, "configured Gateway workspace")
}

fn configured_workspace_home() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "configured Gateway workspace uses `~`, but HOME is not set".to_string())
}

fn canonical_workspace_dir(path: &Path, label: &str) -> Result<PathBuf, String> {
    let canonical = path
        .canonicalize()
        .map_err(|error| format!("{label} `{}` is unavailable: {error}", path.display()))?;
    if !canonical.is_dir() {
        return Err(format!(
            "{label} `{}` is not a directory",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn run_wechat_qr_login() -> Result<(), Box<dyn std::error::Error>> {
    Err("wechat QR login is provided by the `wechat-ilink` Edge message connector; install and enable `cowd-edge-wechat-ilink-message`".into())
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

#[cfg(test)]
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
    Config {
        args: Option<String>,
        output_format: CliOutputFormat,
    },
    Tool {
        args: Option<String>,
        output_format: CliOutputFormat,
    },
    Setup {
        output_format: CliOutputFormat,
    },
    Init {
        output_format: CliOutputFormat,
    },
    Tui {
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
    HelpTopic(LocalHelpTopic),
    // prompt-mode formatting is only supported for non-interactive runs
    Help {
        output_format: CliOutputFormat,
    },
}

#[cfg(test)]
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
#[cfg(test)]
fn parse_args(args: &[String]) -> Result<CliAction, String> {
    let mut model = DEFAULT_MODEL_ALIAS.to_string();
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
                if !matches!(value.as_str(), "low" | "medium" | "high" | "max") {
                    return Err(format!(
                        "invalid value for --reasoning-effort: '{value}'; must be low, medium, high, or max"
                    ));
                }
                reasoning_effort = Some(value.clone());
                index += 2;
            }
            flag if flag.starts_with("--reasoning-effort=") => {
                let value = &flag[19..];
                if !matches!(value, "low" | "medium" | "high" | "max") {
                    return Err(format!(
                        "invalid value for --reasoning-effort: '{value}'; must be low, medium, high, or max"
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
        // rather than starting the interactive terminal shell (which would consume the pipe and
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
        return Ok(CliAction::Tui {
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
        "tui" => Ok(CliAction::Tui {
            model,
            session_id,
            allowed_tools,
            permission_mode,
            base_commit,
            reasoning_effort: reasoning_effort.clone(),
            allow_broad_cwd,
            yolo_mode,
        }),
        "config" => Ok(CliAction::Config {
            args: join_optional_args(&rest[1..]),
            output_format,
        }),
        "tool" => Ok(CliAction::Tool {
            args: join_optional_args(&rest[1..]),
            output_format,
        }),
        "skill" => {
            let args = join_optional_args(&rest[1..]);
            if !is_static_skill_cli_action(args.as_deref()) {
                return Err(
                    "`cowd skill` is limited to static skill management. Start `cowd` and invoke runtime skills inside the TUI."
                        .to_string(),
                );
            }
            Ok(CliAction::Skills {
                args,
                output_format,
            })
        }
        "gateway" => parse_gateway_args(&rest[1..], output_format),
        removed if is_removed_top_level_command(removed) => Err(removed_top_level_error(removed)),

        other if other.starts_with('/') => Err(
            "top-level slash commands were removed. Start the TUI with `cowd` and use slash commands there."
                .to_string(),
        ),
        other => Err(format!(
            "`cowd {other}` is not part of the minimal CLI surface. Start `cowd` for the TUI or run `cowd --help` for supported commands."
        )),
    }
}

#[cfg(test)]
fn parse_local_help_action(rest: &[String]) -> Option<Result<CliAction, String>> {
    if rest.len() != 2 || !is_help_flag(&rest[1]) {
        return None;
    }

    let topic = match rest[0].as_str() {
        "doctor" => LocalHelpTopic::Doctor,
        _ => return None,
    };
    Some(Ok(CliAction::HelpTopic(topic)))
}

#[cfg(test)]
fn is_help_flag(value: &str) -> bool {
    matches!(value, "--help" | "-h")
}

#[cfg(test)]
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
        "doctor" => Some(Ok(CliAction::Doctor { output_format })),
        "tui" => Some(Ok(CliAction::Tui {
            model: model.to_string(),
            session_id: None,
            allowed_tools: None,
            permission_mode: permission_mode_override.unwrap_or_else(default_permission_mode),
            base_commit: None,
            reasoning_effort: None,
            allow_broad_cwd: false,
            yolo_mode: false,
        })),
        other if is_removed_top_level_command(other) => Some(Err(removed_top_level_error(other))),
        other => bare_slash_command_guidance(other).map(Err),
    }
}

#[cfg(test)]
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
        .find(|spec| spec.name == command_name && is_executable_slash_command(spec.name))?;
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

#[cfg(test)]
fn removed_auth_surface_error(command_name: &str) -> String {
    format!(
        "`cowd {command_name}` has been removed. Configure `model` and `providers` in ~/.cowd/config.yaml instead."
    )
}

#[cfg(test)]
fn is_removed_top_level_command(command_name: &str) -> bool {
    matches!(
        command_name,
        "auth"
            | "login"
            | "logout"
            | "run"
            | "chat"
            | "prompt"
            | "daemon"
            | "session"
            | "memory"
            | "matrix"
            | "mfg"
            | "skills"
            | "tools"
            | "agent"
            | "agents"
            | "mcp"
            | "plugin"
            | "plugins"
            | "export"
            | "import-session"
            | "system-prompt"
            | "bootstrap-plan"
            | "dump-manifests"
            | "init"
            | "sandbox"
            | "status"
            | "setup"
            | "state"
            | "install"
    )
}

#[cfg(test)]
fn removed_top_level_error(command_name: &str) -> String {
    match command_name {
        "auth" | "login" | "logout" => removed_auth_surface_error(command_name),
        "daemon" => {
            "`cowd daemon` has been removed. Gateway is the only user-visible runtime entrypoint; use `cowd gateway status|start|restart|doctor` or start `cowd` for the TUI.".to_string()
        }
        "run" | "chat" | "prompt" => {
            format!("`cowd {command_name}` has been removed. Start `cowd` for the TUI or use Gateway/WebUI for chat.")
        }
        "session" | "memory" | "matrix" | "mfg" | "skill" | "skills" | "tool" | "tools" | "agent"
        | "agents" | "mcp" | "plugin" | "plugins" => {
            format!("`cowd {command_name}` is no longer a top-level CLI management surface. Start `cowd` and use the TUI, or use Gateway/WebUI for runtime management.")
        }
        _ => {
            format!("`cowd {command_name}` is not part of the minimal CLI surface. Use `cowd`, `cowd gateway`, `cowd config`, `cowd doctor`, `cowd skill`, or `cowd tool`.")
        }
    }
}

#[cfg(test)]
fn is_static_skill_cli_action(args: Option<&str>) -> bool {
    let Some(args) = args.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let parts = args.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["list" | "help" | "-h" | "--help" | "doctor"] => true,
        ["view" | "show" | "validate" | "install" | "remove" | "import", _] => true,
        _ => false,
    }
}

#[cfg(test)]
fn join_optional_args(args: &[String]) -> Option<String> {
    let joined = args.join(" ");
    let trimmed = joined.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
fn normalize_allowed_tools(values: &[String]) -> Result<Option<AllowedToolSet>, String> {
    if values.is_empty() {
        return Ok(None);
    }
    runtime_bootstrap::load_tool_registry_for_current_dir()?.normalize_allowed_tools(values)
}

#[cfg(test)]
fn permission_mode_from_label(mode: &str) -> PermissionMode {
    cli::permission_mode_from_label(mode)
}

#[cfg(test)]
fn permission_mode_from_resolved(mode: ResolvedPermissionMode) -> PermissionMode {
    cli::permission_mode_from_resolved(mode)
}

#[cfg(test)]
fn provider_label(kind: runtime::ProviderKind) -> &'static str {
    match kind {
        runtime::ProviderKind::Anthropic => "anthropic",
        runtime::ProviderKind::Xai => "xai",
        runtime::ProviderKind::OpenAi => "openai",
    }
}

#[cfg(test)]
fn format_connected_line(model: &str) -> String {
    let provider = provider_label(runtime::detect_provider_kind(model));
    format!("Connected: {model} via {provider}")
}

pub(crate) fn filter_tool_specs(
    tool_registry: &GatewayToolRegistry,
    allowed_tools: Option<&AllowedToolSet>,
) -> Vec<runtime::ProviderToolDefinition> {
    tool_registry
        .definitions(allowed_tools)
        .into_iter()
        .map(|tool| runtime::ProviderToolDefinition {
            name: tool.name,
            description: tool.description,
            input_schema: tool.input_schema,
        })
        .collect()
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
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

    Ok(CliAction::Tui {
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

#[cfg(test)]
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

    slash_command_specs().iter().any(|spec| {
        is_executable_slash_command(spec.name)
            && (spec.name == name || spec.aliases.contains(&name))
    })
}

#[cfg(test)]
fn dump_manifests(
    manifests_dir: Option<&Path>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    dump_manifests_at_path(&workspace_dir, manifests_dir, output_format)
}

const DUMP_MANIFESTS_OVERRIDE_HINT: &str = "Hint: set COWD_UPSTREAM=/path/to/upstream or pass `cowd dump-manifests --manifests-dir /path/to/upstream`.";

// Internal function for testing that accepts a workspace directory path.
#[cfg(test)]
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
#[allow(
    clippy::exit,
    reason = "interactive CLI cancellation must terminate before a broad workspace can be used"
)]
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

#[cfg(test)]
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
    runtime: &GatewayRuntimeEntry,
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
    task: Option<&runtime::TaskAggregate>,
) -> String {
    const VALUE_WIDTH: usize = 59;

    let status = status_context(None).ok();
    let git_branch = status
        .as_ref()
        .and_then(|context| context.git_branch.as_deref())
        .unwrap_or("unknown");
    let directory = status.as_ref().map_or_else(
        || "unknown".to_string(),
        |context| context.cwd.display().to_string(),
    );
    let git_state = status.as_ref().map_or_else(
        || "unknown".to_string(),
        |context| context.git_summary.headline(),
    );
    let task_row = task.map_or_else(String::new, |task| {
        let short_id: String = task.task_id.chars().take(8).collect();
        let objective = truncate_for_banner(&task.objective, 40);
        let mut rows = startup_banner_row(
            "task",
            &format!("{} {} - {}", task.status.as_str(), short_id, objective),
        );
        if let Some(phase) = current_task_phase_for_display(task) {
            rows.push_str(&startup_banner_row(
                "phase",
                &format!("{}:{}", phase.name, phase.status.as_str()),
            ));
        }
        rows
    });
    let short_session = truncate_for_banner(session_id, VALUE_WIDTH);
    format!(
        "{}\
{}\
{}\
{}\
{}\
{}\
{}\
{}\
{}\
{}",
        "╭────────────────────────────────────────────────────────────────────────╮\n",
        startup_banner_title(&format!("COWD v{VERSION}")),
        startup_banner_row("model", model),
        startup_banner_row("directory", &directory),
        startup_banner_row("branch", git_branch),
        startup_banner_row("git", &git_state),
        startup_banner_row("mode", if yolo_mode { "yolo" } else { "standard" }),
        startup_banner_row("session", &short_session),
        task_row,
        "╰────────────────────────────────────────────────────────────────────────╯",
    )
}

fn startup_banner_title(title: &str) -> String {
    format!("│ {:<70} │\n", truncate_for_banner(title, 70))
}

fn startup_banner_row(label: &str, value: &str) -> String {
    format!("│ {:<10} {:<59} │\n", label, truncate_for_banner(value, 59))
}

fn truncate_for_banner(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    if max_chars <= 3 {
        value.chars().take(max_chars).collect()
    } else {
        let truncated: String = value.chars().take(max_chars - 3).collect();
        format!("{truncated}...")
    }
}

fn current_task_phase_for_display(
    task: &runtime::TaskAggregate,
) -> Option<&harness_contract::task::TaskPhase> {
    task.current_phase_id
        .as_deref()
        .and_then(|phase| {
            task.phases
                .iter()
                .rev()
                .find(|candidate| candidate.phase_id == phase)
        })
        .or_else(|| task.phases.last())
}

fn render_terminal_help() -> String {
    [
        "Terminal controls".to_string(),
        "  /exit                Quit the terminal session".to_string(),
        "  /quit                Quit the terminal session".to_string(),
        "  Up/Down              Navigate prompt history".to_string(),
        "  Ctrl-R               Reverse-search prompt history".to_string(),
        "  Tab                  Complete commands, modes, and recent sessions".to_string(),
        "  Ctrl-C               Clear input (or exit on empty prompt)".to_string(),
        "  Shift+Enter/Ctrl+J   Insert a newline".to_string(),
        "  Auto-save            SQLite session store".to_string(),
        "  Resume latest        /resume latest".to_string(),
        "  Browse sessions      /session list".to_string(),
        "  Show prompt history  /history [count]".to_string(),
        "  Gateway tasks         /tasks [start|cancel|complete]".to_string(),
        "  Gateway approvals     /approvals [approve|reject]".to_string(),
        "  Gateway context       /context [runtime|config|memory|cross-plane]".to_string(),
        "  Cross-plane action   /cross-plane [preflight|execute] <json>".to_string(),
        String::new(),
        render_slash_command_help_filtered(NON_EXECUTABLE_SLASH_COMMANDS),
    ]
    .join(
        "
",
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

fn normalize_permission_mode(mode: &str) -> Option<&'static str> {
    match mode.trim() {
        "read-only" => Some("read-only"),
        "workspace-write" => Some("workspace-write"),
        "danger-full-access" => Some("danger-full-access"),
        _ => None,
    }
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

fn recent_user_context(session: &Session, limit: usize) -> String {
    let requests = session
        .messages()
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

fn compact_message_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            ContentBlock::ReasoningSummary { text } => Some(text.as_str()),
            ContentBlock::Image {
                media_type,
                source_path,
                ..
            } => Some(match source_path {
                Some(path) => path.as_str(),
                None => media_type.as_str(),
            }),
            ContentBlock::Thinking { .. } => None,
            ContentBlock::ToolUse { name, .. } => Some(name.as_str()),
            ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
        })
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn session_db_resume_context_packet(session: &Session) -> Option<ResumeContextPacket> {
    if session.history().is_empty()
        && session.compaction.is_none()
        && session.fork.is_none()
        && session.prompt_history.is_empty()
    {
        return None;
    }

    let recent_turns = session
        .messages()
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

/// Convert an already index-resolved semantic checkpoint into Runtime resume
/// context. Durable lookup is async and happens before Runtime construction;
/// this pure conversion never creates a thread or a nested Tokio runtime.
pub(crate) fn semantic_checkpoint_resume_context_packet(
    event: &session::SessionEvent,
    session_id: &str,
) -> Option<ResumeContextPacket> {
    let session_id = session_id.to_string();
    let checkpoint = semantic_checkpoint_from_event(event, &session_id)?;

    let mut recent_decisions = checkpoint
        .decisions
        .iter()
        .map(|value| format!("decision: {value}"))
        .collect::<Vec<_>>();
    recent_decisions.extend(
        checkpoint
            .user_rules
            .iter()
            .map(|value| format!("user rule (must preserve): {value}")),
    );
    recent_decisions.extend(
        checkpoint
            .constraints
            .iter()
            .map(|value| format!("constraint: {value}")),
    );
    recent_decisions.extend(
        checkpoint
            .file_changes
            .iter()
            .map(|value| format!("file change: {value}")),
    );
    recent_decisions.extend(checkpoint.evidence_refs.iter().map(|reference| {
        format!(
            "durable evidence reference: {}",
            serde_json::to_string(reference).unwrap_or_else(|_| "unserializable".to_string())
        )
    }));
    recent_decisions.push(format!(
        "resume cursor: {}",
        serde_json::to_string(&checkpoint.resume_cursor).unwrap_or_default()
    ));

    Some(ResumeContextPacket {
        session_id,
        handoff_summary: (!checkpoint.summary.trim().is_empty()).then_some(checkpoint.summary),
        active_task: checkpoint.goal,
        recent_decisions,
        blockers: checkpoint.unresolved,
        source: ResumeContextSource::SessionDb,
    })
}

pub(crate) fn semantic_checkpoint_from_event(
    event: &session::SessionEvent,
    session_id: &str,
) -> Option<memory::compression::session::SessionSemanticCheckpoint> {
    let checkpoint = session::SessionDomainEvent::from_session_event(event)
        .ok()
        .filter(|event| event.kind == "memory.semantic_checkpoint.created")
        .and_then(|event| event.payload.get("checkpoint").cloned())
        .and_then(|value| {
            serde_json::from_value::<memory::compression::session::SessionSemanticCheckpoint>(
                value,
            )
            .map_err(|error| {
                tracing::warn!(%error, %session_id, "ignoring malformed semantic checkpoint during resume");
                error
            })
            .ok()
        })?;
    if checkpoint.schema_version == 0
        || checkpoint.schema_version
            > memory::compression::session::SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION
    {
        tracing::warn!(
            %session_id,
            checkpoint_schema = checkpoint.schema_version,
            supported_schema = memory::compression::session::SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
            "ignoring unsupported semantic checkpoint during resume"
        );
        return None;
    }
    Some(checkpoint)
}

pub(crate) fn merge_resume_context_packets(
    session_packet: Option<ResumeContextPacket>,
    checkpoint_packet: Option<ResumeContextPacket>,
) -> Option<ResumeContextPacket> {
    match (session_packet, checkpoint_packet) {
        (None, None) => None,
        (Some(packet), None) | (None, Some(packet)) => Some(packet),
        (Some(mut session), Some(checkpoint)) => {
            if checkpoint.handoff_summary.is_some() {
                session.handoff_summary = checkpoint.handoff_summary;
            }
            if checkpoint.active_task.is_some() {
                session.active_task = checkpoint.active_task;
            }
            session.recent_decisions.extend(checkpoint.recent_decisions);
            session.recent_decisions.sort();
            session.recent_decisions.dedup();
            session.blockers.extend(checkpoint.blockers);
            session.blockers.sort();
            session.blockers.dedup();
            session.source = ResumeContextSource::SessionDb;
            Some(session)
        }
    }
}

pub(crate) fn handoff_resume_context_packet(handoff: &memory::HandoffData) -> ResumeContextPacket {
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

pub(crate) fn inject_auto_resume_context(
    runtime: &GatewayRuntimeEntry,
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

pub(crate) fn workspace_context_item(session: &Session, model_ctx: u32) -> runtime::ContextItem {
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

pub(crate) fn runtime_capability_context_item(
    tool_definitions: &[runtime::ProviderToolDefinition],
    allowed_tools: Option<&AllowedToolSet>,
    model_ctx: u32,
) -> runtime::ContextItem {
    let tool_names = tool_definitions
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    let has_tool = |name: &str| tool_names.contains(&name);
    let batch_tools = [
        "workspace_snapshot",
        "read_many",
        "grep_many",
        "glob_many",
        "tool_batch_readonly",
    ]
    .into_iter()
    .filter(|name| has_tool(name))
    .collect::<Vec<_>>();
    let prepared_readonly_tools = tool_definitions
        .iter()
        .filter_map(|tool| {
            let profile = runtime::tool_execution_profile(&tool.name);
            profile
                .prepared_readonly_supported
                .then_some(tool.name.as_str())
        })
        .collect::<Vec<_>>();
    let runtime_query = if has_tool("runtime_capabilities") {
        "runtime_capabilities=registered"
    } else {
        "runtime_capabilities=not_registered"
    };
    let runtime_orchestration = if has_tool("runtime_orchestrate") {
        "runtime_orchestrate=registered"
    } else {
        "runtime_orchestrate=not_registered"
    };
    let context_retrieval = if has_tool("context_retrieve") {
        "context_retrieve=registered; use it when automatic context is incomplete, uncertain, or appears unrelated; discover authorized prior Sessions through source=session_catalog before requesting explicit Session history"
    } else {
        "context_retrieve=not_registered"
    };
    let allowed_state = allowed_tools.map_or_else(
        || "allowed_tools=all available registry tools".to_string(),
        |allowed| format!("allowed_tools=restricted count={}", allowed.len()),
    );
    let content = format!(
        "# Runtime capability catalog\n\
model_context_window={model_ctx}\n\
registered_tool_count={}\n\
{allowed_state}\n\
{runtime_query}\n\
{runtime_orchestration}\n\
{context_retrieval}\n\
registered_batch_readonly_tools={}\n\
registered_prepared_readonly_tools={}\n\
Important: this is a filtered backend catalog, not the current provider function schema set. Runtime injects the authoritative per-request function-call contract separately. Call only functions in that contract; use tool_search to activate eligible deferred candidates. For independent read-only evidence, request multiple active calls together. Distinguish model-callable tools from runtime-owned collaboration/subagent affordances; for complex work, use active runtime orchestration when present. When a path repeats, re-plan from retained evidence rather than querying the same capability catalog again.",
        tool_definitions.len(),
        if batch_tools.is_empty() {
            "none".to_string()
        } else {
            batch_tools.join(",")
        },
        if prepared_readonly_tools.is_empty() {
            "none".to_string()
        } else {
            prepared_readonly_tools.join(",")
        }
    );
    let mut item = runtime::ContextItem::new(
        "runtime.capabilities.active",
        runtime::ContextSourceKind::RuntimeHeader,
        runtime::ContextRole::Orientation,
        content,
    );
    item.authority = runtime::ContextAuthority::Derived;
    item.visibility = runtime::ContextVisibility::Shared;
    item.score = 0.95;
    item.evidence = vec!["gateway.filtered_tool_registry".to_string()];
    item
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
        .args(["-c", &format!("safe.directory={}", root.display())])
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
        .args(["-c", &format!("safe.directory={}", root.display())])
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

#[cfg(not(feature = "code-index"))]
fn workspace_hot_symbols(
    _root: &Path,
    _touched_files: &[String],
    _symbol_limit: usize,
) -> Vec<String> {
    Vec::new()
}

#[cfg(feature = "code-index")]
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

#[cfg(test)]
fn resolve_cli_auth_source_for_cwd() -> Result<AuthSource, provider_crate::ApiError> {
    resolve_startup_auth_source(|| Ok(None))
}

#[cfg(test)]
fn format_user_visible_api_error(session_id: &str, error: &provider_crate::ApiError) -> String {
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

#[cfg(test)]
fn format_context_window_error(session_id: &str, error: &provider_crate::ApiError) -> String {
    let mut lines: Vec<String> = vec!["context_window_blocked".to_string(), String::new()];

    match error {
        provider_crate::ApiError::ContextWindowExceeded {
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
        provider_crate::ApiError::Api {
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
        provider_crate::ApiError::RetriesExhausted {
            attempts,
            last_error,
        } => {
            lines.push("Context window blocked".to_string());
            lines.push(format!("api failed after {attempts} attempts"));
            lines.push(String::new());
            if let Some(rid) = last_error.request_id() {
                lines.push(format!("{:<17}{rid}", "Trace"));
            }
            if let provider_crate::ApiError::Api {
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

fn slash_command_completion_candidates_with_sessions(
    model: &str,
    active_session_id: Option<&str>,
    recent_session_ids: Vec<String>,
) -> Vec<String> {
    let mut completions = BTreeSet::new();

    for spec in slash_command_specs() {
        if NON_EXECUTABLE_SLASH_COMMANDS.contains(&spec.name) {
            continue;
        }
        completions.insert(format!("/{}", spec.name));
        for alias in spec.aliases {
            if !NON_EXECUTABLE_SLASH_COMMANDS.contains(alias) {
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
        "web_search" => parsed
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
    let mut lines = vec![format!("{icon} \x1b[38;5;245mbash\x1b[0m")];
    if let Some(task_id) = parsed
        .get("backgroundTaskId")
        .and_then(|value| value.as_str())
    {
        lines[0].push_str(&format!(" backgrounded ({task_id})"));
    } else if let Some(status) = parsed
        .get("returnCodeInterpretation")
        .and_then(|value| value.as_str())
        .filter(|status| !status.is_empty())
    {
        lines[0].push_str(&format!(" {status}"));
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

pub(crate) fn truncate_for_summary(value: &str, limit: usize) -> String {
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

#[cfg(test)]
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

#[cfg(test)]
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
                write!(out, "{text}")
                    .and_then(|()| out.flush())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                events.push(AssistantEvent::TextDelta(text));
            }
        }
        OutputContentBlock::ReasoningSummary { text } => {
            if !text.is_empty() {
                events.push(AssistantEvent::ReasoningSummaryDelta(text));
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
        OutputContentBlock::Thinking {
            thinking,
            signature,
        } => {
            render_thinking_block_summary(out, Some(thinking.chars().count()), false)?;
            *block_has_thinking_summary = true;
            if !thinking.is_empty() {
                events.push(AssistantEvent::PrivateReasoningDelta(thinking));
            }
            if let Some(signature) = signature.filter(|signature| !signature.is_empty()) {
                events.push(AssistantEvent::SignatureDelta(signature));
            }
        }
        OutputContentBlock::RedactedThinking { .. } => {
            render_thinking_block_summary(out, None, true)?;
            *block_has_thinking_summary = true;
        }
    }
    Ok(())
}

#[cfg(test)]
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

pub(crate) fn permission_policy(
    mode: PermissionMode,
    feature_config: &runtime::RuntimeFeatureConfig,
    tool_registry: &GatewayToolRegistry,
) -> Result<PermissionPolicy, String> {
    Ok(tool_registry.permission_specs(None)?.into_iter().fold(
        PermissionPolicy::new(mode).with_permission_rules(feature_config.permission_rules()),
        |policy, (name, required_permission)| {
            policy
                .with_tool_requirement(name, runtime_permission_mode_from_tool(required_permission))
        },
    ))
}

fn runtime_permission_mode_from_tool(mode: tools::permissions::PermissionMode) -> PermissionMode {
    match mode {
        tools::permissions::PermissionMode::ReadOnly => PermissionMode::ReadOnly,
        tools::permissions::PermissionMode::WorkspaceWrite => PermissionMode::WorkspaceWrite,
        tools::permissions::PermissionMode::DangerFullAccess => PermissionMode::DangerFullAccess,
    }
}

#[cfg(test)]
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
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => {
                        Some(InputContentBlock::Text { text: text.clone() })
                    }
                    ContentBlock::ReasoningSummary { .. } => None,
                    ContentBlock::Image {
                        media_type, data, ..
                    } => Some(InputContentBlock::Image {
                        source: ImageSource::base64(media_type.clone(), data.clone()),
                    }),
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => Some(InputContentBlock::Thinking {
                        thinking: thinking.clone(),
                        signature: signature.clone(),
                    }),
                    ContentBlock::ToolUse { id, name, input } => Some(InputContentBlock::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        input: serde_json::from_str(input)
                            .unwrap_or_else(|_| serde_json::json!({ "raw": input })),
                    }),
                    ContentBlock::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                        ..
                    } => Some(InputContentBlock::ToolResult {
                        tool_use_id: tool_use_id.clone(),
                        content: vec![ToolResultContentBlock::Text {
                            text: output.clone(),
                        }],
                        is_error: *is_error,
                    }),
                })
                .collect::<Vec<_>>();
            (!content.is_empty()).then(|| InputMessage {
                role: role.to_string(),
                content,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::{
        build_memory_config, build_system_prompt_for_mode, cli_turn_context_profile,
        embeddings_endpoint, filter_tool_specs, format_bughunter_report,
        format_commit_preflight_report, format_commit_skipped_report, format_connected_line,
        format_issue_report, format_pr_report, format_startup_banner,
        format_startup_banner_with_task, format_status_report, format_tool_call_start,
        format_tool_result, format_ultraplan_report, format_unknown_slash_command_message,
        format_user_visible_api_error, gateway_auth_token_from_platform,
        handoff_resume_context_packet, load_gateway_runtime_config, merge_prompt_with_stdin,
        normalize_permission_mode, parse_args, parse_gateway_args, parse_git_status_branch,
        parse_git_status_metadata_for, parse_git_workspace_summary, permission_policy,
        print_help_to, push_output_block, render_config_report, render_diff_report,
        render_diff_report_for, render_memory_report, render_setup_json, render_setup_report,
        render_terminal_help, resolve_model_alias_with_config, resolve_tui_model,
        response_to_events, runtime_capability_context_item,
        semantic_checkpoint_resume_context_packet, session_db_resume_context_packet,
        slash_command_completion_candidates_with_sessions, status_context, strip_ansi_for_tui,
        suggestions::format_unknown_slash_command, try_resolve_bare_skill_prompt, validate_no_args,
        workspace_context_item, write_mcp_server_fixture, CliAction, CliOutputFormat,
        GatewayAction, GatewayToolExecutor, GitWorkspaceSummary, LocalHelpTopic, StatusUsage,
        DEFAULT_MODEL_ALIAS, LATEST_SESSION_REFERENCE, NON_EXECUTABLE_SLASH_COMMANDS, SHARED_RT,
    };
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
        let memory_config =
            build_memory_config(&runtime_config, &workspace).expect("memory config");

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
        let error = parse_args(&["--allowedTools".to_string(), "read,glob".to_string()])
            .expect_err("legacy aliases must not bypass the canonical tool contract");
        assert!(error.contains("unsupported tool in --allowedTools: read"));
    }

    #[test]
    fn rejects_unknown_allowed_tools() {
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
            Some(root.to_str().unwrap())
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
                yolo_mode: true,
                max_failures_before_block: 3,
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

        let resolved =
            with_current_dir(&root, || resolve_tui_model(DEFAULT_MODEL_ALIAS.to_string()));

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

        let resolved =
            with_current_dir(&root, || resolve_tui_model(DEFAULT_MODEL_ALIAS.to_string()));

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
                    estimated_cost_usd: 0.0,
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

        let tool_host = Arc::new(tools::ToolHost::new(
            "bootstrap-mcp-test",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(tool_registry),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                Some(mcp_service.clone()),
            ),
        ));
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
            let evaluated = negotiator.assess_effective(
                &policy,
                &runtime::AuthorizationRequest {
                    principal_id: "test:gateway-mcp".to_string(),
                    capability: descriptor.tool_id.clone(),
                    input: value.to_string(),
                    idempotency_key: request_id.clone(),
                    effect: descriptor.clone(),
                    parent_ceiling: PermissionMode::DangerFullAccess,
                    parent_lease_id: None,
                    approval_satisfied: true,
                    recovery_scope: request_id.clone(),
                    context: runtime::PermissionContext::default(),
                    safe_alternatives: Vec::new(),
                },
            );
            let assessment = evaluated.assessment;
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
        let tool_host = Arc::new(tools::ToolHost::new(
            "bootstrap-mcp-unsupported-test",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(tool_registry),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                Some(mcp_service),
            ),
        ));
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
        let test_tool_host = Arc::new(tools::ToolHost::new(
            "runtime-plugin-lifecycle",
            &workspace,
            tools::ToolHostSnapshot::new(
                Arc::new(runtime_plugin_state.tool_registry.clone()),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                None,
            ),
        ));
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
            PermissionMode::DangerFullAccess,
            runtime::AutonomyProfileId::Supervised,
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
}

#[cfg(test)]
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
