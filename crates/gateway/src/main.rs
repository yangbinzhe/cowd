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
#[path = "runtime/active_session/mod.rs"]
mod active_session;
mod api_routes;
mod app_platform;
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
#[path = "core/composition_root.rs"]
mod composition_root;
#[path = "core/doctor.rs"]
mod doctor;
mod entry;
#[path = "core/event_bus.rs"]
mod event_bus;
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
#[path = "infrastructure/ownership_cutover_contract.rs"]
mod ownership_cutover_contract;
#[path = "infrastructure/ownership_cutover_coordinator.rs"]
pub mod ownership_cutover_coordinator;
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

/// Hidden deterministic workload for the route/OpenAPI performance gate.
/// The uncached branch is the pre-P7.5 authority path; the cached branch is
/// the production projection. Both consume the exact same static catalog.
#[doc(hidden)]
pub fn route_openapi_benchmark(iterations: usize, cached: bool) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..iterations.max(1) {
        let document = std::hint::black_box(api_routes::benchmark_openapi_document(cached));
        checksum = checksum.saturating_add(
            document["paths"]
                .as_object()
                .map_or(0, |paths| paths.len() as u64),
        );
        std::hint::black_box(document);
    }
    checksum
}

/// Operator-only storage cutover entry used by the thin CLI binary.
pub fn storage_entry(args: &[String]) -> std::process::ExitCode {
    if args.first().map(String::as_str) == Some("ownership-cutover") {
        return match ownership_cutover_coordinator::run_operator_command(
            args.get(1..).unwrap_or_default(),
        ) {
            Ok(publication) => match serde_json::to_string_pretty(&publication) {
                Ok(output) => {
                    println!("{output}");
                    std::process::ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("ownership cutover result encoding failed: {error}");
                    std::process::ExitCode::from(70)
                }
            },
            Err(error) => {
                eprintln!("ownership cutover failed: {error}");
                std::process::ExitCode::FAILURE
            }
        };
    }
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

    pub fn route_contract_snapshots() -> (
        std::collections::BTreeSet<(String, String)>,
        std::collections::BTreeSet<(String, String)>,
    ) {
        crate::api_routes::route_contract_snapshots()
    }
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
pub(crate) use entry::gateway_lifecycle_entry::systemd_lifecycle_eligible;
use entry::gateway_lifecycle_entry::{
    run_user_gateway_service_action, user_gateway_service_is_loaded, wait_for_managed_gateway_start,
};
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
    // Live-subscription baseline fanout performs large, short-lived allocations
    // on several Tokio workers. Glibc's default per-thread arena policy retains
    // those peaks and made repeated WebUI reconnects grow RSS by nearly 1 GiB
    // per traversal. Preserve an operator override, but keep the managed
    // `gateway start` path on a small, bounded arena count by default.
    if let Some(limit) = gateway_allocator_arena_limit(std::env::var_os("MALLOC_ARENA_MAX")) {
        command.env("MALLOC_ARENA_MAX", limit);
    }
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

fn gateway_allocator_arena_limit(current: Option<std::ffi::OsString>) -> Option<&'static str> {
    current.is_none().then_some("2")
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
/// Missing build-script metadata is fail-closed as dirty, never clean.
const GIT_DIRTY_RAW: Option<&str> = option_env!("GIT_DIRTY");

#[must_use]
pub(crate) fn compiled_runtime_build_identity() -> runtime::RuntimeBuildIdentity {
    runtime::RuntimeBuildIdentity::new(
        VERSION,
        GIT_SHA.unwrap_or("unknown"),
        GIT_DIRTY_RAW != Some("false"),
    )
}
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
    mc.store.vector.max_input_tokens = src.vector.max_input_tokens;
    mc.identity.role = src.identity.role.clone();
    mc.identity.language = src.identity.language.clone();
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
            if user_gateway_service_is_loaded() {
                run_user_gateway_service_action("start")?;
                let status = wait_for_managed_gateway_start(Duration::from_secs(30))?;
                println!("Gateway started (pid: {})", status.pid);
                tracing::info!(pid = status.pid, "managed Gateway service started");
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
            if user_gateway_service_is_loaded() {
                run_user_gateway_service_action("stop")?;
            } else {
                server::stop_server().map_err(|e| e.to_string())?;
            }
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
            if user_gateway_service_is_loaded() {
                run_user_gateway_service_action("restart")?;
                let status = wait_for_managed_gateway_start(Duration::from_secs(30))?;
                println!("Gateway restarted (pid: {})", status.pid);
                tracing::info!(pid = status.pid, "managed Gateway service restarted");
                return Ok(());
            }
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
        ["view" | "show" | "validate" | "plan" | "install" | "status" | "remove" | "import", _] => {
            true
        }
        ["install", _, "--allow-warnings"] | ["rollback", _, _] => true,
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
        ContextProfile::AutonomousGoal
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

pub(crate) fn permission_policy_with_control(
    control: runtime::permissions::SessionExecutionPolicyControl,
    feature_config: &runtime::RuntimeFeatureConfig,
    tool_registry: &GatewayToolRegistry,
) -> Result<PermissionPolicy, String> {
    Ok(tool_registry.permission_specs(None)?.into_iter().fold(
        PermissionPolicy::with_execution_policy_control(control)
            .with_permission_rules(feature_config.permission_rules()),
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
#[path = "core/tests/mod.rs"]
mod tests;

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
