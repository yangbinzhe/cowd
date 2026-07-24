// ── Gateway RuntimeHost ────────────────────────────────────
// Gateway foreground mode is a gateway process with an internal runtime host providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Surface registry (builtin TUI/WebUI plus external JSONL sidecars)
// Shared state: ActiveSessions, CognitiveContextManager, ToolCatalog, SessionEventBus

use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::{
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use session::SessionLeaseRegistry;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::api_routes;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use crate::runtime_service::RuntimeService;
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::SessionLifecycleKernel;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use runtime::mcp_tool_bridge::{McpConnectionStatus, McpToolInfo, McpToolRegistry};
use runtime::{McpServerManager, RuntimeConfig};
use surface::SurfaceManifest;

use runtime::session_lifecycle::{EvictionPolicy, SessionLifecycleConfig, SessionLifecycleManager};

pub mod config_reload;

/// Gateway composition adapter for paired Definition evaluation. It owns no
/// scoring, candidate state, or release decision: `harness-eval` loads and
/// scores frozen scenarios while Runtime performs both real Agent runs.
#[derive(Clone)]
struct GatewayEvolutionScenarioExecutor {
    runtime: Arc<OnceLock<Weak<runtime::RuntimeServices>>>,
}

#[async_trait]
impl harness_eval::DefinitionEvolutionScenarioExecutor for GatewayEvolutionScenarioExecutor {
    async fn execute(
        &self,
        candidate_id: &str,
        scenario: &harness_contract::evaluation::EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<
        (
            harness_contract::evaluation::EvaluationScenarioObservation,
            harness_contract::evaluation::EvaluationScenarioObservation,
        ),
        String,
    > {
        let runtime = self
            .runtime
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| "runtime_evaluation_executor_not_bound".to_string())?;
        runtime
            .execute_evolution_scenario(candidate_id, scenario, sample_index)
            .await
            .map_err(|error| error.to_string())
    }
}

/// Gateway owns the broker process lifetime but never opens its authority
/// directory or signing key.  The child receives enrollment material once on
/// stdin, then serves only its protected Unix socket.
struct AuthBrokerProcess {
    child: Child,
    socket_path: PathBuf,
    socket_identity: Option<(u64, u64)>,
}

impl AuthBrokerProcess {
    fn start(
        config_home: &Path,
        credential: &str,
        catalog: &auth_broker::AuthorizationCatalog,
    ) -> Result<Self, String> {
        let root = config_home.join("auth-broker");
        std::fs::create_dir_all(&root)
            .map_err(|error| format!("failed to create auth broker root: {error}"))?;
        let socket_path = auth_broker::BrokerClient::default_socket(&root);
        let catalog_path = auth_broker::catalog_file(&root);
        auth_broker::write_catalog(&catalog_path, catalog)
            .map_err(|error| format!("failed to write auth broker profile catalogue: {error}"))?;
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)
                .map_err(|error| format!("failed to remove stale auth broker socket: {error}"))?;
        }
        let mut child = sandbox_launcher::cowd_internal_process_command()?
            .arg("__cowd_internal")
            .arg("auth-broker")
            .arg("--root")
            .arg(&root)
            .arg("--socket")
            .arg(&socket_path)
            .arg("--catalog")
            .arg(&catalog_path)
            .arg("--credential-stdin")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| format!("failed to spawn auth broker: {error}"))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "auth broker stdin is unavailable".to_string())?;
        stdin
            .write_all(credential.as_bytes())
            .and_then(|_| stdin.write_all(b"\n"))
            .and_then(|_| stdin.flush())
            .map_err(|error| format!("failed to enroll auth broker: {error}"))?;
        drop(stdin);

        let client = auth_broker::BrokerClient::new(&socket_path);
        for _ in 0..40 {
            if client.trust_metadata().is_ok() {
                let socket_identity = socket_file_identity(&socket_path);
                return Ok(Self {
                    child,
                    socket_path,
                    socket_identity,
                });
            }
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("failed to inspect auth broker: {error}"))?
            {
                return Err(format!("auth broker exited during startup: {status}"));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        let _ = child.wait();
        Err("auth broker did not become ready within one second".to_string())
    }

    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        remove_socket_if_owned(&self.socket_path, self.socket_identity);
    }
}

#[cfg(unix)]
fn socket_file_identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn socket_file_identity(_path: &Path) -> Option<(u64, u64)> {
    None
}

fn remove_socket_if_owned(path: &Path, expected: Option<(u64, u64)>) {
    if expected.is_some() && socket_file_identity(path) == expected {
        let _ = std::fs::remove_file(path);
    }
}

#[derive(Clone, Default)]
struct RuntimeMcpServiceAdapter {
    registry: McpToolRegistry,
}

impl RuntimeMcpServiceAdapter {
    async fn from_runtime_config(config: &RuntimeConfig) -> Self {
        let registry = McpToolRegistry::new();
        for (server_name, server_config) in config.mcp().servers() {
            let single_server = BTreeMap::from([(server_name.clone(), server_config.clone())]);
            let manager = McpServerManager::from_servers(&single_server);
            if manager.server_names().is_empty() {
                registry.register_server(
                    server_name,
                    McpConnectionStatus::Error,
                    vec![],
                    vec![],
                    manager
                        .unsupported_servers()
                        .first()
                        .map(|server| server.reason.clone()),
                );
                continue;
            }

            let discovery = match registry.install_server_manager(server_name, manager) {
                Ok(discovery) => discovery,
                Err(error) => {
                    registry.register_server(
                        server_name,
                        McpConnectionStatus::Error,
                        vec![],
                        vec![],
                        Some(error.clone()),
                    );
                    tracing::warn!(server = server_name, error = %error, "failed to start MCP server worker");
                    continue;
                }
            };
            let failed = discovery
                .failed_servers
                .iter()
                .find(|failure| failure.server_name == *server_name);
            let status = if failed.is_some() {
                McpConnectionStatus::Error
            } else {
                McpConnectionStatus::Connected
            };
            let tools = discovery
                .tools
                .iter()
                .filter(|tool| tool.server_name == *server_name)
                .map(|tool| McpToolInfo {
                    name: tool.raw_name.clone(),
                    description: tool.tool.description.clone(),
                    input_schema: tool.tool.input_schema.clone(),
                })
                .collect::<Vec<_>>();
            registry.register_server(
                server_name,
                status,
                tools,
                vec![],
                failed.map(|failure| failure.error.clone()),
            );
            if status != McpConnectionStatus::Connected {
                if let Err(error) = registry.remove_server_manager(server_name) {
                    tracing::warn!(server = server_name, error = %error, "failed to stop unhealthy MCP server worker");
                }
            }
        }
        Self { registry }
    }

    fn server_projection(
        state: runtime::mcp_tool_bridge::McpServerState,
    ) -> mcp::McpServerProjection {
        mcp::McpServerProjection {
            name: state.server_name,
            transport: mcp::McpTransportKind::ManagedProxy,
            enabled: state.status != McpConnectionStatus::Disconnected,
            status: state.status.to_string(),
            auth_state: (state.status == McpConnectionStatus::AuthRequired)
                .then(|| "auth_required".to_string()),
        }
    }
}

impl mcp::McpService for RuntimeMcpServiceAdapter {
    fn list_servers(&self) -> Result<Vec<mcp::McpServerProjection>, mcp::McpServiceError> {
        Ok(self
            .registry
            .list_servers()
            .into_iter()
            .map(Self::server_projection)
            .collect())
    }

    fn server(&self, name: &str) -> Result<mcp::McpServerProjection, mcp::McpServiceError> {
        self.registry
            .get_server(name)
            .map(Self::server_projection)
            .ok_or_else(|| mcp::McpServiceError::NotFound(name.to_string()))
    }

    fn health(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
        let servers = self.registry.list_servers();
        Ok(serde_json::json!({
            "ok": true,
            "servers": servers.len(),
            "connected": servers.iter().filter(|server| server.status == McpConnectionStatus::Connected).count(),
        }))
    }

    fn reload_config(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
        Ok(serde_json::json!({
            "ok": true,
            "status": "managed_by_gateway_config_reload",
            "source": "runtime_mcp_service_adapter",
            "hint": "use POST /api/runtime/config/reload to rebuild the MCP service from current config"
        }))
    }

    fn list_tools(
        &self,
        server: Option<&str>,
    ) -> Result<Vec<mcp::McpToolProjection>, mcp::McpServiceError> {
        let mut tools = Vec::new();
        for state in self.registry.list_servers() {
            if server.is_some_and(|requested| requested != state.server_name) {
                continue;
            }
            let server_tools = self
                .registry
                .list_tools(&state.server_name)
                .map_err(mcp::McpServiceError::Request)?;
            tools.extend(server_tools.into_iter().map(|tool| mcp::McpToolProjection {
                server: state.server_name.clone(),
                name: tool.name,
                description: tool.description,
                input_schema: tool.input_schema.unwrap_or_else(|| serde_json::json!({})),
            }));
        }
        Ok(tools)
    }

    fn list_resources(
        &self,
        server: Option<&str>,
    ) -> Result<Vec<mcp::McpResourceProjection>, mcp::McpServiceError> {
        let mut resources = Vec::new();
        for state in self.registry.list_servers() {
            if server.is_some_and(|requested| requested != state.server_name) {
                continue;
            }
            let server_resources = self
                .registry
                .list_resources(&state.server_name)
                .map_err(mcp::McpServiceError::Request)?;
            resources.extend(server_resources.into_iter().map(|resource| {
                mcp::McpResourceProjection {
                    server: state.server_name.clone(),
                    uri: resource.uri,
                    name: Some(resource.name),
                    mime_type: resource.mime_type,
                    content: None,
                }
            }));
        }
        Ok(resources)
    }

    fn read_resource(
        &self,
        server: &str,
        uri: &str,
    ) -> Result<mcp::McpResourceProjection, mcp::McpServiceError> {
        let resource = self
            .registry
            .read_resource(server, uri)
            .map_err(mcp::McpServiceError::Request)?;
        Ok(mcp::McpResourceProjection {
            server: server.to_string(),
            uri: resource.uri,
            name: Some(resource.name),
            mime_type: resource.mime_type,
            content: None,
        })
    }

    fn call_tool(
        &self,
        request: mcp::McpToolCallRequest,
    ) -> Result<mcp::McpToolCallReceipt, mcp::McpServiceError> {
        let output = self
            .registry
            .call_tool(&request.server, &request.tool, &request.input)
            .map_err(mcp::McpServiceError::Request)?;
        Ok(mcp::McpToolCallReceipt {
            server: request.server,
            tool: request.tool,
            ok: true,
            output,
        })
    }
}

// ── Background session cleanup task ────────────────────────────

/// Spawns a periodic task that checks all active sessions and closes
/// any that are idle or expired.
///
/// Uses `spawn_blocking` + `block_on` internally because the session
/// entry's `MutexGuard` is `!Send` across `.await` points.
fn spawn_session_cleanup_task(
    session_manager: Arc<crate::unified_session_manager::UnifiedSessionManager>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let unloaded = session_manager.run_resource_cleanup().await;
            if unloaded > 0 {
                tracing::info!(unloaded, "session resource cleanup completed");
            }
        }
    })
}

// ── Config ─────────────────────────────────────────────────────

pub struct RuntimeHostConfig {
    pub http_addr: String,
    pub memory_config: Option<MemoryConfig>,
    pub surface_configs: Vec<SurfaceManifest>,
    pub surface_runtime_configs: BTreeMap<String, serde_json::Value>,
    pub runtime_config: Option<serde_json::Value>,
    pub webui_dir: Option<PathBuf>,
    pub cors_origins: Vec<String>,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SurfaceStartupDiagnostic {
    surface_id: String,
    kind: String,
    capability_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StartupDiagnostics {
    http_addr: String,
    workspace_root: String,
    config_home: String,
    webui_dir: Option<String>,
    webui_status: String,
    webui_available: bool,
    memory_enabled: bool,
    memory_available: bool,
    unified_store_available: bool,
    runtime_config_loaded: bool,
    auth_required: bool,
    cors_origin_count: usize,
    surface_count: usize,
    surfaces: Vec<SurfaceStartupDiagnostic>,
}

fn build_startup_diagnostics(
    config: &RuntimeHostConfig,
    workspace_root: &Path,
    config_home: &Path,
    static_webui: &crate::gateway_static::StaticWebUiSource,
    memory_available: bool,
    unified_store_available: bool,
) -> StartupDiagnostics {
    let surfaces: Vec<SurfaceStartupDiagnostic> = config
        .surface_configs
        .iter()
        .map(|surface| SurfaceStartupDiagnostic {
            surface_id: surface.id.clone(),
            kind: format!("{:?}", surface.kind),
            capability_count: surface.capabilities.len(),
        })
        .collect();

    StartupDiagnostics {
        http_addr: config.http_addr.clone(),
        workspace_root: workspace_root.display().to_string(),
        config_home: config_home.display().to_string(),
        webui_dir: static_webui
            .configured_path
            .as_ref()
            .map(|path| path.display().to_string()),
        webui_status: static_webui.status.as_str().to_string(),
        webui_available: static_webui.available,
        memory_enabled: config.memory_config.is_some(),
        memory_available,
        unified_store_available,
        runtime_config_loaded: config.runtime_config.is_some(),
        auth_required: config.auth_token.is_some(),
        cors_origin_count: config.cors_origins.len(),
        surface_count: surfaces.len(),
        surfaces,
    }
}

fn emit_startup_diagnostics(diagnostics: &StartupDiagnostics) {
    tracing::info!(
        http_addr = %diagnostics.http_addr,
        workspace_root = %diagnostics.workspace_root,
        config_home = %diagnostics.config_home,
        webui_dir = ?diagnostics.webui_dir,
        webui_status = %diagnostics.webui_status,
        webui_available = diagnostics.webui_available,
        memory_enabled = diagnostics.memory_enabled,
        memory_available = diagnostics.memory_available,
        unified_store_available = diagnostics.unified_store_available,
        runtime_config_loaded = diagnostics.runtime_config_loaded,
        auth_required = diagnostics.auth_required,
        cors_origin_count = diagnostics.cors_origin_count,
        surface_count = diagnostics.surface_count,
        "runtime host startup diagnostics"
    );
}

fn emit_execution_startup_recovery(report: &runtime::ExecutionStartupRecoveryReport) {
    tracing::info!(
        examined_graphs = report.examined_graphs,
        recovered_graphs = report.recovered_graphs,
        advanced_graphs = report.advanced_graphs,
        terminal_graphs = report.terminal_graphs,
        waiting_graphs = report.waiting_graphs,
        blocked_graphs = report.blocked_graphs,
        error_count = report.errors.len(),
        "execution graph startup recovery completed"
    );
    for error in &report.errors {
        tracing::warn!(
            graph_id = %error.graph_id,
            error = %error.error,
            "execution graph startup recovery reported an error"
        );
    }
}

// ── PID file guard ──────────────────────────────────────────────

struct PidFileGuard;

impl PidFileGuard {
    fn new() -> Result<Self, String> {
        let pid_path = crate::server::pid_file();
        let pid = std::process::id();
        std::fs::write(&pid_path, pid.to_string())
            .map_err(|e| format!("failed to write PID file {:?}: {e}", pid_path))?;
        tracing::info!(pid, path = %pid_path.display(), "PID file written");
        Ok(Self)
    }
}

async fn webui_not_configured_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({
            "ok": false,
            "error": "webui_not_configured",
            "config_key": "gateway.webui_dir",
            "message": "WebUI static assets are optional; configure gateway.webui_dir with a directory containing index.html to enable browser UI.",
        })),
    )
}

async fn webui_static_fallback_handler(state: Arc<api_routes::AppState>, uri: Uri) -> Response {
    match state.services.surface.resolve_static("webui", uri.path()) {
        Ok(Some(file)) => match tokio::fs::read(&file.file_path).await {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type_for_path(&file.file_path))],
                bytes,
            )
                .into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({
                    "ok": false,
                    "error": format!("failed to read WebUI asset: {error}"),
                })),
            )
                .into_response(),
        },
        Ok(None) => webui_not_configured_handler().await.into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "ok": false,
                "error": error,
            })),
        )
            .into_response(),
    }
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "wasm" => "application/wasm",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn build_cors_layer(explicit_origins: Vec<HeaderValue>) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            explicit_origins.iter().any(|allowed| allowed == origin)
                || is_loopback_web_origin(origin)
        }))
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers(Any)
}

fn is_loopback_web_origin(origin: &HeaderValue) -> bool {
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let Some(rest) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };

    let authority = rest.split('/').next().unwrap_or(rest);
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        let Some(end) = stripped.find(']') else {
            return false;
        };
        &stripped[..end]
    } else {
        authority.split(':').next().unwrap_or("")
    };

    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|addr| addr.is_loopback())
            .unwrap_or(false)
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let pid_path = crate::server::pid_file();
        if pid_path.exists() {
            std::fs::remove_file(&pid_path).ok();
            let _ = std::fs::remove_file(crate::server::addr_file());
            tracing::info!(path = %pid_path.display(), "PID file removed");
        }
    }
}

// ── Gateway entry point ─────────────────────────────────────────

pub async fn run_gateway_runtime(config: RuntimeHostConfig) -> Result<(), String> {
    let started_at = Instant::now();
    // 0. Write PID file (removed on drop via guard)
    let _pid_guard = PidFileGuard::new()?;

    // 1. Resolve configuration and compose every durable owner exactly once.
    let approval_dir = std::env::var_os("COWD_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".cowd"));
    let workspace_root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let loaded = runtime::ConfigLoader::new(&workspace_root, &approval_dir)
        .load_with_diagnostics()
        .map_err(|error| format!("failed to load runtime configuration: {error}"))?;
    let runtime_config = loaded.config;
    let config_diagnostics = loaded.diagnostics;
    for diagnostic in &config_diagnostics {
        tracing::warn!(code = %diagnostic.code, message = %diagnostic.message, "runtime config diagnostic");
    }
    let selected_storage = Arc::new(
        crate::selected_storage::SelectedStorageTopology::compose_for_runtime(
            runtime_config.storage(),
            runtime_config.apps(),
            &approval_dir,
            &workspace_root,
        )
        .map_err(|error| format!("failed to compose selected storage topology: {error}"))?,
    );
    tracing::info!(
        backend = selected_storage.backend_label(),
        health = %selected_storage.health_projection(),
        "selected storage topology is ready"
    );
    let sessions = Arc::new(ActiveSessions::default());
    let unified_store = Some(Arc::clone(&selected_storage.session_store));
    let cognitive: Option<Arc<CognitiveContextManager>> = match &config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager over selected storage...");
            let sqlite_auxiliaries =
                selected_storage.backend == runtime::StorageBackendSelection::Sqlite;
            Some(Arc::new(
                CognitiveContextManager::new_with_selected_store_and_auxiliaries(
                    mem_cfg.clone(),
                    Some(workspace_root.clone()),
                    unified_store.clone(),
                    Arc::clone(&selected_storage.memory_store),
                    sqlite_auxiliaries,
                )
                .await
                .map_err(|error| format!("memory manager initialization failed: {error}"))?,
            ))
        }
        None => None,
    };
    let event_bus = SessionEventBus::new();
    let lease_registry = Arc::new(SessionLeaseRegistry::default());
    let lifecycle_kernel = Arc::new(SessionLifecycleKernel::with_store(Arc::clone(
        &selected_storage.session_store,
    )));
    let session_kernel = Arc::new(SessionKernel::new(
        sessions.clone(),
        unified_store.clone(),
        event_bus.clone(),
    ));
    // Authentication starts before the concrete HTTP host exists.  It uses
    // exactly the same enabled-APP set that will later build AppRegistry, so
    // a disabled APP cannot remain present in the broker's capability
    // catalogue.
    let auth_catalog = auth_broker::AuthorizationCatalog::from_app_descriptors(
        crate::services::enabled_app_descriptors(runtime_config.apps()),
    )
    .map_err(|error| format!("failed to compose auth profile catalogue: {error}"))?;
    let mut auth_broker = config
        .auth_token
        .as_deref()
        .map(|credential| AuthBrokerProcess::start(&approval_dir, credential, &auth_catalog))
        .transpose()?;
    let provider_registry = Arc::new(
        runtime::ProviderRegistry::new(runtime_config.providers().clone()).map_err(|rejected| {
            format!(
                "failed to initialize provider registry: {}",
                rejected.diagnostics.errors.join("; ")
            )
        })?,
    );
    let upgrade_coordinator = Arc::new(runtime::UpgradeCoordinator::new());
    let runtime_bootstrap = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace_root,
        &runtime::ConfigLoader::new(&workspace_root, &approval_dir),
        &runtime_config,
    )
    .map_err(|error| format!("failed to build tool catalog: {error}"))?;
    let tools = Arc::new(runtime_bootstrap.tool_registry.clone());
    if let Some(mcp_state) = runtime_bootstrap.mcp_state {
        let _ = mcp_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shutdown();
    }
    let runtime_mcp_service =
        Arc::new(RuntimeMcpServiceAdapter::from_runtime_config(&runtime_config).await);
    let approval_ledger = Arc::clone(&selected_storage.approval_ledger);
    let approval_gate = Arc::new(runtime::approval_gate::SmartApprovalGate::new(
        Arc::new(
            runtime::permission_enforcer::DestructivePatternDetector::new(approval_dir.clone()),
        ),
        runtime::ApprovalConfig::default(),
        Arc::clone(&approval_ledger),
    ));
    let profile_manager = Arc::new(runtime::ProfileManager::from_config_home(&approval_dir));
    if let Err(e) = profile_manager.initialize() {
        tracing::warn!("failed to initialize profile manager: {e}");
    }
    if let Ok(requested_profile) = std::env::var("COWD_PROFILE") {
        if profile_manager.get_profile(&requested_profile).is_none() {
            if let Err(e) = profile_manager.create_profile(&requested_profile) {
                tracing::warn!("failed to create requested profile {requested_profile}: {e}");
            }
        }
        if let Err(e) = profile_manager.switch_profile(&requested_profile) {
            tracing::warn!("failed to activate requested profile {requested_profile}: {e}");
        }
    }
    let profile_id = profile_manager.active_id();
    let task_kernel = Arc::clone(&selected_storage.task_kernel);

    // Spawn background session cleanup (idle/expired session reaper)
    let lifecycle_config = SessionLifecycleConfig {
        idle_timeout: Some(Duration::from_secs(300)),
        max_ttl: Some(Duration::from_secs(86400)),
        max_active_sessions: 100,
        eviction_policy: EvictionPolicy::Lru,
        cleanup_interval: Duration::from_secs(300),
    };
    let lifecycle = Arc::new(SessionLifecycleManager::new(lifecycle_config));
    let static_webui =
        crate::gateway_static::resolve_static_webui_source(config.webui_dir.as_deref());
    let startup_diagnostics = build_startup_diagnostics(
        &config,
        &workspace_root,
        &approval_dir,
        &static_webui,
        cognitive.is_some(),
        unified_store.is_some(),
    );
    emit_startup_diagnostics(&startup_diagnostics);

    let surface_host = Arc::new(
        crate::surface_host::SurfaceHost::with_configs_and_message_store(
            crate::surface_host::default_surface_roots(&approval_dir),
            config.surface_runtime_configs.clone(),
            Arc::clone(&selected_storage.surface_messages),
        ),
    );
    if let Some(webui_dir) = static_webui.configured_path.as_deref() {
        if static_webui.available {
            surface_host.register_webui_static_resource(webui_dir);
        }
    }
    let surface_discovery = surface_host.discover();
    surface_host.start_monitor();
    tracing::info!(
        discovered = surface_discovery.discovered,
        failures = surface_discovery.failures.len(),
        roots = ?surface_discovery.roots,
        "surface host discovery completed"
    );
    let tool_host = Arc::new(tools::ToolHost::new(
        format!("workspace:{}", workspace_root.display()),
        &workspace_root,
        tools::ToolHostSnapshot::new(
            Arc::clone(&tools),
            Arc::new(tools::lsp_client::LspRegistry::new()),
            Some(runtime_mcp_service),
        ),
    ));
    let gateway_runtime_tool_host = Arc::new(
        crate::gateway_tool_executor::GatewayToolExecutor::from_tool_host(
            None,
            false,
            Arc::clone(&tool_host),
            None,
        ),
    );
    let runtime_tool_host: Arc<dyn runtime::RuntimeExecutionHost> =
        gateway_runtime_tool_host.clone();
    // The evaluator is assembled at the composition root. Its weak Runtime
    // reference is bound only after RuntimeServices exists, avoiding a
    // dependency cycle while keeping Gateway out of scoring and release
    // authorization.
    let evolution_runtime = Arc::new(OnceLock::<Weak<runtime::RuntimeServices>>::new());
    let evolution_scenarios = Arc::new(harness_eval::FileDefinitionEvolutionScenarioCatalog::new(
        approval_dir.join("runtime").join("evolution-scenarios"),
    ));
    let evolution_executor = Arc::new(GatewayEvolutionScenarioExecutor {
        runtime: Arc::clone(&evolution_runtime),
    });
    let evolution_eval_runner: Arc<dyn runtime::EvolutionEvalRunner> =
        Arc::new(harness_eval::DefinitionEvolutionEvalRunner::new(Arc::new(
            harness_eval::RuntimeDefinitionEvolutionWorkload::new(
                evolution_scenarios,
                evolution_executor,
            ),
        )));
    let mut runtime_services_builder =
        runtime::RuntimeServices::builder(&approval_dir, &workspace_root)
            .provider_registry(Arc::clone(&provider_registry))
            .tool_execution_host(runtime_tool_host)
            .runtime_event_store(Arc::clone(&selected_storage.runtime_event_store))
            .reality_recall_port(Arc::new(
                runtime::RealityRecallPort::with_fact_and_matrix_store(
                    &approval_dir,
                    Arc::clone(&selected_storage.fact_ledger),
                    Arc::clone(&selected_storage.matrix_store),
                ),
            ))
            .knowledge_activation(
                runtime::knowledge_activation::KnowledgeActivationRuntime::with_fabric(
                    selected_storage.knowledge_fabric.clone(),
                ),
            )
            .evolution_eval_runner(evolution_eval_runner)
            .mission_schedule_policy(
                runtime_config
                    .runtime_control()
                    .policy
                    .mission_schedule
                    .clone(),
            );
    let startup_skill_assets = crate::services::runtime_skill_assets_for_workspace(&workspace_root);
    runtime_services_builder =
        runtime_services_builder.skill_catalog(runtime::RuntimeSkillCatalog::new(
            startup_skill_assets.profiles,
            startup_skill_assets.prompt_assets,
        ));
    if let Some(memory_manager) = cognitive.as_ref() {
        runtime_services_builder =
            runtime_services_builder.memory_manager(Arc::clone(memory_manager));
    }
    if let Some(store) = unified_store.as_ref() {
        runtime_services_builder = runtime_services_builder.session_store(Arc::clone(store));
    }
    let runtime_services = runtime_services_builder
        .build()
        .map_err(|error| format!("failed to initialize runtime services: {error}"))?;
    evolution_runtime
        .set(Arc::downgrade(&runtime_services))
        .map_err(|_| "failed to bind Runtime evolution evaluation executor".to_string())?;
    run_legacy_execution_startup_migration(&runtime_services, &approval_dir)?;
    gateway_runtime_tool_host
        .bind_runtime_services(Arc::clone(&runtime_services))
        .map_err(|error| format!("failed to bind runtime services: {error}"))?;
    let startup_recovery = runtime_services
        .recover_execution_graphs_on_startup()
        .await
        .map_err(|error| format!("failed to recover execution graphs on startup: {error}"))?;
    emit_execution_startup_recovery(&startup_recovery);
    let runtime_service = Arc::new(
        RuntimeService::new(
            sessions.clone(),
            lease_registry.clone(),
            session_kernel.clone(),
            lifecycle_kernel.clone(),
            started_at,
            Arc::clone(&provider_registry),
            Arc::clone(&upgrade_coordinator),
            runtime_services,
        )
        .map_err(|error| format!("failed to initialize runtime session bridge: {error}"))?
        .with_tool_host(tool_host)
        .with_approval_gate(approval_gate.clone()),
    );
    let session_bridge_store = unified_store.clone().ok_or_else(|| {
        "durable UnifiedSessionStore is required for the Runtime session bridge".to_string()
    })?;
    let session_manager = Arc::new(crate::unified_session_manager::UnifiedSessionManager::new(
        Arc::clone(&runtime_service),
        Arc::clone(&lifecycle),
        100,
    ));
    let session_activation_port: Arc<dyn crate::runtime_service::SessionActivationPort> =
        session_manager.clone();
    runtime_service.install_session_activator(Arc::downgrade(&session_activation_port))?;
    let session_runtime_bridge = crate::session_runtime_bridge::SessionRuntimeBridge::start(
        Arc::clone(&runtime_service),
        session_bridge_store,
        Arc::clone(&event_bus),
    )?;
    let weak_runtime_service = Arc::downgrade(&runtime_service);
    upgrade_coordinator.register_collector(Arc::new(
        runtime::ClosureUpgradeInventoryCollector::new("active_turns", move || {
            weak_runtime_service.upgrade().map_or_else(
                || Ok(Vec::new()),
                |service| Ok(service.upgrade_turn_carriers()),
            )
        }),
    ));
    let config_reload = runtime_service.config_reload();
    let capacity_config = config
        .runtime_config
        .as_ref()
        .and_then(|value| value.pointer("/gateway/capacity"))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let services = crate::services::GatewayServices::new_with_session_manager_and_storage(
        Arc::clone(&runtime_service),
        task_kernel.clone(),
        surface_host.clone(),
        cognitive.clone(),
        approval_gate.clone(),
        approval_ledger,
        Arc::clone(&session_manager),
        &approval_dir,
        capacity_config,
        Arc::clone(&selected_storage),
    );
    let app_registry = crate::services::broker_backed_app_registry_with_storage(
        services.app_host_context(),
        runtime_config.apps(),
        selected_storage.registry.clone(),
        selected_storage.app_topology.clone(),
    )
    .map_err(|error| format!("failed to provision enabled APP storage: {error}"))?;
    let services = Arc::new(services.with_app_registry(app_registry));
    let _cleanup_handle =
        spawn_session_cleanup_task(Arc::clone(&session_manager), Duration::from_secs(300));
    tokio::spawn(async move {
        let summary = session_manager.recover_active_sessions().await;
        tracing::info!(
            discovered = summary.discovered,
            recovered = summary.recovered,
            already_active = summary.already_active,
            failed = summary.failed,
            "session startup recovery completed"
        );
        for failure in summary.failures {
            tracing::warn!(error = %failure, "session startup recovery item failed");
        }
    });

    let app_state = Arc::new(api_routes::AppState {
        tool_registry: tools.clone(),
        config: config.runtime_config.clone(),
        event_bus: event_bus.clone(),
        static_webui: static_webui.clone(),
        approval_gate: Some(approval_gate),
        auth_token: config.auth_token.clone(),
        workspace_root,
        config_home: approval_dir.clone(),
        profile_id,
        profile_manager,
        services,
        session_lease_registry: Some(lease_registry.clone()),
    });
    let mission_schedule_timer = spawn_runtime_schedule_timer(runtime_service.runtime_services());
    config_reload::initialize_config_reload_status(&config_reload, &app_state);
    let _config_reload_watcher = config_reload::spawn_config_reload_watcher(
        config_reload,
        app_state.clone(),
        Duration::from_secs(2),
    );
    crate::surface_host::spawn_surface_ingress_dispatcher(app_state.clone());

    // 2. Build HTTP router (reuse api_routes + SSE)
    let app = {
        let default_origins = [
            "http://localhost:8642",
            "http://127.0.0.1:8642",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
        ];
        let mut cors_origin_values: Vec<HeaderValue> = default_origins
            .iter()
            .filter_map(|origin| origin.parse::<HeaderValue>().ok())
            .collect();
        for origin in &config.cors_origins {
            if let Ok(hv) = origin.parse::<HeaderValue>() {
                cors_origin_values.push(hv);
            }
        }
        let cors = build_cors_layer(cors_origin_values);

        let router = api_routes::api_router(app_state.clone());
        let fallback_state = app_state.clone();
        if static_webui.available {
            tracing::info!(
                path = %static_webui
                    .configured_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                "serving configured WebUI assets through dynamic surface fallback"
            );
            router
                .fallback(move |uri: Uri| {
                    webui_static_fallback_handler(fallback_state.clone(), uri)
                })
                .layer(cors)
        } else {
            tracing::info!(
                status = %static_webui.status.as_str(),
                config_key = static_webui.config_key,
                "WebUI assets disabled; serving gateway API only"
            );
            router
                .fallback(move |uri: Uri| {
                    webui_static_fallback_handler(fallback_state.clone(), uri)
                })
                .layer(cors)
        }
    };

    // 3. HTTP listener
    let listener = TcpListener::bind(&config.http_addr)
        .await
        .map_err(|e| format!("failed to bind HTTP {}: {}", config.http_addr, e))?;
    tracing::info!("HTTP + SSE on {}", config.http_addr);

    if let Err(e) = std::fs::write(
        crate::server::addr_file(),
        format!("http://{}", config.http_addr),
    ) {
        tracing::warn!("failed to write addr file: {e}");
    }

    // 4. Edge surfaces/connectors are discovered and represented in SurfaceHost.
    // External sidecar process launch is driven by edge requests, not by runtime boot.

    // 5. HTTP server with graceful shutdown on SIGINT/SIGTERM
    let (shutdown_started_tx, mut shutdown_started_rx) = tokio::sync::watch::channel(false);
    let shutdown_signal = async move {
        #[cfg(unix)]
        {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(signal) => signal,
                    Err(error) => {
                        tracing::error!("failed to install SIGTERM handler: {error}");
                        return;
                    }
                };
            let mut sigint =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
                    Ok(signal) => signal,
                    Err(error) => {
                        tracing::error!("failed to install SIGINT handler: {error}");
                        return;
                    }
                };
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
                _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => tracing::info!("shutdown signal received"),
                Err(error) => tracing::error!("failed to install ctrl_c handler: {error}"),
            }
        }
        let _ = shutdown_started_tx.send(true);
    };

    let mut server = Box::pin(
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .into_future(),
    );
    let serve_result = tokio::select! {
        result = &mut server => result,
        changed = shutdown_started_rx.changed() => {
            if changed.is_err() || !*shutdown_started_rx.borrow() {
                server.await
            } else {
                const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
                match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, &mut server).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = GRACEFUL_SHUTDOWN_TIMEOUT.as_millis(),
                            "Gateway graceful shutdown deadline expired; closing long-lived HTTP/SSE connections"
                        );
                        Ok(())
                    }
                }
            }
        }
    };
    serve_result.map_err(|e| format!("HTTP server error: {e}"))?;

    // ── Cleanup after shutdown ──
    tracing::info!("cleaning up runtime host resources...");

    session_runtime_bridge.shutdown().await;
    mission_schedule_timer.abort();
    let _ = mission_schedule_timer.await;
    if let Some(broker) = auth_broker.as_mut() {
        broker.shutdown();
    }

    tracing::info!("surface host shutdown complete");

    // PID file is cleaned up by PidFileGuard drop
    tracing::info!("runtime host shutdown complete");
    Ok(())
}

/// The timer is an event source only. It claims due Mission schedules and
/// Managed Agent triggers, then sends both through Runtime's canonical
/// Binding/graph paths. GraphRunner and the Managed Agent dispatcher retain
/// ownership of all execution state, retry and terminal transitions.
fn spawn_runtime_schedule_timer(
    runtime_services: Arc<runtime::RuntimeServices>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let policy = runtime_services.mission_schedule_policy();
            if policy.enabled {
                if let Err(error) = runtime_services
                    .dispatch_due_mission_schedules(epoch_millis())
                    .await
                {
                    tracing::warn!(%error, "mission schedule timer dispatch failed");
                }
            }
            // Managed Agents have independent trigger definitions. They do
            // not become inert merely because Mission scheduling is disabled;
            // the existing interval is only the shared wake-up cadence.
            if let Err(error) = runtime_services
                .dispatch_managed_agents("gateway-runtime-scheduler", 16)
                .await
            {
                tracing::warn!(%error, "managed Agent timer dispatch failed");
            }
            tokio::time::sleep(Duration::from_millis(policy.tick_interval_ms)).await;
        }
    })
}

fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn run_legacy_execution_startup_migration(
    runtime_services: &Arc<runtime::RuntimeServices>,
    config_home: &std::path::Path,
) -> Result<(), String> {
    let migration_root = config_home.join("migrations");
    let legacy_inventory = migration_root.join("v3-active-inventory.json");
    let legacy_receipt = migration_root.join("v3-clean-shutdown-receipt.json");
    if !legacy_inventory.exists() && !legacy_receipt.exists() {
        return Ok(());
    }
    runtime_services
        .import_legacy_execution_receipt(&legacy_receipt, env!("CARGO_PKG_VERSION"))
        .map_err(|error| format!("legacy execution migration failed; startup blocked: {error}"))
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mcp::McpService;
    use memory::MemoryConfig;
    use std::fs;

    fn temp_webui_dir(label: &str) -> std::path::PathBuf {
        let unique = format!(
            "cowd-edge-webui-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        fs::create_dir_all(&dir).expect("create temp webui dir");
        dir
    }

    #[test]
    fn gateway_config_defaults() {
        let config = RuntimeHostConfig {
            http_addr: "0.0.0.0:8642".into(),
            memory_config: None,
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            webui_dir: None,
            cors_origins: vec![],
            auth_token: None,
        };
        assert_eq!(config.http_addr, "0.0.0.0:8642");
        assert!(config.memory_config.is_none());
        assert!(config.surface_configs.is_empty());
        assert!(config.auth_token.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn broker_cleanup_never_unlinks_a_new_socket_generation() {
        use std::os::unix::net::UnixListener;

        let root = temp_webui_dir("broker-socket-generation");
        let socket = root.join("broker.sock");
        let old_listener = UnixListener::bind(&socket).expect("old broker socket");
        let old_identity = socket_file_identity(&socket);
        drop(old_listener);
        fs::remove_file(&socket).expect("replace old broker socket");
        let new_listener = UnixListener::bind(&socket).expect("new broker socket");

        remove_socket_if_owned(&socket, old_identity);

        assert!(socket.exists(), "old broker cleanup removed the new socket");
        drop(new_listener);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_migration_missing_receipt_fails_and_blocks_runtime_mutation() {
        let root = temp_webui_dir("migration-missing-receipt");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(config_home.join("migrations")).unwrap();
        fs::write(
            config_home
                .join("migrations")
                .join("v3-active-inventory.json"),
            b"{}",
        )
        .unwrap();
        let services = runtime::RuntimeServices::builder(&config_home, &workspace)
            .build()
            .unwrap();

        let result = run_legacy_execution_startup_migration(&services, &config_home);

        assert!(result.unwrap_err().contains("startup blocked"));
        assert!(matches!(
            services.ensure_mutation_allowed(),
            Err(runtime::RuntimeServicesError::UpgradeRecoveryRequired)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_mcp_service_projects_configured_servers() {
        let root = temp_webui_dir("mcp-config");
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");
        fs::write(
            home.join("config.yaml"),
            r#"{
              "mcpServers": {
                "remote-server": {
                  "type": "http",
                  "url": "https://example.test/mcp"
                }
              }
            }"#,
        )
        .expect("write config");

        let config = runtime::ConfigLoader::new(&cwd, &home)
            .load()
            .expect("load runtime config");
        let service = RuntimeMcpServiceAdapter::from_runtime_config(&config).await;
        let servers = service.list_servers().expect("list servers");

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "remote-server");
        assert_eq!(servers[0].status, "error");
    }

    #[tokio::test]
    async fn session_lease_rejects_conflicting_exclusive_owner_and_allows_takeover() {
        let registry = SessionLeaseRegistry::default();
        let first = registry.acquire("s1", "tui:1", "exclusive").await;
        assert_eq!(first.get("ok").and_then(|v| v.as_bool()), Some(true));

        let second = registry.acquire("s1", "tui:2", "exclusive").await;
        assert_eq!(second.get("ok").and_then(|v| v.as_bool()), Some(false));
        assert_eq!(second.get("owner").and_then(|v| v.as_str()), Some("tui:1"));

        let takeover = registry.acquire("s1", "tui:2", "takeover").await;
        assert_eq!(takeover.get("ok").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            takeover.get("owner").and_then(|v| v.as_str()),
            Some("tui:2")
        );
        assert_eq!(
            takeover.get("mode").and_then(|v| v.as_str()),
            Some("exclusive")
        );
    }

    #[test]
    fn gateway_config_with_auth() {
        let config = RuntimeHostConfig {
            http_addr: "127.0.0.1:9000".into(),
            memory_config: None,
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            webui_dir: None,
            cors_origins: vec!["http://localhost:3000".into()],
            auth_token: Some("secret-token".into()),
        };
        assert_eq!(config.http_addr, "127.0.0.1:9000");
        assert_eq!(config.auth_token.as_deref(), Some("secret-token"));
        assert_eq!(config.cors_origins, vec!["http://localhost:3000"]);
    }

    #[test]
    fn gateway_config_with_memory() {
        let mem_cfg = MemoryConfig::default();
        let config = RuntimeHostConfig {
            http_addr: "0.0.0.0:8642".into(),
            memory_config: Some(mem_cfg),
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            webui_dir: None,
            cors_origins: vec![],
            auth_token: None,
        };
        assert!(config.memory_config.is_some());
    }

    #[test]
    fn loopback_web_origin_allows_local_webui_ports() {
        for origin in [
            "http://127.0.0.1:9241",
            "http://127.1.2.3:5173",
            "http://localhost:5173",
            "https://localhost:3000",
            "http://[::1]:9241",
        ] {
            let value = HeaderValue::from_str(origin).expect("valid origin");
            assert!(is_loopback_web_origin(&value), "{origin} should be allowed");
        }
    }

    #[test]
    fn loopback_web_origin_rejects_non_local_hosts() {
        for origin in [
            "http://127.0.0.1.evil.test:9241",
            "http://localhost.evil.test:5173",
            "http://192.168.1.10:9241",
            "file://127.0.0.1/index.html",
        ] {
            let value = HeaderValue::from_str(origin).expect("valid origin");
            assert!(
                !is_loopback_web_origin(&value),
                "{origin} should be rejected"
            );
        }
    }

    #[test]
    fn startup_diagnostics_expose_capability_state_without_secret_values() {
        let webui_dir = temp_webui_dir("diagnostics");
        fs::write(webui_dir.join("index.html"), "<!doctype html>").expect("write index");
        let workspace = std::env::temp_dir().join("cowd-diagnostics-workspace");
        let config_home = std::env::temp_dir().join("cowd-diagnostics-config");
        let surface = SurfaceManifest {
            schema: surface::SURFACE_PROTOCOL.to_string(),
            id: "feishu".to_string(),
            name: "Feishu Message Connector".to_string(),
            version: "0.1.0".to_string(),
            kind: surface::SurfaceKind::MessageConnector,
            runtime: Some(surface::SurfaceRuntimeSpec::Managed {
                artifact: "cowd-edge-open-platform-message".to_string(),
                driver_profile: "feishu-message".to_string(),
                transport: surface::SurfaceTransport::UdsHttp2,
            }),
            capabilities: vec![
                "message.ingress".to_string(),
                "message.delivery".to_string(),
            ],
            routes: Vec::new(),
            resources: Vec::new(),
            health: surface::SurfaceHealthSpec {
                mode: surface::SurfaceHealthMode::Jsonl,
                interval_ms: 30_000,
                ..Default::default()
            },
            config_schema: serde_json::json!({"required": ["app_id", "app_secret"]}),
            default_enabled: true,
        };
        let config = RuntimeHostConfig {
            http_addr: "127.0.0.1:9864".into(),
            memory_config: Some(MemoryConfig::default()),
            surface_configs: vec![surface],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: Some(serde_json::json!({"model": "test-model"})),
            webui_dir: Some(webui_dir.clone()),
            cors_origins: vec!["http://localhost:3000".into()],
            auth_token: Some("do-not-log-this-token".into()),
        };

        let static_webui =
            crate::gateway_static::resolve_static_webui_source(config.webui_dir.as_deref());
        let diagnostics =
            build_startup_diagnostics(&config, &workspace, &config_home, &static_webui, true, true);
        let serialized = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");

        assert_eq!(diagnostics.http_addr, "127.0.0.1:9864");
        assert!(diagnostics.webui_available);
        assert!(diagnostics.memory_enabled);
        assert!(diagnostics.memory_available);
        assert!(diagnostics.unified_store_available);
        assert!(diagnostics.runtime_config_loaded);
        assert!(diagnostics.auth_required);
        assert_eq!(diagnostics.surface_count, 1);
        assert_eq!(diagnostics.surfaces[0].surface_id, "feishu");
        assert_eq!(diagnostics.surfaces[0].capability_count, 2);
        assert!(!serialized.contains("do-not-log-this-secret"));
        assert!(!serialized.contains("do-not-log-this-token"));

        let _ = fs::remove_dir_all(&webui_dir);
    }

    #[test]
    fn configured_webui_dir_with_index_is_ready() {
        let dir = temp_webui_dir("configured");
        fs::write(dir.join("index.html"), "<!doctype html>").expect("write index");

        let source = crate::gateway_static::resolve_static_webui_source(Some(&dir));

        assert!(source.available);
        assert_eq!(source.configured_path.as_deref(), Some(dir.as_path()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn configured_webui_dir_without_index_is_optional() {
        let dir = temp_webui_dir("missing-index");

        let source = crate::gateway_static::resolve_static_webui_source(Some(&dir));

        assert!(!source.required);
        assert!(!source.available);
        assert_eq!(
            source.status,
            crate::gateway_static::StaticWebUiStatus::MissingIndex
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
