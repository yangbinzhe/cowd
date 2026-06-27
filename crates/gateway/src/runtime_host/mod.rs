// ── Gateway RuntimeHost ────────────────────────────────────
// Gateway foreground mode is a gateway process with an internal runtime host providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Surface registry (builtin TUI/WebUI plus external JSONL sidecars)
// Shared state: ActiveSessions, CognitiveContextManager, GlobalToolRegistry, SessionEventBus

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::HeaderValue;
use axum::{http::StatusCode, response::IntoResponse};
use serde::Serialize;
use session::SessionLeaseRegistry;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

use crate::api_routes;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use crate::runtime_service::RuntimeService;
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::SessionLifecycleKernel;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use memory::UnifiedSessionStore;
use runtime::mcp_tool_bridge::{McpConnectionStatus, McpToolInfo, McpToolRegistry};
use runtime::{McpServerManager, RuntimeConfig};
use surface::SurfaceManifest;
use tools::GlobalToolRegistry;

use runtime::session_lifecycle::{
    EvictionPolicy, SessionLifecycleConfig, SessionLifecycleManager, SessionStatus,
};

#[derive(Clone, Default)]
struct RuntimeMcpServiceAdapter {
    registry: McpToolRegistry,
}

impl RuntimeMcpServiceAdapter {
    async fn from_runtime_config(config: &RuntimeConfig) -> Self {
        let registry = McpToolRegistry::new();
        for (server_name, server_config) in config.mcp().servers() {
            let single_server = BTreeMap::from([(server_name.clone(), server_config.clone())]);
            let mut manager = McpServerManager::from_servers(&single_server);
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

            let discovery = manager.discover_tools_best_effort().await;
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
            if status == McpConnectionStatus::Connected {
                registry.set_server_manager(server_name, manager);
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
            "status": "reload_not_required",
            "source": "runtime_mcp_service_adapter"
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
    active_sessions: Arc<ActiveSessions>,
    lifecycle: Arc<SessionLifecycleManager>,
    unified_store: Option<Arc<UnifiedSessionStore>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            // Run lifecycle's internal TTL/idle/eviction checks first
            lifecycle.run_cleanup().await;
            let ids = active_sessions.list();
            for id in &ids {
                if let Some(status) = lifecycle.check_session(id).await {
                    if matches!(
                        status,
                        SessionStatus::Expired | SessionStatus::Idle | SessionStatus::Evicted
                    ) {
                        tracing::info!(session_id=%id, ?status, "cleanup: closing session");
                        if let Some(entry) = active_sessions.get(id) {
                            let entry = entry.clone();
                            let store = unified_store.clone();
                            let id = id.clone();
                            tokio::task::spawn_blocking(move || {
                                let handle = tokio::runtime::Handle::current();
                                handle.block_on(async {
                                    let mut runtime = entry.lock().await;
                                    // Shutdown MCP and plugins before dropping
                                    let _ = runtime.shutdown_mcp();
                                    let _ = runtime.shutdown_plugins();
                                    drop(runtime);
                                    if let Some(ref store) = store {
                                        let _ = store.delete_session(&id);
                                    }
                                });
                            })
                            .await
                            .ok();
                        }
                        active_sessions.remove(id);
                        lifecycle.unregister(id).await;
                    }
                } else {
                    // Session tracked in active_sessions but not in lifecycle
                    active_sessions.remove(id);
                }
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

    // 1. Initialise shared state
    let sessions = Arc::new(ActiveSessions::default());
    let tools = Arc::new(GlobalToolRegistry::builtin());

    let cognitive: Option<Arc<CognitiveContextManager>> = match &config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager...");
            match CognitiveContextManager::new(mem_cfg.clone()).await {
                Ok(manager) => Some(Arc::new(manager)),
                Err(err) => {
                    tracing::error!(error = %err, "memory manager initialisation failed");
                    None
                }
            }
        }
        None => None,
    };

    let event_bus = SessionEventBus::new();
    let lease_registry = Arc::new(SessionLeaseRegistry::default());
    let lifecycle_kernel = Arc::new(SessionLifecycleKernel::new());

    let unified_store = crate::get_unified_store().ok().map(|s| Arc::new(s.clone()));
    let session_kernel = Arc::new(SessionKernel::new(
        sessions.clone(),
        unified_store.clone(),
        event_bus.clone(),
    ));
    let approval_dir = std::env::var_os("COWD_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".cowd"));
    let workspace_root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let runtime_config = runtime::ConfigLoader::new(&workspace_root, &approval_dir)
        .load()
        .unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to load runtime config for MCP service");
            RuntimeConfig::empty()
        });
    let mcp_service =
        Arc::new(RuntimeMcpServiceAdapter::from_runtime_config(&runtime_config).await);
    let _ = tools::set_mcp_service(mcp_service);
    let storage_config = storage::StorageConfig::default_for_config_home(&approval_dir);
    storage_config
        .layout
        .ensure_directories()
        .map_err(|e| format!("failed to initialize storage layout: {e}"))?;
    let approval_history_path = storage_config
        .layout
        .file_path("approval_history")
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| approval_dir.join("approval_history.json"));
    let approval_repository =
        approval::FileApprovalRepository::from_storage_layout(&storage_config.layout)
            .map_err(|e| format!("failed to initialize approval repository: {e}"))?;
    let approval_gate = Arc::new(runtime::approval_gate::SmartApprovalGate::new(
        Arc::new(
            runtime::permission_enforcer::DestructivePatternDetector::new(approval_dir.clone()),
        ),
        runtime::ApprovalConfig::default(),
        Some(approval_history_path),
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
    let task_kernel = Arc::new(
        crate::gateway_storage::GatewayStorage::open_task_kernel(&approval_dir)
            .map_err(|e| format!("failed to initialize task kernel: {e}"))?,
    );

    // Spawn background session cleanup (idle/expired session reaper)
    let lifecycle_config = SessionLifecycleConfig {
        idle_timeout: Some(Duration::from_secs(300)),
        max_ttl: Some(Duration::from_secs(86400)),
        max_active_sessions: 100,
        eviction_policy: EvictionPolicy::Lru,
        cleanup_interval: Duration::from_secs(300),
    };
    let lifecycle = Arc::new(SessionLifecycleManager::new(lifecycle_config));
    let _cleanup_handle = spawn_session_cleanup_task(
        sessions.clone(),
        lifecycle,
        unified_store.clone(),
        Duration::from_secs(300),
    );

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

    let surface_host = Arc::new(crate::surface_host::SurfaceHost::default_for_with_configs(
        &approval_dir,
        config.surface_runtime_configs.clone(),
    ));
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
    let services = Arc::new(crate::services::GatewayServices::new(
        Arc::new(RuntimeService::new(
            sessions.clone(),
            lease_registry.clone(),
            session_kernel.clone(),
            lifecycle_kernel.clone(),
            started_at,
        )),
        task_kernel.clone(),
        surface_host.clone(),
        cognitive.clone(),
        approval_gate.clone(),
        approval_repository,
    ));

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
        services: services,
        session_lease_registry: Some(lease_registry.clone()),
    });
    crate::surface_ingress::spawn_surface_ingress_dispatcher(app_state.clone());

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
        if let (true, Some(webui_dir), Some(index_path)) = (
            static_webui.available,
            static_webui.configured_path.clone(),
            static_webui.index_path.clone(),
        ) {
            tracing::info!(path = %webui_dir.display(), "serving configured WebUI assets");
            router
                .fallback_service(ServeDir::new(webui_dir).fallback(ServeFile::new(index_path)))
                .layer(cors)
        } else {
            tracing::info!(
                status = %static_webui.status.as_str(),
                config_key = static_webui.config_key,
                "WebUI assets disabled; serving gateway API only"
            );
            router.fallback(webui_not_configured_handler).layer(cors)
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

    // 4. Surface sidecars are discovered and represented in SurfaceHost. External
    // sidecar process launch is driven by surface requests, not by runtime boot.

    // 5. HTTP server with graceful shutdown on SIGINT/SIGTERM
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("failed to install SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
                _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install ctrl_c handler");
            tracing::info!("shutdown signal received");
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(|e| format!("HTTP server error: {e}"))?;

    // ── Cleanup after shutdown ──
    tracing::info!("cleaning up runtime host resources...");

    tracing::info!("surface host shutdown complete");

    // PID file is cleaned up by PidFileGuard drop
    tracing::info!("runtime host shutdown complete");
    Ok(())
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
            "cowd-surface-webui-{label}-{}-{}",
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
            name: "Feishu Surface".to_string(),
            version: "0.1.0".to_string(),
            kind: surface::SurfaceKind::ExternalIntegration,
            entry: Some("./cowd-surface-feishu".to_string()),
            transport: surface::SurfaceTransport::StdioJsonl,
            lifecycle: surface::SurfaceLifecycle::Managed,
            capabilities: vec!["ingress".to_string(), "delivery".to_string()],
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
