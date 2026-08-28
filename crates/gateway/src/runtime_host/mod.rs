// ── Gateway RuntimeHost ────────────────────────────────────
// Gateway foreground mode is a gateway process with an internal runtime host providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Surface registry (builtin TUI/WebUI plus external JSONL sidecars)
// Shared state: HotSessionPool, CognitiveContextManager, ToolCatalog, SessionProjectionHub

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::{
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use chrono::Timelike;
use fs2::FileExt;
use serde::Serialize;
use session::SessionLeaseRegistry;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::api_routes;
use crate::event_bus::SessionProjectionHub;
use crate::gateway::HotSessionPool;
use crate::runtime_service::RuntimeService;
use crate::services::session_service::presence::SessionPresenceLedger;
use crate::services::session_service::repository::SessionRepository;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use runtime::mcp_tool_bridge::{McpConnectionStatus, McpToolInfo, McpToolRegistry};
use runtime::{McpServerManager, RuntimeConfig};
use surface::SurfaceManifest;

use runtime::session_lifecycle::{
    EvictionPolicy, SessionLifecycleConfig, SessionWorkingSetManager,
};

fn configured_runtime_permission_mode(config: &RuntimeConfig) -> runtime::PermissionMode {
    match config.permission_mode() {
        Some(runtime::ResolvedPermissionMode::ReadOnly) => runtime::PermissionMode::ReadOnly,
        Some(runtime::ResolvedPermissionMode::WorkspaceWrite) => {
            runtime::PermissionMode::WorkspaceWrite
        }
        Some(runtime::ResolvedPermissionMode::DangerFullAccess) => {
            runtime::PermissionMode::DangerFullAccess
        }
        None => runtime::PermissionMode::WorkspaceWrite,
    }
}

fn delay_until_local_hour(hour: u8) -> Duration {
    let now = chrono::Local::now();
    let now_seconds =
        u64::from(now.hour()) * 3_600 + u64::from(now.minute()) * 60 + u64::from(now.second());
    delay_until_hour_from(now_seconds, hour)
}

fn delay_until_hour_from(now_seconds: u64, hour: u8) -> Duration {
    let target_seconds = u64::from(hour.min(23)) * 3_600;
    let delay = if target_seconds > now_seconds {
        target_seconds - now_seconds
    } else {
        86_400 - now_seconds + target_seconds
    };
    Duration::from_secs(delay.max(60))
}

fn start_memory_governance_task(
    gateway_tasks: &Arc<GatewayRuntimeTaskSet>,
    manager: Arc<CognitiveContextManager>,
    knowledge: memory::KnowledgeFabric,
    policy: memory::GovernanceConfig,
    semantic_resolver: Arc<dyn memory::SemanticGovernanceResolver>,
) -> Result<(), String> {
    if !policy.enabled {
        return Ok(());
    }
    gateway_tasks
        .spawn(
            GatewayTaskKind::MemoryGovernance,
            None,
            move |cancellation| {
                let manager = Arc::clone(&manager);
                let semantic_resolver = Arc::clone(&semantic_resolver);
                async move {
                    let initial_delay = Duration::from_secs(policy.startup_delay_secs);
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        () = tokio::time::sleep(initial_delay) => {}
                    }
                    match memory::run_automatic_governance(
                        Arc::clone(&manager),
                        Some(&knowledge),
                        &policy,
                        memory::AutomaticGovernanceMode::Startup,
                    )
                    .await
                    {
                        Ok(report) => tracing::info!(
                            scanned_entries = report.scanned_entries,
                            scanned_candidates = report.scanned_candidates,
                            auto_applied = report.auto_applied_duplicates
                                + report.auto_resolved_conflicts
                                + report.auto_archived_stale
                                + report.auto_validated_authority
                                + report.auto_refreshed_relationships
                                + report.auto_dismissed_obsolete
                                + report.auto_retired_knowledge_conflicts,
                            pending_review = report.pending_human_review,
                            errors = report.errors.len(),
                            "startup memory governance completed"
                        ),
                        Err(memory::MemoryError::GovernanceAlreadyRunning) => tracing::info!(
                            "startup memory governance skipped because another run is active"
                        ),
                        Err(error) => tracing::warn!(%error, "startup memory governance degraded"),
                    }
                    loop {
                        let delay = delay_until_local_hour(policy.deep_scan_hour_local);
                        tokio::select! {
                            _ = cancellation.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                        match memory::run_automatic_governance_with_resolver(
                            Arc::clone(&manager),
                            Some(&knowledge),
                            &policy,
                            memory::AutomaticGovernanceMode::Nightly,
                            Some(semantic_resolver.as_ref()),
                        )
                        .await
                        {
                            Ok(report) => tracing::info!(
                                scanned_entries = report.scanned_entries,
                                scanned_candidates = report.scanned_candidates,
                                pending_review = report.pending_human_review,
                                errors = report.errors.len(),
                                "nightly memory governance completed"
                            ),
                            Err(memory::MemoryError::GovernanceAlreadyRunning) => tracing::info!(
                                "nightly memory governance skipped because another run is active"
                            ),
                            Err(error) => {
                                tracing::warn!(%error, "nightly memory governance degraded")
                            }
                        }
                    }
                }
            },
        )
        .map(|_| ())
        .map_err(|error| format!("failed to start memory governance task: {error}"))
}

pub mod config_reload;
pub(crate) mod memory_governance;
pub(crate) mod task_set;

use task_set::{GatewayRuntimeTaskSet, GatewayTaskKind};

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
    socket_generation: Option<SocketGenerationOwnership>,
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
        let socket_generation = SocketGenerationOwnership::new(&socket_path);
        let generation_lock = open_socket_generation_lock(&socket_generation)?;
        generation_lock
            .lock_exclusive()
            .map_err(|error| format!("failed to lock auth broker socket generation: {error}"))?;
        publish_socket_generation_locked(&socket_path, &socket_generation)?;
        let catalog_path = auth_broker::catalog_file(&root);
        auth_broker::write_catalog(&catalog_path, catalog)
            .map_err(|error| format!("failed to write auth broker profile catalogue: {error}"))?;
        let child = sandbox_launcher::cowd_internal_process_command()?
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
        let mut broker = Self {
            child,
            socket_path,
            socket_generation: None,
        };
        let startup = (|| {
            let mut stdin = broker
                .child
                .stdin
                .take()
                .ok_or_else(|| "auth broker stdin is unavailable".to_string())?;
            stdin
                .write_all(credential.as_bytes())
                .and_then(|_| stdin.write_all(b"\n"))
                .and_then(|_| stdin.flush())
                .map_err(|error| format!("failed to enroll auth broker: {error}"))?;
            drop(stdin);

            let client = auth_broker::BrokerClient::new(&broker.socket_path);
            for _ in 0..40 {
                if client.trust_metadata().is_ok() {
                    return Ok(());
                }
                if let Some(status) = broker
                    .child
                    .try_wait()
                    .map_err(|error| format!("failed to inspect auth broker: {error}"))?
                {
                    return Err(format!("auth broker exited during startup: {status}"));
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err("auth broker did not become ready within one second".to_string())
        })();
        if let Err(error) = startup {
            remove_socket_generation_locked(&broker.socket_path, &socket_generation);
            let _ = FileExt::unlock(&generation_lock);
            return Err(error);
        }
        FileExt::unlock(&generation_lock)
            .map_err(|error| format!("failed to unlock auth broker socket generation: {error}"))?;
        broker.socket_generation = Some(socket_generation);
        Ok(broker)
    }

    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(generation) = self.socket_generation.take() {
            remove_socket_if_owned(&self.socket_path, &generation);
        }
    }
}

impl Drop for AuthBrokerProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone)]
struct SocketGenerationOwnership {
    token: String,
    owner_path: PathBuf,
    lock_path: PathBuf,
}

impl SocketGenerationOwnership {
    fn new(socket_path: &Path) -> Self {
        Self::with_token(socket_path, uuid::Uuid::new_v4().simple().to_string())
    }

    fn with_token(socket_path: &Path, token: String) -> Self {
        Self {
            token,
            owner_path: socket_sidecar_path(socket_path, ".owner"),
            lock_path: socket_sidecar_path(socket_path, ".lock"),
        }
    }
}

fn socket_sidecar_path(socket_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = socket_path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn open_socket_generation_lock(
    generation: &SocketGenerationOwnership,
) -> Result<std::fs::File, String> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&generation.lock_path)
        .map_err(|error| format!("failed to open auth broker socket generation lock: {error}"))
}

fn publish_socket_generation_locked(
    socket_path: &Path,
    generation: &SocketGenerationOwnership,
) -> Result<(), String> {
    if std::fs::symlink_metadata(socket_path).is_ok() {
        std::fs::remove_file(socket_path)
            .map_err(|error| format!("failed to remove stale auth broker socket: {error}"))?;
    }
    std::fs::write(&generation.owner_path, generation.token.as_bytes())
        .map_err(|error| format!("failed to publish auth broker socket generation: {error}"))?;
    std::fs::set_permissions(
        &generation.owner_path,
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .map_err(|error| format!("failed to protect auth broker socket generation: {error}"))
}

fn remove_socket_generation_locked(path: &Path, generation: &SocketGenerationOwnership) {
    let owns_generation = std::fs::read_to_string(&generation.owner_path)
        .is_ok_and(|owner| owner == generation.token);
    if owns_generation {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(&generation.owner_path);
    }
}

fn remove_socket_if_owned(path: &Path, generation: &SocketGenerationOwnership) {
    let Ok(generation_lock) = open_socket_generation_lock(generation) else {
        return;
    };
    if generation_lock.lock_exclusive().is_err() {
        return;
    }
    remove_socket_generation_locked(path, generation);
    let _ = FileExt::unlock(&generation_lock);
}

pub(crate) struct RuntimeMcpServiceAdapter {
    registry: McpToolRegistry,
    runtime_tools: Vec<tools::RuntimeToolDefinition>,
    pending_servers: Vec<String>,
    degraded: Option<runtime::McpDegradedReport>,
}

impl Drop for RuntimeMcpServiceAdapter {
    fn drop(&mut self) {
        if let Err(error) = self.registry.shutdown_all() {
            tracing::warn!(error = %error, "failed to shut down MCP service generation");
        }
    }
}

impl RuntimeMcpServiceAdapter {
    pub(crate) async fn from_runtime_config(config: &RuntimeConfig) -> Self {
        let config = config.clone();
        tokio::task::spawn_blocking(move || Self::build_from_runtime_config(&config))
            .await
            .unwrap_or_else(|error| {
                tracing::error!(%error, "MCP service construction task failed");
                Self {
                    registry: McpToolRegistry::new(),
                    runtime_tools: Vec::new(),
                    pending_servers: Vec::new(),
                    degraded: None,
                }
            })
    }

    fn build_from_runtime_config(config: &RuntimeConfig) -> Self {
        let registry = McpToolRegistry::new();
        let mut runtime_tools = Vec::new();
        let mut has_managed_server = false;
        let mut pending_servers = Vec::new();
        let mut working_servers = Vec::new();
        let mut failed_servers = Vec::new();
        let mut available_tools = Vec::new();
        for (server_name, server_config) in config.mcp().servers() {
            let single_server = BTreeMap::from([(server_name.clone(), server_config.clone())]);
            let manager = McpServerManager::from_servers(&single_server);
            if manager.server_names().is_empty() {
                let unsupported = manager.unsupported_servers().first();
                pending_servers.push(server_name.clone());
                failed_servers.push(runtime::McpFailedServer {
                    server_name: server_name.clone(),
                    phase: runtime::McpLifecyclePhase::ServerRegistration,
                    error: runtime::McpErrorSurface::new(
                        runtime::McpLifecyclePhase::ServerRegistration,
                        Some(server_name.clone()),
                        unsupported
                            .map(|server| server.reason.clone())
                            .unwrap_or_else(|| "unsupported MCP transport".to_string()),
                        unsupported
                            .map(|server| {
                                BTreeMap::from([(
                                    "transport".to_string(),
                                    format!("{:?}", server.transport).to_ascii_lowercase(),
                                )])
                            })
                            .unwrap_or_default(),
                        false,
                    ),
                });
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
            has_managed_server = true;

            let discovery = match registry.install_server_manager(server_name, manager) {
                Ok(discovery) => discovery,
                Err(error) => {
                    pending_servers.push(server_name.clone());
                    failed_servers.push(runtime::McpFailedServer {
                        server_name: server_name.clone(),
                        phase: runtime::McpLifecyclePhase::ToolDiscovery,
                        error: runtime::McpErrorSurface::new(
                            runtime::McpLifecyclePhase::ToolDiscovery,
                            Some(server_name.clone()),
                            error.clone(),
                            BTreeMap::new(),
                            true,
                        ),
                    });
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
            if let Some(failure) = failed {
                pending_servers.push(server_name.clone());
                failed_servers.push(runtime::McpFailedServer {
                    server_name: server_name.clone(),
                    phase: failure.phase,
                    error: runtime::McpErrorSurface::new(
                        failure.phase,
                        Some(server_name.clone()),
                        failure.error.clone(),
                        failure.context.clone(),
                        failure.recoverable,
                    ),
                });
            } else {
                working_servers.push(server_name.clone());
            }
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
            runtime_tools.extend(
                discovery
                    .tools
                    .iter()
                    .filter(|tool| tool.server_name == *server_name)
                    .map(crate::runtime_bootstrap::mcp_runtime_tool_definition),
            );
            available_tools.extend(
                discovery
                    .tools
                    .iter()
                    .filter(|tool| tool.server_name == *server_name)
                    .map(|tool| tool.qualified_name.clone()),
            );
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
        if has_managed_server {
            runtime_tools.extend(crate::runtime_bootstrap::mcp_wrapper_tool_definitions());
        }
        pending_servers.sort();
        pending_servers.dedup();
        let degraded = (!failed_servers.is_empty()).then(|| {
            runtime::McpDegradedReport::new(
                working_servers,
                failed_servers,
                available_tools.clone(),
                available_tools,
            )
        });
        Self {
            registry,
            runtime_tools,
            pending_servers,
            degraded,
        }
    }

    pub(crate) fn runtime_tool_definitions(&self) -> Vec<tools::RuntimeToolDefinition> {
        self.runtime_tools.clone()
    }

    pub(crate) fn shutdown(&self) -> Result<(), String> {
        self.registry.shutdown_all()
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
            "ok": self.pending_servers.is_empty(),
            "servers": servers.len(),
            "connected": servers.iter().filter(|server| server.status == McpConnectionStatus::Connected).count(),
            "pending_servers": self.pending_servers,
            "degraded": self.degraded,
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
            .read_resource_contents(server, uri)
            .map_err(mcp::McpServiceError::Request)?;
        Ok(mcp::McpResourceProjection {
            server: server.to_string(),
            uri: uri.to_string(),
            name: None,
            mime_type: resource
                .contents
                .first()
                .and_then(|content| content.mime_type.clone()),
            content: Some(serde_json::to_value(resource).map_err(|error| {
                mcp::McpServiceError::Request(format!(
                    "failed to serialize MCP resource contents: {error}"
                ))
            })?),
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
// ── Config ─────────────────────────────────────────────────────

pub struct RuntimeHostConfig {
    pub http_addr: String,
    pub workspace_root: PathBuf,
    pub memory_config: Option<MemoryConfig>,
    pub surface_configs: Vec<SurfaceManifest>,
    pub surface_runtime_configs: BTreeMap<String, serde_json::Value>,
    pub runtime_config: Option<serde_json::Value>,
    pub session_recovery: runtime::SessionRecoveryConfig,
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
    if is_api_path(uri.path()) {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({
                "ok": false,
                "error": "api_route_not_found",
                "path": uri.path(),
            })),
        )
            .into_response();
    }
    match state.services.surface.resolve_static("webui", uri.path()) {
        Ok(Some(file)) => match tokio::fs::read(&file.file_path).await {
            Ok(bytes) => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type_for_path(&file.file_path))
                .header(
                    header::CACHE_CONTROL,
                    crate::surface_host::cache_control_for_static_file(&file),
                )
                .body(axum::body::Body::from(bytes))
                .unwrap_or_else(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(serde_json::json!({
                            "ok": false,
                            "error": format!("failed to build WebUI response: {error}"),
                        })),
                    )
                        .into_response()
                }),
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

fn is_api_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
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
            if let Err(error) = std::fs::remove_file(&pid_path) {
                tracing::warn!(path = %pid_path.display(), %error, "failed to remove gateway PID file");
            }
            let addr_path = crate::server::addr_file();
            if let Err(error) = std::fs::remove_file(&addr_path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %addr_path.display(), %error, "failed to remove gateway address file");
                }
            }
            tracing::info!(path = %pid_path.display(), "PID file removed");
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayRuntimeShutdownReport {
    phases: Vec<String>,
    failures: Vec<String>,
    active_turns: Option<crate::runtime_service::ActiveTurnDrainReport>,
    memory: Option<memory::cognitive::MemoryBackgroundShutdownReport>,
    tasks: task_set::GatewayTaskShutdownReport,
}

#[derive(Default)]
struct GatewayRuntimeShutdownState {
    report: Option<GatewayRuntimeShutdownReport>,
}

/// Process-level owner for the complete Gateway close transaction.
///
/// `GatewayRuntimeTaskSet` owns leaf tasks; this coordinator serializes every
/// component phase around it. The first caller is the leader. Followers wait
/// on the same gate and receive the immutable report published by the leader.
struct GatewayRuntimeShutdownCoordinator {
    gateway_tasks: Arc<GatewayRuntimeTaskSet>,
    gate: tokio::sync::Mutex<()>,
    state: Mutex<GatewayRuntimeShutdownState>,
}

impl GatewayRuntimeShutdownCoordinator {
    fn new(gateway_tasks: Arc<GatewayRuntimeTaskSet>) -> Arc<Self> {
        Arc::new(Self {
            gateway_tasks,
            gate: tokio::sync::Mutex::new(()),
            state: Mutex::new(GatewayRuntimeShutdownState::default()),
        })
    }

    fn publish(&self, phase: &str, failures: &[String]) {
        self.gateway_tasks.observe_shutdown_phase(phase, failures);
    }

    fn completed_report(&self) -> Option<GatewayRuntimeShutdownReport> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .report
            .clone()
    }

    fn finish(&self, report: GatewayRuntimeShutdownReport) {
        let phase = if report.failures.is_empty() {
            "closed"
        } else {
            "closed_with_failures"
        };
        self.publish(phase, &report.failures);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .report = Some(report);
    }

    async fn coordinate<F, Fut>(&self, shutdown: F) -> GatewayRuntimeShutdownReport
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = GatewayRuntimeShutdownReport>,
    {
        let _leader = self.gate.lock().await;
        if let Some(report) = self.completed_report() {
            return report;
        }
        let report = shutdown().await;
        self.finish(report.clone());
        report
    }
}

struct GatewayRuntimeShutdownResources<'a> {
    session_worker_supervisor:
        Option<&'a Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>>,
    selected_storage: Option<&'a Arc<crate::selected_storage::SelectedStorageTopology>>,
    session_service: Option<&'a Arc<crate::services::SessionService>>,
    session_activation:
        Option<&'a Arc<crate::services::session_service::activation::SessionActivationCoordinator>>,
    runtime_service: Option<&'a Arc<RuntimeService>>,
    runtime_services: Option<&'a Arc<runtime::RuntimeServices>>,
    cognitive: Option<&'a Arc<CognitiveContextManager>>,
    surface_host: Option<&'a Arc<crate::surface_host::SurfaceHost>>,
    app_platform: Option<&'a Arc<crate::app_platform::GatewayAppPlatform>>,
    core_bridge: Option<&'a Arc<crate::app_platform::CoreBridgeServer>>,
    auth_broker: &'a Arc<tokio::sync::Mutex<Option<AuthBrokerProcess>>>,
}

struct GatewayRuntimeStartupRegistry {
    coordinator: Arc<GatewayRuntimeShutdownCoordinator>,
    auth_broker: Arc<tokio::sync::Mutex<Option<AuthBrokerProcess>>>,
    selected_storage: Option<Arc<crate::selected_storage::SelectedStorageTopology>>,
    cognitive: Option<Arc<CognitiveContextManager>>,
    surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    app_platform: Option<Arc<crate::app_platform::GatewayAppPlatform>>,
    core_bridge: Option<Arc<crate::app_platform::CoreBridgeServer>>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
    runtime_service: Option<Arc<RuntimeService>>,
    session_activation:
        Option<Arc<crate::services::session_service::activation::SessionActivationCoordinator>>,
    session_service: Option<Arc<crate::services::SessionService>>,
    session_worker_supervisor: Option<Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>>,
}

impl GatewayRuntimeStartupRegistry {
    fn new(coordinator: Arc<GatewayRuntimeShutdownCoordinator>) -> Self {
        Self {
            coordinator,
            auth_broker: Arc::new(tokio::sync::Mutex::new(None)),
            selected_storage: None,
            cognitive: None,
            surface_host: None,
            app_platform: None,
            core_bridge: None,
            runtime_services: None,
            runtime_service: None,
            session_activation: None,
            session_service: None,
            session_worker_supervisor: None,
        }
    }

    fn shutdown_resources(&self) -> GatewayRuntimeShutdownResources<'_> {
        GatewayRuntimeShutdownResources {
            session_worker_supervisor: self.session_worker_supervisor.as_ref(),
            selected_storage: self.selected_storage.as_ref(),
            session_service: self.session_service.as_ref(),
            session_activation: self.session_activation.as_ref(),
            runtime_service: self.runtime_service.as_ref(),
            runtime_services: self.runtime_services.as_ref(),
            cognitive: self.cognitive.as_ref(),
            surface_host: self.surface_host.as_ref(),
            app_platform: self.app_platform.as_ref(),
            core_bridge: self.core_bridge.as_ref(),
            auth_broker: &self.auth_broker,
        }
    }

    async fn shutdown(
        &self,
        initiating_failure: Option<(&'static str, String)>,
    ) -> GatewayRuntimeShutdownReport {
        shutdown_runtime_host_resources(
            &self.coordinator,
            self.shutdown_resources(),
            initiating_failure,
        )
        .await
    }

    async fn rollback(&self, error: String) -> String {
        self.shutdown(Some(("startup_rollback", error)))
            .await
            .failures
            .join("; ")
    }
}

fn record_task_shutdown_failures(
    phase: &str,
    report: &task_set::GatewayTaskShutdownReport,
    failures: &mut Vec<String>,
) {
    if report.panicked > 0 {
        failures.push(format!(
            "{phase} observed {} panicked task(s)",
            report.panicked
        ));
    }
    if report.forced_aborts > 0 {
        failures.push(format!(
            "{phase} required {} forced task abort(s)",
            report.forced_aborts
        ));
    }
}

async fn shutdown_runtime_host_resources(
    coordinator: &Arc<GatewayRuntimeShutdownCoordinator>,
    resources: GatewayRuntimeShutdownResources<'_>,
    initiating_failure: Option<(&'static str, String)>,
) -> GatewayRuntimeShutdownReport {
    coordinator
        .coordinate(|| async move {
            let gateway_tasks = &coordinator.gateway_tasks;
            let initial_phase = initiating_failure.as_ref().map(|(phase, _)| *phase);
            let mut failures = initiating_failure
                .into_iter()
                .map(|(_, failure)| failure)
                .collect::<Vec<_>>();
            let mut phases = Vec::new();
            let mut active_turn_report = None;
            let mut memory_report = None;
            let mut enter_phase = |phase: &str, failures: &[String]| {
                phases.push(phase.to_string());
                coordinator.publish(phase, failures);
            };
            if let Some(initial_phase) = initial_phase {
                enter_phase(initial_phase, &failures);
            }

            // Close every ingress fence before cancelling accepted work. Runtime turn
            // admission shares the same registry lock as turn insertion, so the
            // cancellation snapshot cannot race with a newly accepted turn.
            enter_phase("stop_accepting", &failures);
            gateway_tasks.stop_accepting();
            if let Some(supervisor) = resources.session_worker_supervisor {
                supervisor.stop_accepting();
            }
            let cancelled_turns = resources.runtime_service.map_or_else(Vec::new, |runtime| {
                runtime.stop_accepting_and_cancel_active_turns("Gateway process shutdown")
            });

            enter_phase("drain_ingress", &failures);
            let admission_report = gateway_tasks
                .cancel_and_drain_kinds(
                    &[
                        GatewayTaskKind::HttpServer,
                        GatewayTaskKind::ConfigReload,
                        GatewayTaskKind::SurfaceIngress,
                        GatewayTaskKind::SurfaceIngressWork,
                    ],
                    Duration::from_secs(10),
                )
                .await;
            record_task_shutdown_failures(
                "Gateway admission drain",
                &admission_report,
                &mut failures,
            );
            if admission_report.panicked > 0 || admission_report.forced_aborts > 0 {
                coordinator.publish("drain_ingress", &failures);
            }
            tracing::info!(
                joined = admission_report.joined,
                panicked = admission_report.panicked,
                forced_aborts = admission_report.forced_aborts,
                "Gateway admission tasks drained"
            );

            enter_phase("drain_surface", &failures);
            if let Some(surface_host) = resources.surface_host {
                if let Err(error) = surface_host.shutdown().await {
                    failures.push(format!("surface host shutdown incomplete: {error}"));
                    coordinator.publish("drain_surface", &failures);
                }
            }

            enter_phase("drain_apps", &failures);
            if let Some(core_bridge) = resources.core_bridge {
                if let Err(error) = core_bridge.shutdown().await {
                    failures.push(format!("CoreBridge shutdown incomplete: {error}"));
                    coordinator.publish("drain_apps", &failures);
                }
            }
            if let Some(platform) = resources.app_platform {
                if let Err(error) = platform.shutdown().await {
                    failures.push(format!("APP supervisor shutdown incomplete: {error}"));
                    coordinator.publish("drain_apps", &failures);
                }
            }

            enter_phase("drain_live_eval", &failures);
            let live_eval_report = gateway_tasks
                .cancel_and_drain_kinds(
                    &[
                        GatewayTaskKind::LiveSubscription,
                        GatewayTaskKind::EvalWorker,
                    ],
                    Duration::from_secs(30),
                )
                .await;
            record_task_shutdown_failures(
                "Gateway Live/Eval drain",
                &live_eval_report,
                &mut failures,
            );
            if live_eval_report.panicked > 0 || live_eval_report.forced_aborts > 0 {
                coordinator.publish("drain_live_eval", &failures);
            }
            tracing::info!(
                joined = live_eval_report.joined,
                panicked = live_eval_report.panicked,
                forced_aborts = live_eval_report.forced_aborts,
                "Live and Eval tasks drained"
            );

            enter_phase("drain_active_turns", &failures);
            if let Some(runtime_service) = resources.runtime_service {
                let report = runtime_service
                    .wait_for_active_turns(cancelled_turns.len(), Duration::from_secs(30))
                    .await;
                if !report.remaining_turn_ids.is_empty() {
                    failures.push(format!(
                        "Runtime active turn drain timed out with {} turn(s): {}",
                        report.remaining_turn_ids.len(),
                        report.remaining_turn_ids.join(",")
                    ));
                    coordinator.publish("drain_active_turns", &failures);
                }
                active_turn_report = Some(report);
            }

            enter_phase("drain_session_workers", &failures);
            if let Some(supervisor) = resources.session_worker_supervisor {
                supervisor.shutdown().await;
                let worker_health = supervisor.health();
                if worker_health.forced_aborts > 0 {
                    failures.push(format!(
                        "Session worker shutdown required {} forced abort(s)",
                        worker_health.forced_aborts
                    ));
                    coordinator.publish("drain_session_workers", &failures);
                }
                tracing::info!(
                    accepting = worker_health.accepting,
                    workers = worker_health.workers.len(),
                    forced_aborts = worker_health.forced_aborts,
                    "Session worker supervisor drained"
                );
            }

            // A Runtime carrier is removed only after its active turn guard reached
            // terminal state. On timeout retain the carrier and publish failure rather
            // than manufacturing an apparently clean close by dropping live state.
            enter_phase("unload_sessions", &failures);
            let active_turns_drained = active_turn_report
                .as_ref()
                .is_none_or(|report| report.remaining_turn_ids.is_empty());
            let active_session_ids = resources
                .session_service
                .map_or_else(Vec::new, |service| service.list_active_session_ids());
            if active_turns_drained {
                if let Some(session_activation) = resources.session_activation {
                    for session_id in &active_session_ids {
                        session_activation.unload_runtime(session_id).await;
                    }
                }
            }

            enter_phase("drain_runtime", &failures);
            let runtime_report = gateway_tasks
                .cancel_and_drain_kinds(
                    &[
                        GatewayTaskKind::RuntimeRestoration,
                        GatewayTaskKind::MissionSchedule,
                        GatewayTaskKind::MissionOrganizer,
                        GatewayTaskKind::MemoryGovernance,
                    ],
                    Duration::from_secs(10),
                )
                .await;
            record_task_shutdown_failures(
                "Gateway Runtime task drain",
                &runtime_report,
                &mut failures,
            );
            if runtime_report.panicked > 0 || runtime_report.forced_aborts > 0 {
                coordinator.publish("drain_runtime", &failures);
            }
            if let Some(runtime_services) = resources
                .runtime_service
                .map(|runtime| runtime.runtime_services())
                .or_else(|| resources.runtime_services.cloned())
            {
                let execution_report = runtime_services.shutdown_execution().await;
                if execution_report.forced_aborts > 0 {
                    failures.push(format!(
                        "Runtime execution shutdown required {} forced abort(s)",
                        execution_report.forced_aborts
                    ));
                }
                failures.extend(
                    execution_report
                        .errors
                        .iter()
                        .map(|error| format!("Runtime execution shutdown incomplete: {error}")),
                );
                runtime_services.shutdown_maintenance().await;
            }
            if let Some(cognitive) = resources.cognitive {
                let report = cognitive.shutdown_background_tasks().await;
                if report.forced_aborts > 0 {
                    failures.push(format!(
                        "Memory background shutdown required {} forced abort(s)",
                        report.forced_aborts
                    ));
                }
                failures.extend(
                    report
                        .errors
                        .iter()
                        .map(|error| format!("Memory background shutdown incomplete: {error}")),
                );
                if report.forced_aborts > 0 || !report.errors.is_empty() {
                    coordinator.publish("drain_runtime", &failures);
                }
                memory_report = Some(report);
            }
            if let Some(broker) = resources.auth_broker.lock().await.as_mut() {
                broker.shutdown();
            }
            tracing::info!(
                active_session_count = active_session_ids.len(),
                joined = runtime_report.joined,
                panicked = runtime_report.panicked,
                forced_aborts = runtime_report.forced_aborts,
                "Runtime restoration, scheduling, MCP and maintenance drained"
            );

            // Storage is the first durable resource constructed and the last
            // business dependency closed. Runtime maintenance, Memory and MCP
            // retain their journal ports until their workers have joined.
            enter_phase("drain_session_repository", &failures);
            if let Some(selected_storage) = resources.selected_storage {
                match selected_storage
                    .session_store
                    .shutdown_and_drain(Duration::from_secs(10))
                    .await
                {
                    Ok(repository_stats) => tracing::info!(
                        accepting = repository_stats.accepting,
                        drained = repository_stats.drained,
                        active = repository_stats.active,
                        queued = repository_stats.queued,
                        rejected = repository_stats.queue_rejected,
                        "Session repository shutdown complete"
                    ),
                    Err(error) => {
                        failures.push(format!("Session repository shutdown incomplete: {error}"));
                        coordinator.publish("drain_session_repository", &failures);
                    }
                }
            }

            enter_phase("drain_task_set", &failures);
            let gateway_task_report = gateway_tasks.shutdown().await;
            record_task_shutdown_failures(
                "Gateway final task-set drain",
                &gateway_task_report,
                &mut failures,
            );
            if gateway_task_report.panicked > 0 || gateway_task_report.forced_aborts > 0 {
                coordinator.publish("drain_task_set", &failures);
            }
            GatewayRuntimeShutdownReport {
                phases,
                failures,
                active_turns: active_turn_report,
                memory: memory_report,
                tasks: gateway_task_report,
            }
        })
        .await
}

// ── Gateway entry point ─────────────────────────────────────────

pub async fn run_gateway_runtime(config: RuntimeHostConfig) -> Result<(), String> {
    let started_at = Instant::now();
    // 0. Write PID file (removed on drop via guard)
    let _pid_guard = PidFileGuard::new()?;
    let gateway_tasks =
        crate::runtime_host::task_set::GatewayRuntimeTaskSet::new(Duration::from_secs(5));
    let shutdown_coordinator = GatewayRuntimeShutdownCoordinator::new(Arc::clone(&gateway_tasks));
    let mut startup_registry =
        GatewayRuntimeStartupRegistry::new(Arc::clone(&shutdown_coordinator));
    let auth_broker = Arc::clone(&startup_registry.auth_broker);

    // 1. Resolve configuration and compose every durable owner exactly once.
    let approval_dir = std::env::var_os("COWD_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".cowd"));
    let workspace_root = config.workspace_root.clone();
    let loaded =
        match runtime::ConfigLoader::new(&workspace_root, &approval_dir).load_with_diagnostics() {
            Ok(loaded) => loaded,
            Err(error) => {
                let error = format!("failed to load runtime configuration: {error}");
                return Err(startup_registry.rollback(error).await);
            }
        };
    let runtime_config = loaded.config;
    let config_diagnostics = loaded.diagnostics;
    for diagnostic in &config_diagnostics {
        tracing::warn!(code = %diagnostic.code, message = %diagnostic.message, "runtime config diagnostic");
    }
    let provider_registry = match runtime::ProviderRegistry::new(runtime_config.providers().clone())
    {
        Ok(registry) => Arc::new(registry),
        Err(rejected) => {
            let error = format!(
                "failed to initialize provider registry: {}",
                rejected.diagnostics.errors.join("; ")
            );
            return Err(startup_registry.rollback(error).await);
        }
    };
    let provider_transport_pool = Arc::new(runtime::ProviderTransportPool::default());
    let provider_template_cache = Arc::new(runtime::ProviderClientTemplateCache::default());
    let selected_storage =
        match crate::selected_storage::SelectedStorageTopology::compose_for_runtime(
            runtime_config.storage(),
            &approval_dir,
            &workspace_root,
        ) {
            Ok(storage) => Arc::new(storage),
            Err(error) => {
                let error = format!("failed to compose selected storage topology: {error}");
                return Err(startup_registry.rollback(error).await);
            }
        };
    startup_registry.selected_storage = Some(Arc::clone(&selected_storage));
    tracing::info!(
        backend = selected_storage.backend_label(),
        health = %selected_storage.health_projection(),
        "selected storage topology is ready"
    );
    let sessions = Arc::new(HotSessionPool::default());
    let unified_store = Some(Arc::clone(&selected_storage.session_store));
    let cognitive: Option<Arc<CognitiveContextManager>> = match &config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager over selected storage...");
            let sqlite_auxiliaries =
                selected_storage.backend == runtime::StorageBackendSelection::Sqlite;
            let llm_summarizer = if mem_cfg.compression.llm.is_configured() {
                match runtime::RuntimeMemorySummarizer::new(
                    Arc::clone(&provider_registry),
                    Arc::clone(&provider_transport_pool),
                    Arc::clone(&provider_template_cache),
                    mem_cfg.compression.llm.model.clone(),
                    2048,
                ) {
                    Ok(summarizer) => Some(Arc::new(summarizer)
                        as Arc<dyn memory::compression::llm_summarizer::LlmSummarizer>),
                    Err(error) => {
                        let error =
                            format!("failed to initialize Runtime Memory summarizer: {error}");
                        return Err(startup_registry.rollback(error).await);
                    }
                }
            } else {
                None
            };
            match CognitiveContextManager::new_with_selected_store_auxiliaries_and_summarizer(
                mem_cfg.clone(),
                Some(workspace_root.clone()),
                unified_store
                    .as_ref()
                    .map(|store| Arc::new(store.history_reader())),
                Arc::clone(&selected_storage.memory_store),
                sqlite_auxiliaries,
                Some(selected_storage.memory_maintenance_queue.clone()),
                llm_summarizer,
            )
            .await
            {
                Ok(cognitive) => Some(Arc::new(cognitive)),
                Err(error) => {
                    let error = format!("memory manager initialization failed: {error}");
                    return Err(startup_registry.rollback(error).await);
                }
            }
        }
        None => None,
    };
    startup_registry.cognitive.clone_from(&cognitive);
    let growth_projection_services = match crate::services::GrowthProjectionServices::selected(
        cognitive.clone(),
        selected_storage.as_ref(),
    ) {
        Ok(services) => services,
        Err(error) => {
            let error = format!("failed to compose Growth projection services: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    let memory_governance_task =
        cognitive
            .as_ref()
            .zip(config.memory_config.as_ref())
            .map(|(manager, memory_config)| {
                (
                    Arc::clone(manager),
                    selected_storage.knowledge_fabric.clone(),
                    memory_config.governance.clone(),
                )
            });
    let surface_host = Arc::new(
        crate::surface_host::SurfaceHost::with_configs_message_store_and_tasks(
            crate::surface_host::default_surface_roots(&approval_dir),
            config.surface_runtime_configs.clone(),
            Arc::clone(&selected_storage.surface_messages),
            Arc::clone(&gateway_tasks),
        ),
    );
    startup_registry.surface_host = Some(Arc::clone(&surface_host));
    let event_bus = SessionProjectionHub::new();
    let lease_registry = Arc::new(SessionLeaseRegistry::default());
    let presence_ledger = Arc::new(SessionPresenceLedger::with_store_and_ttl(
        Arc::clone(&selected_storage.session_store),
        std::time::Duration::from_secs(runtime_config.gateway().presence.ttl_seconds),
    ));
    let session_repository = Arc::new(SessionRepository::new(
        sessions.clone(),
        unified_store.clone(),
        event_bus.clone(),
    ));
    // APP discovery is a one-shot startup transaction. Its immutable admitted
    // manifests become the sole input to authentication and V1 routes.
    let app_platform =
        match crate::app_platform::GatewayAppPlatform::build(runtime_config.apps()).await {
            Ok(platform) => platform,
            Err(error) => {
                let error = format!("failed to compose dynamic APP platform: {error}");
                return Err(startup_registry.rollback(error).await);
            }
        };
    startup_registry.app_platform = Some(Arc::clone(&app_platform));
    let core_bridge = match crate::app_platform::CoreBridgeServer::start(
        runtime_config.apps().core_bridge_socket().to_path_buf(),
        Arc::clone(app_platform.core_bridge_registry()),
        Arc::clone(&selected_storage.matrix_store),
        Arc::clone(&selected_storage.runtime_event_store),
    )
    .await
    {
        Ok(server) => server,
        Err(error) => {
            let error = format!("failed to start CoreBridge: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    startup_registry.core_bridge = Some(Arc::clone(&core_bridge));
    let auth_catalog = match auth_broker::AuthorizationCatalog::from_app_manifests(
        app_platform.catalog().apps().map(|app| &app.manifest),
    ) {
        Ok(catalog) => catalog,
        Err(error) => {
            let error = format!("failed to compose auth profile catalogue: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    let started_auth_broker = match config
        .auth_token
        .as_deref()
        .map(|credential| AuthBrokerProcess::start(&approval_dir, credential, &auth_catalog))
        .transpose()
    {
        Ok(broker) => broker,
        Err(error) => {
            return Err(startup_registry.rollback(error).await);
        }
    };
    *auth_broker.lock().await = started_auth_broker;
    let upgrade_coordinator = Arc::new(runtime::UpgradeCoordinator::new());
    let mut runtime_bootstrap = match crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &workspace_root,
        &runtime::ConfigLoader::new(&workspace_root, &approval_dir),
        &runtime_config,
    ) {
        Ok(runtime_bootstrap) => runtime_bootstrap,
        Err(error) => {
            let error = format!("failed to build tool catalog: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    let runtime_mcp_service =
        Arc::new(RuntimeMcpServiceAdapter::from_runtime_config(&runtime_config).await);
    runtime_bootstrap.tool_registry = match runtime_bootstrap
        .tool_registry
        .clone()
        .extend_runtime_tools(runtime_mcp_service.runtime_tool_definitions())
    {
        Ok(tool_registry) => tool_registry,
        Err(error) => {
            let error = format!("failed to register MCP tool catalog: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    let runtime_session_bootstrap = runtime_bootstrap.session_snapshot();
    let tools = Arc::new(runtime_bootstrap.tool_registry.clone());
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
    // Spawn background session cleanup (idle/expired session reaper)
    let lifecycle_config = SessionLifecycleConfig {
        idle_timeout: Some(Duration::from_secs(300)),
        max_ttl: Some(Duration::from_secs(86400)),
        max_active_sessions: None,
        eviction_policy: EvictionPolicy::Lru,
        cleanup_interval: Duration::from_secs(300),
    };
    let lifecycle = Arc::new(SessionWorkingSetManager::new(lifecycle_config));
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

    if let Some(webui_dir) = static_webui.configured_path.as_deref() {
        if static_webui.available {
            surface_host.register_webui_static_resource(webui_dir);
        }
    }
    let surface_discovery = surface_host.discover();
    tracing::info!(
        discovered = surface_discovery.discovered,
        failures = surface_discovery.failures.len(),
        roots = ?surface_discovery.roots,
        "surface host discovery completed"
    );
    let tool_host = Arc::new(
        tools::ToolHost::new(
            format!("workspace:{}", workspace_root.display()),
            &workspace_root,
            tools::ToolHostSnapshot::new(
                Arc::clone(&tools),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                Some(runtime_mcp_service.clone()),
            ),
        )
        .with_authorization_lease_verifier(Arc::new(
            runtime::AuthorizationNegotiator::verify_lease_signature,
        )),
    );
    let gateway_runtime_tool_host = Arc::new(
        crate::gateway_tool_executor::GatewayToolExecutor::from_tool_host(
            None,
            false,
            Arc::clone(&tool_host),
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
    let session_runtime_port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
    let skill_revision_pointer_cache = Arc::new(runtime::SkillRevisionPointerCache::default());
    let mut runtime_services_builder =
        runtime::RuntimeServices::builder(&approval_dir, &workspace_root)
            .runtime_build_identity(crate::compiled_runtime_build_identity())
            .provider_registry(Arc::clone(&provider_registry))
            .provider_transport_pool(Arc::clone(&provider_transport_pool))
            .provider_template_cache(Arc::clone(&provider_template_cache))
            .provider_resource_config(runtime_config.provider_resources().clone())
            .provider_fallbacks(runtime_config.fallbacks().iter().cloned())
            .tool_execution_host(runtime_tool_host)
            .runtime_event_store(Arc::clone(&selected_storage.runtime_event_store))
            .projection_lane(crate::services::growth_projection_lane(
                approval_dir.clone(),
                Arc::clone(&selected_storage.runtime_event_store),
                growth_projection_services.growth.clone(),
                growth_projection_services.memory.clone(),
                growth_projection_services.matrix.clone(),
            ))
            .task_aggregate_service(Arc::clone(&selected_storage.task_service))
            .artifact_store(Arc::clone(&selected_storage.artifact_store))
            .reality_recall_port(Arc::new(
                runtime::RealityRecallPort::with_fact_and_matrix_store(
                    &approval_dir,
                    Arc::clone(&selected_storage.fact_ledger),
                    Arc::clone(&selected_storage.matrix_store),
                )
                .with_workspace_scope(&workspace_root),
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
            )
            .hot_state_config(runtime_config.hot_state().clone())
            .approval_config(runtime_config.approval().clone())
            .collaboration_capacity(
                runtime_config.runtime_control().policy.capacity.clone(),
                runtime_config
                    .runtime_control()
                    .policy
                    .agent
                    .max_parallel_agents,
            )
            .skill_revision_pointer_cache(Arc::clone(&skill_revision_pointer_cache))
            .session_ports(
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
            );
    let skill_usage_sink: Arc<dyn runtime::RuntimeSkillUsageSink> =
        Arc::new(runtime::RuntimeSkillUsageRecorder::with_pointer_cache(
            Arc::clone(&selected_storage.runtime_event_store),
            skill_revision_pointer_cache,
        ));
    crate::services::attach_workspace_skill_usage_sink(&workspace_root, skill_usage_sink);
    let skill_catalog_started_at = Instant::now();
    let startup_skill_assets = crate::services::runtime_skill_assets_for_workspace(&workspace_root);
    tracing::info!(
        elapsed_ms = skill_catalog_started_at.elapsed().as_millis() as u64,
        profiles = startup_skill_assets.profiles.len(),
        prompt_assets = startup_skill_assets.prompt_assets.len(),
        "runtime skill catalog startup projection completed"
    );
    let mut startup_skill_catalog = runtime::RuntimeSkillCatalog::new(
        startup_skill_assets.profiles,
        startup_skill_assets.prompt_assets,
    );
    if let Some(source) = startup_skill_assets.instruction_source {
        startup_skill_catalog = startup_skill_catalog.with_instruction_source(source);
    }
    runtime_services_builder = runtime_services_builder.skill_catalog(startup_skill_catalog);
    if let Some(memory_manager) = cognitive.as_ref() {
        runtime_services_builder =
            runtime_services_builder.memory_manager(Arc::clone(memory_manager));
        // P9 startup self-check: L0 identity must be present after bootstrap.
        // A missing role/language degrades memory orientation silently, so
        // surface the exact missing key instead.
        let memory_manager = Arc::clone(memory_manager);
        tokio::spawn(async move {
            match memory_manager.identity_entries().await {
                Ok(entries) => {
                    let titles = entries
                        .iter()
                        .map(|entry| entry.title.as_str())
                        .collect::<std::collections::BTreeSet<_>>();
                    if !titles.contains("assistant-role") || !titles.contains("response-language") {
                        tracing::warn!(
                            has_role = titles.contains("assistant-role"),
                            has_language = titles.contains("response-language"),
                            "memory L0 identity bootstrap is incomplete; configure memory.identity.role/language"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "memory L0 identity self-check failed");
                }
            }
        });
    }
    let runtime_services_started_at = Instant::now();
    let runtime_services = match runtime_services_builder.build() {
        Ok(runtime_services) => runtime_services,
        Err(error) => {
            let error = format!("failed to initialize runtime services: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    tracing::info!(
        elapsed_ms = runtime_services_started_at.elapsed().as_millis() as u64,
        "Runtime services composition completed"
    );
    startup_registry.runtime_services = Some(Arc::clone(&runtime_services));
    if evolution_runtime
        .set(Arc::downgrade(&runtime_services))
        .is_err()
    {
        let error = "failed to bind Runtime evolution evaluation executor".to_string();
        return Err(startup_registry.rollback(error).await);
    }
    if let Err(error) =
        gateway_runtime_tool_host.bind_runtime_services(Arc::clone(&runtime_services))
    {
        let error = format!("failed to bind runtime services: {error}");
        return Err(startup_registry.rollback(error).await);
    }
    let execution_recovery_started_at = Instant::now();
    let startup_recovery = match runtime_services.recover_execution_graphs_on_startup().await {
        Ok(report) => report,
        Err(error) => {
            let error = format!("failed to recover execution graphs on startup: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    tracing::info!(
        elapsed_ms = execution_recovery_started_at.elapsed().as_millis() as u64,
        "Runtime execution startup recovery completed"
    );
    emit_execution_startup_recovery(&startup_recovery);
    let runtime_service = match RuntimeService::new_with_gateway_tasks(
        sessions.clone(),
        lease_registry.clone(),
        session_runtime_port.clone(),
        event_bus.clone(),
        started_at,
        runtime_config.resolved_model(),
        Arc::clone(&provider_registry),
        Arc::clone(&upgrade_coordinator),
        Arc::clone(&runtime_services),
        Arc::clone(&gateway_tasks),
    ) {
        Ok(runtime_service) => Arc::new(
            runtime_service
                .with_permission_mode(configured_runtime_permission_mode(&runtime_config))
                .with_approval_profile(runtime_config.approval().profile)
                .with_tool_host(tool_host)
                .with_session_bootstrap(runtime_session_bootstrap),
        ),
        Err(error) => {
            let error = format!("failed to initialize runtime session bridge: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    };
    startup_registry.runtime_service = Some(Arc::clone(&runtime_service));
    let session_activation = Arc::new(
        crate::services::session_service::activation::SessionActivationCoordinator::new(
            Arc::clone(&runtime_service),
            Arc::clone(&session_repository),
            Arc::clone(&presence_ledger),
            Arc::clone(&lifecycle),
            None,
            config.session_recovery,
        ),
    );
    startup_registry.session_activation = Some(Arc::clone(&session_activation));
    let session_service = Arc::new(crate::services::SessionService::new_unbound(
        Arc::clone(&runtime_service),
        Arc::clone(&session_activation),
    ));
    startup_registry.session_service = Some(Arc::clone(&session_service));
    if let Err(error) = session_runtime_port.bind(&session_service) {
        return Err(startup_registry.rollback(error).await);
    }
    let listener = match TcpListener::bind(&config.http_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            let error = format!("failed to bind HTTP {}: {error}", config.http_addr);
            return Err(startup_registry.rollback(error).await);
        }
    };
    let session_worker_supervisor =
        match crate::session_runtime_bridge::SessionWorkerSupervisor::start(
            Arc::clone(&runtime_service),
            Arc::clone(&session_service),
            Arc::clone(&event_bus),
        )
        .await
        {
            Ok(supervisor) => supervisor,
            Err(error) => {
                return Err(startup_registry.rollback(error).await);
            }
        };
    startup_registry.session_worker_supervisor = Some(Arc::clone(&session_worker_supervisor));
    if let Err(error) = session_service.install_supervisor(Arc::clone(&session_worker_supervisor)) {
        return Err(startup_registry.rollback(error).await);
    }
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
    let services = crate::services::GatewayServices::new_with_bound_session_and_storage(
        Arc::clone(&runtime_service),
        Arc::clone(&session_service),
        surface_host.clone(),
        cognitive.clone(),
        Arc::clone(&session_activation),
        Arc::clone(&session_worker_supervisor),
        &approval_dir,
        capacity_config,
        Arc::clone(&selected_storage),
        growth_projection_services,
    );
    if let Some(executor) = &selected_storage.postgres_executor {
        if let Err(error) = executor.verify_registered_migration_catalogs() {
            let error = format!("failed to verify enabled APP storage catalogs: {error}");
            return Err(startup_registry.rollback(error).await);
        }
    }
    let services = Arc::new(services.with_app_platform(Arc::clone(&app_platform)));
    let app_state = Arc::new(api_routes::AppState {
        tool_registry: tools.clone(),
        config: config.runtime_config.clone(),
        static_webui: static_webui.clone(),
        auth_token: config.auth_token.clone(),
        workspace_root,
        config_home: approval_dir.clone(),
        profile_id,
        profile_manager,
        services,
        session_lease_registry: Some(lease_registry.clone()),
        live_registry: Arc::new(api_routes::live_routes::LiveRegistry::new()),
    });
    if let Err(error) = core_bridge.bind_app_state(Arc::clone(&app_state)) {
        let error = format!("failed to bind CoreBridge Gateway dependencies: {error}");
        return Err(startup_registry.rollback(error).await);
    }
    if let Err(error) = app_platform.start_resident().await {
        let error = format!("failed to start resident APPs: {error}");
        return Err(startup_registry.rollback(error).await);
    }
    config_reload::initialize_config_reload_status(&config_reload, &app_state);

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

    // 3. HTTP listener was bound before worker startup so bind failure cannot
    // strand a running supervisor.
    tracing::info!("HTTP + SSE on {}", config.http_addr);

    if let Err(e) = std::fs::write(
        crate::server::addr_file(),
        format!("http://{}", config.http_addr),
    ) {
        tracing::warn!("failed to write addr file: {e}");
    }

    // 4. Edge surfaces/connectors are discovered and represented in SurfaceHost.
    // External sidecar process launch is driven by edge requests, not by runtime boot.

    // 5. Start every Gateway-owned background task behind one admission and
    // shutdown owner. Any partial startup failure falls through to the same
    // cleanup path as SIGINT/SIGTERM.
    let (server_result_tx, server_result_rx) = tokio::sync::oneshot::channel();
    let background_start = (|| -> Result<(), String> {
        surface_host.start_monitor()?;
        spawn_event_loop_lag_probe(Arc::clone(&gateway_tasks))?;
        spawn_runtime_schedule_timer(
            runtime_service.runtime_services(),
            Arc::clone(&gateway_tasks),
        )?;
        spawn_mission_organizer_worker(
            runtime_service.runtime_services(),
            Arc::clone(&gateway_tasks),
        )?;
        config_reload::spawn_config_reload_watcher(
            config_reload,
            app_state.clone(),
            Duration::from_secs(2),
            Arc::clone(&gateway_tasks),
        )
        .map_err(|error| format!("failed to start config reload watcher: {error}"))?;
        crate::surface_host::spawn_surface_ingress_dispatcher(
            app_state.clone(),
            Arc::clone(&gateway_tasks),
        )
        .map_err(|error| format!("failed to start surface ingress dispatcher: {error}"))?;
        gateway_tasks
            .spawn(
                GatewayTaskKind::HttpServer,
                None,
                move |cancellation| async move {
                    let shutdown = async move {
                        cancellation.cancelled().await;
                    };
                    let result = axum::serve(listener, app)
                        .with_graceful_shutdown(shutdown)
                        .await;
                    let _ = server_result_tx.send(result);
                },
            )
            .map_err(|error| format!("failed to start HTTP server: {error}"))?;
        let recovery_service = Arc::clone(&session_service);
        let recovery_supervisor = Arc::clone(&session_worker_supervisor);
        gateway_tasks
            .spawn(
                GatewayTaskKind::RuntimeRestoration,
                None,
                move |cancellation| async move {
                    let recovery = tokio::select! {
                        _ = cancellation.cancelled() => return,
                        recovery = recovery_service.recover_required_sessions() => recovery,
                    };
                    match recovery {
                        Ok(summary) => recovery_supervisor.record_recovery(summary),
                        Err(error) => {
                            tracing::error!(
                                %error,
                                "Session startup recovery failed before producing a summary"
                            );
                            let mut summary =
                                crate::services::session_service::activation::SessionRecoverySummary {
                                    failed: 1,
                                    ..Default::default()
                                };
                            summary.failures.push(error);
                            recovery_supervisor.record_recovery(summary);
                        }
                    }
                },
            )
            .map_err(|error| format!("failed to start Session restoration task: {error}"))?;
        if let Some((manager, knowledge, policy)) = memory_governance_task {
            start_memory_governance_task(
                &gateway_tasks,
                manager,
                knowledge,
                policy,
                Arc::new(memory_governance::GatewaySemanticGovernanceResolver::new(
                    &runtime_service,
                )),
            )?;
        }
        Ok(())
    })();

    let serve_result = match background_start {
        Err(error) => Err(error),
        Ok(()) => {
            tokio::select! {
                signal = wait_for_shutdown_signal() => signal,
                result = server_result_rx => match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(format!("HTTP server error: {error}")),
                    Err(_) => Err("HTTP server task exited without a completion result".to_string()),
                },
            }
        }
    };
    // ── Cleanup after shutdown ──
    tracing::info!("cleaning up runtime host resources...");
    let shutdown_report = startup_registry
        .shutdown(
            serve_result
                .as_ref()
                .err()
                .cloned()
                .map(|error| ("runtime_failure", error)),
        )
        .await;
    // PID file is cleaned up by PidFileGuard drop
    tracing::info!(
        phases = ?shutdown_report.phases,
        active_turns_cancelled = shutdown_report
            .active_turns
            .as_ref()
            .map_or(0, |report| report.cancelled),
        active_turns_remaining = shutdown_report
            .active_turns
            .as_ref()
            .map_or(0, |report| report.remaining_turn_ids.len()),
        memory_forced_aborts = shutdown_report
            .memory
            .as_ref()
            .map_or(0, |report| report.forced_aborts),
        tasks_joined = shutdown_report.tasks.joined,
        tasks_forced_aborts = shutdown_report.tasks.forced_aborts,
        failures = shutdown_report.failures.len(),
        "runtime host shutdown complete"
    );
    if shutdown_report.failures.is_empty() {
        Ok(())
    } else {
        Err(shutdown_report.failures.join("; "))
    }
}

async fn wait_for_shutdown_signal() -> Result<(), String> {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|error| format!("failed to install SIGTERM handler: {error}"))?;
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .map_err(|error| format!("failed to install SIGINT handler: {error}"))?;
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| format!("failed to install ctrl_c handler: {error}"))?;
        tracing::info!("shutdown signal received");
    }
    Ok(())
}

fn spawn_event_loop_lag_probe(tasks: Arc<GatewayRuntimeTaskSet>) -> Result<u64, String> {
    tasks
        .spawn(
            GatewayTaskKind::EventLoopProbe,
            None,
            |cancellation| async move {
                let interval = Duration::from_millis(250);
                loop {
                    let started = Instant::now();
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(interval) => {
                            runtime::execution_core::performance::observe_duration(
                                "event_loop_lag_ms",
                                started.elapsed().saturating_sub(interval),
                            );
                        }
                    }
                }
            },
        )
        .map_err(|error| format!("failed to start event-loop lag probe: {error}"))
}

/// The timer is an event source only. It claims due Mission schedules and
/// Managed Agent triggers, then sends both through Runtime's canonical
/// Binding/graph paths. GraphRunner and the Managed Agent dispatcher retain
/// ownership of all execution state, retry and terminal transitions.
fn spawn_runtime_schedule_timer(
    runtime_services: Arc<runtime::RuntimeServices>,
    tasks: Arc<GatewayRuntimeTaskSet>,
) -> Result<u64, String> {
    tasks
        .spawn(GatewayTaskKind::MissionSchedule, None, move |cancellation| async move {
            loop {
                let policy = runtime_services.mission_schedule_policy();
                if policy.enabled {
                    let dispatch = runtime_services.wake_due_mission_schedules(epoch_millis());
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        result = dispatch => {
                            if let Err(error) = result {
                                tracing::warn!(%error, "mission schedule timer dispatch failed");
                            }
                        }
                    }
                }
                // Managed Agents have independent trigger definitions. They do
                // not become inert merely because Mission scheduling is disabled;
                // the existing interval is only the shared wake-up cadence.
                let dispatch = runtime_services
                    .wake_managed_agents("gateway-runtime-scheduler".to_string(), 16);
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    result = dispatch => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "managed Agent timer dispatch failed");
                        }
                    }
                }
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(policy.tick_interval_ms)) => {}
                }
            }
        })
        .map_err(|error| format!("failed to start Mission schedule timer: {error}"))
}

fn spawn_mission_organizer_worker(
    runtime_services: Arc<runtime::RuntimeServices>,
    tasks: Arc<GatewayRuntimeTaskSet>,
) -> Result<u64, String> {
    tasks
        .spawn(
            GatewayTaskKind::MissionOrganizer,
            None,
            move |cancellation| async move {
                let organizer = runtime::MissionOrganizer::new(runtime_services);
                loop {
                    if let Err(error) = organizer.enqueue_pending_roots(64) {
                        tracing::warn!(%error, "Mission organizer ingress recovery failed");
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        result = organizer.run_once("gateway.mission-organizer", None) => {
                            if let Err(error) = result {
                                tracing::warn!(%error, "Mission organizer decision failed");
                            }
                        }
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                }
            },
        )
        .map_err(|error| format!("failed to start Mission organizer worker: {error}"))
}

fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mcp::McpService;
    use memory::MemoryConfig;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_webui_dir(label: &str) -> std::path::PathBuf {
        let unique = format!(
            "cowd-edge-webui-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let configured = std::env::temp_dir().join(&unique);
        // Linux sockaddr_un leaves only 107 bytes for a pathname (including
        // every component supplied by TMPDIR). Keep enough headroom for the
        // broker socket and its generation sidecars. This matters when a CI
        // lane deliberately gives the suite a descriptive, isolated TMPDIR.
        let dir = if configured.as_os_str().as_encoded_bytes().len() <= 80 {
            configured
        } else {
            std::path::Path::new("/tmp").join(unique)
        };
        fs::create_dir_all(&dir).expect("create temp webui dir");
        dir
    }

    #[test]
    fn gateway_config_defaults() {
        let config = RuntimeHostConfig {
            http_addr: "0.0.0.0:8642".into(),
            workspace_root: std::env::temp_dir().join("cowd-gateway-config-defaults"),
            memory_config: None,
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            session_recovery: runtime::SessionRecoveryConfig::default(),
            webui_dir: None,
            cors_origins: vec![],
            auth_token: None,
        };
        assert_eq!(config.http_addr, "0.0.0.0:8642");
        assert!(config.memory_config.is_none());
        assert!(config.surface_configs.is_empty());
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn nightly_memory_governance_schedule_rolls_forward_without_busy_retry() {
        assert_eq!(
            delay_until_hour_from(60 * 60, 3),
            Duration::from_secs(2 * 60 * 60)
        );
        assert_eq!(
            delay_until_hour_from(4 * 60 * 60, 3),
            Duration::from_secs(23 * 60 * 60)
        );
        assert_eq!(
            delay_until_hour_from(3 * 60 * 60, 3),
            Duration::from_secs(24 * 60 * 60)
        );
    }

    #[cfg(unix)]
    fn publish_test_socket_generation(socket: &Path, token: &str) -> SocketGenerationOwnership {
        let generation = SocketGenerationOwnership::with_token(socket, token.to_owned());
        let lock = open_socket_generation_lock(&generation).expect("generation lock");
        lock.lock_exclusive().expect("exclusive generation lock");
        publish_socket_generation_locked(socket, &generation).expect("publish generation");
        FileExt::unlock(&lock).expect("unlock generation");
        generation
    }

    #[cfg(unix)]
    #[test]
    fn broker_cleanup_never_unlinks_a_new_socket_generation() {
        use std::os::unix::net::UnixListener;

        let root = temp_webui_dir("broker-socket-generation");
        let socket = root.join("broker.sock");
        let old_generation = publish_test_socket_generation(&socket, "old-generation");
        let old_listener = UnixListener::bind(&socket).expect("old broker socket");
        drop(old_listener);
        fs::remove_file(&socket).expect("replace old broker socket");
        let new_generation = publish_test_socket_generation(&socket, "new-generation");
        let new_listener = UnixListener::bind(&socket).expect("new broker socket");

        remove_socket_if_owned(&socket, &old_generation);

        assert!(socket.exists(), "old broker cleanup removed the new socket");
        drop(new_listener);
        remove_socket_if_owned(&socket, &new_generation);
        assert!(!socket.exists(), "new broker cleanup left its socket");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_stale_broker_cleanups_preserve_the_current_generation() {
        use std::os::unix::net::UnixListener;

        let root = temp_webui_dir("broker-concurrent");
        let socket = root.join("broker.sock");
        let current = publish_test_socket_generation(&socket, "current-generation");
        let listener = UnixListener::bind(&socket).expect("current broker socket");
        let mut cleanups = Vec::new();
        for index in 0..16 {
            let socket = socket.clone();
            cleanups.push(std::thread::spawn(move || {
                let stale = SocketGenerationOwnership::with_token(
                    &socket,
                    format!("stale-generation-{index}"),
                );
                remove_socket_if_owned(&socket, &stale);
            }));
        }
        for cleanup in cleanups {
            cleanup.join().expect("stale cleanup thread");
        }

        assert!(
            socket.exists(),
            "stale concurrent cleanup removed current socket"
        );
        drop(listener);
        remove_socket_if_owned(&socket, &current);
        assert!(!socket.exists(), "current cleanup left its socket");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn current_broker_generation_cleanup_removes_socket_and_owner() {
        use std::os::unix::net::UnixListener;

        let root = temp_webui_dir("broker-socket-normal-cleanup");
        let socket = root.join("broker.sock");
        let generation = publish_test_socket_generation(&socket, "normal-generation");
        let listener = UnixListener::bind(&socket).expect("broker socket");
        drop(listener);

        remove_socket_if_owned(&socket, &generation);

        assert!(!socket.exists(), "current cleanup left its socket");
        assert!(
            !generation.owner_path.exists(),
            "current cleanup left its owner token"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn complete_shutdown_followers_wait_for_one_immutable_report() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let coordinator = GatewayRuntimeShutdownCoordinator::new(Arc::clone(&tasks));
        let executions = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut callers = Vec::new();

        for _ in 0..12 {
            let coordinator = Arc::clone(&coordinator);
            let body_coordinator = Arc::clone(&coordinator);
            let executions = Arc::clone(&executions);
            let release = Arc::clone(&release);
            callers.push(tokio::spawn(async move {
                coordinator
                    .coordinate(move || async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        body_coordinator.publish("drain_active_turns", &[]);
                        release
                            .acquire()
                            .await
                            .expect("shutdown test permit")
                            .forget();
                        let tasks = body_coordinator.gateway_tasks.shutdown().await;
                        GatewayRuntimeShutdownReport {
                            phases: vec![
                                "stop_accepting".to_string(),
                                "drain_active_turns".to_string(),
                                "drain_task_set".to_string(),
                            ],
                            failures: Vec::new(),
                            active_turns: None,
                            memory: None,
                            tasks,
                        }
                    })
                    .await
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while executions.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one shutdown leader should start");
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert!(callers.iter().all(|caller| !caller.is_finished()));
        assert_eq!(tasks.health().shutdown_phase, "drain_active_turns");

        release.add_permits(1);
        let mut reports = Vec::new();
        for caller in callers {
            reports.push(caller.await.expect("shutdown caller"));
        }

        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert!(reports.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(tasks.health().shutdown_phase, "closed");
    }

    #[tokio::test]
    async fn startup_failure_uses_the_complete_coordinator_and_stays_observable() {
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(1));
        let coordinator = GatewayRuntimeShutdownCoordinator::new(Arc::clone(&tasks));
        let registry = GatewayRuntimeStartupRegistry::new(Arc::clone(&coordinator));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_in_task = Arc::clone(&cancelled);
        tasks
            .spawn(
                GatewayTaskKind::ConfigReload,
                None,
                move |token| async move {
                    token.cancelled().await;
                    cancelled_in_task.store(true, Ordering::SeqCst);
                },
            )
            .expect("startup task");

        let first_error = registry
            .rollback("injected startup failure".to_string())
            .await;
        let first = coordinator
            .completed_report()
            .expect("startup rollback report");
        let second = registry
            .shutdown(Some((
                "runtime_failure",
                "follower must not replace the report".to_string(),
            )))
            .await;

        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(first_error, "injected startup failure");
        assert_eq!(first, second);
        assert_eq!(
            first.phases,
            vec![
                "startup_rollback",
                "stop_accepting",
                "drain_ingress",
                "drain_surface",
                "drain_apps",
                "drain_live_eval",
                "drain_active_turns",
                "drain_session_workers",
                "unload_sessions",
                "drain_runtime",
                "drain_session_repository",
                "drain_task_set",
            ]
        );
        let health = tasks.health();
        assert_eq!(health.shutdown_phase, "closed_with_failures");
        assert_eq!(health.shutdown_failures, vec!["injected startup failure"]);
        assert_eq!(health.active, 0);
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
            workspace_root: std::env::temp_dir().join("cowd-gateway-config-auth"),
            memory_config: None,
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            session_recovery: runtime::SessionRecoveryConfig::default(),
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
            workspace_root: std::env::temp_dir().join("cowd-gateway-config-memory"),
            memory_config: Some(mem_cfg),
            surface_configs: vec![],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: None,
            session_recovery: runtime::SessionRecoveryConfig::default(),
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
                state: surface::SurfaceStateMode::Ephemeral,
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
            workspace_root: workspace.clone(),
            memory_config: Some(MemoryConfig::default()),
            surface_configs: vec![surface],
            surface_runtime_configs: BTreeMap::new(),
            runtime_config: Some(serde_json::json!({"model": "test-model"})),
            session_recovery: runtime::SessionRecoveryConfig::default(),
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

    #[test]
    fn api_paths_never_fall_through_to_the_spa() {
        assert!(is_api_path("/api"));
        assert!(is_api_path("/api/sessions/missing"));
        assert!(!is_api_path("/application"));
        assert!(!is_api_path("/chat/session-1"));
    }
}
