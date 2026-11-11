// ── Gateway RuntimeHost ────────────────────────────────────
// Gateway foreground mode is a gateway process with an internal runtime host providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Surface registry (builtin TUI/WebUI plus external JSONL sidecars)
// Shared state: ActiveSessionDirectory, CognitiveContextManager, ToolCatalog, SessionProjectionHub

use std::collections::{BTreeMap, BTreeSet};
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
    gateway_tasks
        .spawn(
            GatewayTaskKind::MemoryGovernance,
            None,
            move |cancellation| {
                let manager = Arc::clone(&manager);
                let semantic_resolver = Arc::clone(&semantic_resolver);
                async move {
                    let mut startup = true;
                    let mut next_governance =
                        tokio::time::Instant::now() + Duration::from_secs(policy.startup_delay_secs);
                    loop {
                        match manager.recover_memory_cleanup(&knowledge, 64).await {
                            Ok(errors) if !errors.is_empty() => {
                                tracing::warn!(errors=?errors,"required Memory cleanup remains pending")
                            }
                            Err(error) => tracing::warn!(%error,"required Memory cleanup replay failed"),
                            _ => {}
                        }
                        if policy.enabled && tokio::time::Instant::now() >= next_governance {
                            let mode = if startup {
                                memory::AutomaticGovernanceMode::Startup
                            } else {
                                memory::AutomaticGovernanceMode::Nightly
                            };
                            let resolver = if startup {
                                None
                            } else {
                                Some(semantic_resolver.as_ref())
                            };
                            match memory::run_automatic_governance_with_resolver(
                                Arc::clone(&manager),
                                Some(&knowledge),
                                &policy,
                                mode,
                                resolver,
                            )
                            .await
                            {
                                Ok(report) => tracing::info!(
                                    scanned_entries = report.scanned_entries,
                                    errors = report.errors.len(),
                                    "optional Memory governance completed"
                                ),
                                Err(error) => tracing::warn!(%error,"optional Memory governance failed"),
                            }
                            startup = false;
                            next_governance =
                                tokio::time::Instant::now() + delay_until_local_hour(policy.deep_scan_hour_local);
                        }
                        // Required recovery shares the existing joined owner and
                        // remains live independently of optional governance policy.
                        let retry = tokio::time::Instant::now() + Duration::from_secs(30);
                        let wake = if policy.enabled {
                            retry.min(next_governance)
                        } else {
                            retry
                        };
                        tokio::select! {
                            _=cancellation.cancelled()=>break,
                            _=manager.memory_cleanup_notified()=>{},
                            ()=tokio::time::sleep_until(wake)=>{},
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

fn configured_evolution_eval_runner(
    config_home: &Path,
    runtime: Arc<OnceLock<Weak<runtime::RuntimeServices>>>,
) -> Arc<dyn runtime::EvolutionEvalRunner> {
    let scenarios = Arc::new(harness_eval::FileDefinitionEvolutionScenarioCatalog::new(
        config_home.join("runtime").join("evolution-scenarios"),
    ));
    let executor = Arc::new(GatewayEvolutionScenarioExecutor { runtime });
    Arc::new(harness_eval::DefinitionEvolutionEvalRunner::new(Arc::new(
        harness_eval::RuntimeDefinitionEvolutionWorkload::new(scenarios, executor),
    )))
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
    /// Device/inode identity of the authority root this helper owns. The root
    /// lives inside the Gateway's private config home, which an isolated
    /// evaluation launcher places in a tmpfs runtime root; re-verifying the
    /// identity (not merely existence) is what tells "the helper restarted"
    /// apart from "somebody replaced the directory under us".
    root_identity: (u64, u64),
    /// Set once the owner retires this helper, so the liveness watchdog never
    /// resurrects a broker the Gateway has already shut down.
    retired: Arc<std::sync::atomic::AtomicBool>,
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
        use std::os::unix::fs::MetadataExt;
        let root_identity = std::fs::metadata(&root)
            .map(|meta| (meta.dev(), meta.ino()))
            .map_err(|error| format!("failed to identify auth broker root: {error}"))?;
        let mut broker = Self {
            child,
            socket_path,
            socket_generation: None,
            root_identity,
            retired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        self.retired
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(generation) = self.socket_generation.take() {
            remove_socket_if_owned(&self.socket_path, &generation);
        }
    }

    /// Whether the owner has already retired this helper.
    fn is_retired(&self) -> bool {
        self.retired.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Verify the helper this process owns is still serving, by identity.
    ///
    /// The authority root is the Gateway's private `auth-broker` directory. An
    /// unrelated cleanup path that reclaims the surrounding runtime root
    /// removes that directory while the Gateway is still live, and the only
    /// observable symptom is a vanished socket: every later authenticated
    /// request then fails to connect. Detect the loss here - child liveness,
    /// socket presence and kind, and root device/inode identity - so the owner
    /// can re-establish the helper instead of answering requests forever.
    fn ensure_live(&mut self) -> Result<(), String> {
        if self.is_retired() {
            return Ok(());
        }
        if let Some(status) = self
            .child
            .try_wait()
            .map_err(|error| format!("failed to inspect auth broker: {error}"))?
        {
            return Err(format!("auth broker helper exited: {status}"));
        }
        let socket = std::fs::symlink_metadata(&self.socket_path)
            .map_err(|error| format!("auth broker socket vanished: {error}"))?;
        use std::os::unix::fs::FileTypeExt;
        if !socket.file_type().is_socket() {
            return Err("auth broker socket is no longer a socket".to_string());
        }
        let root = self
            .socket_path
            .parent()
            .ok_or_else(|| "auth broker socket has no authority root".to_string())?;
        use std::os::unix::fs::MetadataExt;
        let current = std::fs::metadata(root)
            .map_err(|error| format!("auth broker authority root vanished: {error}"))?;
        if (current.dev(), current.ino()) != self.root_identity {
            return Err("auth broker authority root was replaced".to_string());
        }
        Ok(())
    }
}

impl Drop for AuthBrokerProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Keep the owned auth-broker helper alive for the Gateway's whole lifetime.
///
/// The helper's authority root is a directory inside the Gateway's private
/// config home. Nothing else on the machine owns that directory's lifetime, so
/// an unrelated cleanup path that reclaims the surrounding runtime root can
/// delete it while the Gateway is still serving. Before this watchdog the only
/// symptom was a vanished socket, surfaced per request as
/// `authority storage error: No such file or directory` - a liveness failure
/// misreported as a storage failure, which silently killed a whole isolated
/// evaluation round at zero model cost.
///
/// The watchdog re-establishes the helper when its observable liveness is lost,
/// and stops as soon as the owner retires it, so shutdown is never undone.
fn spawn_auth_broker_liveness_watchdog(
    registry: Arc<tokio::sync::Mutex<Option<AuthBrokerProcess>>>,
    approval_dir: std::path::PathBuf,
    credential: Option<String>,
    catalog: auth_broker::AuthorizationCatalog,
) {
    let Some(credential) = credential else {
        return;
    };
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(AUTH_BROKER_LIVENESS_INTERVAL).await;
            let mut guard = registry.lock().await;
            let Some(broker) = guard.as_mut() else {
                return;
            };
            if broker.is_retired() {
                return;
            }
            if let Err(reason) = broker.ensure_live() {
                let retired = broker.is_retired();
                drop(guard);
                if retired {
                    return;
                }
                tracing::error!(
                    reason = %reason,
                    "auth broker helper lost its authority storage or socket; re-establishing"
                );
                match AuthBrokerProcess::start(&approval_dir, &credential, &catalog) {
                    Ok(fresh) => {
                        *registry.lock().await = Some(fresh);
                        tracing::warn!("auth broker helper re-established after liveness loss");
                    }
                    Err(error) => {
                        tracing::error!(error = %error, "auth broker re-establishment failed");
                    }
                }
            }
        }
    });
}

/// How often the owned auth-broker helper's liveness is verified. Short enough
/// that a request never waits on a long dead helper, long enough that the check
/// is not a hot path.
const AUTH_BROKER_LIVENESS_INTERVAL: Duration = Duration::from_secs(2);

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
        deferred_graphs = report.deferred_graphs,
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
    execution_started: bool,
    execution: Option<runtime::RuntimeExecutionShutdownReport>,
    active_turns: Option<crate::runtime_service::ActiveTurnDrainReport>,
    memory: Option<memory::cognitive::MemoryBackgroundShutdownReport>,
    tasks: task_set::GatewayTaskShutdownReport,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ShutdownReceiptIdentity {
    nonce: String,
    pid: u32,
    starttime: u64,
    image_sha256: String,
    config_sha256: String,
}
struct ShutdownReceiptTarget {
    directory: std::fs::File,
    identity: ShutdownReceiptIdentity,
}
impl ShutdownReceiptTarget {
    fn from_environment(config: &RuntimeHostConfig) -> Result<Option<Self>, String> {
        let Some(nonce) = std::env::var_os("COWD_TEST_SHUTDOWN_RECEIPT_NONCE") else {
            return Ok(None);
        };
        let nonce = nonce
            .into_string()
            .map_err(|_| "shutdown fixture nonce is not UTF-8")?;
        Self::validate_nonce(&nonce)?;
        #[cfg(not(target_os = "linux"))]
        return Err("shutdown fixture image identity requires Linux".into());
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            let root = std::env::var_os("XDG_RUNTIME_DIR")
                .ok_or("shutdown fixture requires private XDG_RUNTIME_DIR")?;
            let root = PathBuf::from(root);
            if !root.is_absolute() {
                return Err("shutdown fixture directory must be absolute".into());
            }
            let directory = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&root)
                .map_err(|e| e.to_string())?;
            let metadata = directory.metadata().map_err(|e| e.to_string())?;
            let owner = std::fs::metadata("/proc/self")
                .map_err(|e| e.to_string())?
                .uid();
            if !metadata.is_dir() || metadata.uid() != owner || metadata.mode() & 0o777 != 0o700 {
                return Err("shutdown fixture directory is not private and owned".into());
            }
            let image_sha256 = Self::image_hash(Path::new("/proc/self/exe"))?;
            let starttime = Self::starttime(std::process::id())?;
            let config_bytes=serde_json::to_vec(&serde_json::json!({
                "http_addr":config.http_addr,"workspace":config.workspace_root,"runtime":config.runtime_config,
                "surfaces":config.surface_configs,"surface_runtime":config.surface_runtime_configs,
                "cors_origins":config.cors_origins,
            })).map_err(|e|e.to_string())?;
            use sha2::{Digest, Sha256};
            let config_sha256 = format!("sha256:{:x}", Sha256::digest(config_bytes));
            let target = Self {
                directory,
                identity: ShutdownReceiptIdentity {
                    nonce,
                    pid: std::process::id(),
                    starttime,
                    image_sha256,
                    config_sha256,
                },
            };
            if target.path().symlink_metadata().is_ok() {
                return Err("shutdown fixture receipt destination already exists".into());
            }
            Ok(Some(target))
        }
    }
    fn validate_nonce(nonce: &str) -> Result<(), String> {
        if nonce.len() != 64
            || !nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("shutdown fixture nonce requires 64 lowercase hex characters".into());
        }
        Ok(())
    }
    fn starttime(pid: u32) -> Result<u64, String> {
        let stat =
            std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?;
        stat.rsplit_once(") ")
            .and_then(|(_, tail)| tail.split_whitespace().nth(19))
            .and_then(|v| v.parse().ok())
            .ok_or("shutdown process starttime unavailable".into())
    }
    fn image_hash(path: &Path) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut input = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let mut hash = Sha256::new();
        let mut bytes = [0; 64 * 1024];
        loop {
            let n = input.read(&mut bytes).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            hash.update(&bytes[..n]);
        }
        Ok(format!("sha256:{:x}", hash.finalize()))
    }
    #[cfg(target_os = "linux")]
    fn path(&self) -> PathBuf {
        use std::os::fd::AsRawFd;
        PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()))
            .join(format!("shutdown-{}.v1.json", self.identity.nonce))
    }
    #[cfg(not(target_os = "linux"))]
    fn path(&self) -> PathBuf {
        PathBuf::new()
    }
    fn publish(&self, report: &GatewayRuntimeShutdownReport) -> Result<(), String> {
        #[cfg(not(target_os = "linux"))]
        return Err("shutdown fixture export unsupported".into());
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let destination = self.path();
            let temporary = destination.with_file_name(format!(
                ".shutdown-{}-{}.tmp",
                self.identity.nonce,
                uuid::Uuid::new_v4()
            ));
            let body = serde_json::to_vec(
                &serde_json::json!({"schema_version":1,"identity":self.identity,"report":report}),
            )
            .map_err(|e| e.to_string())?;
            let mut published = false;
            let result = (|| {
                let mut output = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(&temporary)
                    .map_err(|e| e.to_string())?;
                use std::os::unix::fs::MetadataExt;
                let metadata = output.metadata().map_err(|error| error.to_string())?;
                let directory = self
                    .directory
                    .metadata()
                    .map_err(|error| error.to_string())?;
                if !metadata.is_file()
                    || metadata.mode() & 0o777 != 0o600
                    || metadata.uid() != directory.uid()
                {
                    return Err("shutdown receipt filesystem did not enforce owned0600 file".into());
                }
                output.write_all(&body).map_err(|e| e.to_string())?;
                output.sync_all().map_err(|e| e.to_string())?;
                // Same-directory hard link is atomic and cannot replace an old
                // receipt or a symlink. Directory capability stays pinned.
                std::fs::hard_link(&temporary, &destination).map_err(|e| e.to_string())?;
                published = true;
                self.directory.sync_all().map_err(|e| e.to_string())?;
                Ok(())
            })();
            let _ = std::fs::remove_file(&temporary);
            if result.is_err() && published {
                let _ = std::fs::remove_file(&destination);
            }
            result
        }
    }
}
impl serde::Serialize for GatewayRuntimeShutdownReport {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_json::json!({"phases":self.phases,"failures":self.failures,
            "execution_started":self.execution_started,"execution":self.execution,
            "active_turns":self.active_turns,"memory":self.memory,
            "tasks":{"joined":self.tasks.joined,"panicked":self.tasks.panicked,"forced_aborts":self.tasks.forced_aborts,"rejected":self.tasks.rejected}
        }).serialize(serializer)
    }
}

#[derive(Default)]
struct GatewayRuntimeShutdownState {
    report: Option<GatewayRuntimeShutdownReport>,
    original_driver: Option<tokio::sync::watch::Receiver<Option<Result<GatewayRuntimeShutdownReport, String>>>>,
}

/// Process-level owner for the complete Gateway close transaction.
///
/// `GatewayRuntimeTaskSet` owns leaf tasks; this coordinator serializes every
/// component phase around it. The first caller is the leader. Followers wait
/// on the same gate and receive the immutable report published by the leader.
struct GatewayRuntimeShutdownCoordinator {
    gateway_tasks: Arc<GatewayRuntimeTaskSet>,
    receipt: Option<ShutdownReceiptTarget>,
    gate: tokio::sync::Mutex<()>,
    state: Mutex<GatewayRuntimeShutdownState>,
}

impl GatewayRuntimeShutdownCoordinator {
    fn new(gateway_tasks: Arc<GatewayRuntimeTaskSet>) -> Arc<Self> {
        Self::with_receipt(gateway_tasks, None)
    }
    fn with_receipt(
        gateway_tasks: Arc<GatewayRuntimeTaskSet>,
        receipt: Option<ShutdownReceiptTarget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            gateway_tasks,
            receipt,
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

    fn finish(&self, mut report: GatewayRuntimeShutdownReport) -> GatewayRuntimeShutdownReport {
        if let Some(existing) = self.completed_report() {
            return existing;
        }
        if report.execution_started && report.execution.is_none() {
            report
                .failures
                .push("Runtime execution shutdown report missing after startup".into());
        }
        if let Some(execution) = &report.execution {
            if execution.remaining_keys > 0
                || execution.owners.values().any(|owner| owner.active > 0)
            {
                report
                    .failures
                    .push("Runtime execution shutdown retains active owners/keys".into());
            }
            if execution.forced_aborts > 0
                && !report
                    .failures
                    .iter()
                    .any(|failure| failure.contains("forced abort"))
            {
                report
                    .failures
                    .push("Runtime execution shutdown forced abort".into());
            }
            for error in &execution.errors {
                if !report
                    .failures
                    .iter()
                    .any(|failure| failure.contains(error))
                {
                    report
                        .failures
                        .push(format!("Runtime execution shutdown incomplete: {error}"));
                }
            }
        }
        if report.tasks.forced_aborts > 0 || report.tasks.panicked > 0 {
            report
                .failures
                .push("Gateway task drain did not close naturally".into());
        }
        if report
            .active_turns
            .as_ref()
            .is_some_and(|active| !active.remaining_turn_ids.is_empty())
        {
            report
                .failures
                .push("Gateway active turn drain retains turns".into());
        }
        if report
            .memory
            .as_ref()
            .is_some_and(|memory| memory.forced_aborts > 0 || !memory.errors.is_empty())
        {
            report
                .failures
                .push("Gateway Memory drain incomplete".into());
        }
        if let Some(receipt) = &self.receipt {
            if let Err(error) = receipt.publish(&report) {
                report.failures.push(format!(
                    "Gateway final shutdown receipt publication failed: {error}"
                ));
            }
        }
        let phase = if report.failures.is_empty() {
            "closed"
        } else {
            "closed_with_failures"
        };
        self.publish(phase, &report.failures);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .report = Some(report.clone());
        report
    }

    // Close the one original shutdown transaction at its first real deadline.
    //
    // The deadline rejection releases the caller that owns process
    // termination, and the original finalizer may still be unresolved when
    // that caller exits. The receipt lives in the original private directory,
    // so it must already exist before the release: publish-before-release is
    // the real receipt/directory lifetime order, instead of tying the receipt
    // only to a finalizer completion that may never precede process exit.
    // The record stays honest: the started-but-unfinished execution axes are
    // kept as observed and `finish` adds its own failures for them; the first
    // failure entry carries the typed
    // `shutdown_unresolved_at_original_deadline` reason. Nothing clean is
    // manufactured, and a later completed close can only rejoin this report.
    fn finish_unresolved_at_original_deadline(
        &self,
        phases: Vec<String>,
        first_observations: Vec<String>,
        execution_started: bool,
        active_turns: Option<crate::runtime_service::ActiveTurnDrainReport>,
        memory: Option<memory::cognitive::MemoryBackgroundShutdownReport>,
    ) -> String {
        let reason = format!(
            "Gateway Runtime shutdown is unresolved at its original deadline: {}",
            first_observations.join("; ")
        );
        self.finish(GatewayRuntimeShutdownReport {
            phases,
            failures: vec![format!("shutdown_unresolved_at_original_deadline: {reason}")],
            execution_started,
            execution: None,
            active_turns,
            memory,
            tasks: Default::default(),
        });
        reason
    }

    // Register the one original shutdown before any caller awaits it. A dropped
    // waiter does not drop the driver or its original dependency Arcs.
    async fn coordinate_original<F, Fut>(self: &Arc<Self>, shutdown: F) -> Result<GatewayRuntimeShutdownReport, String>
    where
        F: FnOnce(tokio::sync::watch::Sender<Option<Result<GatewayRuntimeShutdownReport, String>>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<GatewayRuntimeShutdownReport, String>> + Send + 'static,
    {
        let mut receiver = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(receiver) = &state.original_driver {
                receiver.clone()
            } else {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                tokio::spawn(async move {
                    use futures::FutureExt;
                    let result = std::panic::AssertUnwindSafe(shutdown(sender.clone())).catch_unwind().await
                        .unwrap_or_else(|_| Err("original Gateway shutdown driver panicked; custody unresolved".into()));
                    let _ = sender.send(Some(result));
                });
                state.original_driver = Some(receiver.clone());
                receiver
            }
        };
        loop {
            if let Some(report) = receiver.borrow().clone() { return report; }
            if receiver.changed().await.is_err() {
                return Err("original Gateway shutdown driver ended without a report; custody unresolved".into());
            }
        }
    }

    async fn coordinate_result<F, Fut>(&self, shutdown: F) -> Result<GatewayRuntimeShutdownReport, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<GatewayRuntimeShutdownReport, String>>,
    {
        let _leader = self.gate.lock().await;
        if let Some(report) = self.completed_report() { return Ok(report); }
        Ok(self.finish(shutdown().await?))
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
        self.finish(report)
    }
}

#[derive(Clone)]
struct GatewayRuntimeShutdownResources {
    session_worker_supervisor: Option<Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>>,
    selected_storage: Option<Arc<crate::selected_storage::SelectedStorageTopology>>,
    session_service: Option<Arc<crate::services::SessionService>>,
    session_activation: Option<Arc<crate::services::session_service::activation::SessionActivationCoordinator>>,
    runtime_service: Option<Arc<RuntimeService>>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
    cognitive: Option<Arc<CognitiveContextManager>>,
    surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    app_platform: Option<Arc<crate::app_platform::GatewayAppPlatform>>,
    core_bridge: Option<Arc<crate::app_platform::CoreBridgeServer>>,
    auth_broker: Arc<tokio::sync::Mutex<Option<AuthBrokerProcess>>>,
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

    fn shutdown_resources(&self) -> GatewayRuntimeShutdownResources {
        GatewayRuntimeShutdownResources {
            session_worker_supervisor: self.session_worker_supervisor.clone(),
            selected_storage: self.selected_storage.clone(),
            session_service: self.session_service.clone(),
            session_activation: self.session_activation.clone(),
            runtime_service: self.runtime_service.clone(),
            runtime_services: self.runtime_services.clone(),
            cognitive: self.cognitive.clone(),
            surface_host: self.surface_host.clone(),
            app_platform: self.app_platform.clone(),
            core_bridge: self.core_bridge.clone(),
            auth_broker: Arc::clone(&self.auth_broker),
        }
    }

    async fn shutdown(
        &self,
        initiating_failure: Option<(&'static str, String)>,
    ) -> Result<GatewayRuntimeShutdownReport, String> {
        let owner = Arc::clone(&self.coordinator);
        let resources = self.shutdown_resources();
        self.coordinator.coordinate_original(move |progress| async move {
            shutdown_runtime_host_resources(&owner, resources, initiating_failure, progress).await
        }).await
    }

    async fn rollback(&self, error: String) -> String {
        match self.shutdown(Some(("startup_rollback", error.clone()))).await {
            Ok(report) => report.failures.join("; "),
            Err(cleanup) => format!("{error}; Gateway shutdown unresolved: {cleanup}"),
        }
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
    resources: GatewayRuntimeShutdownResources,
    initiating_failure: Option<(&'static str, String)>,
    progress: tokio::sync::watch::Sender<Option<Result<GatewayRuntimeShutdownReport, String>>>,
) -> Result<GatewayRuntimeShutdownReport, String> {
    coordinator
        .coordinate_result(|| async move {
            let gateway_tasks = &coordinator.gateway_tasks;
            let initial_phase = initiating_failure.as_ref().map(|(phase, _)| *phase);
            let mut failures = initiating_failure
                .into_iter()
                .map(|(_, failure)| failure)
                .collect::<Vec<_>>();
            let mut phases = Vec::new();
            let mut active_turn_report = None;
            let mut memory_report = None;
            let mut execution_shutdown_report = None;
            let mut execution_started = false;
            let enter_phase = |phases: &mut Vec<String>, phase: &str, failures: &[String]| {
                phases.push(phase.to_string());
                coordinator.publish(phase, failures);
            };
            if let Some(initial_phase) = initial_phase {
                enter_phase(&mut phases, initial_phase, &failures);
            }

            // Close every ingress fence before draining accepted work. Process
            // shutdown preserves durable execution for restart; only an
            // explicit Session cancellation is allowed to terminalize it.
            enter_phase(&mut phases, "stop_accepting", &failures);
            gateway_tasks.stop_accepting();
            if let Some(supervisor) = resources.session_worker_supervisor.as_ref() {
                supervisor.stop_accepting();
            }
            let preserved_turns = resources
                .runtime_service
                .as_ref().map_or_else(Vec::new, |runtime| runtime.stop_accepting_for_recovery());

            enter_phase(&mut phases, "drain_ingress", &failures);
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

            enter_phase(&mut phases, "drain_active_turns", &failures);
            if let Some(runtime_service) = resources.runtime_service.as_ref() {
                let report = runtime_service
                    .wait_for_active_turns(0, Duration::from_secs(30))
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

            enter_phase(&mut phases, "drain_runtime", &failures);
            let runtime_services = resources.runtime_service.as_ref().map(|runtime| runtime.runtime_services())
                .or_else(|| resources.runtime_services.clone());
            if let Some(runtime_services) = runtime_services.as_ref() {
                execution_started = true;
                let first_report = runtime_services.shutdown_execution().await;
                // The first deadline is only an observation. Keep every original
                // dependency until the already-started finalizer actually closes.
                failures.extend(first_report.errors.iter().map(|error| format!("Runtime execution first observation: {error}")));
                let mut completion = runtime_services.subscribe_execution_shutdown_completion()
                    .ok_or("original Runtime execution finalizer was not registered; custody unresolved")?;
                let deadline = runtime_services.execution_shutdown_deadline()
                    .ok_or("original Runtime execution deadline unavailable; custody unresolved")?;
                if completion.borrow().is_none() {
                    match tokio::time::timeout_at(deadline, completion.changed()).await {
                        Ok(Ok(())) => {},
                        Ok(Err(_)) => return Err("original Runtime execution completion channel closed; custody unresolved".into()),
                        Err(_) => {
                            enter_phase(&mut phases, "drain_runtime_pending", &failures);
                            // The original deadline rejection releases the caller that
                            // owns process termination, so the truthful failed receipt
                            // is published into the original private directory first;
                            // the outer harness then rejects with the receipt's typed
                            // reason instead of a bare FileNotFoundError. The already
                            // started finalizer keeps its original dependency custody
                            // below; a late clean close can only rejoin, never rewrite.
                            let reason = coordinator.finish_unresolved_at_original_deadline(
                                phases.clone(),
                                failures.clone(),
                                execution_started,
                                active_turn_report.clone(),
                                memory_report.clone(),
                            );
                            let _ = progress.send(Some(Err(reason)));
                        }
                    }
                }
                let execution_report = loop {
                    if let Some(done) = completion.borrow().clone() {
                        match done {
                            runtime::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed: true } => break report,
                            runtime::RuntimeExecutionShutdownCompletion::Finished { report, custody_closed: false } =>
                                return Err(format!("original Runtime execution finalizer ended with custody unresolved; first observations: {}; final errors: {}", failures.join("; "), report.errors.join("; "))),
                        }
                    }
                    completion.changed().await.map_err(|_| "original Runtime execution finalizer ended without a completion; custody unresolved")?;
                };
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
                execution_shutdown_report = Some(execution_report);
            }
            // Re-read the original admitted turn controls without starting a new
            // wait or deadline. A prior timeout is kept in failures even if the
            // original turn later drained while the execution owner completed.
            if resources.runtime_service.as_ref().is_some_and(|runtime|
                !runtime.stop_accepting_for_recovery().is_empty()) {
                return Err("Gateway active turn custody remains unresolved after execution finalization".into());
            }

            enter_phase(&mut phases, "drain_surface", &failures);
            if let Some(surface_host) = resources.surface_host.as_ref() {
                if let Err(error) = surface_host.shutdown().await {
                    failures.push(format!("surface host shutdown incomplete: {error}"));
                    coordinator.publish("drain_surface", &failures);
                }
            }

            enter_phase(&mut phases, "drain_apps", &failures);
            if let Some(core_bridge) = resources.core_bridge.as_ref() {
                if let Err(error) = core_bridge.shutdown().await {
                    failures.push(format!("CoreBridge shutdown incomplete: {error}"));
                    coordinator.publish("drain_apps", &failures);
                }
            }
            if let Some(platform) = resources.app_platform.as_ref() {
                if let Err(error) = platform.shutdown().await {
                    failures.push(format!("APP supervisor shutdown incomplete: {error}"));
                    coordinator.publish("drain_apps", &failures);
                }
            }

            enter_phase(&mut phases, "drain_live_eval", &failures);
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

            enter_phase(&mut phases, "drain_session_workers", &failures);
            if let Some(supervisor) = resources.session_worker_supervisor.as_ref() {
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
            // A Runtime carrier is removed only after its active turn guard reached
            // terminal state. On timeout retain the carrier and publish failure rather
            // than manufacturing an apparently clean close by dropping live state.
            enter_phase(&mut phases, "unload_sessions", &failures);
            let active_turns_drained = resources.runtime_service.as_ref()
                .is_none_or(|runtime| runtime.stop_accepting_for_recovery().is_empty());
            let active_session_ids = resources
                .session_service
                .as_ref().map_or_else(Vec::new, |service| service.list_active_session_ids());
            if active_turns_drained {
                if let Some(session_activation) = resources.session_activation.as_ref() {
                    for session_id in &active_session_ids {
                        if let Err(error) = session_activation.unload_runtime(session_id).await {
                            failures.push(format!(
                                "Session {session_id} physical unload incomplete: {error}"
                            ));
                            coordinator.publish("unload_sessions", &failures);
                        }
                    }
                }
            }

            if let Some(runtime_services) = runtime_services.as_ref() {
                runtime_services.shutdown_maintenance().await;
            }
            if let Some(cognitive) = resources.cognitive.as_ref() {
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
                preserved_turn_count = preserved_turns.len(),
                joined = runtime_report.joined,
                panicked = runtime_report.panicked,
                forced_aborts = runtime_report.forced_aborts,
                "Runtime restoration, scheduling, MCP and maintenance drained"
            );

            // Storage is the first durable resource constructed and the last
            // business dependency closed. Runtime maintenance, Memory and MCP
            // retain their journal ports until their workers have joined.
            enter_phase(&mut phases, "drain_session_repository", &failures);
            if let Some(selected_storage) = resources.selected_storage.as_ref() {
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

            enter_phase(&mut phases, "drain_task_set", &failures);
            let gateway_task_report = gateway_tasks.shutdown().await;
            record_task_shutdown_failures(
                "Gateway final task-set drain",
                &gateway_task_report,
                &mut failures,
            );
            if gateway_task_report.panicked > 0 || gateway_task_report.forced_aborts > 0 {
                coordinator.publish("drain_task_set", &failures);
            }
            Ok(GatewayRuntimeShutdownReport {
                phases,
                failures,
                execution_started,
                execution: execution_shutdown_report,
                active_turns: active_turn_report,
                memory: memory_report,
                tasks: gateway_task_report,
            })
        })
        .await
}

// ── Gateway entry point ─────────────────────────────────────────

pub async fn run_gateway_runtime(config: RuntimeHostConfig) -> Result<(), String> {
    let started_at = Instant::now();
    let receipt = ShutdownReceiptTarget::from_environment(&config)?;
    // 0. Write PID file (removed on drop via guard)
    let _pid_guard = PidFileGuard::new()?;
    let composition = crate::composition_root::GatewayCompositionRoot::new(Duration::from_secs(5));
    let gateway_tasks = composition.gateway_tasks();
    let shutdown_coordinator =
        GatewayRuntimeShutdownCoordinator::with_receipt(Arc::clone(&gateway_tasks), receipt);
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
    emit_config_diagnostics(&config_diagnostics);
    let provider_registry = match runtime::ProviderRegistry::new_with_context_windows(
        runtime_config.providers().clone(),
        runtime_config.model_context_windows().clone(),
    ) {
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
    let sessions = composition.active_sessions();
    let unified_store = Some(Arc::clone(&selected_storage.session_store));
    // The existing composition binding is shared by late-bound consumers.
    let evolution_runtime = Arc::new(OnceLock::<Weak<runtime::RuntimeServices>>::new());
    let cognitive: Option<Arc<CognitiveContextManager>> = match &config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager over selected storage...");
            let llm_summarizer = if mem_cfg.compression.llm.is_configured() {
                match runtime::RuntimeMemorySummarizer::new(
                    Arc::clone(&provider_registry),
                    Arc::clone(&provider_transport_pool),
                    Arc::clone(&provider_template_cache),
                    mem_cfg.compression.llm.model.clone(),
                    2048,
                ) {
                    Ok(summarizer) => {
                        let source = Arc::clone(&evolution_runtime);
                        Some(Arc::new(summarizer.with_resource_source(move || {
                            let services = source.get()?.upgrade()?;
                            Some((
                                Arc::clone(services.resource_manager()),
                                services.provider_resource_config(),
                            ))
                        }))
                            as Arc<
                                dyn memory::compression::llm_summarizer::LlmSummarizer,
                            >)
                    }
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
                Arc::clone(&selected_storage.memory_store),
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
    // The original durable source fence is already injected by selected storage.
    // Replay one bounded cleanup page before any Memory consumer is exposed;
    // downstream outage stays durable pending and never reopens its source.
    if let Some(manager) = cognitive.as_ref() {
        match manager
            .recover_memory_cleanup(&selected_storage.knowledge_fabric, 64)
            .await
        {
            Ok(errors) if !errors.is_empty() => {
                tracing::warn!(errors=?errors,"startup Memory cleanup remains pending")
            }
            Ok(_) => {}
            Err(error) => {
                return Err(startup_registry
                    .rollback(format!("restore Memory cleanup source fence: {error}"))
                    .await)
            }
        }
    }
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
    spawn_auth_broker_liveness_watchdog(
        Arc::clone(&auth_broker),
        approval_dir.clone(),
        config.auth_token.clone(),
        auth_catalog.clone(),
    );
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

    let evolution_eval_runner = runtime_config
        .background_work()
        .evolution
        .then(|| configured_evolution_eval_runner(&approval_dir, Arc::clone(&evolution_runtime)));
    let session_runtime_port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
    let mut runtime_services_builder =
        runtime::RuntimeServices::builder(&approval_dir, &workspace_root)
            .runtime_build_identity(crate::compiled_runtime_build_identity())
            .provider_registry(Arc::clone(&provider_registry))
            .provider_transport_pool(Arc::clone(&provider_transport_pool))
            .provider_template_cache(Arc::clone(&provider_template_cache))
            .provider_resource_config(runtime_config.provider_resources().clone())
            .background_work(runtime_config.background_work())
            .process_jsonl_commands(
                runtime_config
                    .agent_executor_commands()
                    .iter()
                    .map(runtime::ProcessJsonlSpec::from_config),
            )
            .provider_fallbacks(runtime_config.fallbacks().iter().cloned())
            .tool_execution_host(runtime_tool_host)
            .runtime_event_store(Arc::clone(&selected_storage.runtime_event_store))
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
            .session_ports(
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
            );
    if let Some(runner) = evolution_eval_runner {
        runtime_services_builder = runtime_services_builder.evolution_eval_runner(runner);
    }
    let growth_ready = crate::services::growth_projection_lane::initialize_growth_policy(
        &selected_storage.runtime_event_store,
        runtime_config.background_work().growth,
    )
    .and_then(|()| {
        crate::services::growth_projection_lane::growth_checkpoint_status(
            &selected_storage.runtime_event_store,
        )
    });
    match growth_ready {
        Ok(settled) if runtime_config.background_work().growth || !settled => {
            runtime_services_builder =
                runtime_services_builder.projection_lane(crate::services::growth_projection_lane(
                    approval_dir.clone(),
                    Arc::clone(&selected_storage.runtime_event_store),
                    growth_projection_services.growth.clone(),
                    growth_projection_services.memory.clone(),
                    growth_projection_services.matrix.clone(),
                )?);
        }
        Ok(_) => (),
        Err(error) => {
            // A failed automatic projection remains visible to its management
            // owner. It cannot take down unrelated foreground/Memory/Skill.
            tracing::error!(%error,"Growth automatic admission unavailable; configuration status will report error");
            let mut effective = runtime_config.background_work();
            effective.growth = false;
            runtime_services_builder = runtime_services_builder.background_work(effective);
        }
    }
    let skill_catalog_started_at = Instant::now();
    let startup_skill_assets = crate::services::runtime_skill_assets_for_workspace(&workspace_root);
    tracing::info!(
        elapsed_ms = skill_catalog_started_at.elapsed().as_millis() as u64,
        profiles = startup_skill_assets.profiles.len(),
        "runtime skill catalog startup projection completed"
    );
    let mut startup_skill_catalog =
        runtime::RuntimeSkillCatalog::new(startup_skill_assets.profiles);
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
    let skill_usage_sink: Arc<dyn runtime::RuntimeSkillUsageSink> =
        match runtime_services.skill_usage_recorder() {
            Ok(recorder) => recorder,
            Err(error) => return Err(startup_registry.rollback(error).await),
        };
    crate::services::attach_workspace_skill_usage_sink(&workspace_root, skill_usage_sink);
    if evolution_runtime
        .set(Arc::downgrade(&runtime_services))
        .is_err()
    {
        let error = "failed to bind Runtime evolution evaluation executor".to_string();
        return Err(startup_registry.rollback(error).await);
    }
    if let Err(error) = gateway_runtime_tool_host.bind_runtime_services(&runtime_services) {
        let error = format!("failed to bind runtime services: {error}");
        return Err(startup_registry.rollback(error).await);
    }
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
    if let Err(error) = selected_storage
        .postgres_executor
        .verify_registered_migration_catalogs()
    {
        let error = format!("failed to verify enabled APP storage catalogs: {error}");
        return Err(startup_registry.rollback(error).await);
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
    config_reload::initialize_config_reload_status(&config_reload, &app_state);

    // No business ingress or autonomous producer may run until every owner
    // required by durable work has been restored in dependency order. Keeping
    // this gate synchronous avoids a second, partially enforced "restoring"
    // mode across HTTP, Surface, scheduler, organizer and graph dispatch.
    if let Err(error) = restore_runtime_before_admission(
        &session_service,
        &session_worker_supervisor,
        &runtime_services,
    )
    .await
    {
        return Err(startup_registry.rollback(error).await);
    }
    if let Err(error) = runtime_services.release_recovered_producers().await {
        let error = format!("failed to release recovered Runtime producers: {error}");
        return Err(startup_registry.rollback(error).await);
    }
    session_worker_supervisor.release_recovered_producers();
    if let Err(error) = app_platform.start_resident().await {
        let error = format!("failed to start resident APPs after Runtime recovery: {error}");
        return Err(startup_registry.rollback(error).await);
    }

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
        .await.map_err(|error| format!("Gateway shutdown unresolved: {error}"))?;
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

pub(crate) async fn restore_runtime_before_admission(
    session_service: &Arc<crate::services::SessionService>,
    recovery_supervisor: &Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
    runtime_services: &Arc<runtime::RuntimeServices>,
) -> Result<(), String> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut retry_delay = Duration::from_millis(250);
    let mut last_failures = Vec::new();
    for attempt in 1..=MAX_ATTEMPTS {
        let mut summary = session_service
            .recover_required_sessions()
            .await
            .map_err(|error| format!("Session startup recovery failed: {error}"))?;
        let excluded_sessions = summary.failed_session_ids.clone();
        let mut runtime_recovery_failed = false;
        if summary.global_failures == 0 {
            let execution_recovery_started_at = Instant::now();
            match runtime_services
                .recover_execution_graphs_on_startup_excluding_sessions(&excluded_sessions)
                .await
            {
                Ok(report) if report.errors.is_empty() => {
                    tracing::info!(
                        elapsed_ms = execution_recovery_started_at.elapsed().as_millis() as u64,
                        "Runtime execution startup recovery completed after Session hydration"
                    );
                    emit_execution_startup_recovery(&report);
                    if let Err(error) =
                        reconcile_agentic_startup(runtime_services, &excluded_sessions).await
                    {
                        runtime_recovery_failed = true;
                        summary.failed = summary.failed.saturating_add(1);
                        summary.failures.push(error);
                    }
                }
                Ok(report) => {
                    emit_execution_startup_recovery(&report);
                    runtime_recovery_failed = true;
                    summary.failed = summary.failed.saturating_add(report.errors.len());
                    summary
                        .failures
                        .extend(report.errors.into_iter().map(|error| {
                            format!("execution graph {}: {}", error.graph_id, error.error)
                        }));
                }
                Err(error) => {
                    runtime_recovery_failed = true;
                    summary.failed = summary.failed.saturating_add(1);
                    summary.failures.push(format!(
                        "failed to recover execution graphs on startup: {error}"
                    ));
                }
            }
        }
        // Global recovery authority must be healthy before producers are
        // released. A corrupt Session is quarantined by id and excluded from
        // graph/program replay, so it must not take unrelated Sessions or new
        // work offline. Backend-wide and Runtime recovery failures still fence
        // admission.
        let complete = startup_recovery_pass_is_complete(&summary, runtime_recovery_failed);
        last_failures.clone_from(&summary.failures);
        let recovery_state = if complete {
            crate::session_runtime_bridge::SessionStartupRecoveryState::Completed
        } else {
            crate::session_runtime_bridge::SessionStartupRecoveryState::Failed
        };
        recovery_supervisor.record_recovery(summary, recovery_state);
        if complete {
            runtime_services
                .install_startup_recovery_exclusions(excluded_sessions)
                .map_err(|error| {
                    format!("failed to install recovered Session partition: {error}")
                })?;
            return Ok(());
        }
        if attempt < MAX_ATTEMPTS {
            tracing::warn!(
                attempt,
                retry_delay_ms = retry_delay.as_millis() as u64,
                "Ordered Runtime restoration failed before admission; retrying"
            );
            tokio::time::sleep(retry_delay).await;
            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(2));
        }
    }
    Err(format!(
        "ordered Runtime restoration did not complete after {MAX_ATTEMPTS} attempts: {}",
        last_failures.join("; ")
    ))
}

fn startup_recovery_pass_is_complete(
    summary: &crate::services::session_service::activation::SessionRecoverySummary,
    runtime_recovery_failed: bool,
) -> bool {
    summary.global_failures == 0 && !runtime_recovery_failed
}

async fn reconcile_agentic_startup(
    services: &Arc<runtime::RuntimeServices>,
    excluded_sessions: &BTreeSet<String>,
) -> Result<(), String> {
    let dispatches = services
        .recover_agentic_programs_on_startup_excluding_sessions(excluded_sessions)
        .await
        .map_err(|error| format!("failed to reconcile Agent-first Programs on startup: {error}"))?;
    let waits = services
        .recover_agentic_program_waits_on_startup_excluding_sessions(excluded_sessions)
        .await
        .map_err(|error| {
            format!("failed to reconcile Agent-first Program waits on startup: {error}")
        })?;
    tracing::info!(
        recovered_dispatches = dispatches.len(),
        recovered_waits = waits,
        "Agent-first Program startup reconciliation completed"
    );
    Ok(())
}

fn emit_config_diagnostics(diagnostics: &[runtime::RuntimeConfigDiagnostic]) {
    for diagnostic in diagnostics {
        tracing::warn!(code = %diagnostic.code, message = %diagnostic.message, "runtime config diagnostic");
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

    #[tokio::test]
    async fn required_memory_cleanup_runs_and_joins_with_optional_governance_disabled() {
        use memory::store::{EphemeralMemoryStore, MemoryStore};
        struct NoSemanticCalls;
        #[async_trait::async_trait]
        impl memory::SemanticGovernanceResolver for NoSemanticCalls {
            async fn resolve(
                &self,
                _: memory::SemanticGovernanceRequest,
            ) -> Result<memory::SemanticGovernanceResponse, String> {
                panic!("disabled optional governance must not call its resolver")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(EphemeralMemoryStore::new());
        let fabric = memory::KnowledgeFabric::with_store(Arc::new(
            memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()),
        ))
        .with_memory_sources(store.clone());
        let mut config = MemoryConfig::default();
        config.store.blob_dir = root.path().join("blobs");
        config.store.enable_vector_index = false;
        config.governance.enabled = false;
        config.governance.startup_delay_secs = 86400;
        let manager = Arc::new(
            CognitiveContextManager::new_with_selected_store(config.clone(), None, store.clone())
                .await
                .unwrap(),
        );
        let id = manager
            .orchestrator()
            .write(
                memory::MemoryLayer::L3,
                memory::MemoryCategory::Reference,
                "original source",
                "retained text",
                memory::Priority::Normal,
                memory::MemorySource::Import,
                vec![],
                memory::MemoryScope::Global,
            )
            .await
            .unwrap();
        let mut document = memory::DocumentContent::new("derived pack", "Retain evidence.");
        document.source = Some(format!("memory:{id}"));
        document.memory_source_revision = Some(
            memory::store::MemoryEntryRevision::of(&store.get(&id).await.unwrap().unwrap())
                .unwrap(),
        );
        let receipt = fabric
            .ingest_document(
                harness_contract::knowledge::KnowledgeNamespace::SharedLibrary("global".into()),
                harness_contract::knowledge::KnowledgeActivationPolicy::BlockingPolicy,
                harness_contract::knowledge::KnowledgeGovernanceLevel::Required,
                document,
            )
            .unwrap();
        memory::MemoryKernel::new(manager.clone())
            .archive(
                &memory::MemoryTurnContext::new("archive", "operator"),
                id,
                "user archive",
            )
            .await
            .unwrap();
        assert_eq!(
            store.pending_memory_cleanup(None, 16).await.unwrap().len(),
            1
        );
        // A pre-fence retirement has no pending row for the old worker to
        // discover. Reopen the manager and let the actual startup worker adopt
        // it from the original body/history, without a test-side retry.
        let legacy_id = manager
            .orchestrator()
            .write(
                memory::MemoryLayer::L3,
                memory::MemoryCategory::Reference,
                "legacy source",
                "retained legacy text",
                memory::Priority::Normal,
                memory::MemorySource::Import,
                vec![],
                memory::MemoryScope::Global,
            )
            .await
            .unwrap();
        let legacy_event = memory::MemoryLifecycleEvent {
            memory_id: legacy_id,
            from: None,
            to: memory::MemoryState::Superseded,
            reason: "legacy retirement".into(),
            session_id: "original-session".into(),
            agent_id: "original-agent".into(),
            occurred_at: chrono::Utc::now(),
        };
        store
            .kv_put(
                &format!("memory_lifecycle:{legacy_id}"),
                &serde_json::to_string(&vec![legacy_event]).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.pending_memory_cleanup(None, 16).await.unwrap().len(),
            1
        );
        let old = Arc::downgrade(&manager);
        drop(manager);
        assert!(old.upgrade().is_none());
        let manager = Arc::new(
            CognitiveContextManager::new_with_selected_store(config.clone(), None, store.clone())
                .await
                .unwrap(),
        );
        let tasks = GatewayRuntimeTaskSet::new(Duration::from_secs(2));
        start_memory_governance_task(
            &tasks,
            manager.clone(),
            fabric.clone(),
            config.governance,
            Arc::new(NoSemanticCalls),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store
                .pending_memory_cleanup(None, 16)
                .await
                .unwrap()
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("required cleanup cannot wait for optional startup delay");
        let legacy_fence = memory::store::MemorySourceFenceReader::read_source_fence(
            store.as_ref(),
            &format!("memory:{legacy_id}"),
        )
        .unwrap()
        .unwrap();
        assert!(legacy_fence.retired);
        assert_eq!(legacy_fence.delete_epoch, 1);
        let report = tasks.shutdown().await;
        assert_eq!(report.forced_aborts, 0);
        assert!(store.get(&id).await.unwrap().is_some());
        let pack = fabric
            .snapshot()
            .unwrap()
            .packs
            .into_iter()
            .find(|p| p.pack_id == receipt.pack.pack_id)
            .unwrap();
        assert_eq!(
            pack.state,
            harness_contract::knowledge::KnowledgeObjectState::Quarantined
        );
    }

    #[test]
    fn ordered_recovery_quarantines_local_sessions_but_fences_global_failures() {
        let summary = crate::services::session_service::activation::SessionRecoverySummary {
            failed: 1,
            global_failures: 0,
            failed_session_ids: BTreeSet::from(["quarantined-session".to_string()]),
            failures: vec!["quarantined-session: corrupt transcript".to_string()],
            ..Default::default()
        };

        assert!(startup_recovery_pass_is_complete(&summary, false));
        assert!(!startup_recovery_pass_is_complete(&summary, true));

        assert!(startup_recovery_pass_is_complete(
            &crate::services::session_service::activation::SessionRecoverySummary::default(),
            false,
        ));

        let global = crate::services::session_service::activation::SessionRecoverySummary {
            failed: 1,
            global_failures: 1,
            failures: vec!["manifest store unavailable".to_string()],
            ..Default::default()
        };
        assert!(!startup_recovery_pass_is_complete(&global, false));
    }

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
                            execution_started: false,
                            execution: None,
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
            .await.expect("same original shutdown report");

        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(first_error, "injected startup failure");
        assert_eq!(first, second);
        assert_eq!(
            first.phases,
            vec![
                "startup_rollback",
                "stop_accepting",
                "drain_ingress",
                "drain_active_turns",
                "drain_runtime",
                "drain_surface",
                "drain_apps",
                "drain_live_eval",
                "drain_session_workers",
                "unload_sessions",
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

#[cfg(all(test, target_os = "linux"))]
mod final_shutdown_receipt_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn private_receipt_directory() -> tempfile::TempDir {
        use std::os::unix::fs::MetadataExt;
        let parent = Path::new("/run/user/1000");
        let owner = std::fs::metadata("/proc/self").unwrap().uid();
        let metadata = std::fs::symlink_metadata(parent).unwrap();
        assert!(metadata.is_dir());
        assert_eq!(metadata.uid(), owner);
        assert_eq!(metadata.mode() & 0o777, 0o700);
        let directory = tempfile::Builder::new()
            .prefix("cowd-f2-receipt-")
            .tempdir_in(parent)
            .unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let metadata = std::fs::metadata(directory.path()).unwrap();
        assert_eq!(metadata.uid(), owner);
        assert_eq!(metadata.mode() & 0o777, 0o700);
        directory
    }
    fn target(root: &Path) -> ShutdownReceiptTarget {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
        ShutdownReceiptTarget {
            directory: std::fs::File::open(root).unwrap(),
            identity: ShutdownReceiptIdentity {
                nonce: "a".repeat(64),
                pid: std::process::id(),
                starttime: ShutdownReceiptTarget::starttime(std::process::id()).unwrap(),
                image_sha256: ShutdownReceiptTarget::image_hash(Path::new("/proc/self/exe"))
                    .unwrap(),
                config_sha256: "sha256:fixture-config".into(),
            },
        }
    }
    fn report() -> GatewayRuntimeShutdownReport {
        GatewayRuntimeShutdownReport {
            phases: vec!["original shutdown complete".into()],
            failures: vec![],
            execution_started: false,
            execution: None,
            active_turns: None,
            memory: None,
            tasks: Default::default(),
        }
    }
    #[tokio::test]
    async fn dropped_observer_rejoins_one_original_shutdown_driver() {
        let coordinator = GatewayRuntimeShutdownCoordinator::new(GatewayRuntimeTaskSet::new(Duration::from_secs(1)));
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let resource = Arc::new(());
        let resource_witness = Arc::downgrade(&resource);
        let first_owner = Arc::clone(&coordinator);
        let count = Arc::clone(&executions);
        let signal = Arc::clone(&release);
        let first = tokio::spawn(async move {
            first_owner.coordinate_original(move |_progress| async move {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                signal.acquire().await.unwrap().forget();
                drop(resource);
                Ok(report())
            }).await
        });
        while executions.load(std::sync::atomic::Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
        first.abort();
        let _ = first.await;
        assert!(resource_witness.upgrade().is_some(), "original dependency was dropped with observer");
        let second_owner = Arc::clone(&coordinator);
        let second = tokio::spawn(async move {
            second_owner.coordinate_original(|_| async { panic!("original shutdown was restarted") }).await
        });
        release.add_permits(1);
        assert_eq!(second.await.unwrap().unwrap(), report());
        assert!(resource_witness.upgrade().is_none(), "original dependency retained after completion");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn original_shutdown_deadline_reports_unknown_and_late_completion_rejoins() {
        let coordinator = GatewayRuntimeShutdownCoordinator::new(GatewayRuntimeTaskSet::new(Duration::from_secs(1)));
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let owner = Arc::clone(&count);
        let gate = Arc::clone(&release);
        let first = coordinator.coordinate_original(move |progress| async move {
            owner.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = progress.send(Some(Err("original deadline unresolved".into())));
            gate.acquire().await.unwrap().forget();
            Ok(report())
        }).await;
        assert_eq!(first.unwrap_err(), "original deadline unresolved");
        assert!(coordinator.completed_report().is_none());
        let mut original = coordinator.state.lock().unwrap().original_driver.as_ref().unwrap().clone();
        let _ = original.borrow_and_update();
        release.add_permits(1);
        original.changed().await.unwrap();
        let late = coordinator.coordinate_original(|_| async { panic!("must not start a second driver") }).await;
        assert_eq!(late.unwrap(), report());
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn original_shutdown_driver_panic_is_not_a_clean_report_or_restart() {
        let coordinator = GatewayRuntimeShutdownCoordinator::new(GatewayRuntimeTaskSet::new(Duration::from_secs(1)));
        let first = coordinator.coordinate_original(|_| async { panic!("original driver failed") }).await;
        assert!(first.as_ref().is_err_and(|error| error.contains("custody unresolved")));
        assert!(coordinator.completed_report().is_none());
        let second = coordinator.coordinate_original(|_| async { panic!("must not restart") }).await;
        assert_eq!(first, second);
        assert!(coordinator.completed_report().is_none());
    }

    #[tokio::test]
    async fn final_receipt_is_once_and_failed_export_is_same_for_leader_followers() {
        for existing in [false, true] {
            let root = private_receipt_directory();
            let target = target(root.path());
            let path = target.path();
            if existing {
                std::fs::write(&path, b"old receipt").unwrap();
            }
            let coordinator = GatewayRuntimeShutdownCoordinator::with_receipt(
                GatewayRuntimeTaskSet::new(Duration::from_secs(1)),
                Some(target),
            );
            let first = coordinator.coordinate(|| async { report() }).await;
            let second = coordinator
                .coordinate(|| async { panic!("follower must not execute another shutdown") })
                .await;
            assert_eq!(first, second);
            assert_eq!(first.failures.is_empty(), !existing);
            if existing {
                assert_eq!(std::fs::read(&path).unwrap(), b"old receipt");
            } else {
                let bytes = std::fs::read(&path).unwrap();
                let page: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(page["schema_version"], 1);
                assert_eq!(page["identity"]["pid"], std::process::id());
                assert_eq!(page["identity"]["nonce"], "a".repeat(64));
                assert_eq!(page["report"]["execution"], serde_json::Value::Null);
                assert_eq!(
                    std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    bytes,
                    "follower cannot overwrite first report"
                );
            }
        }
    }
    #[tokio::test]
    async fn complete_execution_report_and_unclosed_axes_cannot_be_erased() {
        for missing in [false, true] {
            let root = private_receipt_directory();
            let target = target(root.path());
            let path = target.path();
            let coordinator = GatewayRuntimeShutdownCoordinator::with_receipt(
                GatewayRuntimeTaskSet::new(Duration::from_secs(1)),
                Some(target),
            );
            let mut original = report();
            original.execution_started = true;
            if !missing {
                original.execution = Some(runtime::RuntimeExecutionShutdownReport {
                    remaining_keys: 2,
                    forced_aborts: 3,
                    errors: vec!["original owner unknown".into()],
                    ..Default::default()
                });
            }
            let final_report = coordinator.coordinate(|| async { original.clone() }).await;
            assert!(!final_report.failures.is_empty());
            assert_eq!(final_report.execution, original.execution);
            let page: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(page["report"]["execution_started"], true);
            assert!(!page["report"]["failures"].as_array().unwrap().is_empty());
            if !missing {
                assert_eq!(page["report"]["execution"]["remaining_keys"], 2);
                assert_eq!(page["report"]["execution"]["forced_aborts"], 3);
            }
        }
    }
    #[tokio::test]
    async fn unresolved_original_deadline_receipt_closes_before_caller_release() {
        // Proven 46b #5/#8-c shape: the first deadline rejects the original
        // close while the already started finalizer is still unresolved, the
        // released caller can end the process, and the outer harness used to
        // see only a bare FileNotFoundError because the receipt was tied to
        // a completion that never arrived.
        let root = private_receipt_directory();
        let target = target(root.path());
        let path = target.path();
        let coordinator = GatewayRuntimeShutdownCoordinator::with_receipt(
            GatewayRuntimeTaskSet::new(Duration::from_secs(1)),
            Some(target),
        );
        let owner = Arc::clone(&coordinator);
        let probe = path.clone();
        let observations = vec![
            "Runtime execution first observation: original Runtime finalization remains unresolved after shutdown deadline".to_string(),
        ];
        let first = coordinator
            .coordinate_original(move |progress| async move {
                let reason = owner.finish_unresolved_at_original_deadline(
                    vec![
                        "stop_accepting".into(),
                        "drain_ingress".into(),
                        "drain_runtime".into(),
                        "drain_runtime_pending".into(),
                    ],
                    observations,
                    true,
                    None,
                    None,
                );
                assert!(
                    probe.symlink_metadata().is_ok(),
                    "the receipt must already live in the original private directory when the rejection releases the caller"
                );
                let _ = progress.send(Some(Err(reason)));
                std::future::pending::<()>().await;
                Ok(report())
            })
            .await;
        let expected = "Gateway Runtime shutdown is unresolved at its original deadline: Runtime execution first observation: original Runtime finalization remains unresolved after shutdown deadline";
        assert_eq!(first.unwrap_err(), expected);
        let bytes = std::fs::read(&path).unwrap();
        let page: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(page["schema_version"], 1);
        assert_eq!(page["identity"]["nonce"], "a".repeat(64));
        assert_eq!(page["report"]["execution_started"], true);
        assert_eq!(page["report"]["execution"], serde_json::Value::Null);
        let failures: Vec<&str> = page["report"]["failures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        assert_eq!(failures[0], format!("shutdown_unresolved_at_original_deadline: {expected}"));
        assert!(failures
            .iter()
            .any(|failure| failure.contains("Runtime execution shutdown report missing after startup")));
        // A late clean close rejoins the rejected close and can never
        // manufacture a passing receipt over the real not-closed facts.
        let late = coordinator.coordinate(|| async { panic!("late close must not rerun a shutdown") }).await;
        assert!(!late.failures.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn unresolved_original_deadline_rejection_is_terminal_without_receipt_fixture() {
        // Production shape (no fixture receipt): the deadline rejection is
        // terminal for the original close transaction. Followers receive the
        // same typed unresolved report and never start a second shutdown.
        let coordinator =
            GatewayRuntimeShutdownCoordinator::new(GatewayRuntimeTaskSet::new(Duration::from_secs(1)));
        let reason = coordinator.finish_unresolved_at_original_deadline(
            vec!["drain_runtime".into(), "drain_runtime_pending".into()],
            vec!["original Runtime observation".into()],
            true,
            None,
            None,
        );
        assert_eq!(
            reason,
            "Gateway Runtime shutdown is unresolved at its original deadline: original Runtime observation"
        );
        let existing = coordinator.completed_report().expect("deadline rejection closes the original transaction");
        assert_eq!(existing.failures[0], format!("shutdown_unresolved_at_original_deadline: {reason}"));
        assert!(existing.execution.is_none());
        let follower = coordinator
            .coordinate(|| async { panic!("follower must not rerun the original shutdown") })
            .await;
        assert_eq!(follower, existing);
    }

    #[test]
    fn startup_image_identity_is_captured_and_nonce_is_strict() {
        for invalid in [
            "",
            "../receipt",
            &"A".repeat(64),
            &"a".repeat(63),
            &"g".repeat(64),
        ] {
            assert!(ShutdownReceiptTarget::validate_nonce(invalid).is_err());
        }
        assert!(ShutdownReceiptTarget::validate_nonce(&"0".repeat(64)).is_ok());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("image");
        std::fs::write(&path, b"running original bytes").unwrap();
        let captured = ShutdownReceiptTarget::image_hash(&path).unwrap();
        let next = root.path().join("next");
        std::fs::write(&next, b"replacement image").unwrap();
        std::fs::rename(&next, &path).unwrap();
        assert_ne!(captured, ShutdownReceiptTarget::image_hash(&path).unwrap());
        assert_eq!(
            captured,
            "sha256:f098edc257f9739f9de555b1d6aff583af3bc2ad3d4fb8be17573048acd9899d"
        );
    }
}
